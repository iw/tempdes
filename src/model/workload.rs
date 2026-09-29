//! Workload drivers: workflow starts, signals, queries/describes, visibility queries, hot entity
//! workflows, schedules, the periodic sampler and the warm-up reset.

use std::cell::RefCell;
use std::rc::Rc;

use crate::config::scenario::{Arrival, VisibilityOp};
use crate::sim::executor::{Time, now, sleep, sleep_until, spawn};

use super::history::{self, StartOrigin};
use super::infra::*;
use super::matching;
use super::metrics::Sample;
use super::queues::history_call;
use super::sdk::{self, Conn, Retry, conn_pod, sdk_call};
use super::types::*;
use super::world::*;

/// Current start rate per workflow type (events may change it).
pub struct Rates {
    pub start: Vec<f64>,
    /// Live multiplier on every start and signal rate, changed between steps by `tempdes ui`.
    /// Always 1.0 in a CLI run.
    pub scale: f64,
}

fn rate_at(ctx: &Ctx, rates: &Rc<RefCell<Rates>>, t: usize) -> f64 {
    let base = {
        let r = rates.borrow();
        r.start[t] * r.scale
    };
    match ctx.p.wf_types[t].ramp {
        Some((from, over)) if over > 0 && now() < over => {
            base * (from + (1.0 - from) * now() as f64 / over as f64)
        }
        _ => base,
    }
}

pub fn start_generators(ctx: &Ctx, rates: &Rc<RefCell<Rates>>, client_base: &[usize]) {
    for (t, tp) in ctx.p.wf_types.iter().enumerate() {
        if tp.start_rate <= 0.0 || tp.system_scheduler {
            continue;
        }
        let c = ctx.clone();
        let rates = rates.clone();
        let base = client_base[t];
        let n_clients = tp.starters as usize;
        spawn(async move {
            loop {
                let r = rate_at(&c, &rates, t);
                if r <= 0.0 {
                    sleep(1_000_000).await;
                    continue;
                }
                let gap = match c.p.wf_types[t].arrival {
                    Arrival::Poisson => c.rng.borrow_mut().exp(1e6 / r),
                    Arrival::Uniform => 1e6 / r,
                };
                sleep(gap.max(1.0) as Time).await;
                let client = base + c.rand_index(n_clients);
                let c2 = c.clone();
                spawn(async move { start_flow(c2, t, client).await });
            }
        });
    }
}

/// A client starting one workflow (optionally eager, optionally waiting for its result).
pub async fn start_flow(ctx: Ctx, wf_type: usize, client: usize) {
    let tp = &ctx.p.wf_types[wf_type];
    let ns = tp.ns;
    let key = history::alloc_key(&ctx);
    let shard = history::shard_for(&ctx, ns, wf_type, key);
    // Eager start needs a local worker with a free workflow task slot (same client process).
    let mut eager_worker = None;
    if tp.eager_start
        && let Some(fleet) = ctx.p.fleet_for_tq(tp.tq)
    {
        let candidates: Vec<usize> = ctx
            .workers
            .borrow()
            .iter()
            .enumerate()
            .filter(|(_, w)| w.fleet == fleet)
            .map(|(i, _)| i)
            .collect();
        if !candidates.is_empty() {
            let wk = candidates[ctx.rand_index(candidates.len())];
            let slots = ctx.workers.borrow()[wk].wft_slots.clone();
            if let Some(p) = slots.try_acquire(1) {
                eager_worker = Some((wk, p));
            }
        }
    }
    let eager = eager_worker.is_some();
    let r = sdk_call(
        &ctx,
        Conn::Client(client),
        ns,
        Api::StartWorkflowExecution,
        Retry::DEFAULT,
        0.0,
        move |c, _fe| async move {
            history_call(&c, shard, |c2, hp| {
                let c2 = c2.clone();
                async move {
                    history::start_workflow(
                        &c2,
                        hp,
                        shard,
                        key,
                        wf_type,
                        StartOrigin::Client,
                        eager,
                        now() + 10_000_000,
                    )
                    .await
                }
            })
            .await
        },
    )
    .await;
    match r {
        Ok((wf, wgen, eager_wft)) => {
            if let (Some(info), Some((wk, permit))) = (eager_wft, eager_worker) {
                let c = ctx.clone();
                spawn(async move { sdk::process_wft(c, wk, info, permit).await });
            }
            if tp.await_result {
                await_result(&ctx, client, ns, wf, wgen, shard).await;
            }
        }
        Err(_) => {
            ctx.m.borrow_mut().wf[wf_type].start_failed += 1;
        }
    }
}

/// Client long-polls GetWorkflowExecutionHistory(WaitNewEvent) until the workflow closes.
async fn await_result(ctx: &Ctx, client: usize, ns: usize, wf: WfId, wgen: u32, shard: ShardId) {
    for _ in 0..1000 {
        let closed = ctx
            .wfs
            .borrow()
            .get(wf, wgen)
            .map(|w| w.status == WfStatus::Closed)
            .unwrap_or(true);
        if closed {
            return;
        }
        let r = sdk_call(
            ctx,
            Conn::Client(client),
            ns,
            Api::GetWorkflowExecutionHistory,
            Retry::DEFAULT,
            0.0,
            move |c, _fe| async move {
                history_call(&c, shard, |c2, hp| {
                    let c2 = c2.clone();
                    async move {
                        history::get_history(&c2, hp, wf, wgen, 1, true, now() + 60_000_000).await
                    }
                })
                .await
            },
        )
        .await;
        match r {
            Ok(true) => return,
            Ok(false) => continue,
            Err(_) => sleep(1_000_000).await,
        }
    }
}

/// Start hot "entity" workflows targeted by `target: hot` signal loads.
pub async fn start_entities(ctx: Ctx, client: usize) {
    for (si, s) in ctx.p.signals.iter().enumerate() {
        if !s.hot {
            continue;
        }
        let mut ids = Vec::new();
        for _ in 0..s.hot_workflows {
            let wf_type = s.wf_type;
            let ns = ctx.p.wf_types[wf_type].ns;
            let key = history::alloc_key(&ctx);
            let shard = history::shard_for(&ctx, ns, wf_type, key);
            let r = sdk_call(
                &ctx,
                Conn::Client(client),
                ns,
                Api::StartWorkflowExecution,
                Retry::DEFAULT,
                0.0,
                move |c, _fe| async move {
                    history_call(&c, shard, |c2, hp| {
                        let c2 = c2.clone();
                        async move {
                            history::start_workflow(
                                &c2,
                                hp,
                                shard,
                                key,
                                wf_type,
                                StartOrigin::Entity,
                                false,
                                now() + 10_000_000,
                            )
                            .await
                        }
                    })
                    .await
                },
            )
            .await;
            if let Ok((wf, wgen, _)) = r {
                ids.push((wf, wgen));
            }
        }
        let mut wfs = ctx.wfs.borrow_mut();
        if wfs.hot.len() <= si {
            wfs.hot.resize_with(si + 1, Vec::new);
        }
        wfs.hot[si] = ids.into_iter().map(|(w, _)| w).collect();
    }
}

pub fn start_signalers(ctx: &Ctx, rates: &Rc<RefCell<Rates>>, client_base: usize) {
    for (si, s) in ctx.p.signals.iter().enumerate() {
        if s.rate <= 0.0 {
            continue;
        }
        let c = ctx.clone();
        let s = s.clone();
        let rates = rates.clone();
        spawn(async move {
            // hot entities are started at t=0; give them a moment
            sleep(500_000).await;
            loop {
                let rate = s.rate * rates.borrow().scale;
                if rate <= 0.0 {
                    sleep(1_000_000).await;
                    continue;
                }
                let gap = c.rng.borrow_mut().exp(1e6 / rate);
                sleep(gap.max(1.0) as Time).await;
                let target = {
                    let wfs = c.wfs.borrow();
                    if s.hot {
                        wfs.hot.get(si).and_then(|v| {
                            if v.is_empty() {
                                None
                            } else {
                                let id = v[c.rand_index(v.len())];
                                wfs.slots[id as usize].as_ref().map(|w| (id, w.wgen))
                            }
                        })
                    } else {
                        wfs.running_by_type.get(s.wf_type).and_then(|v| {
                            if v.is_empty() {
                                None
                            } else {
                                let id = v[c.rand_index(v.len())];
                                wfs.slots[id as usize].as_ref().map(|w| (id, w.wgen))
                            }
                        })
                    }
                };
                let Some((wf, wgen)) = target else { continue };
                let client = client_base + c.rand_index(s.clients as usize);
                let c2 = c.clone();
                let wf_type = s.wf_type;
                spawn(async move {
                    let ns = c2.p.wf_types[wf_type].ns;
                    let Some(shard) = c2.wf_shard(wf, wgen) else {
                        return;
                    };
                    let r = sdk_call(
                        &c2,
                        Conn::Client(client),
                        ns,
                        Api::SignalWorkflowExecution,
                        Retry::DEFAULT,
                        0.0,
                        move |c, _fe| async move {
                            history_call(&c, shard, |c3, hp| {
                                let c3 = c3.clone();
                                async move {
                                    history::signal(&c3, hp, wf, wgen, now() + 10_000_000).await
                                }
                            })
                            .await
                        },
                    )
                    .await;
                    let mut m = c2.m.borrow_mut();
                    if r.is_ok() {
                        m.wf[wf_type].signals_sent += 1;
                    } else {
                        m.wf[wf_type].signals_failed += 1;
                    }
                });
            }
        });
    }
}

pub fn start_queries(ctx: &Ctx, client_base: usize, n_clients: usize) {
    for q in &ctx.p.queries {
        if q.rate <= 0.0 {
            continue;
        }
        let c = ctx.clone();
        let q = q.clone();
        spawn(async move {
            loop {
                let gap = c.rng.borrow_mut().exp(1e6 / q.rate);
                sleep(gap.max(1.0) as Time).await;
                let target = {
                    let wfs = c.wfs.borrow();
                    wfs.running_by_type.get(q.wf_type).and_then(|v| {
                        if v.is_empty() {
                            None
                        } else {
                            let id = v[c.rand_index(v.len())];
                            wfs.slots[id as usize].as_ref().map(|w| (id, w.wgen))
                        }
                    })
                };
                let Some((wf, wgen)) = target else { continue };
                let client = client_base + c.rand_index(n_clients.max(1));
                let c2 = c.clone();
                let describe = q.describe;
                let wf_type = q.wf_type;
                spawn(async move {
                    let ns = c2.p.wf_types[wf_type].ns;
                    let Some(shard) = c2.wf_shard(wf, wgen) else {
                        return;
                    };
                    let (api, happ) = if describe {
                        (
                            Api::DescribeWorkflowExecution,
                            HistApi::DescribeWorkflowExecution,
                        )
                    } else {
                        (Api::QueryWorkflow, HistApi::QueryWorkflow)
                    };
                    let tq = c2.p.wf_types[wf_type].tq;
                    let _ = sdk_call(
                        &c2,
                        Conn::Client(client),
                        ns,
                        api,
                        Retry::DEFAULT,
                        0.0,
                        move |c, _fe| async move {
                            history_call(&c, shard, |c3, hp| {
                                let c3 = c3.clone();
                                async move {
                                    history::describe(&c3, hp, wf, wgen, happ, now() + 10_000_000)
                                        .await
                                }
                            })
                            .await?;
                            if !describe {
                                // query task dispatched through matching to a worker and answered
                                let parts = c
                                    .matching
                                    .borrow()
                                    .by_tq
                                    .get(&(tq, TqKind::Workflow))
                                    .cloned();
                                if let Some(parts) = parts {
                                    let pid = parts[c.rand_index(parts.len())];
                                    let host = c.matching.borrow().parts[pid].host;
                                    hop(&c).await;
                                    matching_admit(&c, host)?;
                                    cpu(
                                        &c,
                                        host,
                                        c.p.costs.matching[MatchApi::QueryWorkflow.idx()],
                                    )
                                    .await;
                                    // worker round trip + processing
                                    client_hop(&c).await;
                                    sleep(2_000).await;
                                    client_hop(&c).await;
                                    hop(&c).await;
                                }
                            }
                            Ok(())
                        },
                    )
                    .await;
                });
            }
        });
    }
}

pub fn start_visibility(ctx: &Ctx, client_base: usize, n_clients: usize) {
    for v in &ctx.p.vis_loads {
        if v.rate <= 0.0 {
            continue;
        }
        let c = ctx.clone();
        let v = v.clone();
        spawn(async move {
            loop {
                let gap = c.rng.borrow_mut().exp(1e6 / v.rate);
                sleep(gap.max(1.0) as Time).await;
                let client = client_base + c.rand_index(n_clients.max(1));
                let c2 = c.clone();
                let (api, op) = match v.op {
                    VisibilityOp::List => (
                        Api::ListWorkflowExecutions,
                        PersistOp::ListWorkflowExecutions,
                    ),
                    VisibilityOp::Count => (
                        Api::CountWorkflowExecutions,
                        PersistOp::CountWorkflowExecutions,
                    ),
                };
                let ns = v.ns;
                spawn(async move {
                    let _ = sdk_call(
                        &c2,
                        Conn::Client(client),
                        ns,
                        api,
                        Retry::NONE,
                        0.0,
                        move |c, fe| async move { vis_read(&c, fe, op).await },
                    )
                    .await;
                });
            }
        });
    }
}

/// Create scheduler workflows (one per schedule) on the per-namespace worker queue.
pub async fn start_schedules(ctx: Ctx, client: usize) {
    for (si, s) in ctx.p.schedules.iter().enumerate() {
        for i in 0..s.count {
            let wf_type = s.scheduler_type;
            let ns = s.ns;
            let key = history::alloc_key(&ctx);
            let shard = history::shard_for(&ctx, ns, wf_type, key);
            // first fire: aligned schedules fire together at the next interval boundary
            let first = if s.aligned {
                s.interval
            } else {
                s.interval * u64::from(i + 1) / u64::from(s.count.max(1))
            };
            let r = sdk_call(
                &ctx,
                Conn::Client(client),
                ns,
                Api::StartWorkflowExecution,
                Retry::DEFAULT,
                0.0,
                move |c, _fe| async move {
                    history_call(&c, shard, |c2, hp| {
                        let c2 = c2.clone();
                        async move {
                            history::start_workflow(
                                &c2,
                                hp,
                                shard,
                                key,
                                wf_type,
                                StartOrigin::Schedule,
                                false,
                                now() + 10_000_000,
                            )
                            .await
                        }
                    })
                    .await
                },
            )
            .await;
            if let Ok((wf, wgen, _)) = r
                && let Some(w) = ctx.wfs.borrow_mut().get_mut(wf, wgen)
            {
                w.entity = false;
                w.schedule = Some(ScheduleState {
                    sched: si,
                    due_actions: 0,
                    next_fire: first.max(now() + 1_000),
                    waiting_rate_limit: false,
                });
            }
        }
    }
}

/// Periodic samples for time series in reports (per-interval utilisation).
pub fn start_sampler(ctx: &Ctx, interval: Time) {
    let c = ctx.clone();
    spawn(async move {
        let mut last_done: Vec<f64> = Vec::new();
        let mut last_db = c.db.borrow().servers.done_abs();
        let mut last_started = 0u64;
        let mut last_completed = 0u64;
        let mut last_rej = 0u64;
        let mut last_err = 0u64;
        let mut last_t = now();
        loop {
            sleep(interval).await;
            let t = now();
            let dt = (t - last_t) as f64;
            last_t = t;
            let mut sample = Sample {
                t: t as f64 / 1e6,
                ..Default::default()
            };
            {
                let pods = c.pods.borrow();
                if last_done.len() < pods.len() {
                    last_done.resize(pods.len(), 0.0);
                }
                for (i, p) in pods.iter().enumerate() {
                    let done = p.cpu.done_abs();
                    let util =
                        ((done - last_done[i]) / (dt * f64::from(p.cpu.servers()))).clamp(0.0, 1.0);
                    last_done[i] = done;
                    if p.alive {
                        sample.cpu[p.svc.idx()].push(util);
                    }
                }
                let db = c.db.borrow();
                let done = db.servers.done_abs();
                sample.db_util =
                    ((done - last_db) / (dt * f64::from(db.servers.servers()))).clamp(0.0, 1.0);
                last_db = done;
            }
            {
                let m = c.m.borrow();
                let started: u64 = m.wf.iter().map(|w| w.started).sum();
                let completed: u64 = m.wf.iter().map(|w| w.completed).sum();
                let rej: u64 = m.rejections.values().sum();
                let err: u64 = m.client.iter().map(|o| o.error_count()).sum();
                // counters reset at the end of warm-up
                let d = |now: u64, last: u64| if now >= last { now - last } else { now };
                sample.started = d(started, last_started);
                sample.completed = d(completed, last_completed);
                sample.rejections = d(rej, last_rej);
                sample.api_errors = d(err, last_err);
                last_started = started;
                last_completed = completed;
                last_rej = rej;
                last_err = err;
            }
            {
                let mm = c.matching.borrow();
                sample.backlog = mm.parts.iter().map(|p| p.backlog_len()).sum();
            }
            {
                let sh = c.shards.borrow();
                sample.history_pending = sh
                    .iter()
                    .map(|s| {
                        s.queues
                            .iter()
                            .map(|q| u64::from(q.pending) + q.unloaded.len() as u64)
                            .sum::<u64>()
                    })
                    .sum();
            }
            sample.running = c.wfs.borrow().running() as u64;
            c.m.borrow_mut().samples.push(sample);
        }
    });
}

/// Reset statistics at the end of warm-up.
pub fn schedule_warmup_reset(ctx: &Ctx, at: Time) {
    let c = ctx.clone();
    spawn(async move {
        sleep_until(at).await;
        reset_stats(&c);
    });
}

pub fn reset_stats(c: &Ctx) {
    *c.measuring.borrow_mut() = true;
    c.m.borrow_mut().reset();
    for p in c.pods.borrow_mut().iter_mut() {
        p.cpu.reset_stats();
        p.db_pool.reset_stats();
        p.persist_limiter.reset_stats();
        p.rps_limiter.reset_stats();
        if let Some(h) = p.hist.as_mut() {
            h.cache.reset_stats();
            for s in &h.schedulers {
                s.reset_stats();
            }
            h.sched_throttled = 0;
            h.sched_limiter.refused_ns = 0;
            h.sched_limiter.refused_host = 0;
        }
        if let Some(fe) = p.fe.as_mut() {
            fe.concurrent_max.clear();
            for l in &mut fe.ns_limiters {
                l.reset_stats();
            }
            fe.vis_limiter.reset_stats();
        }
    }
    {
        let mut db = c.db.borrow_mut();
        db.servers.reset_stats();
        db.vis_servers.reset_stats();
    }
    for s in c.shards.borrow_mut().iter_mut() {
        s.io_sem.reset_stats();
        s.writes = 0;
        s.api_requests = 0;
        s.persistence_ops = 0;
        s.events_cache.reset_stats();
    }
    for p in c.matching.borrow_mut().parts.iter_mut() {
        p.sync_matches = 0;
        p.async_matches = 0;
        p.forwarded_tasks = 0;
        p.forwarded_polls = 0;
        p.remote_matches = 0;
        p.adds = 0;
        p.polls = 0;
        p.poll_timeouts = 0;
        p.writes = 0;
        p.write_rejects = 0;
        p.backlog_gauge.reset();
        p.pollers_gauge.reset();
        p.poll_wait = Default::default();
        p.task_wait = Default::default();
    }
    for w in c.workers.borrow_mut().iter_mut() {
        w.sticky_cache.reset_stats();
        w.wft_slots.reset_stats();
        w.act_slots.reset_stats();
        if let Some(cpu) = w.cpu.as_mut() {
            cpu.reset_stats();
        }
    }
    // workflow locks: reset those of currently running workflows
    for w in c.wfs.borrow().slots.iter().flatten() {
        if w.status == WfStatus::Running {
            w.lock.reset_stats();
        }
    }
}

pub fn client_conn_init(ctx: &Ctx, idx: usize) {
    let _ = conn_pod(ctx, Conn::Client(idx));
}

pub fn unused(_: &matching::Polled) {}
