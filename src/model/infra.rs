//! Infrastructure primitives shared by all service flows: CPU bursts on pods, network hops,
//! persistence calls (rate limiting → connection pool → database), visibility store writes, the
//! shard IO semaphore and the workflow lock / mutable state cache.

use std::rc::Rc;

use crate::sim::executor::{Time, now, sleep, sleep_until, spawn, timeout};
use crate::sim::sync::{Permit, Prio};

use super::types::*;
use super::world::*;

/// Execute `cost_us` of CPU on `pod` (FCFS over its cores), scaled by calibration.
pub async fn cpu(ctx: &Ctx, pod: PodId, cost_us: f64) {
    let end = {
        let mut pods = ctx.pods.borrow_mut();
        let p = &mut pods[pod];
        let scale = ctx.p.costs.scale[p.svc.idx()];
        p.cpu.schedule(cost_us * scale)
    };
    sleep_until(end).await;
}

/// One-way hop between Temporal pods.
pub async fn hop(ctx: &Ctx) {
    let t = ctx.p.net_internal;
    if t > 0 {
        sleep(t).await;
    }
}

/// One-way hop between SDK and frontend.
pub async fn client_hop(ctx: &Ctx) {
    let t = ctx.p.net_client;
    if t > 0 {
        sleep(t).await;
    }
}

/// Persistence caller class → priority in the persistence priority limiter
/// (`common/persistence/client/quotas.go`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Caller {
    /// API caller with explicit priority (1 for Start/Signal, 2 otherwise).
    Api(usize),
    BackgroundHigh,
    BackgroundLow,
    Preemptable,
    /// queue loads (GetHistoryTasks) = 3
    QueueLoad,
    /// shard management / range completion = 1
    ShardMgmt,
}

impl Caller {
    pub fn persistence_priority(self) -> usize {
        match self {
            Caller::Api(p) => p,
            Caller::ShardMgmt => 1,
            Caller::QueueLoad => 3,
            Caller::BackgroundHigh => 4,
            Caller::BackgroundLow => 5,
            Caller::Preemptable => 6,
        }
    }

    /// history.rps priority (Operator 0, API 1, BackgroundHigh 2, BackgroundLow 3, Preemptable 4).
    pub fn history_rps_priority(self) -> usize {
        match self {
            Caller::Api(_) => 1,
            Caller::BackgroundHigh | Caller::QueueLoad | Caller::ShardMgmt => 2,
            Caller::BackgroundLow => 3,
            Caller::Preemptable => 4,
        }
    }

    pub fn is_api(self) -> bool {
        matches!(self, Caller::Api(_))
    }
}

/// A persistence call from `pod`. Fails immediately with PERSISTENCE_LIMIT when the pod's
/// priority limiter rejects it (Temporal does not wait for a token).
pub async fn persist(ctx: &Ctx, pod: PodId, op: PersistOp, caller: Caller) -> Res<()> {
    if op.rate_limited() {
        ctx.m.borrow_mut().persist_limited_pod(pod);
        let ok = {
            let mut pods = ctx.pods.borrow_mut();
            pods[pod]
                .persist_limiter
                .allow(caller.persistence_priority())
        };
        if !ok {
            let svc = ctx.pods.borrow()[pod].svc;
            let mut m = ctx.m.borrow_mut();
            m.persist[op.idx()].record(
                0,
                Some(Err::ResourceExhausted(
                    ReCause::PersistenceLimit,
                    Scope::System,
                )),
            );
            m.reject(
                &format!("{}.persistenceMaxQPS", svc.as_str()),
                ctx.pods.borrow()[pod].addr.clone(),
            );
            return Err(Err::ResourceExhausted(
                ReCause::PersistenceLimit,
                Scope::System,
            ));
        }
    }
    cpu(ctx, pod, ctx.p.costs.persistence_client).await;
    let start = now();
    let (pool, svc) = {
        let pods = ctx.pods.borrow();
        (pods[pod].db_pool.clone(), pods[pod].svc)
    };
    let conn = pool.acquire().await;
    let conn_wait = now() - start;
    let service = {
        let mut rng = ctx.rng.borrow_mut();
        ctx.p.db_latency[op.idx()].sample(&mut rng)
    };
    let end = ctx.db.borrow_mut().servers.schedule(service);
    sleep_until(end).await;
    drop(conn);
    let lat = now() - start;
    let mut m = ctx.m.borrow_mut();
    m.persist[op.idx()].record(lat, None);
    m.persist_pod(pod);
    m.persist_conn_wait[svc.idx()].record(conn_wait);
    Ok(())
}

/// Visibility store write (history visibility queue). Elasticsearch writes go through the
/// per-host bulk processor; SQL visibility writes hit the visibility database directly.
pub async fn vis_write(ctx: &Ctx, pod: PodId, op: PersistOp) -> Res<()> {
    let start = now();
    // system.visibilityPersistenceMaxWriteQPS (9000/host) is not modelled: visibility writes
    // are bounded by the ES bulk processor and the visibility store's capacity instead.
    let es = matches!(
        ctx.p.vis.kind,
        crate::config::scenario::VisibilityKind::Elasticsearch
            | crate::config::scenario::VisibilityKind::Opensearch
    );
    if es {
        let (tx, rx) = crate::sim::executor::oneshot();
        let flush_now = {
            let mut es = ctx.es.borrow_mut();
            let slot = ctx.pods.borrow()[pod].ordinal;
            if es.len() <= slot {
                es.resize_with(slot + 1, || EsBulk {
                    buffer: Vec::new(),
                    flush_scheduled: false,
                    inflight: 0,
                });
            }
            let b = &mut es[slot];
            b.buffer.push(tx);
            if b.buffer.len() as u32 >= ctx.p.k.es_bulk_actions && b.inflight < ctx.p.k.es_workers {
                true
            } else {
                if !b.flush_scheduled {
                    b.flush_scheduled = true;
                    let c = ctx.clone();
                    spawn(async move {
                        sleep(c.p.k.es_flush_interval).await;
                        es_flush(&c, pod, true).await;
                    });
                }
                false
            }
        };
        if flush_now {
            let c = ctx.clone();
            spawn(async move { es_flush(&c, pod, false).await });
        }
        let _ = rx.await;
    } else {
        cpu(ctx, pod, ctx.p.costs.persistence_client).await;
        let service = {
            let mut rng = ctx.rng.borrow_mut();
            ctx.p.vis.write.sample(&mut rng)
        };
        let end = ctx.db.borrow_mut().vis_servers.schedule(service);
        sleep_until(end).await;
    }
    ctx.m.borrow_mut().vis_persist[op.idx()].record(now() - start, None);
    Ok(())
}

async fn es_flush(ctx: &Ctx, pod: PodId, from_timer: bool) {
    let slot = ctx.pods.borrow()[pod].ordinal;
    let batch = {
        let mut es = ctx.es.borrow_mut();
        let b = &mut es[slot];
        if from_timer {
            b.flush_scheduled = false;
        }
        if b.buffer.is_empty() {
            return;
        }
        if b.inflight >= ctx.p.k.es_workers {
            // all bulk workers busy: retry shortly
            if !b.flush_scheduled {
                b.flush_scheduled = true;
                let c = ctx.clone();
                spawn(async move {
                    sleep(10_000).await;
                    es_flush(&c, pod, true).await;
                });
            }
            return;
        }
        b.inflight += 1;
        let n = (ctx.p.k.es_bulk_actions as usize).min(b.buffer.len());
        b.buffer.drain(..n).collect::<Vec<_>>()
    };
    let service = {
        let mut rng = ctx.rng.borrow_mut();
        ctx.p.vis.bulk.sample(&mut rng) * (1.0 + batch.len() as f64 / 2000.0)
    };
    let end = ctx.db.borrow_mut().vis_servers.schedule(service);
    sleep_until(end).await;
    for tx in batch {
        let _ = tx.send(());
    }
    let again = {
        let mut es = ctx.es.borrow_mut();
        let b = &mut es[slot];
        b.inflight -= 1;
        !b.buffer.is_empty() && (b.buffer.len() as u32 >= ctx.p.k.es_bulk_actions)
    };
    if again {
        Box::pin(es_flush(ctx, pod, false)).await;
    }
}

/// Visibility read (frontend List/Count).
pub async fn vis_read(ctx: &Ctx, pod: PodId, op: PersistOp) -> Res<()> {
    let start = now();
    cpu(ctx, pod, ctx.p.costs.persistence_client).await;
    let service = {
        let mut rng = ctx.rng.borrow_mut();
        ctx.p.vis.read.sample(&mut rng)
    };
    let end = ctx.db.borrow_mut().vis_servers.schedule(service);
    sleep_until(end).await;
    ctx.m.borrow_mut().vis_persist[op.idx()].record(now() - start, None);
    Ok(())
}

// --- history shard helpers --------------------------------------------------------------------

/// Verify `pod` owns `shard` and wait for shard acquisition to finish.
pub async fn shard_ready(ctx: &Ctx, pod: PodId, shard: ShardId, deadline: Time) -> Res<()> {
    let (owner, avail) = {
        let s = &ctx.shards.borrow()[(shard - 1) as usize];
        (s.owner, s.available_at)
    };
    if owner != pod {
        return Err(Err::ShardOwnershipLost);
    }
    let t0 = now();
    if avail <= t0 {
        return Ok(());
    }
    // shard is being acquired: requests block until the engine is ready or their deadline
    loop {
        let (owner, avail) = {
            let s = &ctx.shards.borrow()[(shard - 1) as usize];
            (s.owner, s.available_at)
        };
        if owner != pod {
            return Err(Err::ShardOwnershipLost);
        }
        let t = now();
        if avail <= t {
            ctx.m.borrow_mut().shard_unavailable_waits.record(t - t0);
            return Ok(());
        }
        if t >= deadline {
            ctx.m.borrow_mut().shard_unavailable_waits.record(t - t0);
            return Err(Err::Unavailable);
        }
        sleep((avail - t).min(deadline - t).min(20_000)).await;
    }
}

/// Persist a workflow write under the shard IO semaphore: optional AppendHistoryNodes then the
/// execution write, sequentially (as the SQL/Cassandra execution stores do).
pub async fn shard_write(
    ctx: &Ctx,
    pod: PodId,
    shard: ShardId,
    op: PersistOp,
    append_history: bool,
    caller: Caller,
    deadline: Time,
) -> Res<()> {
    let sem = ctx.shards.borrow()[(shard - 1) as usize].io_sem.clone();
    let prio = if caller == Caller::Preemptable {
        Prio::Low
    } else {
        Prio::High
    };
    let wait_start = now();
    let remaining = deadline.saturating_sub(now());
    let permit: Permit = match sem.acquire_timeout_prio(remaining, prio).await {
        Ok(p) => p,
        Err(_) => return Err(Err::DeadlineExceeded),
    };
    ctx.m.borrow_mut().shard_io_wait.record(now() - wait_start);
    // ownership may have moved while waiting
    if ctx.shard_owner(shard) != pod {
        return Err(Err::ShardOwnershipLost);
    }
    if append_history {
        persist(ctx, pod, PersistOp::AppendHistoryNodes, caller).await?;
    }
    let r = persist(ctx, pod, op, caller).await;
    drop(permit);
    {
        let mut shards = ctx.shards.borrow_mut();
        let s = &mut shards[(shard - 1) as usize];
        s.writes += 1;
        s.persistence_ops += 1 + u64::from(append_history);
    }
    r
}

/// Acquire the per-workflow lock. API callers wait until `deadline - 500ms`; other callers wait
/// at most `history.cacheNonUserContextLockTimeout`. Timeout → BUSY_WORKFLOW.
pub async fn lock_wf(
    ctx: &Ctx,
    wf: WfId,
    wgen: u32,
    caller: Caller,
    deadline: Time,
) -> Res<Permit> {
    let sem = match ctx.wfs.borrow().get(wf, wgen) {
        Some(w) => w.lock.clone(),
        None => return Err(Err::NotFound),
    };
    let t = now();
    let (limit, prio) = if caller.is_api() {
        (
            deadline.saturating_sub(500_000).saturating_sub(t),
            Prio::High,
        )
    } else {
        (
            ctx.p
                .k
                .cache_non_user_lock_timeout
                .min(deadline.saturating_sub(t)),
            Prio::Low,
        )
    };
    match sem.acquire_timeout_prio(limit, prio).await {
        Ok(p) => {
            ctx.m.borrow_mut().lock_wait.record(now() - t);
            Ok(p)
        }
        Err(_) => {
            let mut m = ctx.m.borrow_mut();
            m.lock_wait.record(now() - t);
            m.lock_timeouts += 1;
            Err(Err::ResourceExhausted(
                ReCause::BusyWorkflow,
                Scope::Namespace,
            ))
        }
    }
}

/// Load mutable state through the host-level cache; a miss costs GetWorkflowExecution.
pub async fn load_ms(
    ctx: &Ctx,
    pod: PodId,
    shard: ShardId,
    wf: WfId,
    wgen: u32,
    caller: Caller,
) -> Res<()> {
    let key = {
        let wfs = ctx.wfs.borrow();
        let Some(w) = wfs.get(wf, wgen) else {
            return Err(Err::NotFound);
        };
        let epoch = ctx.shards.borrow()[(shard - 1) as usize].epoch;
        w.key ^ (u64::from(epoch) << 48)
    };
    let hit = {
        let mut pods = ctx.pods.borrow_mut();
        let h = pods[pod].hist.as_mut().expect("history pod");
        h.cache.access(key).0
    };
    if !hit {
        persist(ctx, pod, PersistOp::GetWorkflowExecution, caller).await?;
        cpu(ctx, pod, ctx.p.costs.history_cache_miss).await;
    }
    Ok(())
}

/// Drop a workflow from the host cache (e.g. after a failed write, like Temporal's
/// `clearMutableState`).
pub fn evict_ms(ctx: &Ctx, pod: PodId, shard: ShardId, wf: WfId, wgen: u32) {
    let key = {
        let wfs = ctx.wfs.borrow();
        let Some(w) = wfs.get(wf, wgen) else { return };
        let epoch = ctx.shards.borrow()[(shard - 1) as usize].epoch;
        w.key ^ (u64::from(epoch) << 48)
    };
    if let Some(h) = ctx.pods.borrow_mut()[pod].hist.as_mut() {
        h.cache.remove(key);
    }
}

/// Run `fut` as an independent task and wait for its result with a deadline. The callee keeps
/// running after the caller gives up, like a server-side handler whose client timed out.
pub async fn call_with_timeout<T: 'static>(
    dur: Time,
    fut: impl std::future::Future<Output = Res<T>> + 'static,
) -> Res<T> {
    let (tx, rx) = crate::sim::executor::oneshot();
    spawn(async move {
        let r = fut.await;
        let _ = tx.send(r);
    });
    match timeout(dur, rx).await {
        Ok(Some(r)) => r,
        Ok(None) => Err(Err::Unavailable),
        Err(_) => Err(Err::DeadlineExceeded),
    }
}

/// Handler admission on a history pod (`history.rps`).
pub fn history_admit(ctx: &Ctx, pod: PodId, caller: Caller) -> Res<()> {
    let ok = ctx.pods.borrow_mut()[pod]
        .rps_limiter
        .allow(caller.history_rps_priority());
    if ok {
        Ok(())
    } else {
        let addr = ctx.pods.borrow()[pod].addr.clone();
        ctx.m.borrow_mut().reject("history.rps", addr);
        Err(Err::ResourceExhausted(ReCause::RpsLimit, Scope::System))
    }
}

/// Handler admission on a matching pod (`matching.rps`).
pub fn matching_admit(ctx: &Ctx, pod: PodId) -> Res<()> {
    let ok = ctx.pods.borrow_mut()[pod].rps_limiter.allow(1);
    if ok {
        Ok(())
    } else {
        let addr = ctx.pods.borrow()[pod].addr.clone();
        ctx.m.borrow_mut().reject("matching.rps", addr);
        Err(Err::ResourceExhausted(ReCause::RpsLimit, Scope::System))
    }
}

/// Exponential backoff helper: `initial * coeff^(attempt-1)` capped, with 20% jitter.
pub fn backoff(ctx: &Ctx, initial: Time, coeff: f64, cap: Time, attempt: u32) -> Time {
    let base = (initial as f64 * coeff.powi(attempt.saturating_sub(1) as i32)).min(cap as f64);
    let j = 0.8 + 0.2 * ctx.rand();
    (base * j) as Time
}

pub fn rc<T>(v: T) -> Rc<T> {
    Rc::new(v)
}
