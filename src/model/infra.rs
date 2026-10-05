//! Infrastructure primitives shared by all service flows: CPU bursts on pods, network hops,
//! persistence calls (rate limiting → connection pool → database), visibility store writes, the
//! shard IO semaphore and the workflow lock / mutable state cache.

use std::rc::Rc;

use crate::sim::executor::{Time, now, sleep, sleep_until, spawn, timeout};
use crate::sim::sync::{Permit, Prio};

use super::ratelimit::PriorityLimiter;
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

/// Who makes a persistence call: its priority in the persistence priority limiters
/// (`common/persistence/client/quotas.go`) and, for calls made on behalf of a namespace, the
/// namespace, which subjects them to the namespace persistence limiters. API handlers and history
/// task executors carry the namespace (`NewBackgroundHighCallerInfo(ns)` in `executable.go`), as
/// do matching's task queue managers; queue loads and shard management run as the system.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Caller {
    /// API caller: priority (1 for Start/Signal/GetWorkflowExecutionHistory, 2 otherwise) and
    /// namespace.
    Api(usize, usize),
    /// background work of a namespace: history tasks at high priority, matching's writes
    BackgroundHigh(usize),
    /// history timeout tasks of a namespace
    BackgroundLow(usize),
    Preemptable,
    /// queue loads (GetHistoryTasks) = 3
    QueueLoad,
    /// shard management / range completion = 1
    ShardMgmt,
}

impl Caller {
    pub fn persistence_priority(self) -> usize {
        match self {
            Caller::Api(p, _) => p,
            Caller::ShardMgmt => 1,
            Caller::QueueLoad => 3,
            Caller::BackgroundHigh(_) => 4,
            Caller::BackgroundLow(_) => 5,
            Caller::Preemptable => 6,
        }
    }

    /// history.rps priority (Operator 0, API 1, BackgroundHigh 2, BackgroundLow 3, Preemptable 4).
    pub fn history_rps_priority(self) -> usize {
        match self {
            Caller::Api(..) => 1,
            Caller::BackgroundHigh(_) | Caller::QueueLoad | Caller::ShardMgmt => 2,
            Caller::BackgroundLow(_) => 3,
            Caller::Preemptable => 4,
        }
    }

    pub fn is_api(self) -> bool {
        matches!(self, Caller::Api(..))
    }

    /// The namespace the call is made for; `None` for system calls, which the namespace
    /// limiters skip (`hasCaller`).
    pub fn namespace(self) -> Option<usize> {
        match self {
            Caller::Api(_, ns) | Caller::BackgroundHigh(ns) | Caller::BackgroundLow(ns) => Some(ns),
            _ => None,
        }
    }
}

/// The persistence rate limiters a call from `pod` meets, in Temporal's order (`allow` in
/// `persistence_rate_limited_clients.go`): the namespace's per-shard limit (history, when
/// `history.persistencePerShardNamespaceMaxQPS` is set), the namespace limit, then the pod's
/// limit. A refusal fails the call at once with `ResourceExhausted` (`PERSISTENCE_LIMIT`), of
/// namespace scope for the first two and system scope for the last; there is no waiting for a
/// token.
fn admit_persistence(
    ctx: &Ctx,
    pod: PodId,
    op: PersistOp,
    caller: Caller,
    shard: Option<ShardId>,
) -> Res<()> {
    let prio = caller.persistence_priority();
    let ns = caller.namespace();
    let refused = {
        let mut pods = ctx.pods.borrow_mut();
        let p = &mut pods[pod];
        let svc = p.svc;
        let mut refused = None;
        if let (Some(ns), Some(shard)) = (ns, shard)
            && svc == Service::History
            && !p.persist_limiter.is_unlimited()
        {
            let rate = ctx.p.namespaces[ns].hist_persist_shard_ns_qps;
            if rate > 0.0 {
                let k = &ctx.p.k;
                let l = p.shard_ns_limiters.entry((ns, shard)).or_insert_with(|| {
                    PriorityLimiter::new(
                        7,
                        rate,
                        rate * k.persistence_burst_ratio,
                        Some(k.operator_rps_ratio),
                    )
                });
                if !l.allow(prio) {
                    refused = Some(("persistencePerShardNamespaceMaxQPS", Scope::Namespace));
                }
            }
        }
        if refused.is_none()
            && let Some(ns) = ns
            && let Some(l) = p.ns_persist_limiters.get_mut(ns)
        {
            *ctx.m
                .borrow_mut()
                .persist_ns_limited
                .entry((pod, ns))
                .or_default() += 1;
            if !l.allow(prio) {
                refused = Some(("persistenceNamespaceMaxQPS", Scope::Namespace));
            }
        }
        if refused.is_none() {
            ctx.m.borrow_mut().persist_limited_pod(pod);
            if !p.persist_limiter.allow(prio) {
                refused = Some(("persistenceMaxQPS", Scope::System));
            }
        }
        refused.map(|(limit, scope)| (svc, p.addr.clone(), limit, scope))
    };
    let Some((svc, addr, limit, scope)) = refused else {
        return Ok(());
    };
    let e = Err::ResourceExhausted(ReCause::PersistenceLimit, scope);
    let mut m = ctx.m.borrow_mut();
    m.persist[op.idx()].record(0, Some(e));
    let place = match ns {
        Some(ns) if scope == Scope::Namespace => {
            format!("{addr} ns={}", ctx.p.namespaces[ns].name)
        }
        _ => addr,
    };
    m.reject(&format!("{}.{limit}", svc.as_str()), place);
    Err(e)
}

/// One database statement from `pod`: the persistence client's CPU, a connection from the pod's
/// pool, then the database station, for the operation's service time plus the time its `bytes`
/// of history take to write, or to read for `ReadHistoryBranch`.
async fn db_statement(ctx: &Ctx, pod: PodId, op: PersistOp, bytes: f64) {
    cpu(ctx, pod, ctx.p.costs.persistence_client).await;
    let start = now();
    let (pool, svc) = {
        let pods = ctx.pods.borrow();
        (pods[pod].db_pool.clone(), pods[pod].svc)
    };
    let conn = pool.acquire().await;
    let conn_wait = now() - start;
    let per_byte = if op == PersistOp::ReadHistoryBranch {
        ctx.p.db_read_us_per_byte
    } else {
        ctx.p.db_write_us_per_byte
    };
    let service = {
        let mut rng = ctx.rng.borrow_mut();
        ctx.p.db_latency[op.idx()].sample(&mut rng) + bytes * per_byte
    };
    let end = ctx.db.borrow_mut().servers.schedule(service);
    sleep_until(end).await;
    drop(conn);
    ctx.m.borrow_mut().persist_conn_wait[svc.idx()].record(conn_wait);
}

/// A persistence call from `pod`: the rate limiters, then the call's database statements.
/// `shard` is the history shard the call is for, when it has one.
pub async fn persist(
    ctx: &Ctx,
    pod: PodId,
    op: PersistOp,
    caller: Caller,
    shard: Option<ShardId>,
) -> Res<()> {
    persist_call(ctx, pod, op, 0.0, caller, shard).await
}

/// `ReadHistoryBranch` of `bytes` of history from `pod`.
pub async fn read_history(
    ctx: &Ctx,
    pod: PodId,
    bytes: f64,
    caller: Caller,
    shard: Option<ShardId>,
) -> Res<()> {
    if PersistOp::ReadHistoryBranch.rate_limited() {
        admit_persistence(ctx, pod, PersistOp::ReadHistoryBranch, caller, shard)?;
    }
    let start = now();
    db_statement(ctx, pod, PersistOp::ReadHistoryBranch, bytes).await;
    let lat = now() - start;
    let mut m = ctx.m.borrow_mut();
    m.persist[PersistOp::ReadHistoryBranch.idx()].record(lat, None);
    m.persist_pod(pod);
    Ok(())
}

/// A persistence call, optionally carrying `append` bytes of new history events.
/// Create/UpdateWorkflowExecution persist their events inside the same call: the SQL and
/// Cassandra stores append the history nodes, then write the mutable state
/// (`UpdateWorkflowExecution` in `common/persistence/sql/execution.go` and
/// `cassandra/execution_store.go`). The append is not charged to the rate limiters, and the
/// call's latency, including both statements, is recorded under the call's own operation, as
/// Temporal's `persistence_latency` does. When calibration has fitted the operation's latency to
/// production, whose measurements already include the append, the call is one statement drawn
/// from that distribution, carrying the append's bytes.
async fn persist_call(
    ctx: &Ctx,
    pod: PodId,
    op: PersistOp,
    append: f64,
    caller: Caller,
    shard: Option<ShardId>,
) -> Res<()> {
    if op.rate_limited() {
        admit_persistence(ctx, pod, op, caller, shard)?;
    }
    let start = now();
    let mut bytes = 0.0;
    if append > 0.0 {
        if ctx.p.db_includes_append[op.idx()] {
            bytes = append;
        } else {
            db_statement(ctx, pod, PersistOp::AppendHistoryNodes, append).await;
        }
    }
    db_statement(ctx, pod, op, bytes).await;
    let lat = now() - start;
    let mut m = ctx.m.borrow_mut();
    m.persist[op.idx()].record(lat, None);
    m.persist_pod(pod);
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

/// Persist a workflow write under the shard IO semaphore (`ContextImpl.UpdateWorkflowExecution`
/// in `service/history/shard/context_impl.go` takes it before calling the execution manager),
/// with its new history events appended inside the same call.
pub async fn shard_write(
    ctx: &Ctx,
    pod: PodId,
    shard: ShardId,
    op: PersistOp,
    append: f64,
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
    let r = persist_call(ctx, pod, op, append, caller, Some(shard)).await;
    drop(permit);
    {
        let mut shards = ctx.shards.borrow_mut();
        let s = &mut shards[(shard - 1) as usize];
        s.writes += 1;
        s.persistence_ops += 1;
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

/// A workflow's key in the host-level cache: its own, with the shard's epoch, so a shard that
/// moves starts cold on its new owner.
pub fn ms_key(ctx: &Ctx, shard: ShardId, wf: WfId, wgen: u32) -> Option<u64> {
    let key = ctx.wfs.borrow().get(wf, wgen)?.key;
    let epoch = ctx.shards.borrow()[(shard - 1) as usize].epoch;
    Some(key ^ (u64::from(epoch) << 48))
}

/// Load mutable state through the host-level cache. A miss (including a workflow cached for
/// longer than `history.cacheTTL`), or a cached workflow whose mutable state was cleared, costs
/// GetWorkflowExecution (`LoadMutableState` in `service/history/workflow/context.go`).
pub async fn load_ms(
    ctx: &Ctx,
    pod: PodId,
    shard: ShardId,
    wf: WfId,
    wgen: u32,
    caller: Caller,
) -> Res<()> {
    let Some(key) = ms_key(ctx, shard, wf, wgen) else {
        return Err(Err::NotFound);
    };
    let loaded = {
        let mut pods = ctx.pods.borrow_mut();
        let h = pods[pod].hist.as_mut().expect("history pod");
        let (hit, evicted) = h.cache.access_at(key, now());
        if let Some(k) = evicted {
            h.unloaded.remove(&k);
        }
        hit && !h.unloaded.contains(&key)
    };
    if !loaded {
        let r = persist(
            ctx,
            pod,
            PersistOp::GetWorkflowExecution,
            caller,
            Some(shard),
        )
        .await;
        {
            // a failed load leaves the cached workflow without mutable state, for the next access
            // to load
            let mut pods = ctx.pods.borrow_mut();
            let h = pods[pod].hist.as_mut().expect("history pod");
            if r.is_ok() {
                h.unloaded.remove(&key);
            } else if h.cache.contains(key) {
                h.unloaded.insert(key);
            }
        }
        r?;
        cpu(ctx, pod, ctx.p.costs.history_cache_miss).await;
    }
    Ok(())
}

/// Clear a workflow's cached mutable state, as Temporal does when a call or task holding the
/// workflow fails: the write methods of `service/history/workflow/context.go` call
/// `ContextImpl.Clear` on any error, and so does the workflow cache's release function
/// (`service/history/workflow/cache/cache.go`) for any error it is released with. The workflow
/// stays in the cache; its next access loads the mutable state again.
pub fn clear_ms(ctx: &Ctx, pod: PodId, shard: ShardId, wf: WfId, wgen: u32) {
    let Some(key) = ms_key(ctx, shard, wf, wgen) else {
        return;
    };
    if let Some(h) = ctx.pods.borrow_mut()[pod].hist.as_mut()
        && h.cache.contains(key)
    {
        h.unloaded.insert(key);
    }
}

/// Pass `r` on, clearing the workflow's mutable state if it is an error: a call that fails while
/// holding the workflow releases it with the error.
pub fn clear_on_err<T>(
    ctx: &Ctx,
    pod: PodId,
    shard: ShardId,
    wf: WfId,
    wgen: u32,
    r: Res<T>,
) -> Res<T> {
    if r.is_err() {
        clear_ms(ctx, pod, shard, wf, wgen);
    }
    r
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
