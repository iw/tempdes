//! Matching service, modelled on the 1.31.0 "new matcher" (`service/matching/pri_*.go`).
//!
//! * History adds tasks to a uniformly random write partition; each frontend balances polls to
//!   the partition with the fewest outstanding polls it has sent (per-process load balancer).
//! * AddTask matches immediately with a waiting poller (no sync-match wait) unless the partition
//!   has a non-negligible backlog (head older than `matching.backlogNegligibleAge`). A child
//!   partition with no poller forwards the task to the root through its forwarder (1 in flight,
//!   `matching.forwarderMaxRatePerSecond`). Otherwise the task is spooled: the writer buffers up
//!   to `matching.outstandingTaskAppendsThreshold` appends (then rejects) and issues one
//!   `CreateTasks` of up to `matching.maxTaskBatchSize` at a time.
//! * A sync-matched AddTask blocks until the poller's RecordTaskStarted to history completes
//!   (1s timeout; 10s for backlog tasks).
//! * Children forward one waiting poll at a time to the root.
//! * Backlog reads (`GetTasks`, batch `matching.getTasksBatchSize`) are needed only when the
//!   in-memory buffer can't hold new tasks (fast path ≤ 1000 in memory).

use crate::sim::executor::{Time, now, oneshot, sleep, spawn, timeout};

use super::history::{self, ActTaskInfo, WftInfo};
use super::infra::*;
use super::queues::history_call;
use super::types::*;
use super::world::*;

const MEM_FAST_PATH: usize = 1000;

pub enum Polled {
    Wft(WftInfo),
    Act(ActTaskInfo),
}

fn tqp(ctx: &Ctx, tq: usize, kind: TqKind) -> &super::params::TqTypeParams {
    let t = &ctx.p.task_queues[tq];
    match kind {
        TqKind::Workflow => &t.wf,
        TqKind::Activity => &t.act,
    }
}

fn part_params(ctx: &Ctx, pid: usize) -> (usize, TqKind) {
    let m = ctx.matching.borrow();
    (m.parts[pid].tq, m.parts[pid].kind)
}

/// Can this partition forward (tasks or polls) to its parent right now?
fn forwarding_allowed(ctx: &Ctx, p: &Partition) -> bool {
    let tp = tqp(ctx, p.tq, p.kind);
    if p.backlog_len() == 0 || p.backlog_head_age() < tp.backlog_negligible_age {
        return true;
    }
    now().saturating_sub(p.last_poll) > tp.max_wait_for_poller_before_fwd
}

enum Sync {
    Matched(Res<()>),
    NoPoller,
    Busy,
}

/// Try to hand `task` to a waiting poller on `pid`.
async fn try_sync(ctx: &Ctx, pid: usize, task: MTask) -> Sync {
    loop {
        let waiter = {
            let mut m = ctx.matching.borrow_mut();
            let p = &mut m.parts[pid];
            let tp = tqp(ctx, p.tq, p.kind);
            if p.backlog_len() > 0 && p.backlog_head_age() >= tp.backlog_negligible_age {
                return Sync::NoPoller;
            }
            if let Some(l) = p.dispatch_limiter.as_mut()
                && p.pollers.iter().any(|w| !w.tx.is_canceled())
                && !l.allow()
            {
                return Sync::NoPoller;
            }
            let mut found = None;
            while let Some(w) = p.pollers.pop_front() {
                if !w.tx.is_canceled() && w.deadline > now() {
                    found = Some(w);
                    break;
                }
            }
            p.update_gauges();
            found
        };
        let Some(w) = waiter else {
            return Sync::NoPoller;
        };
        let (dtx, drx) = oneshot::<Res<()>>();
        let forwarded = w.forwarded;
        let since = w.since;
        let matched = Matched {
            task,
            at_partition: pid,
            sync_done: Some(dtx),
            query_done: None,
        };
        if w.tx.send(matched).is_err() {
            continue;
        }
        {
            let mut m = ctx.matching.borrow_mut();
            let p = &mut m.parts[pid];
            p.sync_matches += 1;
            if forwarded {
                p.remote_matches += 1;
            }
            p.poll_wait.record(now() - since);
        }
        return match drx.await {
            Some(Ok(())) => Sync::Matched(Ok(())),
            Some(Err(Err::ResourceExhausted(ReCause::BusyWorkflow, _))) => Sync::Busy,
            Some(Err(e)) => Sync::Matched(Err(e)),
            None => Sync::Matched(Err(Err::Unavailable)),
        };
    }
}

/// AddWorkflowTask / AddActivityTask from history.
pub async fn add_task(
    ctx: &Ctx,
    _from_pod: PodId,
    tq: usize,
    kind: TqKind,
    task: MTask,
    sticky_worker: Option<usize>,
) -> Res<()> {
    let pid = match sticky_worker {
        Some(wk) => match ctx.matching.borrow().sticky.get(&wk) {
            Some(&p) => p,
            None => return Err(Err::StickyWorkerUnavailable),
        },
        None => {
            let m = ctx.matching.borrow();
            let parts = &m.by_tq[&(tq, kind)];
            let write = tqp(ctx, tq, kind).write_partitions as usize;
            let i = ctx.rand_index(write.min(parts.len()));
            parts[i]
        }
    };
    let host = ctx.matching.borrow().parts[pid].host;
    let api = match kind {
        TqKind::Workflow => MatchApi::AddWorkflowTask,
        TqKind::Activity => MatchApi::AddActivityTask,
    };
    hop(ctx).await;
    let t0 = now();
    let r = async {
        matching_admit(ctx, host)?;
        cpu(ctx, host, ctx.p.costs.matching[api.idx()]).await;
        if sticky_worker.is_some() {
            let (loaded, last, waiting) = {
                let m = ctx.matching.borrow();
                let p = &m.parts[pid];
                (
                    p.loaded,
                    p.last_poll,
                    p.pollers.iter().any(|w| !w.tx.is_canceled()),
                )
            };
            if !loaded || (!waiting && now().saturating_sub(last) > 10_000_000) {
                return Err(Err::StickyWorkerUnavailable);
            }
        }
        ctx.matching.borrow_mut().parts[pid].adds += 1;
        offer(ctx, pid, task).await
    }
    .await;
    ctx.m
        .borrow_mut()
        .match_op(host, api)
        .record(now() - t0, r.as_ref().err().copied());
    hop(ctx).await;
    r
}

async fn offer(ctx: &Ctx, pid: usize, task: MTask) -> Res<()> {
    match try_sync(ctx, pid, task).await {
        Sync::Matched(r) => return r,
        Sync::Busy => return write_backlog(ctx, pid, task).await,
        Sync::NoPoller => {}
    }
    // forward to the parent partition through the task forwarder
    let fwd = {
        let mut m = ctx.matching.borrow_mut();
        let p = &mut m.parts[pid];
        let tp = tqp(ctx, p.tq, p.kind);
        match p.parent {
            Some(parent)
                if p.fwd_tasks_inflight < tp.fwd_max_outstanding_tasks
                    && forwarding_allowed(ctx, p)
                    && p.fwd_limiter.allow() =>
            {
                p.fwd_tasks_inflight += 1;
                p.forwarded_tasks += 1;
                Some(parent)
            }
            _ => None,
        }
    };
    if let Some(parent) = fwd {
        let phost = ctx.matching.borrow().parts[parent].host;
        hop(ctx).await;
        let r = if matching_admit(ctx, phost).is_ok() {
            cpu(ctx, phost, ctx.p.costs.matching_forward).await;
            Some(try_sync(ctx, parent, task).await)
        } else {
            None
        };
        hop(ctx).await;
        ctx.matching.borrow_mut().parts[pid].fwd_tasks_inflight -= 1;
        match r {
            Some(Sync::Matched(res)) => return res,
            Some(Sync::Busy) | Some(Sync::NoPoller) | None => {}
        }
    }
    write_backlog(ctx, pid, task).await
}

async fn write_backlog(ctx: &Ctx, pid: usize, task: MTask) -> Res<()> {
    let (tx, rx) = oneshot::<Res<()>>();
    let start_writer = {
        let mut m = ctx.matching.borrow_mut();
        let p = &mut m.parts[pid];
        let tp = tqp(ctx, p.tq, p.kind);
        if p.write_queue.len() as u32 >= tp.outstanding_appends_threshold {
            p.write_rejects += 1;
            drop(m);
            ctx.m.borrow_mut().reject(
                "matching.outstandingTaskAppendsThreshold",
                format!("partition {pid}"),
            );
            return Err(Err::ResourceExhausted(
                ReCause::SystemOverloaded,
                Scope::System,
            ));
        }
        let mut t = task;
        t.from_backlog = true;
        p.write_queue.push_back(WriteReq { task: t, done: tx });
        if !p.writer_active {
            p.writer_active = true;
            true
        } else {
            false
        }
    };
    if start_writer {
        let c = ctx.clone();
        spawn(async move { writer(c, pid).await });
    }
    rx.await.unwrap_or(Err(Err::Unavailable))
}

async fn writer(ctx: Ctx, pid: usize) {
    loop {
        let (batch, host) = {
            let mut m = ctx.matching.borrow_mut();
            let p = &mut m.parts[pid];
            if p.write_queue.is_empty() {
                p.writer_active = false;
                return;
            }
            let tp = tqp(&ctx, p.tq, p.kind);
            let n = (tp.max_task_batch as usize).min(p.write_queue.len());
            (p.write_queue.drain(..n).collect::<Vec<_>>(), p.host)
        };
        let n = batch.len();
        let r = persist(&ctx, host, PersistOp::CreateTasks, Caller::BackgroundHigh).await;
        cpu(&ctx, host, ctx.p.costs.matching_backlog_per_task * n as f64).await;
        match r {
            Err(e) => {
                for req in batch {
                    let _ = req.done.send(Err(e));
                }
            }
            Ok(()) => {
                let renew = {
                    let mut m = ctx.matching.borrow_mut();
                    let p = &mut m.parts[pid];
                    for req in &batch {
                        if p.backlog_db.is_empty() && p.backlog_mem.len() < MEM_FAST_PATH {
                            p.backlog_mem.push_back(req.task);
                        } else {
                            p.backlog_db.push_back(req.task);
                        }
                    }
                    p.writes += n as u64;
                    let renew = p.range_left < n as u32;
                    p.range_left = if renew {
                        100_000
                    } else {
                        p.range_left - n as u32
                    };
                    p.update_gauges();
                    renew
                };
                if renew {
                    let _ = persist(
                        &ctx,
                        host,
                        PersistOp::UpdateTaskQueue,
                        Caller::BackgroundHigh,
                    )
                    .await;
                }
                for req in batch {
                    let _ = req.done.send(Ok(()));
                }
                try_dispatch(&ctx, pid);
                ensure_reader(&ctx, pid);
            }
        }
    }
}

fn ensure_reader(ctx: &Ctx, pid: usize) {
    let start = {
        let mut m = ctx.matching.borrow_mut();
        let p = &mut m.parts[pid];
        let tp = tqp(ctx, p.tq, p.kind);
        if !p.reader_active
            && !p.backlog_db.is_empty()
            && p.backlog_mem.len() as u32 <= tp.get_tasks_reload_at
        {
            p.reader_active = true;
            true
        } else {
            false
        }
    };
    if start {
        let c = ctx.clone();
        spawn(async move { reader(c, pid).await });
    }
}

async fn reader(ctx: Ctx, pid: usize) {
    loop {
        let host = {
            let mut m = ctx.matching.borrow_mut();
            let p = &mut m.parts[pid];
            let tp = tqp(&ctx, p.tq, p.kind);
            if p.backlog_db.is_empty() || p.backlog_mem.len() as u32 > tp.get_tasks_reload_at {
                p.reader_active = false;
                return;
            }
            p.host
        };
        if persist(&ctx, host, PersistOp::GetTasks, Caller::BackgroundHigh)
            .await
            .is_err()
        {
            sleep(3_000_000).await;
            continue;
        }
        let n = {
            let mut m = ctx.matching.borrow_mut();
            let p = &mut m.parts[pid];
            let tp = tqp(&ctx, p.tq, p.kind);
            let n = (tp.get_tasks_batch as usize).min(p.backlog_db.len());
            for _ in 0..n {
                let t = p.backlog_db.pop_front().unwrap();
                p.backlog_mem.push_back(t);
            }
            n
        };
        cpu(
            &ctx,
            host,
            ctx.p.costs.matching_backlog_per_task * n as f64 * 0.5,
        )
        .await;
        try_dispatch(&ctx, pid);
    }
}

/// Match in-memory backlog tasks with waiting pollers.
fn try_dispatch(ctx: &Ctx, pid: usize) {
    let mut acked = 0u32;
    let mut limited_delay: Option<Time> = None;
    loop {
        let mut m = ctx.matching.borrow_mut();
        let p = &mut m.parts[pid];
        if p.backlog_mem.is_empty() {
            break;
        }
        // find a live poller
        let mut waiter = None;
        while let Some(w) = p.pollers.pop_front() {
            if !w.tx.is_canceled() && w.deadline > now() {
                waiter = Some(w);
                break;
            }
        }
        let Some(w) = waiter else { break };
        if let Some(l) = p.dispatch_limiter.as_mut()
            && !l.allow()
        {
            p.pollers.push_front(w);
            limited_delay = Some(l.reserve_delay().max(1_000));
            break;
        }
        let task = p.backlog_mem.pop_front().unwrap();
        let forwarded = w.forwarded;
        let since = w.since;
        let matched = Matched {
            task,
            at_partition: pid,
            sync_done: None,
            query_done: None,
        };
        match w.tx.send(matched) {
            Ok(()) => {
                p.async_matches += 1;
                if forwarded {
                    p.remote_matches += 1;
                }
                p.task_wait.record(now().saturating_sub(task.created));
                p.poll_wait.record(now() - since);
                acked += 1;
            }
            Err(back) => {
                p.backlog_mem.push_front(back.task);
            }
        }
        p.update_gauges();
    }
    if acked > 0 {
        let cleanup = {
            let mut m = ctx.matching.borrow_mut();
            let p = &mut m.parts[pid];
            p.acked_since_delete += acked;
            let tp = tqp(ctx, p.tq, p.kind);
            if p.acked_since_delete >= tp.max_task_delete_batch
                || now().saturating_sub(p.last_delete) >= tp.task_delete_interval
            {
                p.acked_since_delete = 0;
                p.last_delete = now();
                Some(p.host)
            } else {
                None
            }
        };
        if let Some(host) = cleanup {
            let c = ctx.clone();
            spawn(async move {
                let _ = persist(
                    &c,
                    host,
                    PersistOp::CompleteTasksLessThan,
                    Caller::BackgroundHigh,
                )
                .await;
            });
        }
    }
    if let Some(d) = limited_delay {
        let c = ctx.clone();
        spawn(async move {
            sleep(d).await;
            try_dispatch(&c, pid);
        });
    }
    ensure_reader(ctx, pid);
}

/// PollWorkflowTaskQueue / PollActivityTaskQueue arriving at matching from frontend `fe`.
pub async fn poll(
    ctx: &Ctx,
    fe: PodId,
    tq: usize,
    kind: TqKind,
    sticky_worker: Option<usize>,
    deadline: Time,
) -> Res<Option<Polled>> {
    // partition choice: sticky queue, or the frontend's least-loaded read partition
    let (pid, lb_slot) = match sticky_worker {
        Some(wk) => match ctx.matching.borrow().sticky.get(&wk) {
            Some(&p) => (p, None),
            None => return Err(Err::Unavailable),
        },
        None => {
            let read = tqp(ctx, tq, kind).read_partitions as usize;
            let parts = ctx.matching.borrow().by_tq[&(tq, kind)].clone();
            let n = read.min(parts.len()).max(1);
            let start = ctx.rand_index(n);
            let mut pods = ctx.pods.borrow_mut();
            let fes = pods[fe].fe.as_mut().expect("frontend pod");
            let counts = fes.poll_lb.entry((tq, kind)).or_insert_with(|| vec![0; n]);
            if counts.len() < n {
                counts.resize(n, 0);
            }
            let mut best = start;
            for k in 0..n {
                let i = (start + k) % n;
                if counts[i] < counts[best] {
                    best = i;
                }
            }
            counts[best] += 1;
            (parts[best], Some(best))
        }
    };
    let host = ctx.matching.borrow().parts[pid].host;
    let api = match kind {
        TqKind::Workflow => MatchApi::PollWorkflowTaskQueue,
        TqKind::Activity => MatchApi::PollActivityTaskQueue,
    };
    hop(ctx).await;
    let t0 = now();
    let r = async {
        matching_admit(ctx, host)?;
        cpu(ctx, host, ctx.p.costs.matching[api.idx()]).await;
        {
            let mut m = ctx.matching.borrow_mut();
            let p = &mut m.parts[pid];
            p.last_poll = now();
            p.loaded = true;
            p.polls += 1;
        }
        poll_partition(ctx, pid, deadline).await
    }
    .await;
    ctx.m
        .borrow_mut()
        .match_op(host, api)
        .record(now() - t0, r.as_ref().err().copied());
    hop(ctx).await;
    if let Some(slot) = lb_slot {
        let mut pods = ctx.pods.borrow_mut();
        if let Some(fes) = pods[fe].fe.as_mut()
            && let Some(c) = fes.poll_lb.get_mut(&(tq, kind))
            && let Some(v) = c.get_mut(slot)
        {
            *v = v.saturating_sub(1);
        }
    }
    r
}

async fn poll_partition(ctx: &Ctx, pid: usize, deadline: Time) -> Res<Option<Polled>> {
    let (tq, kind) = part_params(ctx, pid);
    let tp_expire = tqp(ctx, tq, kind).long_poll_expiration;
    let expire_at = (now() + tp_expire).min(deadline.saturating_sub(1_000_000));
    loop {
        if now() >= expire_at {
            ctx.matching.borrow_mut().parts[pid].poll_timeouts += 1;
            return Ok(None);
        }
        // backlog first
        let immediate = {
            let mut m = ctx.matching.borrow_mut();
            let p = &mut m.parts[pid];
            let ok = match p.dispatch_limiter.as_mut() {
                Some(l) => p.backlog_mem.front().is_some() && l.allow(),
                None => true,
            };
            if ok {
                p.backlog_mem.pop_front().inspect(|t| {
                    p.async_matches += 1;
                    p.task_wait.record(now().saturating_sub(t.created));
                    p.poll_wait.record(0);
                    p.update_gauges();
                })
            } else {
                None
            }
        };
        let matched = match immediate {
            Some(task) => {
                ensure_reader(ctx, pid);
                Matched {
                    task,
                    at_partition: pid,
                    sync_done: None,
                    query_done: None,
                }
            }
            None => {
                // register as a waiting poller, possibly parked at the parent (poll forwarding)
                let target = {
                    let mut m = ctx.matching.borrow_mut();
                    let p = &m.parts[pid];
                    let tp = tqp(ctx, p.tq, p.kind);
                    let can_fwd = p.parent.is_some()
                        && p.fwd_polls_inflight < tp.fwd_max_outstanding_polls
                        && p.backlog_mem.is_empty()
                        && forwarding_allowed(ctx, p);
                    let parent = p.parent;
                    if can_fwd {
                        let p = &mut m.parts[pid];
                        p.fwd_polls_inflight += 1;
                        p.forwarded_polls += 1;
                        parent.unwrap()
                    } else {
                        pid
                    }
                };
                if target != pid {
                    let phost = ctx.matching.borrow().parts[target].host;
                    hop(ctx).await;
                    if matching_admit(ctx, phost).is_err() {
                        ctx.matching.borrow_mut().parts[pid].fwd_polls_inflight -= 1;
                        sleep(10_000).await;
                        continue;
                    }
                    cpu(ctx, phost, ctx.p.costs.matching_forward).await;
                }
                let (tx, rx) = oneshot::<Matched>();
                {
                    let mut m = ctx.matching.borrow_mut();
                    let p = &mut m.parts[target];
                    p.pollers.push_back(PollWaiter {
                        tx,
                        since: now(),
                        deadline: expire_at,
                        forwarded: target != pid,
                    });
                    p.update_gauges();
                }
                // a waiting poller may be able to take backlog right away at the target
                if target != pid {
                    try_dispatch(ctx, target);
                } else {
                    try_dispatch(ctx, pid);
                }
                let wait = expire_at.saturating_sub(now());
                let res = timeout(wait, rx).await;
                if target != pid {
                    ctx.matching.borrow_mut().parts[pid].fwd_polls_inflight -= 1;
                    hop(ctx).await;
                }
                match res {
                    Ok(Some(m)) => m,
                    _ => {
                        let mut m = ctx.matching.borrow_mut();
                        m.parts[pid].poll_timeouts += 1;
                        m.parts[target].update_gauges();
                        return Ok(None);
                    }
                }
            }
        };
        // RecordTaskStarted from the host where the match happened
        let at_host = ctx.matching.borrow().parts[matched.at_partition].host;
        let sync = matched.sync_done.is_some();
        let limit = if sync { 1_000_000 } else { 10_000_000 };
        let task = matched.task;
        let r = record_started(ctx, at_host, task, limit).await;
        match r {
            Ok(polled) => {
                if let Some(tx) = matched.sync_done {
                    let _ = tx.send(Ok(()));
                }
                let wf_type = match &polled {
                    Polled::Wft(i) => i.wf_type,
                    Polled::Act(a) => a.wf_type,
                };
                let lat = now().saturating_sub(task.created);
                let mut mm = ctx.m.borrow_mut();
                match &polled {
                    Polled::Wft(_) => mm.wf[wf_type].wft_sched_to_start.record(lat),
                    Polled::Act(_) => mm.wf[wf_type].act_sched_to_start.record(lat),
                }
                return Ok(Some(polled));
            }
            Err(e) => {
                if let Some(tx) = matched.sync_done {
                    let _ = tx.send(Err(e));
                } else if task.from_backlog {
                    let transient = matches!(
                        e,
                        Err::Unavailable
                            | Err::DeadlineExceeded
                            | Err::ResourceExhausted(_, Scope::System)
                            | Err::ResourceExhausted(ReCause::BusyWorkflow, _)
                    );
                    if transient {
                        let mut m = ctx.matching.borrow_mut();
                        m.parts[matched.at_partition].backlog_mem.push_front(task);
                    }
                }
                // keep polling with the remaining time
            }
        }
    }
}

/// Matching → history RecordWorkflowTaskStarted / RecordActivityTaskStarted.
async fn record_started(ctx: &Ctx, _at_host: PodId, task: MTask, limit: Time) -> Res<Polled> {
    let Some(shard) = ctx.wf_shard(task.wf, task.wf_gen) else {
        return Err(Err::NotFound);
    };
    let c = ctx.clone();
    call_with_timeout(limit, async move {
        let deadline = now() + limit;
        match task.kind {
            TqKind::Workflow => history_call(&c, shard, |c2, hp| {
                let c2 = c2.clone();
                async move {
                    history::record_wft_started(&c2, hp, task.wf, task.wf_gen, task.r, deadline)
                        .await
                }
            })
            .await
            .map(Polled::Wft),
            TqKind::Activity => history_call(&c, shard, |c2, hp| {
                let c2 = c2.clone();
                async move {
                    history::record_activity_started(
                        &c2,
                        hp,
                        task.wf,
                        task.wf_gen,
                        task.r,
                        task.r2,
                        deadline,
                    )
                    .await
                }
            })
            .await
            .map(Polled::Act),
        }
    })
    .await
}

/// Drop every waiting poller of partitions hosted on `pod` (partition moved / pod removed).
pub fn evict_partitions(ctx: &Ctx, moved: &[usize]) {
    let mut m = ctx.matching.borrow_mut();
    for &pid in moved {
        let p = &mut m.parts[pid];
        p.pollers.clear(); // dropping senders wakes pollers with None -> they re-poll
        // in-memory backlog must be re-read by the new owner
        while let Some(t) = p.backlog_mem.pop_back() {
            p.backlog_db.push_front(t);
        }
        p.reader_active = false;
        p.loaded = p.sticky_of.is_none() && p.loaded;
        p.update_gauges();
    }
    drop(m);
    for &pid in moved {
        ensure_reader(ctx, pid);
    }
}
