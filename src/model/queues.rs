//! History task queues (transfer, timer, visibility) — `service/history/queues`.
//!
//! * Tasks are written with the workflow transaction and later *re-read* from the database by a
//!   per-shard reader (`GetTransferTasks` / `GetTimerTasks` / `GetVisibilityTasks`, batch
//!   `history.*TaskBatchSize`, rate limited per shard by `history.*ProcessorMaxPollRPS` and per
//!   host by `history.*ProcessorMaxPollHostRPS`), paused when a shard has
//!   `history.queuePendingTasksMaxCount` tasks loaded.
//! * Loaded tasks go to the host-level scheduler: interleaved weighted round robin over
//!   (namespace, priority) channels (`history.*ProcessorSchedulerActiveRoundRobinWeights`, high 10
//!   / low 9 by default) in front of `history.*ProcessorSchedulerWorkerCount` workers, plus the
//!   optional execution queue scheduler for busy workflows.
//! * Execution takes the workflow lock as a non-API caller (≤ `cacheNonUserContextLockTimeout`),
//!   loads mutable state and performs the task. Failures follow `executable.go`: immediate
//!   resubmits while the attempt is at most 10 (throttling gets one), then backoff 1s·1.1ⁿ⁻¹, or
//!   for throttling max(1s·1.1ⁿ⁻¹, 3s·1.5ᵐ⁻¹) with m the throttles in a row.
//! * Every `history.*ProcessorUpdateAckInterval` a shard checkpoints: RangeCompleteHistoryTasks,
//!   plus UpdateShard at most every `history.shardUpdateMinInterval` / 1000 tasks.

use crate::sim::executor::{Time, now, sleep, sleep_until, spawn};
use crate::sim::sync::{Permit, Prio, Semaphore, WeightedPermit};

use super::activity;
use super::history::{self, Cached, StartOrigin, record_child_completed, start_workflow};
use super::infra::*;
use super::matching;
use super::types::*;
use super::world::*;

/// A task to generate from a transaction.
#[derive(Clone, Copy, Debug)]
pub struct TaskSpec {
    pub kind: TaskType,
    pub fire_at: Time,
    pub r: u32,
    pub r2: u32,
    pub bytes: f64,
}

impl TaskSpec {
    pub fn now(kind: TaskType, r: u32, r2: u32) -> Self {
        TaskSpec {
            kind,
            fire_at: 0,
            r,
            r2,
            bytes: 0.0,
        }
    }
    pub fn at(kind: TaskType, fire_at: Time, r: u32, r2: u32) -> Self {
        TaskSpec {
            kind,
            fire_at,
            r,
            r2,
            bytes: 0.0,
        }
    }
    /// The size of the batch of events the task's event was written in.
    pub fn batch(self, bytes: f64) -> Self {
        TaskSpec { bytes, ..self }
    }
}

/// Make committed tasks visible to the shard's queues and wake readers.
pub fn commit_tasks(ctx: &Ctx, shard: ShardId, wf: WfId, wgen: u32, specs: &[TaskSpec]) {
    if specs.is_empty() {
        return;
    }
    let t = now();
    let mut wake = [false; 3];
    let mut timer_wake: Option<Time> = None;
    {
        let mut shards = ctx.shards.borrow_mut();
        let s = &mut shards[(shard - 1) as usize];
        for sp in specs {
            let task = HistTask {
                kind: sp.kind,
                wf,
                wf_gen: wgen,
                created: t,
                fire_at: if sp.fire_at == 0 { t } else { sp.fire_at },
                r: sp.r,
                r2: sp.r2,
                bytes: sp.bytes,
            };
            let c = sp.kind.category();
            match c {
                Category::Timer => {
                    let seq = ctx.next_timer_seq();
                    let q = &mut s.queues[c.idx()];
                    q.push_timer(task, seq);
                    let wake_at = task.fire_at.saturating_sub(ctx.p.k.timer_max_time_shift);
                    let earlier = match q.timer_wake_at {
                        Some(w) => wake_at < w,
                        None => true,
                    };
                    if earlier || !q.reader_active {
                        timer_wake = Some(timer_wake.map_or(wake_at, |x: Time| x.min(wake_at)));
                    }
                }
                _ => {
                    s.queues[c.idx()].unloaded.push_back(task);
                    if !s.queues[c.idx()].reader_active {
                        s.queues[c.idx()].reader_active = true;
                        wake[c.idx()] = true;
                    }
                }
            }
        }
    }
    for c in [Category::Transfer, Category::Visibility] {
        if wake[c.idx()] {
            let ctx2 = ctx.clone();
            spawn(async move { immediate_reader(ctx2, shard, c).await });
        }
    }
    if let Some(at) = timer_wake {
        arm_timer_reader(ctx, shard, at);
    }
}

fn arm_timer_reader(ctx: &Ctx, shard: ShardId, at: Time) {
    let seq = {
        let mut shards = ctx.shards.borrow_mut();
        let q = &mut shards[(shard - 1) as usize].queues[Category::Timer.idx()];
        q.timer_wake_seq += 1;
        q.timer_wake_at = Some(at);
        q.reader_active = true;
        q.timer_wake_seq
    };
    let ctx2 = ctx.clone();
    spawn(async move { timer_reader(ctx2, shard, seq, at).await });
}

async fn wait_load_tokens(ctx: &Ctx, pod: PodId, shard: ShardId, c: Category) {
    let d1 = ctx.shards.borrow_mut()[(shard - 1) as usize].queues[c.idx()]
        .load_limiter
        .reserve_delay();
    let d2 = {
        let mut pods = ctx.pods.borrow_mut();
        pods[pod]
            .hist
            .as_mut()
            .map(|h| h.load_limiters[c.idx()].reserve_delay())
            .unwrap_or(0)
    };
    let d = d1.max(d2);
    if d > 0 {
        sleep(d).await;
    }
}

/// Reader for transfer / visibility queues of one shard.
async fn immediate_reader(ctx: Ctx, shard: ShardId, c: Category) {
    loop {
        let (owner, avail, empty, pending) = {
            let shards = ctx.shards.borrow();
            let s = &shards[(shard - 1) as usize];
            let q = &s.queues[c.idx()];
            (s.owner, s.available_at, q.unloaded.is_empty(), q.pending)
        };
        if empty {
            ctx.shards.borrow_mut()[(shard - 1) as usize].queues[c.idx()].reader_active = false;
            return;
        }
        if avail > now() {
            sleep((avail - now()).min(100_000)).await;
            continue;
        }
        if pending >= ctx.p.k.queue_pending_max {
            sleep(5_000_000).await;
            continue;
        }
        wait_load_tokens(&ctx, owner, shard, c).await;
        // tasks committed before the read starts are visible to it
        let visible = ctx.shards.borrow()[(shard - 1) as usize].queues[c.idx()]
            .unloaded
            .len();
        if persist(&ctx, owner, c.load_op(), Caller::QueueLoad, None)
            .await
            .is_err()
        {
            sleep(3_000_000).await;
            continue;
        }
        if ctx.shard_owner(shard) != owner {
            // shard moved while loading; the new owner's reader will pick the tasks up
            sleep(1_000_000).await;
            continue;
        }
        let batch: Vec<HistTask> = {
            let mut shards = ctx.shards.borrow_mut();
            let q = &mut shards[(shard - 1) as usize].queues[c.idx()];
            let n = visible
                .min(q.unloaded.len())
                .min(ctx.p.k.task_batch[c.idx()] as usize);
            let v: Vec<HistTask> = q.unloaded.drain(..n).collect();
            q.pending += v.len() as u32;
            v
        };
        cpu(
            &ctx,
            owner,
            ctx.p.costs.queue_load_base + ctx.p.costs.queue_load_per_task * batch.len() as f64,
        )
        .await;
        let loaded_at = now();
        for task in batch {
            let c2 = ctx.clone();
            spawn(async move { run_task(c2, shard, task, loaded_at).await });
        }
    }
}

/// Timer queue reader: wakes `timerProcessorMaxTimeShift` before the earliest timer, loads due
/// timers in one read and fires each at its time.
async fn timer_reader(ctx: Ctx, shard: ShardId, my_seq: u64, wake_at: Time) {
    if wake_at > now() {
        sleep_until(wake_at).await;
    }
    loop {
        {
            let shards = ctx.shards.borrow();
            let q = &shards[(shard - 1) as usize].queues[Category::Timer.idx()];
            if q.timer_wake_seq != my_seq {
                return; // superseded by an earlier wake-up
            }
        }
        let (owner, avail, next, pending) = {
            let shards = ctx.shards.borrow();
            let s = &shards[(shard - 1) as usize];
            let q = &s.queues[Category::Timer.idx()];
            (s.owner, s.available_at, q.next_timer_at(), q.pending)
        };
        let Some(next) = next else {
            let mut shards = ctx.shards.borrow_mut();
            let q = &mut shards[(shard - 1) as usize].queues[Category::Timer.idx()];
            q.reader_active = false;
            q.timer_wake_at = None;
            return;
        };
        let shift = ctx.p.k.timer_max_time_shift;
        let wake = next.saturating_sub(shift);
        if wake > now() {
            {
                let mut shards = ctx.shards.borrow_mut();
                shards[(shard - 1) as usize].queues[Category::Timer.idx()].timer_wake_at =
                    Some(wake);
            }
            sleep_until(wake).await;
            continue;
        }
        if avail > now() {
            sleep((avail - now()).min(100_000)).await;
            continue;
        }
        if pending >= ctx.p.k.queue_pending_max {
            sleep(5_000_000).await;
            continue;
        }
        wait_load_tokens(&ctx, owner, shard, Category::Timer).await;
        if persist(
            &ctx,
            owner,
            PersistOp::GetTimerTasks,
            Caller::QueueLoad,
            None,
        )
        .await
        .is_err()
        {
            sleep(3_000_000).await;
            continue;
        }
        let horizon = now() + shift;
        let batch: Vec<HistTask> = {
            let mut shards = ctx.shards.borrow_mut();
            let q = &mut shards[(shard - 1) as usize].queues[Category::Timer.idx()];
            let mut v = Vec::new();
            while v.len() < ctx.p.k.task_batch[Category::Timer.idx()] as usize {
                match q.next_timer_at() {
                    Some(t) if t <= horizon => v.push(q.pop_timer().unwrap()),
                    _ => break,
                }
            }
            q.pending += v.len() as u32;
            v
        };
        cpu(
            &ctx,
            owner,
            ctx.p.costs.queue_load_base + ctx.p.costs.queue_load_per_task * batch.len() as f64,
        )
        .await;
        let loaded_at = now();
        for task in batch {
            let c2 = ctx.clone();
            spawn(async move {
                if task.fire_at > now() {
                    sleep_until(task.fire_at).await;
                }
                run_task(c2, shard, task, loaded_at.max(task.fire_at)).await;
            });
        }
    }
}

enum Outcome {
    Done,
    Noop,
    Retry(Err),
    Drop,
}

/// How a task's next run gets a worker.
enum RunPermit {
    /// a worker of the host scheduler's pool
    Pool(WeightedPermit),
    /// a worker of its workflow's execution queue on `pod`
    Exec(Permit, PodId),
}

/// Schedule and execute one history task with Temporal's retry policy (`executable.go`).
///
/// * Every run goes through the host scheduler: its (namespace, priority) channel waits its
///   interleaved weighted round robin turn for a pool worker. A task of a workflow that has an
///   execution queue runs there instead of on a pool worker (`ExecutionAwareScheduler`).
/// * A failure increments the attempt (`HandleErr`), and a throttling error also the throttle
///   count, which busy-workflow errors leave alone and any other error resets.
/// * `Nack`: with the execution queue scheduler on, a busy-workflow failure moves the task to
///   its workflow's queue. Otherwise the task is resubmitted immediately while its attempt is at
///   most 10, except that throttling allows one immediate resubmit (`shouldResubmitOnNack`).
///   Then it backs off 1s·1.1ⁿ⁻¹ for attempt n, or for throttling the larger of that and
///   3s·1.5ᵐ⁻¹ for the m-th throttle in a row (`backoffDuration`).
async fn run_task(ctx: Ctx, shard: ShardId, task: HistTask, loaded_at: Time) {
    let c = task.kind.category();
    let mut attempt: u32 = 1;
    let mut throttles: u32 = 0;
    {
        let mut m = ctx.m.borrow_mut();
        m.tasks[task.kind.idx()]
            .load_latency
            .record(loaded_at.saturating_sub(task.fire_at));
    }
    let ns = ctx
        .wfs
        .borrow()
        .get(task.wf, task.wf_gen)
        .map_or(0, |w| w.ns);
    let (prio, level) = if task.kind.low_priority() {
        (Prio::Low, 1)
    } else {
        (Prio::High, 0)
    };
    let channel = (ns as u64) << 2 | level as u64;
    let weight = ctx.p.namespaces[ns].sched_weights[c.idx()][level];
    let exec_key = (task.wf, task.wf_gen);
    // set when a busy-workflow failure moved the task to its workflow's execution queue
    let mut routed: Option<(PodId, Semaphore)> = None;
    loop {
        let owner = ctx.shard_owner(shard);
        let enq = now();
        let permit = match routed.take() {
            Some((pod, workers)) => RunPermit::Exec(workers.acquire().await, pod),
            None => {
                let sched = {
                    let pods = ctx.pods.borrow();
                    pods[owner]
                        .hist
                        .as_ref()
                        .map(|h| h.schedulers[c.idx()].clone())
                };
                let Some(sched) = sched else { break };
                if ctx.p.k.task_sched_enabled {
                    wait_for_scheduler_limiter(&ctx, owner, &task, prio, attempt).await;
                }
                let p = sched.acquire(channel, weight).await;
                match exec_queue(&ctx, owner, c, exec_key, false) {
                    Some(workers) => {
                        drop(p);
                        RunPermit::Exec(workers.acquire().await, owner)
                    }
                    None => RunPermit::Pool(p),
                }
            }
        };
        let start = now();
        if attempt == 1 {
            ctx.m.borrow_mut().tasks[task.kind.idx()]
                .schedule_latency
                .record(start - enq);
        }
        let outcome = execute(&ctx, owner, shard, &task).await;
        match permit {
            RunPermit::Pool(p) => drop(p),
            RunPermit::Exec(p, pod) => {
                drop(p);
                if let Some(h) = ctx.pods.borrow_mut()[pod].hist.as_mut() {
                    h.exec_queues[c.idx()].done(exec_key, now());
                }
                ctx.m.borrow_mut().tasks[task.kind.idx()].exec_queue_runs += 1;
            }
        }
        let proc_time = now() - start;
        {
            let mut m = ctx.m.borrow_mut();
            let ts = &mut m.tasks[task.kind.idx()];
            ts.processing.record(proc_time);
            m.task_pod(owner);
        }
        let e = match outcome {
            Outcome::Done | Outcome::Noop | Outcome::Drop => {
                let mut m = ctx.m.borrow_mut();
                let ts = &mut m.tasks[task.kind.idx()];
                ts.count += 1;
                if matches!(outcome, Outcome::Noop) {
                    ts.noop += 1;
                }
                ts.queue_latency.record(now().saturating_sub(task.fire_at));
                ts.attempts.record(u64::from(attempt));
                break;
            }
            Outcome::Retry(e) => e,
        };
        // HandleErr
        attempt += 1;
        let busy = matches!(e, Err::ResourceExhausted(ReCause::BusyWorkflow, _));
        let throttled = e.is_resource_exhausted() && !busy;
        {
            let mut m = ctx.m.borrow_mut();
            let ts = &mut m.tasks[task.kind.idx()];
            match e {
                _ if busy => ts.busy_errors += 1,
                Err::ResourceExhausted(cause, _) => {
                    ts.throttled_errors += 1;
                    *ts.throttled_by.entry(cause).or_default() += 1;
                }
                _ => ts.other_errors += 1,
            }
        }
        if throttled {
            throttles += 1;
        } else if !busy {
            throttles = 0;
        }
        if attempt > 200 {
            break;
        }
        // Nack
        if busy
            && ctx.p.k.eqs_enabled
            && let Some(workers) = exec_queue(&ctx, owner, c, exec_key, true)
        {
            routed = Some((owner, workers));
            continue;
        }
        if resubmits(attempt, throttles, e) {
            continue;
        }
        let mut delay = backoff(&ctx, 1_000_000, 1.1, 180_000_000, attempt);
        if throttled {
            delay = delay.max(backoff(&ctx, 3_000_000, 1.5, 300_000_000, throttles));
        }
        sleep(delay).await;
    }
    // completion bookkeeping & checkpointing
    let checkpoint = {
        let mut shards = ctx.shards.borrow_mut();
        let s = &mut shards[(shard - 1) as usize];
        let q = &mut s.queues[c.idx()];
        q.pending = q.pending.saturating_sub(1);
        q.completed_since_ack += 1;
        s.tasks_completed_since_update += 1;
        let t = now();
        if t.saturating_sub(q.last_ack) >= ctx.p.k.ack_interval[c.idx()] {
            q.last_ack = t;
            q.completed_since_ack = 0;
            true
        } else {
            false
        }
    };
    if checkpoint {
        checkpoint_shard(&ctx, shard, c).await;
    }
}

/// `shouldResubmitOnNack`: whether a task that failed with `e`, now at `attempt` (incremented by
/// the failure) and `throttles` throttling errors in a row, is resubmitted at once rather than
/// backed off. Up to attempt 10 it is, except that throttling allows one immediate resubmit and
/// a lost shard none.
fn resubmits(attempt: u32, throttles: u32, e: Err) -> bool {
    let throttled =
        e.is_resource_exhausted() && !matches!(e, Err::ResourceExhausted(ReCause::BusyWorkflow, _));
    attempt <= 10 && !(throttled && throttles > 1) && e != Err::ShardOwnershipLost
}

/// The execution queue of workflow `key` on `pod`'s `c` scheduler, when the execution queue
/// scheduler is on: an existing queue takes every task of its workflow; `create` (a
/// busy-workflow failure) also opens one, unless `MaxQueues` queues exist.
fn exec_queue(
    ctx: &Ctx,
    pod: PodId,
    c: Category,
    key: (WfId, u32),
    create: bool,
) -> Option<Semaphore> {
    let k = &ctx.p.k;
    if !k.eqs_enabled {
        return None;
    }
    let mut pods = ctx.pods.borrow_mut();
    let h = pods[pod].hist.as_mut()?;
    h.exec_queues[c.idx()].submit(
        key,
        create,
        now(),
        k.eqs_queue_ttl,
        k.eqs_max_queues,
        k.eqs_queue_concurrency,
    )
}

/// The task scheduler's rate limiter, which the queue reader meets in `TrySubmit`
/// (`common/tasks/rate_limited_scheduler.go`). A refused task counts as
/// `task_scheduler_throttled`. In shadow mode it runs anyway. Otherwise it goes to the
/// rescheduler: first for the task backoff (1s × 1.1ⁿ, up to 20% less), then every 2s ± 50%
/// (`taskChanFullBackoff`) until the limiter admits it.
async fn wait_for_scheduler_limiter(
    ctx: &Ctx,
    pod: PodId,
    task: &HistTask,
    prio: Prio,
    attempt: u32,
) {
    let k = &ctx.p.k;
    let level = match prio {
        Prio::High => 0,
        Prio::Low => 1,
    };
    let ns = ctx
        .wfs
        .borrow()
        .get(task.wf, task.wf_gen)
        .map_or(0, |w| w.ns);
    let mut refusals = 0u32;
    loop {
        let admitted = {
            let mut pods = ctx.pods.borrow_mut();
            let Some(h) = pods[pod].hist.as_mut() else {
                return;
            };
            if now() < h.started_at + k.task_sched_startup_delay {
                return;
            }
            let ok = h.sched_limiter.allow(level, ns);
            if !ok {
                h.sched_throttled += 1;
            }
            ok
        };
        if admitted {
            return;
        }
        ctx.m.borrow_mut().tasks[task.kind.idx()].sched_throttled += 1;
        if k.task_sched_shadow {
            return;
        }
        let delay = if refusals == 0 {
            1e6 * 1.1f64.powi(attempt as i32 - 1) * (0.8 + 0.2 * ctx.rand())
        } else {
            2e6 * (0.5 + ctx.rand())
        };
        refusals += 1;
        sleep(delay as Time).await;
    }
}

async fn checkpoint_shard(ctx: &Ctx, shard: ShardId, c: Category) {
    let owner = ctx.shard_owner(shard);
    let _ = persist(ctx, owner, c.range_complete_op(), Caller::ShardMgmt, None).await;
    let update = {
        let mut shards = ctx.shards.borrow_mut();
        let s = &mut shards[(shard - 1) as usize];
        let t = now();
        if t.saturating_sub(s.last_shard_update) >= ctx.p.k.shard_update_min_interval
            || (ctx.p.k.shard_update_min_tasks > 0
                && s.tasks_completed_since_update >= ctx.p.k.shard_update_min_tasks)
        {
            s.last_shard_update = t;
            s.tasks_completed_since_update = 0;
            true
        } else {
            false
        }
    };
    if update {
        let deadline = now() + 5_000_000;
        let _ = shard_write(
            ctx,
            owner,
            shard,
            PersistOp::UpdateShard,
            0.0,
            Caller::ShardMgmt,
            deadline,
        )
        .await;
    }
}

/// Task executors call persistence as their namespace, at the task's priority
/// (`executable.go`: `NewBackgroundHighCallerInfo(ns)` / `NewBackgroundLowCallerInfo(ns)`).
fn caller_for(ctx: &Ctx, task: &HistTask) -> Caller {
    let ns = ctx.wf_ns(task.wf, task.wf_gen);
    if task.kind.low_priority() {
        Caller::BackgroundLow(ns)
    } else {
        Caller::BackgroundHigh(ns)
    }
}

/// Execute a task once. Returns how it ended.
async fn execute(ctx: &Ctx, pod: PodId, shard: ShardId, task: &HistTask) -> Outcome {
    let caller = caller_for(ctx, task);
    let (wf, wgen) = (task.wf, task.wf_gen);
    // workflow gone (closed & released) -> nothing to do
    if ctx.wfs.borrow().get(wf, wgen).is_none() {
        return Outcome::Drop;
    }
    if ctx.shard_owner(shard) != pod {
        return Outcome::Retry(Err::ShardOwnershipLost);
    }
    let base_cost = ctx.p.costs.task[task.kind.idx()];
    let deadline = now() + 3_000_000; // transfer executor timeout 3s (applied to all)
    match task.kind {
        TaskType::TransferWorkflowTask => {
            cpu(ctx, pod, base_cost).await;
            let lock = match lock_wf(ctx, wf, wgen, caller, deadline).await {
                Ok(l) => l,
                Err(e) => return Outcome::Retry(e),
            };
            if let Err(e) = load_ms(ctx, pod, shard, wf, wgen, caller).await {
                return Outcome::Retry(e);
            }
            let dispatch = {
                let wfs = ctx.wfs.borrow();
                let Some(w) = wfs.get(wf, wgen) else {
                    return Outcome::Drop;
                };
                match w.wft {
                    WftState::Scheduled { seq, sticky, .. }
                        if seq == task.r && w.status == WfStatus::Running =>
                    {
                        Some((
                            sticky.then_some(w.sticky_worker).flatten(),
                            ctx.p.wf_types[w.wf_type].tq,
                        ))
                    }
                    _ => None,
                }
            };
            drop(lock);
            let Some((sticky_worker, tq)) = dispatch else {
                return Outcome::Noop;
            };
            let mt = MTask {
                wf,
                wf_gen: wgen,
                kind: TqKind::Workflow,
                r: task.r,
                r2: 0,
                created: now(),
                from_backlog: false,
                query: false,
            };
            let r =
                call_with_timeout(3_000_000, {
                    let c = ctx.clone();
                    async move {
                        matching::add_task(&c, pod, tq, TqKind::Workflow, mt, sticky_worker).await
                    }
                })
                .await;
            match r {
                Ok(()) => Outcome::Done,
                Err(Err::StickyWorkerUnavailable) => {
                    // fall back to the normal queue immediately
                    {
                        let mut wfs = ctx.wfs.borrow_mut();
                        if let Some(w) = wfs.get_mut(wf, wgen) {
                            if let WftState::Scheduled {
                                seq, attempt, at, ..
                            } = w.wft
                            {
                                w.wft = WftState::Scheduled {
                                    seq,
                                    attempt,
                                    sticky: false,
                                    at,
                                };
                            }
                            w.sticky_worker = None;
                        }
                    }
                    let wf_type = ctx
                        .wfs
                        .borrow()
                        .get(wf, wgen)
                        .map(|w| w.wf_type)
                        .unwrap_or(0);
                    ctx.m.borrow_mut().wf[wf_type].sticky_unavailable += 1;
                    match matching::add_task(ctx, pod, tq, TqKind::Workflow, mt, None).await {
                        Ok(()) => Outcome::Done,
                        Err(e) => Outcome::Retry(e),
                    }
                }
                Err(e) => Outcome::Retry(e),
            }
        }
        TaskType::TransferActivityTask => {
            cpu(ctx, pod, base_cost).await;
            let lock = match lock_wf(ctx, wf, wgen, caller, deadline).await {
                Ok(l) => l,
                Err(e) => return Outcome::Retry(e),
            };
            if let Err(e) = load_ms(ctx, pod, shard, wf, wgen, caller).await {
                return Outcome::Retry(e);
            }
            let tq = {
                let wfs = ctx.wfs.borrow();
                let Some(w) = wfs.get(wf, wgen) else {
                    return Outcome::Drop;
                };
                if w.status != WfStatus::Running {
                    None
                } else {
                    w.activities
                        .iter()
                        .find(|a| {
                            a.seq == task.r
                                && a.attempt == task.r2
                                && a.state == ActState::Scheduled
                        })
                        .map(|a| a.tq)
                }
            };
            drop(lock);
            let Some(tq) = tq else { return Outcome::Noop };
            push_activity(ctx, pod, task, tq).await
        }
        TaskType::TransferCloseExecution => {
            cpu(ctx, pod, base_cost).await;
            let lock = match lock_wf(ctx, wf, wgen, caller, deadline).await {
                Ok(l) => l,
                Err(e) => return Outcome::Retry(e),
            };
            if let Err(e) = load_ms(ctx, pod, shard, wf, wgen, caller).await {
                return Outcome::Retry(e);
            }
            let (parent, key) = ctx
                .wfs
                .borrow()
                .get(wf, wgen)
                .map_or((None, 0), |w| (w.parent, w.key));
            // a child reads its close event, with the result it reports to the parent, through
            // the events cache (`GetCompletionEvent`)
            let result = f64::from(task.r);
            if parent.is_some()
                && let Err(e) = history::get_event(
                    ctx,
                    pod,
                    shard,
                    history::event_key(key, Cached::Closed, 0),
                    EVENT_BYTES + result,
                    task.bytes,
                    caller,
                )
                .await
            {
                return Outcome::Retry(e);
            }
            drop(lock);
            let out = if let Some((pw, pg)) = parent {
                let Some(pshard) = ctx.wf_shard(pw, pg) else {
                    return Outcome::Done;
                };
                let r = history_call(ctx, pshard, |c, hp| {
                    let c = c.clone();
                    async move {
                        record_child_completed(&c, hp, pw, pg, result, now() + 3_000_000).await
                    }
                })
                .await;
                match r {
                    Ok(()) | Err(Err::NotFound) => Outcome::Done,
                    Err(e) => Outcome::Retry(e),
                }
            } else {
                Outcome::Done
            };
            if matches!(out, Outcome::Done) {
                // no further tasks reference this execution: free the slot
                release_later(ctx, wf, wgen);
            }
            out
        }
        TaskType::TransferStartChildExecution => {
            cpu(ctx, pod, base_cost).await;
            let lock = match lock_wf(ctx, wf, wgen, caller, deadline).await {
                Ok(l) => l,
                Err(e) => return Outcome::Retry(e),
            };
            if let Err(e) = load_ms(ctx, pod, shard, wf, wgen, caller).await {
                return Outcome::Retry(e);
            }
            let (running, key, payload) =
                ctx.wfs.borrow().get(wf, wgen).map_or((false, 0, 0.0), |w| {
                    (
                        w.status == WfStatus::Running,
                        w.key,
                        ctx.p.wf_types[w.wf_type].payload_bytes,
                    )
                });
            if !running {
                return Outcome::Noop;
            }
            // the initiated event, with the child's input, through the events cache
            // (`GetChildExecutionInitiatedEvent`)
            if let Err(e) = history::get_event(
                ctx,
                pod,
                shard,
                history::event_key(key, Cached::ChildInitiated, task.r2),
                EVENT_BYTES + payload,
                task.bytes,
                caller,
            )
            .await
            {
                return Outcome::Retry(e);
            }
            drop(lock);
            let child_type = task.r as usize;
            let key = history::alloc_key(ctx);
            let ns = ctx.p.wf_types[child_type].ns;
            let cshard = history::shard_for(ctx, ns, child_type, key);
            let r = history_call(ctx, cshard, |c, hp| {
                let c = c.clone();
                async move {
                    start_workflow(
                        &c,
                        hp,
                        cshard,
                        key,
                        child_type,
                        StartOrigin::Child {
                            parent: wf,
                            parent_gen: wgen,
                        },
                        false,
                        now() + 3_000_000,
                    )
                    .await
                }
            })
            .await;
            match r {
                Ok(_) => {
                    // ChildWorkflowExecutionStarted recorded on the parent (another write)
                    let lock = match lock_wf(ctx, wf, wgen, caller, now() + 3_000_000).await {
                        Ok(l) => l,
                        Err(_) => return Outcome::Done,
                    };
                    let started = Append::new(1, 0.0);
                    let _ = shard_write(
                        ctx,
                        pod,
                        shard,
                        PersistOp::UpdateWorkflowExecution,
                        started.bytes(),
                        caller,
                        now() + 3_000_000,
                    )
                    .await;
                    if let Some(w) = ctx.wfs.borrow_mut().get_mut(wf, wgen) {
                        w.grow(started);
                    }
                    drop(lock);
                    Outcome::Done
                }
                Err(e) => Outcome::Retry(e),
            }
        }
        TaskType::TimerWorkflowTaskTimeout => {
            let lock = match lock_wf(ctx, wf, wgen, caller, deadline).await {
                Ok(l) => l,
                Err(e) => return Outcome::Retry(e),
            };
            if let Err(e) = load_ms(ctx, pod, shard, wf, wgen, caller).await {
                return Outcome::Retry(e);
            }
            // r = wft seq, r2 = 0 for sticky schedule-to-start, attempt for start-to-close
            let timed_out = {
                let wfs = ctx.wfs.borrow();
                let Some(w) = wfs.get(wf, wgen) else {
                    return Outcome::Drop;
                };
                if w.status != WfStatus::Running {
                    false
                } else {
                    match w.wft {
                        WftState::Started { seq, attempt, .. } => {
                            task.r2 != 0 && seq == task.r && attempt == task.r2
                        }
                        WftState::Scheduled { seq, sticky, .. } => {
                            task.r2 == 0 && sticky && seq == task.r
                        }
                        WftState::None => false,
                    }
                }
            };
            if !timed_out {
                drop(lock);
                cpu(ctx, pod, ctx.p.costs.task_noop).await;
                return Outcome::Noop;
            }
            cpu(ctx, pod, base_cost).await;
            // WorkflowTaskTimedOut and the next WorkflowTaskScheduled
            let timed_out = Append::new(2, 0.0);
            if let Err(e) = shard_write(
                ctx,
                pod,
                shard,
                PersistOp::UpdateWorkflowExecution,
                timed_out.bytes(),
                caller,
                deadline,
            )
            .await
            {
                evict_ms(ctx, pod, shard, wf, wgen);
                return Outcome::Retry(e);
            }
            let t = now();
            let mut tasks = Vec::new();
            let wf_type = {
                let mut wfs = ctx.wfs.borrow_mut();
                let Some(w) = wfs.get_mut(wf, wgen) else {
                    return Outcome::Drop;
                };
                let attempt = match w.wft {
                    WftState::Started { attempt, .. } | WftState::Scheduled { attempt, .. } => {
                        attempt
                    }
                    WftState::None => 1,
                };
                // timed out: clear stickiness, reschedule on the normal queue
                w.sticky_worker = None;
                w.wft_seq += 1;
                w.wft = WftState::Scheduled {
                    seq: w.wft_seq,
                    attempt: attempt + 1,
                    sticky: false,
                    at: t,
                };
                w.grow(timed_out);
                tasks.push(TaskSpec::now(TaskType::TransferWorkflowTask, w.wft_seq, 0));
                w.wf_type
            };
            commit_tasks(ctx, shard, wf, wgen, &tasks);
            drop(lock);
            ctx.m.borrow_mut().wf[wf_type].wft_timeouts += 1;
            Outcome::Done
        }
        TaskType::TimerActivityTimeout => {
            // executeActivityTimeoutTask: process every expired activity timeout
            let lock = match lock_wf(ctx, wf, wgen, caller, deadline).await {
                Ok(l) => l,
                Err(e) => return Outcome::Retry(e),
            };
            if let Err(e) = load_ms(ctx, pod, shard, wf, wgen, caller).await {
                return Outcome::Retry(e);
            }
            let plan = {
                let wfs = ctx.wfs.borrow();
                let Some(w) = wfs.get(wf, wgen) else {
                    return Outcome::Drop;
                };
                if w.status != WfStatus::Running {
                    None
                } else {
                    activity::plan_timeouts(ctx, w, task.r, task.r2, task.fire_at, now())
                }
            };
            let Some(plan) = plan else {
                // errNoTimerFired: stale, or a heartbeat that already moved on
                drop(lock);
                cpu(ctx, pod, ctx.p.costs.task_noop).await;
                return Outcome::Noop;
            };
            cpu(ctx, pod, base_cost).await;
            // a failed activity adds ActivityTaskTimedOut (and a workflow task) to history
            let events = !plan.failed_steps.is_empty();
            let timed_out = Append::new(plan.failed_steps.len() as u32, 0.0);
            if let Err(e) = shard_write(
                ctx,
                pod,
                shard,
                PersistOp::UpdateWorkflowExecution,
                timed_out.bytes(),
                caller,
                deadline,
            )
            .await
            {
                evict_ms(ctx, pod, shard, wf, wgen);
                return Outcome::Retry(e);
            }
            let t = now();
            let mut tasks = plan.tasks;
            let wf_type = {
                let mut wfs = ctx.wfs.borrow_mut();
                let Some(w) = wfs.get_mut(wf, wgen) else {
                    return Outcome::Drop;
                };
                w.activities = plan.activities;
                for &(step, member) in &plan.failed_steps {
                    if step == w.step {
                        // a step that goes on after a failure counts the activity as done
                        if ctx.p.wf_types[w.wf_type].fails_workflow(step, member) {
                            w.failed_in_step += 1;
                        } else {
                            w.completed_in_step += 1;
                        }
                    }
                }
                w.grow(timed_out);
                if events {
                    history::deliver_event(ctx, w, t, &mut tasks);
                }
                activity::create_next_timer(ctx, w, &mut tasks);
                w.wf_type
            };
            commit_tasks(ctx, shard, wf, wgen, &tasks);
            drop(lock);
            let mut m = ctx.m.borrow_mut();
            let ws = &mut m.wf[wf_type];
            for (n, fired) in ws.activity_timeouts.iter_mut().zip(plan.fired) {
                *n += fired;
            }
            ws.activities_failed += plan.failed_steps.len() as u64;
            Outcome::Done
        }
        TaskType::TimerUserTimer => {
            let lock = match lock_wf(ctx, wf, wgen, caller, deadline).await {
                Ok(l) => l,
                Err(e) => return Outcome::Retry(e),
            };
            if let Err(e) = load_ms(ctx, pod, shard, wf, wgen, caller).await {
                return Outcome::Retry(e);
            }
            let fire = {
                let wfs = ctx.wfs.borrow();
                let Some(w) = wfs.get(wf, wgen) else {
                    return Outcome::Drop;
                };
                w.status == WfStatus::Running && w.timer_pending == Some(task.r)
            };
            if !fire {
                drop(lock);
                cpu(ctx, pod, ctx.p.costs.task_noop).await;
                return Outcome::Noop;
            }
            cpu(ctx, pod, base_cost).await;
            let fired = Append::new(1, 0.0);
            if let Err(e) = shard_write(
                ctx,
                pod,
                shard,
                PersistOp::UpdateWorkflowExecution,
                fired.bytes(),
                caller,
                deadline,
            )
            .await
            {
                evict_ms(ctx, pod, shard, wf, wgen);
                return Outcome::Retry(e);
            }
            let t = now();
            let mut tasks = Vec::new();
            {
                let mut wfs = ctx.wfs.borrow_mut();
                let Some(w) = wfs.get_mut(wf, wgen) else {
                    return Outcome::Drop;
                };
                w.timer_pending = None;
                w.timer_fired = true;
                w.grow(fired);
                if let Some(s) = w.schedule.as_mut() {
                    // scheduler workflow timer: an action becomes due at each nominal fire time
                    // (rate-limit retry timers don't add actions)
                    let interval = ctx.p.schedules[s.sched].interval;
                    while s.next_fire <= t + 1_000 {
                        s.due_actions += 1;
                        s.next_fire += interval;
                    }
                }
                history::deliver_event(ctx, w, t, &mut tasks);
            }
            commit_tasks(ctx, shard, wf, wgen, &tasks);
            drop(lock);
            Outcome::Done
        }
        TaskType::TimerActivityRetryTimer => {
            // executeActivityRetryTimerTask: the failure's write already recorded the next
            // attempt, so the timer only pushes it to matching. It writes no mutable state and
            // creates no transfer task.
            let lock = match lock_wf(ctx, wf, wgen, caller, deadline).await {
                Ok(l) => l,
                Err(e) => return Outcome::Retry(e),
            };
            if let Err(e) = load_ms(ctx, pod, shard, wf, wgen, caller).await {
                return Outcome::Retry(e);
            }
            let tq = {
                let mut wfs = ctx.wfs.borrow_mut();
                let Some(w) = wfs.get_mut(wf, wgen) else {
                    return Outcome::Drop;
                };
                if w.status != WfStatus::Running {
                    None
                } else {
                    // not started yet: in backoff, or scheduled by a push that failed
                    w.activities
                        .iter_mut()
                        .find(|a| {
                            a.seq == task.r
                                && a.attempt == task.r2
                                && matches!(a.state, ActState::Backoff | ActState::Scheduled)
                        })
                        .map(|a| {
                            a.state = ActState::Scheduled;
                            // the attempt's scheduled time is when the retry was due
                            // (updateActivityInfoForRetries), not when this timer task ran
                            a.scheduled_at = task.fire_at;
                            a.tq
                        })
                }
            };
            drop(lock);
            let Some(tq) = tq else { return Outcome::Noop };
            cpu(ctx, pod, base_cost).await;
            push_activity(ctx, pod, task, tq).await
        }
        TaskType::VisibilityStartExecution
        | TaskType::VisibilityUpsertExecution
        | TaskType::VisibilityCloseExecution => {
            cpu(ctx, pod, base_cost).await;
            let lock = match lock_wf(ctx, wf, wgen, caller, deadline).await {
                Ok(l) => l,
                Err(e) => return Outcome::Retry(e),
            };
            if let Err(e) = load_ms(ctx, pod, shard, wf, wgen, caller).await {
                return Outcome::Retry(e);
            }
            drop(lock);
            let op = match task.kind {
                TaskType::VisibilityStartExecution => PersistOp::RecordWorkflowExecutionStarted,
                TaskType::VisibilityUpsertExecution => PersistOp::UpsertWorkflowExecution,
                _ => PersistOp::RecordWorkflowExecutionClosed,
            };
            match vis_write(ctx, pod, op).await {
                Ok(()) => Outcome::Done,
                Err(e) => Outcome::Retry(e),
            }
        }
    }
}

/// Free a closed workflow's slot a while after it closed. A timer of the closed workflow that
/// fires before then finds it closed and does nothing; one that fires later fails the
/// generation check and is dropped, so both are harmless.
fn release_later(ctx: &Ctx, wf: WfId, wgen: u32) {
    let c = ctx.clone();
    spawn(async move {
        sleep(3_700_000_000).await; // past the default activity start-to-close ceiling (1h)
        let ok = c
            .wfs
            .borrow()
            .get(wf, wgen)
            .map(|w| w.status == WfStatus::Closed)
            .unwrap_or(false);
        if ok {
            c.wfs.borrow_mut().release(wf);
        }
    });
}

/// `AddActivityTask` to matching for the activity attempt a transfer or retry timer task refers
/// to (`r` is the activity, `r2` the attempt). A failure retries the history task.
async fn push_activity(ctx: &Ctx, pod: PodId, task: &HistTask, tq: usize) -> Outcome {
    let mt = MTask {
        wf: task.wf,
        wf_gen: task.wf_gen,
        kind: TqKind::Activity,
        r: task.r,
        r2: task.r2,
        created: now(),
        from_backlog: false,
        query: false,
    };
    let r = call_with_timeout(3_000_000, {
        let c = ctx.clone();
        async move { matching::add_task(&c, pod, tq, TqKind::Activity, mt, None).await }
    })
    .await;
    match r {
        Ok(()) => Outcome::Done,
        Err(e) => Outcome::Retry(e),
    }
}

/// Call a history API on the current owner of `shard` from another Temporal service, following
/// ShardOwnershipLost redirects and retrying once on system-scoped ResourceExhausted (the
/// internal history client policy).
pub async fn history_call<T, F, Fut>(ctx: &Ctx, shard: ShardId, f: F) -> Res<T>
where
    F: Fn(&Ctx, PodId) -> Fut,
    Fut: std::future::Future<Output = Res<T>>,
{
    let mut attempts = 0u32;
    let mut redirects = 0u32;
    loop {
        let owner = ctx.shard_owner(shard);
        hop(ctx).await;
        let r = f(ctx, owner).await;
        hop(ctx).await;
        match r {
            Err(Err::ShardOwnershipLost) if redirects < 20 => {
                redirects += 1;
                sleep(50_000).await;
            }
            Err(Err::ResourceExhausted(c, Scope::System)) if attempts < 1 => {
                attempts += 1;
                let _ = c;
                sleep(backoff(ctx, 1_000_000, 2.0, 10_000_000, 1)).await;
            }
            Err(Err::Unavailable) if attempts < 1 => {
                attempts += 1;
                sleep(50_000).await;
            }
            other => return other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resubmit_follows_executable_go() {
        let busy = Err::ResourceExhausted(ReCause::BusyWorkflow, Scope::Namespace);
        let throttle = Err::ResourceExhausted(ReCause::PersistenceLimit, Scope::System);
        // busy workflow: resubmitted until the tenth attempt (the first run is attempt 1, and
        // each failure increments it before the decision)
        assert!(resubmits(2, 0, busy));
        assert!(resubmits(10, 0, busy));
        assert!(!resubmits(11, 0, busy));
        // throttling: one immediate resubmit, then backoff
        assert!(resubmits(2, 1, throttle));
        assert!(!resubmits(3, 2, throttle));
        // other errors: like busy workflow; a lost shard never
        assert!(resubmits(5, 0, Err::Unavailable));
        assert!(!resubmits(2, 0, Err::ShardOwnershipLost));
    }
}
