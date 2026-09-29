//! History task queues (transfer, timer, visibility) — `service/history/queues`.
//!
//! * Tasks are written with the workflow transaction and later *re-read* from the database by a
//!   per-shard reader (`GetTransferTasks` / `GetTimerTasks` / `GetVisibilityTasks`, batch
//!   `history.*TaskBatchSize`, rate limited per shard by `history.*ProcessorMaxPollRPS` and per
//!   host by `history.*ProcessorMaxPollHostRPS`), paused when a shard has
//!   `history.queuePendingTasksMaxCount` tasks loaded.
//! * Loaded tasks go to the host-level scheduler (`history.*ProcessorSchedulerWorkerCount`
//!   workers, High before Low priority).
//! * Execution takes the workflow lock as a non-API caller (≤ `cacheNonUserContextLockTimeout`),
//!   loads mutable state and performs the task; busy-workflow failures are resubmitted
//!   immediately up to 10 attempts then backed off 1s·1.1^n, throttling errors back off
//!   max(1s·1.1^n, 3s·1.5^(n-1)).
//! * Every `history.*ProcessorUpdateAckInterval` a shard checkpoints: RangeCompleteHistoryTasks,
//!   plus UpdateShard at most every `history.shardUpdateMinInterval` / 1000 tasks.

use crate::sim::executor::{Time, now, sleep, sleep_until, spawn};
use crate::sim::sync::Prio;

use super::history::{self, StartOrigin, record_child_completed, start_workflow};
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
}

impl TaskSpec {
    pub fn now(kind: TaskType, r: u32, r2: u32) -> Self {
        TaskSpec {
            kind,
            fire_at: 0,
            r,
            r2,
        }
    }
    pub fn at(kind: TaskType, fire_at: Time, r: u32, r2: u32) -> Self {
        TaskSpec {
            kind,
            fire_at,
            r,
            r2,
        }
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
        if persist(&ctx, owner, c.load_op(), Caller::QueueLoad)
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
        if persist(&ctx, owner, PersistOp::GetTimerTasks, Caller::QueueLoad)
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

/// Schedule and execute one history task with Temporal's retry policy.
async fn run_task(ctx: Ctx, shard: ShardId, task: HistTask, loaded_at: Time) {
    let c = task.kind.category();
    let mut attempt: u32 = 1;
    {
        let mut m = ctx.m.borrow_mut();
        m.tasks[task.kind.idx()]
            .load_latency
            .record(loaded_at.saturating_sub(task.fire_at));
    }
    loop {
        let owner = ctx.shard_owner(shard);
        let sched = {
            let pods = ctx.pods.borrow();
            pods[owner]
                .hist
                .as_ref()
                .map(|h| h.schedulers[c.idx()].clone())
        };
        let Some(sched) = sched else { break };
        let enq = now();
        let prio = if task.kind.low_priority() {
            Prio::Low
        } else {
            Prio::High
        };
        if ctx.p.k.task_sched_enabled {
            wait_for_scheduler_limiter(&ctx, owner, &task, prio, attempt).await;
        }
        let permit = sched.acquire_prio(prio).await;
        let start = now();
        if attempt == 1 {
            ctx.m.borrow_mut().tasks[task.kind.idx()]
                .schedule_latency
                .record(start - enq);
        }
        let outcome = execute(&ctx, owner, shard, &task).await;
        drop(permit);
        let proc_time = now() - start;
        {
            let mut m = ctx.m.borrow_mut();
            let ts = &mut m.tasks[task.kind.idx()];
            ts.processing.record(proc_time);
            m.task_pod(owner);
        }
        match outcome {
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
            Outcome::Retry(e) => {
                let delay = {
                    let mut m = ctx.m.borrow_mut();
                    let ts = &mut m.tasks[task.kind.idx()];
                    match e {
                        Err::ResourceExhausted(ReCause::BusyWorkflow, _) => {
                            ts.busy_errors += 1;
                            if attempt <= 10 {
                                0
                            } else {
                                backoff(&ctx, 1_000_000, 1.1, 180_000_000, attempt - 10)
                            }
                        }
                        Err::ResourceExhausted(cause, _) => {
                            ts.throttled_errors += 1;
                            *ts.throttled_by.entry(cause).or_default() += 1;
                            backoff(&ctx, 1_000_000, 1.1, 180_000_000, attempt).max(backoff(
                                &ctx,
                                3_000_000,
                                1.5,
                                300_000_000,
                                attempt,
                            ))
                        }
                        _ => {
                            ts.other_errors += 1;
                            if attempt <= 1 {
                                0
                            } else {
                                backoff(&ctx, 1_000_000, 1.1, 180_000_000, attempt)
                            }
                        }
                    }
                };
                attempt += 1;
                if delay > 0 {
                    sleep(delay).await;
                }
                if attempt > 200 {
                    break;
                }
            }
        }
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
    let _ = persist(ctx, owner, c.range_complete_op(), Caller::ShardMgmt).await;
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
            false,
            Caller::ShardMgmt,
            deadline,
        )
        .await;
    }
}

fn caller_for(task: &HistTask) -> Caller {
    if task.kind.low_priority() {
        Caller::BackgroundLow
    } else {
        Caller::BackgroundHigh
    }
}

/// Execute a task once. Returns how it ended.
async fn execute(ctx: &Ctx, pod: PodId, shard: ShardId, task: &HistTask) -> Outcome {
    let caller = caller_for(task);
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
            let parent = ctx.wfs.borrow().get(wf, wgen).and_then(|w| w.parent);
            drop(lock);
            let out = if let Some((pw, pg)) = parent {
                let Some(pshard) = ctx.wf_shard(pw, pg) else {
                    return Outcome::Done;
                };
                let r = history_call(ctx, pshard, |c, hp| {
                    let c = c.clone();
                    async move { record_child_completed(&c, hp, pw, pg, now() + 3_000_000).await }
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
            let running = ctx
                .wfs
                .borrow()
                .get(wf, wgen)
                .map(|w| w.status == WfStatus::Running)
                .unwrap_or(false);
            drop(lock);
            if !running {
                return Outcome::Noop;
            }
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
                    let _ = shard_write(
                        ctx,
                        pod,
                        shard,
                        PersistOp::UpdateWorkflowExecution,
                        true,
                        caller,
                        now() + 3_000_000,
                    )
                    .await;
                    if let Some(w) = ctx.wfs.borrow_mut().get_mut(wf, wgen) {
                        w.history_events += 1;
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
            if let Err(e) = shard_write(
                ctx,
                pod,
                shard,
                PersistOp::UpdateWorkflowExecution,
                true,
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
                w.history_events += 2;
                tasks.push(TaskSpec::now(TaskType::TransferWorkflowTask, w.wft_seq, 0));
                w.wf_type
            };
            commit_tasks(ctx, shard, wf, wgen, &tasks);
            drop(lock);
            ctx.m.borrow_mut().wf[wf_type].wft_timeouts += 1;
            Outcome::Done
        }
        TaskType::TimerActivityTimeout => {
            let lock = match lock_wf(ctx, wf, wgen, caller, deadline).await {
                Ok(l) => l,
                Err(e) => return Outcome::Retry(e),
            };
            if let Err(e) = load_ms(ctx, pod, shard, wf, wgen, caller).await {
                return Outcome::Retry(e);
            }
            drop(lock);
            cpu(ctx, pod, ctx.p.costs.task_noop).await;
            Outcome::Noop
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
            if let Err(e) = shard_write(
                ctx,
                pod,
                shard,
                PersistOp::UpdateWorkflowExecution,
                true,
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
                w.history_events += 1;
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

/// Free a closed workflow's slot once its outstanding timers can no longer matter (after the
/// longest timer horizon we generate).
fn release_later(ctx: &Ctx, wf: WfId, wgen: u32) {
    let c = ctx.clone();
    spawn(async move {
        sleep(3_700_000_000).await; // > max activity timeout (1h) + margin
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
