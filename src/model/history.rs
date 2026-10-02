//! History service API handlers (Temporal 1.31.0 `service/history/api/*`).
//!
//! Each handler follows the real sequence: admission (`history.rps`) → CPU → shard ownership →
//! workflow lock (API callers High priority) → mutable state via the host cache (miss =
//! GetWorkflowExecution) → validation → persistence write under the shard IO semaphore
//! (AppendHistoryNodes + Update/CreateWorkflowExecution) → task generation → lock release →
//! post-lock reads (e.g. ReadHistoryBranch for workflow task history).
//!
//! State changes are computed first, persisted, and applied only on success, so a throttled or
//! timed-out write leaves the workflow unchanged (Temporal clears the mutable state instead).

use crate::config::scenario::StartWith;
use crate::sim::executor::{Receiver, Time, now, oneshot, spawn};
use crate::util::farmhash::workflow_id_to_history_shard;

use super::activity;
use super::infra::*;
use super::queues::{TaskSpec, commit_tasks};
use super::types::*;
use super::world::*;

/// SDK-side workflow program position (persisted with the workflow task completion).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ProgState {
    pub step: usize,
    pub step_started: bool,
    pub step_scheduled: u32,
    pub signals_consumed: u32,
    pub done: bool,
}

/// Server-side event counters visible to a workflow task (snapshotted at WFT start).
#[derive(Clone, Copy, Debug, Default)]
pub struct Snapshot {
    pub completed_in_step: u32,
    /// activities of the step that failed for good and fail the workflow
    pub failed_in_step: u32,
    pub timer_fired: bool,
    pub children_done: u32,
    pub signals_received: u32,
    pub due_actions: u32,
}

/// Information returned to the poller for a started workflow task.
#[derive(Clone, Copy, Debug)]
pub struct WftInfo {
    pub wf: WfId,
    pub wgen: u32,
    pub wf_type: usize,
    pub seq: u32,
    pub attempt: u32,
    pub sticky: bool,
    pub total_events: u32,
    pub events_in_response: u32,
    pub prog: ProgState,
    pub snap: Snapshot,
    pub scheduled_at: Time,
    /// workflow updates this task carries (protocol messages), to accept and complete
    pub updates: u32,
}

#[derive(Clone, Copy, Debug)]
pub struct ActTaskInfo {
    pub wf: WfId,
    pub wgen: u32,
    pub wf_type: usize,
    pub seq: u32,
    pub attempt: u32,
    pub step: usize,
    /// its member of a parallel step (0 otherwise)
    pub member: u8,
    pub scheduled_at: Time,
    /// when the activity was first scheduled (the poll response's `ScheduledTime`)
    pub first_scheduled_at: Time,
    /// how the activity's attempts go (none: they fail at the step's failure rate)
    pub plan: super::params::AttemptPlan,
}

/// `count` activities of step `step` (its member `member` when the step is parallel) to
/// schedule on task queue `tq`.
#[derive(Clone, Copy, Debug)]
pub struct ScheduleActivities {
    pub step: usize,
    pub member: u8,
    pub tq: usize,
    pub count: u32,
}

/// Commands produced by a workflow task.
#[derive(Clone, Debug, Default)]
pub struct Commands {
    pub schedule_activities: Vec<ScheduleActivities>,
    pub start_timer: Option<Time>,
    /// (child workflow type, count)
    pub start_children: Vec<(usize, u32)>,
    pub complete: bool,
    /// close the workflow as failed (with `complete`): an activity failed for good
    pub fail: bool,
    pub markers: u32,
    pub new_prog: ProgState,
    pub sticky_worker: Option<usize>,
    /// scheduler workflows: actions performed in this task
    pub actions_done: u32,
    /// how many of the scheduled activities the worker takes eagerly, of those on its task
    /// queue `eager_tq` (the SDK requests eager execution only for those)
    pub eager_activities: u32,
    pub eager_tq: usize,
    /// workflow updates accepted and completed in this task (their handlers don't block)
    pub updates: u32,
}

pub struct RespondResult {
    pub new_wft: Option<WftInfo>,
    pub eager: Vec<ActTaskInfo>,
}

pub const HISTORY_PAGE: u32 = 256;

/// Kinds of events the shard events cache holds (`writeEventToCache` in
/// `service/history/workflow/mutable_state_impl.go`): the start event, activities' scheduled
/// events, children's initiated events and the close event.
#[derive(Clone, Copy)]
pub enum Cached {
    ActivityScheduled = 0,
    Started = 1,
    ChildInitiated = 2,
    Closed = 3,
}

/// The events cache key of event `n` of kind `kind` in the workflow with key `wf_key`.
pub fn event_key(wf_key: u64, kind: Cached, n: u32) -> u64 {
    wf_key.wrapping_mul(1_000_003) ^ ((kind as u64) << 48) ^ u64::from(n)
}

/// Put an event of `bytes` into shard `shard`'s events cache.
fn cache_event(ctx: &Ctx, shard: ShardId, key: u64, bytes: f64) {
    ctx.shards.borrow_mut()[(shard - 1) as usize]
        .events_cache
        .put(key, bytes as u64);
}

/// Read the event with `key`, of `bytes`, through shard `shard`'s events cache: on a miss,
/// `GetEvent` reads the batch the event was written in, `batch_bytes`, from the database and
/// caches the event again.
pub async fn get_event(
    ctx: &Ctx,
    pod: PodId,
    shard: ShardId,
    key: u64,
    bytes: f64,
    batch_bytes: f64,
    caller: Caller,
) -> Res<()> {
    let hit = ctx.shards.borrow_mut()[(shard - 1) as usize]
        .events_cache
        .get(key);
    {
        let mut m = ctx.m.borrow_mut();
        if hit {
            m.events_cache_hits += 1;
        } else {
            m.events_cache_misses += 1;
        }
    }
    if !hit {
        read_history(ctx, pod, batch_bytes, caller, Some(shard)).await?;
        cache_event(ctx, shard, key, bytes);
    }
    Ok(())
}

/// Where a new workflow comes from.
#[derive(Clone, Copy, Debug)]
pub enum StartOrigin {
    Client,
    Child { parent: WfId, parent_gen: u32 },
    Schedule,
    Entity,
}

/// Compute the shard for a new workflow (`common.WorkflowIDToHistoryShard`).
pub fn shard_for(ctx: &Ctx, ns: usize, wf_type: usize, key: u64) -> ShardId {
    let id = format!("{}-{key}", ctx.p.wf_types[wf_type].name);
    workflow_id_to_history_shard(&ctx.p.namespaces[ns].id, &id, ctx.p.num_shards)
}

pub fn alloc_key(ctx: &Ctx) -> u64 {
    let mut w = ctx.wfs.borrow_mut();
    let k = w.next_key;
    w.next_key += 1;
    k
}

fn deadline_after(d: Time) -> Time {
    now() + d
}

/// StartWorkflowExecution on the owning history pod. Returns the new workflow and, for eager
/// starts, the first workflow task (already started).
#[allow(clippy::too_many_arguments)]
pub async fn start_workflow(
    ctx: &Ctx,
    pod: PodId,
    shard: ShardId,
    key: u64,
    wf_type: usize,
    origin: StartOrigin,
    eager: bool,
    deadline: Time,
) -> Res<(WfId, u32, Option<WftInfo>)> {
    let caller = Caller::Api(1, ctx.p.wf_types[wf_type].ns);
    history_admit(ctx, pod, caller)?;
    let t0 = now();
    let r = start_inner(ctx, pod, shard, key, wf_type, origin, eager, deadline).await;
    ctx.m
        .borrow_mut()
        .hist_op(pod, HistApi::StartWorkflowExecution)
        .record(now() - t0, r.as_ref().err().copied());
    r
}

#[allow(clippy::too_many_arguments)]
async fn start_inner(
    ctx: &Ctx,
    pod: PodId,
    shard: ShardId,
    key: u64,
    wf_type: usize,
    origin: StartOrigin,
    eager: bool,
    deadline: Time,
) -> Res<(WfId, u32, Option<WftInfo>)> {
    cpu(
        ctx,
        pod,
        ctx.p.costs.history[HistApi::StartWorkflowExecution.idx()],
    )
    .await;
    shard_ready(ctx, pod, shard, deadline).await?;
    create_execution(
        ctx,
        pod,
        shard,
        key,
        wf_type,
        origin,
        eager,
        StartWith::Start,
        deadline,
    )
    .await
}

/// Write a brand-new execution and its first events. With signal-with-start the signal is among
/// them; an update-with-start's update stays in the update registry (memory) until the first
/// workflow task carries it.
#[allow(clippy::too_many_arguments)]
async fn create_execution(
    ctx: &Ctx,
    pod: PodId,
    shard: ShardId,
    key: u64,
    wf_type: usize,
    origin: StartOrigin,
    eager: bool,
    with: StartWith,
    deadline: Time,
) -> Res<(WfId, u32, Option<WftInfo>)> {
    let tp = &ctx.p.wf_types[wf_type];
    let payload = tp.payload_bytes;
    let signal = with == StartWith::Signal;
    // WorkflowExecutionStarted with its input, WorkflowExecutionSignaled with the signal's for
    // signal-with-start, WorkflowTaskScheduled (and Started when eager)
    let append = Append::new(
        2 + u32::from(eager) + u32::from(signal),
        payload + if signal { SIGNAL_BYTES } else { 0.0 },
    );
    // Brand new execution: no contention on its lock (unique IDs), write under the shard sem.
    shard_write(
        ctx,
        pod,
        shard,
        PersistOp::CreateWorkflowExecution,
        append.bytes(),
        Caller::Api(1, tp.ns),
        deadline,
    )
    .await?;
    cache_event(
        ctx,
        shard,
        event_key(key, Cached::Started, 0),
        EVENT_BYTES + payload,
    );
    let t = now();
    let (parent, entity) = match origin {
        StartOrigin::Child { parent, parent_gen } => (Some((parent, parent_gen)), false),
        StartOrigin::Entity => (None, true),
        _ => (None, false),
    };
    let wf = Wf {
        wgen: 0,
        key,
        wf_type,
        ns: tp.ns,
        shard,
        lock: crate::sim::sync::Semaphore::new(1),
        status: WfStatus::Running,
        start_time: t,
        parent,
        history_events: append.events,
        history_bytes: append.bytes(),
        wft: if eager {
            WftState::Started {
                seq: 1,
                attempt: 1,
                sticky: false,
                at: t,
            }
        } else {
            WftState::Scheduled {
                seq: 1,
                attempt: 1,
                sticky: false,
                at: t,
            }
        },
        wft_seq: 1,
        sticky_worker: None,
        last_started_event: 0,
        buffered_events: 0,
        step: 0,
        step_started: false,
        step_remaining: 0,
        completed_in_step: 0,
        failed_in_step: 0,
        timer_seq: 0,
        timer_pending: None,
        timer_fired: false,
        signals_received: u32::from(signal),
        signals_consumed: 0,
        updates_admitted: u32::from(with == StartWith::Update),
        updates_delivered: 0,
        updates_done: 0,
        update_waiters: Vec::new(),
        wft_speculative: false,
        activities: Vec::new(),
        next_act_seq: 0,
        children_pending: 0,
        children_done: 0,
        children_initiated: 0,
        entity,
        close_waiters: Vec::new(),
        history_waiters: Vec::new(),
        running_idx: 0,
        schedule: None,
    };
    let (id, wgen) = {
        let mut wfs = ctx.wfs.borrow_mut();
        let id = wfs.insert(wf);
        let wgen = wfs.slots[id as usize].as_ref().map(|w| w.wgen).unwrap_or(0);
        (id, wgen)
    };
    {
        let mut m = ctx.m.borrow_mut();
        m.wf[wf_type].started += 1;
        if eager {
            m.wf[wf_type].eager_starts += 1;
        }
    }
    let mut tasks = vec![TaskSpec::now(TaskType::VisibilityStartExecution, 0, 0)];
    if eager {
        let to = ctx.p.namespaces[tp.ns].default_wft_timeout;
        tasks.push(TaskSpec::at(
            TaskType::TimerWorkflowTaskTimeout,
            t + to,
            1,
            1,
        ));
    } else {
        tasks.push(TaskSpec::now(TaskType::TransferWorkflowTask, 1, 0));
    }
    commit_tasks(ctx, shard, id, wgen, &tasks);
    let eager_wft = eager.then_some(WftInfo {
        wf: id,
        wgen,
        wf_type,
        seq: 1,
        attempt: 1,
        sticky: false,
        total_events: 3,
        events_in_response: 3,
        prog: ProgState::default(),
        snap: Snapshot::default(),
        scheduled_at: t,
        // only StartWorkflowExecution starts eagerly, so the task carries no update
        updates: 0,
    });
    Ok((id, wgen, eager_wft))
}

/// Hand the admitted updates that no task carries yet to the workflow task starting now.
fn take_updates(w: &mut Wf) -> u32 {
    let n = w.updates_admitted - w.updates_delivered;
    w.updates_delivered = w.updates_admitted;
    n
}

/// Read the program/progress snapshot for a started workflow task.
fn snapshot(w: &Wf) -> (ProgState, Snapshot) {
    (
        ProgState {
            step: w.step,
            step_started: w.step_started,
            step_scheduled: w.step_remaining,
            signals_consumed: w.signals_consumed,
            done: w.status == WfStatus::Closed,
        },
        Snapshot {
            completed_in_step: w.completed_in_step,
            failed_in_step: w.failed_in_step,
            timer_fired: w.timer_fired,
            children_done: w.children_done,
            signals_received: w.signals_received,
            due_actions: w.schedule.map(|s| s.due_actions).unwrap_or(0),
        },
    )
}

/// RecordWorkflowTaskStarted (called by matching after a match).
pub async fn record_wft_started(
    ctx: &Ctx,
    pod: PodId,
    wf: WfId,
    wgen: u32,
    seq: u32,
    deadline: Time,
) -> Res<WftInfo> {
    let caller = Caller::Api(2, ctx.wf_ns(wf, wgen));
    history_admit(ctx, pod, caller)?;
    let t0 = now();
    let r = record_wft_started_inner(ctx, pod, wf, wgen, seq, deadline).await;
    ctx.m
        .borrow_mut()
        .hist_op(pod, HistApi::RecordWorkflowTaskStarted)
        .record(now() - t0, r.as_ref().err().copied());
    r
}

async fn record_wft_started_inner(
    ctx: &Ctx,
    pod: PodId,
    wf: WfId,
    wgen: u32,
    seq: u32,
    deadline: Time,
) -> Res<WftInfo> {
    let caller = Caller::Api(2, ctx.wf_ns(wf, wgen));
    cpu(
        ctx,
        pod,
        ctx.p.costs.history[HistApi::RecordWorkflowTaskStarted.idx()],
    )
    .await;
    let shard = ctx.wf_shard(wf, wgen).ok_or(Err::NotFound)?;
    shard_ready(ctx, pod, shard, deadline).await?;
    let lock = lock_wf(ctx, wf, wgen, caller, deadline).await?;
    load_ms(ctx, pod, shard, wf, wgen, caller).await?;
    // validate
    let (attempt, sticky, scheduled_at, speculative) = {
        let wfs = ctx.wfs.borrow();
        let w = wfs.get(wf, wgen).ok_or(Err::NotFound)?;
        match w.wft {
            WftState::Scheduled {
                seq: s,
                attempt,
                sticky,
                at,
            } if s == seq && w.status == WfStatus::Running => {
                (attempt, sticky, at, w.wft_speculative)
            }
            _ => return Err(Err::NotFound),
        }
    };
    // WorkflowTaskStarted; a transient workflow task (attempt > 1) is a mutable-state-only write,
    // and a speculative one starts in memory: its events are written when it completes
    let started = Append::new(1, 0.0);
    if !speculative {
        let r = shard_write(
            ctx,
            pod,
            shard,
            PersistOp::UpdateWorkflowExecution,
            if attempt == 1 { started.bytes() } else { 0.0 },
            caller,
            deadline,
        )
        .await;
        if let Err(e) = r {
            evict_ms(ctx, pod, shard, wf, wgen);
            return Err(e);
        }
    }
    let t = now();
    let (info, ns, page_bytes) = {
        let mut wfs = ctx.wfs.borrow_mut();
        let w = wfs.get_mut(wf, wgen).ok_or(Err::NotFound)?;
        w.wft = WftState::Started {
            seq,
            attempt,
            sticky,
            at: t,
        };
        if !speculative {
            w.grow(started);
        }
        let events_in_response = if sticky {
            w.history_events.saturating_sub(w.last_started_event).max(1)
        } else {
            w.history_events.min(HISTORY_PAGE)
        };
        w.last_started_event = w.history_events;
        let (prog, snap) = snapshot(w);
        let page_bytes = f64::from(events_in_response) * w.event_bytes();
        let updates = take_updates(w);
        (
            WftInfo {
                wf,
                wgen,
                wf_type: w.wf_type,
                seq,
                attempt,
                sticky,
                total_events: w.history_events,
                events_in_response,
                prog,
                snap,
                scheduled_at,
                updates,
            },
            w.ns,
            page_bytes,
        )
    };
    let to = ctx.p.namespaces[ns].default_wft_timeout;
    commit_tasks(
        ctx,
        shard,
        wf,
        wgen,
        &[TaskSpec::at(
            TaskType::TimerWorkflowTaskTimeout,
            t + to,
            seq,
            attempt,
        )],
    );
    drop(lock);
    // first page of history for the poll response (outside the lock)
    read_history(ctx, pod, page_bytes, caller, Some(shard)).await?;
    cpu(
        ctx,
        pod,
        ctx.p.costs.history_per_event_read * f64::from(info.events_in_response),
    )
    .await;
    Ok(info)
}

/// Schedule a new workflow task if none is outstanding. Returns (seq, sticky) of the new task.
fn maybe_schedule_wft(w: &mut Wf, t: Time) -> Option<(u32, bool)> {
    if w.wft == WftState::None && w.status == WfStatus::Running {
        w.wft_seq += 1;
        let sticky = w.sticky_worker.is_some();
        w.wft = WftState::Scheduled {
            seq: w.wft_seq,
            attempt: 1,
            sticky,
            at: t,
        };
        w.grow(Append::new(1, 0.0));
        Some((w.wft_seq, sticky))
    } else {
        None
    }
}

fn wft_tasks(ctx: &Ctx, w: &Wf, seq: u32, sticky: bool, t: Time, out: &mut Vec<TaskSpec>) {
    out.push(TaskSpec::now(TaskType::TransferWorkflowTask, seq, 0));
    if sticky {
        // sticky SCHEDULE_TO_START timeout
        let fleet = w
            .sticky_worker
            .and_then(|wk| ctx.workers.borrow().get(wk).map(|x| x.fleet));
        let to = fleet
            .map(|f| ctx.p.fleets[f].sticky_timeout)
            .unwrap_or(5_000_000);
        out.push(TaskSpec::at(
            TaskType::TimerWorkflowTaskTimeout,
            t + to,
            seq,
            0,
        ));
    }
}

/// RespondWorkflowTaskCompleted.
pub async fn respond_wft_completed(
    ctx: &Ctx,
    pod: PodId,
    info: WftInfo,
    cmds: Commands,
    deadline: Time,
) -> Res<RespondResult> {
    let caller = Caller::Api(2, ctx.wf_ns(info.wf, info.wgen));
    history_admit(ctx, pod, caller)?;
    let t0 = now();
    let r = respond_wft_inner(ctx, pod, info, cmds, deadline).await;
    ctx.m
        .borrow_mut()
        .hist_op(pod, HistApi::RespondWorkflowTaskCompleted)
        .record(now() - t0, r.as_ref().err().copied());
    r
}

async fn respond_wft_inner(
    ctx: &Ctx,
    pod: PodId,
    info: WftInfo,
    cmds: Commands,
    deadline: Time,
) -> Res<RespondResult> {
    let caller = Caller::Api(2, ctx.wf_ns(info.wf, info.wgen));
    let n_cmds = cmds
        .schedule_activities
        .iter()
        .map(|x| x.count)
        .sum::<u32>()
        + u32::from(cmds.start_timer.is_some())
        + cmds.start_children.iter().map(|c| c.1).sum::<u32>()
        + u32::from(cmds.complete)
        + cmds.markers;
    // each update's acceptance and response messages, and their events
    let n_msgs = 2 * cmds.updates;
    cpu(
        ctx,
        pod,
        ctx.p.costs.history[HistApi::RespondWorkflowTaskCompleted.idx()]
            + ctx.p.costs.history_per_command * f64::from(n_cmds + n_msgs),
    )
    .await;
    let (wf, wgen) = (info.wf, info.wgen);
    let shard = ctx.wf_shard(wf, wgen).ok_or(Err::NotFound)?;
    shard_ready(ctx, pod, shard, deadline).await?;
    let lock = lock_wf(ctx, wf, wgen, caller, deadline).await?;
    load_ms(ctx, pod, shard, wf, wgen, caller).await?;
    // validate the task is still the started one
    let (buffered, history_bytes, ns, speculative, updates_waiting) = {
        let wfs = ctx.wfs.borrow();
        let w = wfs.get(wf, wgen).ok_or(Err::NotFound)?;
        match w.wft {
            WftState::Started { seq, .. } if seq == info.seq && w.status == WfStatus::Running => {}
            _ => return Err(Err::NotFound),
        }
        (
            w.buffered_events,
            w.history_bytes,
            w.ns,
            w.wft_speculative,
            // admitted while this task ran: they need the next one
            w.updates_admitted > w.updates_delivered,
        )
    };
    // a running workflow whose history is over `limit.historySize.error` is terminated instead
    // of updated (`enforceHistorySizeCheck` in `service/history/workflow/context.go`); the
    // caller's InvalidArgument (`ErrHistorySizeExceedsLimit`) isn't retried
    if history_bytes > ctx.p.namespaces[ns].history_size_error {
        // `forceTerminateWorkflow` discards the pending changes and loads the mutable state
        // again before it terminates the workflow
        evict_ms(ctx, pod, shard, wf, wgen);
        load_ms(ctx, pod, shard, wf, wgen, caller).await?;
        // WorkflowExecutionTerminated
        let terminated = Append::new(1, 0.0);
        let r = shard_write(
            ctx,
            pod,
            shard,
            PersistOp::UpdateWorkflowExecution,
            terminated.bytes(),
            caller,
            deadline,
        )
        .await;
        if let Err(e) = r {
            evict_ms(ctx, pod, shard, wf, wgen);
            return Err(e);
        }
        let (wf_type, parent, key) = {
            let mut wfs = ctx.wfs.borrow_mut();
            let w = wfs.get_mut(wf, wgen).ok_or(Err::NotFound)?;
            w.grow(terminated);
            w.wft = WftState::None;
            (w.wf_type, w.parent, w.key)
        };
        cache_event(ctx, shard, event_key(key, Cached::Closed, 0), EVENT_BYTES);
        close_workflow(ctx, wf, wgen, wf_type, parent, Close::Terminated);
        // a parent learns of the termination, which carries no result
        commit_tasks(
            ctx,
            shard,
            wf,
            wgen,
            &[
                TaskSpec::now(TaskType::TransferCloseExecution, 0, 0).batch(terminated.bytes()),
                TaskSpec::now(TaskType::VisibilityCloseExecution, 0, 0),
            ],
        );
        drop(lock);
        return Err(Err::NotFound);
    }
    let eager_n = cmds.eager_activities;
    let inline_new_wft = (buffered > 0 || updates_waiting) && !cmds.complete;
    // WorkflowTaskCompleted and the commands' events, with the payloads of activity and child
    // inputs, local activity results and the workflow's result; then a new workflow task inline
    let payload = ctx.p.wf_types[info.wf_type].payload_bytes;
    let payloads = cmds
        .schedule_activities
        .iter()
        .map(|x| x.count)
        .sum::<u32>()
        + cmds.start_children.iter().map(|c| c.1).sum::<u32>()
        + cmds.markers
        + u32::from(cmds.complete)
        // WorkflowExecutionUpdateAccepted with the request, Completed with the outcome
        + n_msgs;
    let append = Append::new(
        1 + n_cmds
            + u32::from(cmds.complete)
            + n_msgs
            + if inline_new_wft { 2 } else { 0 }
            // a speculative task's WorkflowTaskScheduled and Started are written only now
            + if speculative { 2 } else { 0 },
        payload * f64::from(payloads),
    );
    let r = shard_write(
        ctx,
        pod,
        shard,
        PersistOp::UpdateWorkflowExecution,
        append.bytes(),
        caller,
        deadline,
    )
    .await;
    if let Err(e) = r {
        evict_ms(ctx, pod, shard, wf, wgen);
        return Err(e);
    }
    let t = now();
    let mut tasks: Vec<TaskSpec> = Vec::new();
    let mut eager_out = Vec::new();
    let mut new_wft = None;
    let mut closed = false;
    let mut parent = None;
    let mut inline_bytes = 0.0;
    let mut updates_completed = Vec::new();
    let (ns, wf_type) = {
        let mut wfs = ctx.wfs.borrow_mut();
        let w = wfs.get_mut(wf, wgen).ok_or(Err::NotFound)?;
        let ns = w.ns;
        let wf_type = w.wf_type;
        let before = w.history_bytes;
        w.grow(append);
        {
            let mut m = ctx.m.borrow_mut();
            let ws = &mut m.wf[wf_type];
            let warn = ctx.p.namespaces[ns].history_size_warn;
            if before <= warn && w.history_bytes > warn {
                ws.over_size_warn += 1;
            }
            ws.max_history_bytes = ws.max_history_bytes.max(w.history_bytes);
        }
        w.wft = WftState::None;
        w.wft_speculative = false;
        w.updates_done += cmds.updates;
        let done = w.updates_done;
        updates_completed.extend(
            w.update_waiters
                .extract_if(.., |(n, _)| *n < done)
                .map(|(_, tx)| tx),
        );
        // apply program state; step change resets per-step counters
        if cmds.new_prog.step != w.step {
            w.completed_in_step = 0;
            w.failed_in_step = 0;
            w.timer_fired = false;
            w.children_done = 0;
            // the SDK cancels a timer whose step completed early (e.g. a wait-signal timeout)
            w.timer_pending = None;
        }
        w.step = cmds.new_prog.step;
        w.step_started = cmds.new_prog.step_started;
        w.step_remaining = cmds.new_prog.step_scheduled;
        w.signals_consumed = cmds.new_prog.signals_consumed;
        w.sticky_worker = cmds.sticky_worker;
        if let Some(s) = w.schedule.as_mut() {
            s.due_actions = s.due_actions.saturating_sub(cmds.actions_done);
        }
        // activities
        let mut eager_left = eager_n;
        for &ScheduleActivities {
            step,
            member,
            tq,
            count,
        } in &cmds.schedule_activities
        {
            for _ in 0..count {
                w.next_act_seq += 1;
                let seq = w.next_act_seq;
                // ActivityTaskScheduled, with its input (`ApplyActivityTaskScheduledEvent`)
                cache_event(
                    ctx,
                    shard,
                    event_key(w.key, Cached::ActivityScheduled, seq),
                    EVENT_BYTES + payload,
                );
                let eager = eager_left > 0 && tq == cmds.eager_tq;
                if eager {
                    eager_left -= 1;
                }
                let plan = match ctx.p.wf_types[wf_type].activity(step, member) {
                    Some(super::params::StepP::Activity {
                        attempts: Some(a), ..
                    }) => a.sample(ctx.rand()),
                    _ => Default::default(),
                };
                w.activities.push(ActInfo {
                    seq,
                    attempt: 1,
                    state: if eager {
                        ActState::Started
                    } else {
                        ActState::Scheduled
                    },
                    step,
                    member,
                    tq,
                    scheduled_at: t,
                    first_scheduled_at: t,
                    started_at: if eager { t } else { 0 },
                    last_heartbeat: 0,
                    timers: 0,
                    hb_timer_at: 0,
                    plan,
                    batch_bytes: append.bytes(),
                });
                if eager {
                    eager_out.push(ActTaskInfo {
                        wf,
                        wgen,
                        wf_type,
                        seq,
                        attempt: 1,
                        step,
                        member,
                        scheduled_at: t,
                        first_scheduled_at: t,
                        plan,
                    });
                } else {
                    tasks.push(TaskSpec::now(TaskType::TransferActivityTask, seq, 1));
                }
            }
        }
        activity::create_next_timer(ctx, w, &mut tasks);
        if let Some(d) = cmds.start_timer {
            w.timer_seq += 1;
            w.timer_pending = Some(w.timer_seq);
            tasks.push(TaskSpec::at(
                TaskType::TimerUserTimer,
                t + d,
                w.timer_seq,
                0,
            ));
        }
        for &(child_type, count) in &cmds.start_children {
            w.children_pending += count;
            for _ in 0..count {
                // StartChildWorkflowExecutionInitiated, with the child's input
                let n = w.children_initiated;
                w.children_initiated += 1;
                cache_event(
                    ctx,
                    shard,
                    event_key(w.key, Cached::ChildInitiated, n),
                    EVENT_BYTES + payload,
                );
                tasks.push(
                    TaskSpec::now(TaskType::TransferStartChildExecution, child_type as u32, n)
                        .batch(append.bytes()),
                );
            }
        }
        if cmds.complete {
            // the close event, with the result
            cache_event(
                ctx,
                shard,
                event_key(w.key, Cached::Closed, 0),
                EVENT_BYTES + payload,
            );
            tasks.push(
                TaskSpec::now(TaskType::TransferCloseExecution, payload as u32, 0)
                    .batch(append.bytes()),
            );
            tasks.push(TaskSpec::now(TaskType::VisibilityCloseExecution, 0, 0));
            closed = true;
            parent = w.parent;
        } else if inline_new_wft {
            // ReturnNewWorkflowTask: scheduled + started inline, no transfer task
            w.buffered_events = 0;
            w.wft_seq += 1;
            let seq = w.wft_seq;
            w.wft = WftState::Started {
                seq,
                attempt: 1,
                sticky: true,
                at: t,
            };
            let events_in_response = w.history_events.saturating_sub(w.last_started_event).max(1);
            w.last_started_event = w.history_events;
            inline_bytes = f64::from(events_in_response) * w.event_bytes();
            let (prog, snap) = snapshot(w);
            let updates = take_updates(w);
            new_wft = Some(WftInfo {
                wf,
                wgen,
                wf_type,
                seq,
                attempt: 1,
                sticky: true,
                total_events: w.history_events,
                events_in_response,
                prog,
                snap,
                scheduled_at: t,
                updates,
            });
            let to = ctx.p.namespaces[ns].default_wft_timeout;
            tasks.push(TaskSpec::at(
                TaskType::TimerWorkflowTaskTimeout,
                t + to,
                seq,
                1,
            ));
        } else if w.buffered_events > 0 {
            w.buffered_events = 0;
        }
        (ns, wf_type)
    };
    let _ = ns;
    if closed {
        let outcome = if cmds.fail {
            Close::Failed
        } else {
            Close::Completed
        };
        close_workflow(ctx, wf, wgen, wf_type, parent, outcome);
    }
    commit_tasks(ctx, shard, wf, wgen, &tasks);
    drop(lock);
    for tx in updates_completed {
        let _ = tx.send(true);
    }
    if new_wft.is_some() {
        read_history(ctx, pod, inline_bytes, caller, Some(shard)).await?;
    }
    {
        let mut m = ctx.m.borrow_mut();
        m.wf[wf_type].wft_completed += 1;
    }
    Ok(RespondResult {
        new_wft,
        eager: eager_out,
    })
}

/// How a workflow closed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Close {
    Completed,
    Failed,
    /// terminated by the server: its history grew over `limit.historySize.error`
    Terminated,
}

/// Bookkeeping when a workflow closes.
fn close_workflow(
    ctx: &Ctx,
    wf: WfId,
    wgen: u32,
    wf_type: usize,
    _parent: Option<(WfId, u32)>,
    outcome: Close,
) {
    let (waiters, hwaiters, uw, start) = {
        let mut wfs = ctx.wfs.borrow_mut();
        let Some(w) = wfs.get_mut(wf, wgen) else {
            return;
        };
        let waiters = std::mem::take(&mut w.close_waiters);
        let hw = std::mem::take(&mut w.history_waiters);
        // updates still waiting for a workflow task fail with the workflow's close
        let uw = std::mem::take(&mut w.update_waiters);
        let st = w.start_time;
        wfs.mark_closed(wf);
        (waiters, hw, uw, st)
    };
    for tx in waiters {
        let _ = tx.send(());
    }
    for (_, tx) in uw {
        let _ = tx.send(false);
    }
    for tx in hwaiters {
        let _ = tx.send(());
    }
    let mut m = ctx.m.borrow_mut();
    match outcome {
        Close::Failed => m.wf[wf_type].failed += 1,
        Close::Terminated => m.wf[wf_type].terminated += 1,
        Close::Completed => {
            m.wf[wf_type].completed += 1;
            m.wf[wf_type].e2e.record(now() - start);
        }
    }
    // remove from sticky caches lazily (worker side handles missing entries)
}

/// RecordActivityTaskStarted.
pub async fn record_activity_started(
    ctx: &Ctx,
    pod: PodId,
    wf: WfId,
    wgen: u32,
    seq: u32,
    attempt: u32,
    deadline: Time,
) -> Res<ActTaskInfo> {
    let caller = Caller::Api(2, ctx.wf_ns(wf, wgen));
    history_admit(ctx, pod, caller)?;
    let t0 = now();
    let r = async {
        cpu(
            ctx,
            pod,
            ctx.p.costs.history[HistApi::RecordActivityTaskStarted.idx()],
        )
        .await;
        let shard = ctx.wf_shard(wf, wgen).ok_or(Err::NotFound)?;
        shard_ready(ctx, pod, shard, deadline).await?;
        let lock = lock_wf(ctx, wf, wgen, caller, deadline).await?;
        load_ms(ctx, pod, shard, wf, wgen, caller).await?;
        let (step, member, scheduled_at, first_scheduled_at, plan, wf_type, batch_bytes, ev_key) = {
            let wfs = ctx.wfs.borrow();
            let w = wfs.get(wf, wgen).ok_or(Err::NotFound)?;
            if w.status != WfStatus::Running {
                return Err(Err::NotFound);
            }
            let a = w
                .activities
                .iter()
                .find(|a| a.seq == seq && a.attempt == attempt && a.state == ActState::Scheduled)
                .ok_or(Err::NotFound)?;
            (
                a.step,
                a.member,
                a.scheduled_at,
                a.first_scheduled_at,
                a.plan,
                w.wf_type,
                a.batch_bytes,
                event_key(w.key, Cached::ActivityScheduled, seq),
            )
        };
        // the scheduled event, with the activity's input (while holding the lock)
        let payload = ctx.p.wf_types[wf_type].payload_bytes;
        get_event(
            ctx,
            pod,
            shard,
            ev_key,
            EVENT_BYTES + payload,
            batch_bytes,
            caller,
        )
        .await?;
        let r = shard_write(
            ctx,
            pod,
            shard,
            PersistOp::UpdateWorkflowExecution,
            0.0,
            caller,
            deadline,
        )
        .await;
        if let Err(e) = r {
            evict_ms(ctx, pod, shard, wf, wgen);
            return Err(e);
        }
        let t = now();
        let mut tasks = Vec::new();
        {
            let mut wfs = ctx.wfs.borrow_mut();
            let w = wfs.get_mut(wf, wgen).ok_or(Err::NotFound)?;
            if let Some(a) = w.activities.iter_mut().find(|a| a.seq == seq) {
                a.state = ActState::Started;
                a.started_at = t;
                a.last_heartbeat = 0;
            }
            // the attempt's start-to-close or heartbeat timer, if it is now the earliest
            activity::create_next_timer(ctx, w, &mut tasks);
        }
        if !tasks.is_empty() {
            commit_tasks(ctx, shard, wf, wgen, &tasks);
        }
        drop(lock);
        Ok(ActTaskInfo {
            wf,
            wgen,
            wf_type,
            seq,
            attempt,
            step,
            member,
            scheduled_at,
            first_scheduled_at,
            plan,
        })
    }
    .await;
    ctx.m
        .borrow_mut()
        .hist_op(pod, HistApi::RecordActivityTaskStarted)
        .record(now() - t0, r.as_ref().err().copied());
    r
}

/// RespondActivityTaskCompleted / RespondActivityTaskFailed; a `non_retryable` failure is not
/// retried.
pub async fn respond_activity(
    ctx: &Ctx,
    pod: PodId,
    info: ActTaskInfo,
    failed: bool,
    non_retryable: bool,
    deadline: Time,
) -> Res<()> {
    let caller = Caller::Api(2, ctx.wf_ns(info.wf, info.wgen));
    history_admit(ctx, pod, caller)?;
    let api = if failed {
        HistApi::RespondActivityTaskFailed
    } else {
        HistApi::RespondActivityTaskCompleted
    };
    let t0 = now();
    let r = async {
        cpu(ctx, pod, ctx.p.costs.history[api.idx()]).await;
        let (wf, wgen) = (info.wf, info.wgen);
        let shard = ctx.wf_shard(wf, wgen).ok_or(Err::NotFound)?;
        shard_ready(ctx, pod, shard, deadline).await?;
        let lock = lock_wf(ctx, wf, wgen, caller, deadline).await?;
        load_ms(ctx, pod, shard, wf, wgen, caller).await?;
        let t0w = now();
        let retry = {
            let wfs = ctx.wfs.borrow();
            let w = wfs.get(wf, wgen).ok_or(Err::NotFound)?;
            if w.status != WfStatus::Running {
                return Err(Err::NotFound);
            }
            let a = w
                .activities
                .iter()
                .find(|a| {
                    a.seq == info.seq && a.attempt == info.attempt && a.state == ActState::Started
                })
                .ok_or(Err::NotFound)?;
            // RespondActivityTaskFailed: the retry policy decides between a new attempt and
            // failing the activity, unless the failure is non-retryable (`RetryActivity` returns
            // RETRY_STATE_NON_RETRYABLE_FAILURE, `service/history/workflow/
            // mutable_state_impl.go`)
            if !failed || non_retryable {
                None
            } else if let activity::Next::Retry(d) =
                activity::retry_decision(ctx, w.wf_type, a, None, t0w)
            {
                Some(d)
            } else {
                None
            }
        };
        let gave_up = failed && retry.is_none();
        // ActivityTaskStarted and the close event, with the result when it completed; a failure
        // with retry is a mutable-state-only write (server-side retry)
        let closed = Append::new(
            2,
            if failed {
                0.0
            } else {
                ctx.p.wf_types[info.wf_type].payload_bytes
            },
        );
        let r = shard_write(
            ctx,
            pod,
            shard,
            PersistOp::UpdateWorkflowExecution,
            if retry.is_none() { closed.bytes() } else { 0.0 },
            caller,
            deadline,
        )
        .await;
        if let Err(e) = r {
            evict_ms(ctx, pod, shard, wf, wgen);
            return Err(e);
        }
        let t = now();
        let mut tasks = Vec::new();
        {
            let mut wfs = ctx.wfs.borrow_mut();
            let w = wfs.get_mut(wf, wgen).ok_or(Err::NotFound)?;
            if let Some(delay) = retry {
                if let Some(a) = w.activities.iter_mut().find(|a| a.seq == info.seq) {
                    tasks.push(activity::schedule_retry(a, t, delay));
                }
            } else {
                w.activities.retain(|a| a.seq != info.seq);
                w.grow(closed);
                if gave_up && ctx.p.wf_types[w.wf_type].fails_workflow(info.step, info.member) {
                    w.failed_in_step += 1;
                } else {
                    w.completed_in_step += 1;
                }
                deliver_event(ctx, w, t, &mut tasks);
            }
            activity::create_next_timer(ctx, w, &mut tasks);
        }
        commit_tasks(ctx, shard, wf, wgen, &tasks);
        drop(lock);
        let mut m = ctx.m.borrow_mut();
        let ws = &mut m.wf[info.wf_type];
        if failed {
            ws.activity_failures += 1;
            if gave_up {
                ws.activities_failed += 1;
                if non_retryable {
                    ws.activities_non_retryable += 1;
                }
            }
        } else {
            ws.activities_completed += 1;
        }
        Ok(())
    }
    .await;
    ctx.m
        .borrow_mut()
        .hist_op(pod, api)
        .record(now() - t0, r.as_ref().err().copied());
    r
}

/// An event that wakes the workflow: schedule a WFT, or buffer if one is in flight.
pub fn deliver_event(ctx: &Ctx, w: &mut Wf, t: Time, tasks: &mut Vec<TaskSpec>) {
    // a write with new events turns a speculative task into a normal one; its scheduled and
    // started events go out with that write
    w.wft_speculative = false;
    match w.wft {
        WftState::Started { .. } => {
            w.buffered_events += 1;
        }
        WftState::Scheduled { .. } => {}
        WftState::None => {
            if let Some((seq, sticky)) = maybe_schedule_wft(w, t) {
                wft_tasks(ctx, w, seq, sticky, t, tasks);
            }
        }
    }
}

/// RecordActivityTaskHeartbeat.
pub async fn heartbeat(ctx: &Ctx, pod: PodId, info: ActTaskInfo, deadline: Time) -> Res<()> {
    let caller = Caller::Api(2, ctx.wf_ns(info.wf, info.wgen));
    history_admit(ctx, pod, caller)?;
    let t0 = now();
    let r = async {
        cpu(
            ctx,
            pod,
            ctx.p.costs.history[HistApi::RecordActivityTaskHeartbeat.idx()],
        )
        .await;
        let shard = ctx.wf_shard(info.wf, info.wgen).ok_or(Err::NotFound)?;
        shard_ready(ctx, pod, shard, deadline).await?;
        let lock = lock_wf(ctx, info.wf, info.wgen, caller, deadline).await?;
        load_ms(ctx, pod, shard, info.wf, info.wgen, caller).await?;
        {
            // only the running attempt may heartbeat
            let wfs = ctx.wfs.borrow();
            let w = wfs.get(info.wf, info.wgen).ok_or(Err::NotFound)?;
            let running = w.status == WfStatus::Running
                && w.activities.iter().any(|a| {
                    a.seq == info.seq && a.attempt == info.attempt && a.state == ActState::Started
                });
            if !running {
                return Err(Err::NotFound);
            }
        }
        let r = shard_write(
            ctx,
            pod,
            shard,
            PersistOp::UpdateWorkflowExecution,
            0.0,
            caller,
            deadline,
        )
        .await;
        if r.is_ok() {
            let t = now();
            let mut tasks = Vec::new();
            {
                let mut wfs = ctx.wfs.borrow_mut();
                if let Some(w) = wfs.get_mut(info.wf, info.wgen) {
                    if let Some(a) = w.activities.iter_mut().find(|a| a.seq == info.seq) {
                        a.last_heartbeat = t;
                    }
                    activity::create_next_timer(ctx, w, &mut tasks);
                }
            }
            commit_tasks(ctx, shard, info.wf, info.wgen, &tasks);
        } else {
            evict_ms(ctx, pod, shard, info.wf, info.wgen);
        }
        drop(lock);
        r
    }
    .await;
    ctx.m
        .borrow_mut()
        .hist_op(pod, HistApi::RecordActivityTaskHeartbeat)
        .record(now() - t0, r.as_ref().err().copied());
    r
}

/// SignalWorkflowExecution.
pub async fn signal(ctx: &Ctx, pod: PodId, wf: WfId, wgen: u32, deadline: Time) -> Res<()> {
    let caller = Caller::Api(1, ctx.wf_ns(wf, wgen));
    history_admit(ctx, pod, caller)?;
    let t0 = now();
    let r = async {
        cpu(
            ctx,
            pod,
            ctx.p.costs.history[HistApi::SignalWorkflowExecution.idx()],
        )
        .await;
        let shard = ctx.wf_shard(wf, wgen).ok_or(Err::NotFound)?;
        shard_ready(ctx, pod, shard, deadline).await?;
        // signal without RunID: GetCurrentExecution
        persist(
            ctx,
            pod,
            PersistOp::GetCurrentExecution,
            caller,
            Some(shard),
        )
        .await?;
        if signal_running(ctx, pod, shard, wf, wgen, caller, deadline).await? {
            Ok(())
        } else {
            Err(Err::NotFound)
        }
    }
    .await;
    ctx.m
        .borrow_mut()
        .hist_op(pod, HistApi::SignalWorkflowExecution)
        .record(now() - t0, r.as_ref().err().copied());
    r
}

/// Append WorkflowExecutionSignaled to a workflow and wake it. Ok(false) when it isn't running.
async fn signal_running(
    ctx: &Ctx,
    pod: PodId,
    shard: ShardId,
    wf: WfId,
    wgen: u32,
    caller: Caller,
    deadline: Time,
) -> Res<bool> {
    let lock = lock_wf(ctx, wf, wgen, caller, deadline).await?;
    load_ms(ctx, pod, shard, wf, wgen, caller).await?;
    {
        let wfs = ctx.wfs.borrow();
        let w = wfs.get(wf, wgen).ok_or(Err::NotFound)?;
        if w.status != WfStatus::Running {
            return Ok(false);
        }
    }
    // WorkflowExecutionSignaled, with a small input
    let signaled = Append::new(1, SIGNAL_BYTES);
    let r = shard_write(
        ctx,
        pod,
        shard,
        PersistOp::UpdateWorkflowExecution,
        signaled.bytes(),
        caller,
        deadline,
    )
    .await;
    if let Err(e) = r {
        evict_ms(ctx, pod, shard, wf, wgen);
        return Err(e);
    }
    let t = now();
    let mut tasks = Vec::new();
    {
        let mut wfs = ctx.wfs.borrow_mut();
        let w = wfs.get_mut(wf, wgen).ok_or(Err::NotFound)?;
        w.signals_received += 1;
        w.grow(signaled);
        deliver_event(ctx, w, t, &mut tasks);
    }
    commit_tasks(ctx, shard, wf, wgen, &tasks);
    drop(lock);
    Ok(true)
}

/// SignalWithStartWorkflowExecution (`service/history/api/signalwithstartworkflow`): signal the
/// workflow ID's running workflow, or start one with the signal among its first events. Returns
/// the workflow and whether this call started it.
#[allow(clippy::too_many_arguments)]
pub async fn signal_with_start(
    ctx: &Ctx,
    pod: PodId,
    shard: ShardId,
    key: u64,
    wf_type: usize,
    target: Option<(WfId, u32)>,
    deadline: Time,
) -> Res<(WfId, u32, bool)> {
    let caller = Caller::Api(1, ctx.p.wf_types[wf_type].ns);
    history_admit(ctx, pod, caller)?;
    let t0 = now();
    let r = async {
        cpu(
            ctx,
            pod,
            ctx.p.costs.history[HistApi::SignalWithStartWorkflowExecution.idx()],
        )
        .await;
        shard_ready(ctx, pod, shard, deadline).await?;
        // the workflow ID's current run
        persist(
            ctx,
            pod,
            PersistOp::GetCurrentExecution,
            caller,
            Some(shard),
        )
        .await?;
        if let Some((wf, wgen)) = target {
            if signal_running(ctx, pod, shard, wf, wgen, caller, deadline).await? {
                return Ok((wf, wgen, false));
            }
            // the current run has closed: the consistency check reads the current run again
            persist(
                ctx,
                pod,
                PersistOp::GetCurrentExecution,
                caller,
                Some(shard),
            )
            .await?;
        }
        let (wf, wgen, _) = create_execution(
            ctx,
            pod,
            shard,
            key,
            wf_type,
            StartOrigin::Client,
            false,
            StartWith::Signal,
            deadline,
        )
        .await?;
        Ok((wf, wgen, true))
    }
    .await;
    ctx.m
        .borrow_mut()
        .hist_op(pod, HistApi::SignalWithStartWorkflowExecution)
        .record(now() - t0, r.as_ref().err().copied());
    r
}

/// The outcome of an update-with-start call.
#[derive(Clone, Copy, Debug)]
pub struct UpdateStart {
    pub wf: WfId,
    pub wgen: u32,
    /// the update's number in its workflow
    pub update: u32,
    /// this call started the workflow
    pub created: bool,
    /// the update completed before the call returned; otherwise the caller polls for it
    pub done: bool,
}

/// ExecuteMultiOperation with a start and an update (`service/history/api/multioperation`):
/// update the workflow ID's running workflow (conflict policy USE_EXISTING), or start one whose
/// first workflow task carries the update, then wait for the update to complete, for up to
/// `history.longPollExpirationInterval`. Its persistence calls run at priority 2: the call isn't
/// among Start, Signal and SignalWithStart in the persistence limiter's table.
#[allow(clippy::too_many_arguments)]
pub async fn update_with_start(
    ctx: &Ctx,
    pod: PodId,
    shard: ShardId,
    key: u64,
    wf_type: usize,
    target: Option<(WfId, u32)>,
    deadline: Time,
) -> Res<UpdateStart> {
    let ns = ctx.p.wf_types[wf_type].ns;
    let caller = Caller::Api(2, ns);
    history_admit(ctx, pod, caller)?;
    let t0 = now();
    let r = async {
        cpu(
            ctx,
            pod,
            ctx.p.costs.history[HistApi::ExecuteMultiOperation.idx()],
        )
        .await;
        shard_ready(ctx, pod, shard, deadline).await?;
        // the workflow ID's current run
        persist(
            ctx,
            pod,
            PersistOp::GetCurrentExecution,
            caller,
            Some(shard),
        )
        .await?;
        let admitted = match target {
            Some((wf, wgen)) => {
                let a = admit_update(ctx, pod, shard, wf, wgen, caller, deadline).await?;
                if a.is_none() {
                    // the current run has closed: the consistency check reads it again
                    persist(
                        ctx,
                        pod,
                        PersistOp::GetCurrentExecution,
                        caller,
                        Some(shard),
                    )
                    .await?;
                }
                a
            }
            None => None,
        };
        if let Some((n, rx)) = admitted {
            let (wf, wgen) = target.unwrap_or_default();
            match wait_update(ctx, ns, rx, deadline).await {
                Ok(done) => {
                    return Ok(UpdateStart {
                        wf,
                        wgen,
                        update: n,
                        created: false,
                        done,
                    });
                }
                // the workflow closed before its task took the update: the server starts a new
                // run with it (`history.enableUpdateWithStartRetryOnClosedWorkflowAbort`)
                Err(Err::NotFound) => {
                    persist(
                        ctx,
                        pod,
                        PersistOp::GetCurrentExecution,
                        caller,
                        Some(shard),
                    )
                    .await?;
                }
                Err(e) => return Err(e),
            }
        }
        let (wf, wgen, rx) = start_with_update(ctx, pod, shard, key, wf_type, deadline).await?;
        let done = wait_update(ctx, ns, rx, deadline).await?;
        Ok(UpdateStart {
            wf,
            wgen,
            update: 0,
            created: true,
            done,
        })
    }
    .await;
    ctx.m
        .borrow_mut()
        .hist_op(pod, HistApi::ExecuteMultiOperation)
        .record(now() - t0, r.as_ref().err().copied());
    r
}

/// Start a run whose registry holds update 0 for its first workflow task, and wait on it.
async fn start_with_update(
    ctx: &Ctx,
    pod: PodId,
    shard: ShardId,
    key: u64,
    wf_type: usize,
    deadline: Time,
) -> Res<(WfId, u32, Receiver<bool>)> {
    let (wf, wgen, _) = create_execution(
        ctx,
        pod,
        shard,
        key,
        wf_type,
        StartOrigin::Client,
        false,
        StartWith::Update,
        deadline,
    )
    .await?;
    let (tx, rx) = oneshot();
    ctx.wfs
        .borrow_mut()
        .get_mut(wf, wgen)
        .ok_or(Err::NotFound)?
        .update_waiters
        .push((0, tx));
    Ok((wf, wgen, rx))
}

/// A re-sent update-with-start for an update that was still waiting when the last call
/// returned: it attaches to the update by its ID and waits again (`multioperation/api.go`).
/// Ok(true) once the update has completed.
pub async fn reattach_update(
    ctx: &Ctx,
    pod: PodId,
    wf: WfId,
    wgen: u32,
    update: u32,
    deadline: Time,
) -> Res<bool> {
    let ns = ctx.wf_ns(wf, wgen);
    let caller = Caller::Api(2, ns);
    history_admit(ctx, pod, caller)?;
    let t0 = now();
    let r = async {
        cpu(
            ctx,
            pod,
            ctx.p.costs.history[HistApi::ExecuteMultiOperation.idx()],
        )
        .await;
        let shard = ctx.wf_shard(wf, wgen).ok_or(Err::NotFound)?;
        shard_ready(ctx, pod, shard, deadline).await?;
        persist(
            ctx,
            pod,
            PersistOp::GetCurrentExecution,
            caller,
            Some(shard),
        )
        .await?;
        let lock = lock_wf(ctx, wf, wgen, caller, deadline).await?;
        load_ms(ctx, pod, shard, wf, wgen, caller).await?;
        let rx = {
            let mut wfs = ctx.wfs.borrow_mut();
            let w = wfs.get_mut(wf, wgen).ok_or(Err::NotFound)?;
            if w.updates_done > update {
                drop(lock);
                return Ok(true);
            }
            if w.status != WfStatus::Running {
                return Err(Err::NotFound);
            }
            let (tx, rx) = oneshot();
            w.update_waiters.push((update, tx));
            rx
        };
        drop(lock);
        wait_update(ctx, ns, rx, deadline).await
    }
    .await;
    ctx.m
        .borrow_mut()
        .hist_op(pod, HistApi::ExecuteMultiOperation)
        .record(now() - t0, r.as_ref().err().copied());
    r
}

/// Admit an update to a running workflow: the registry keeps it in memory and nothing is
/// written. With no workflow task outstanding, a speculative one goes straight to matching to
/// carry it. Returns the update's number and a receiver for its outcome, or None when the
/// workflow isn't running.
async fn admit_update(
    ctx: &Ctx,
    pod: PodId,
    shard: ShardId,
    wf: WfId,
    wgen: u32,
    caller: Caller,
    deadline: Time,
) -> Res<Option<(u32, Receiver<bool>)>> {
    let lock = lock_wf(ctx, wf, wgen, caller, deadline).await?;
    load_ms(ctx, pod, shard, wf, wgen, caller).await?;
    let t = now();
    let (n, rx, dispatch) = {
        let mut wfs = ctx.wfs.borrow_mut();
        let Some(w) = wfs.get_mut(wf, wgen) else {
            return Ok(None);
        };
        if w.status != WfStatus::Running {
            return Ok(None);
        }
        // `history.maxInFlightUpdates`: admitted updates not yet completed
        if w.updates_admitted - w.updates_done >= ctx.p.namespaces[w.ns].max_in_flight_updates {
            return Err(Err::ResourceExhausted(
                ReCause::ConcurrentLimit,
                Scope::Namespace,
            ));
        }
        let n = w.updates_admitted;
        w.updates_admitted += 1;
        let (tx, rx) = oneshot();
        w.update_waiters.push((n, tx));
        let dispatch = if w.wft == WftState::None {
            w.wft_seq += 1;
            let sticky = w.sticky_worker.is_some();
            w.wft = WftState::Scheduled {
                seq: w.wft_seq,
                attempt: 1,
                sticky,
                at: t,
            };
            w.wft_speculative = true;
            let sticky_worker = if sticky { w.sticky_worker } else { None };
            Some((w.wft_seq, sticky_worker, ctx.p.wf_types[w.wf_type].tq))
        } else {
            None
        };
        (n, rx, dispatch)
    };
    if let Some((seq, sticky_worker, tq)) = dispatch {
        // SCHEDULE_TO_START: the sticky timeout on a sticky queue, else 5 s
        // (`tasks.SpeculativeWorkflowTaskScheduleToStartTimeout`). Temporal keeps a speculative
        // task's timers in memory; here they go through the timer queue.
        let to = sticky_worker
            .and_then(|wk| ctx.workers.borrow().get(wk).map(|x| x.fleet))
            .map(|f| ctx.p.fleets[f].sticky_timeout)
            .unwrap_or(5_000_000);
        commit_tasks(
            ctx,
            shard,
            wf,
            wgen,
            &[TaskSpec::at(
                TaskType::TimerWorkflowTaskTimeout,
                t + to,
                seq,
                0,
            )],
        );
        let c = ctx.clone();
        spawn(async move { dispatch_speculative(c, pod, wf, wgen, seq, tq, sticky_worker).await });
    }
    drop(lock);
    Ok(Some((n, rx)))
}

/// AddWorkflowTask for a speculative workflow task, straight from history (no transfer task),
/// falling back to the normal queue when the sticky worker is gone. Other errors are only
/// logged: the task's schedule-to-start timeout recovers it.
async fn dispatch_speculative(
    ctx: Ctx,
    pod: PodId,
    wf: WfId,
    wgen: u32,
    seq: u32,
    tq: usize,
    sticky_worker: Option<usize>,
) {
    let task = MTask {
        wf,
        wf_gen: wgen,
        kind: TqKind::Workflow,
        r: seq,
        r2: 0,
        created: now(),
        from_backlog: false,
        query: false,
    };
    let add = |c: Ctx, sticky: Option<usize>| async move {
        call_with_timeout(3_000_000, {
            let c2 = c.clone();
            async move {
                super::matching::add_task(&c2, pod, tq, TqKind::Workflow, task, sticky).await
            }
        })
        .await
    };
    if let Err(Err::StickyWorkerUnavailable) = add(ctx.clone(), sticky_worker).await {
        {
            let mut wfs = ctx.wfs.borrow_mut();
            if let Some(w) = wfs.get_mut(wf, wgen) {
                if let WftState::Scheduled {
                    seq: s,
                    attempt,
                    at,
                    ..
                } = w.wft
                    && s == seq
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
        let _ = add(ctx.clone(), None).await;
    }
}

/// Wait for an update's outcome for up to `history.longPollExpirationInterval` (and the call's
/// deadline): Ok(true) when it completed, Ok(false) when the wait ran out, NotFound when the
/// workflow closed first.
async fn wait_update(ctx: &Ctx, ns: usize, rx: Receiver<bool>, deadline: Time) -> Res<bool> {
    let lp = ctx.p.namespaces[ns].history_long_poll;
    let wait = lp.min(deadline.saturating_sub(now()));
    match crate::sim::executor::timeout(wait, rx).await {
        Ok(Some(true)) => Ok(true),
        Ok(_) => Err(Err::NotFound),
        Err(_) => Ok(false),
    }
}

/// RecordChildExecutionCompleted on the parent (from the child's CloseExecution task).
pub async fn record_child_completed(
    ctx: &Ctx,
    pod: PodId,
    wf: WfId,
    wgen: u32,
    result: f64,
    deadline: Time,
) -> Res<()> {
    let ns = ctx.wf_ns(wf, wgen);
    let caller = Caller::BackgroundHigh(ns);
    history_admit(ctx, pod, caller)?;
    let t0 = now();
    let r = async {
        cpu(
            ctx,
            pod,
            ctx.p.costs.history[HistApi::RecordChildExecutionCompleted.idx()],
        )
        .await;
        let shard = ctx.wf_shard(wf, wgen).ok_or(Err::NotFound)?;
        shard_ready(ctx, pod, shard, deadline).await?;
        let lock = lock_wf(ctx, wf, wgen, Caller::Api(2, ns), deadline).await?;
        load_ms(ctx, pod, shard, wf, wgen, caller).await?;
        {
            let wfs = ctx.wfs.borrow();
            let w = wfs.get(wf, wgen).ok_or(Err::NotFound)?;
            if w.status != WfStatus::Running {
                return Err(Err::NotFound);
            }
        }
        // ChildWorkflowExecutionCompleted, with the child's `result`
        let completed = Append::new(1, result);
        let r = shard_write(
            ctx,
            pod,
            shard,
            PersistOp::UpdateWorkflowExecution,
            completed.bytes(),
            caller,
            deadline,
        )
        .await;
        if let Err(e) = r {
            evict_ms(ctx, pod, shard, wf, wgen);
            return Err(e);
        }
        let t = now();
        let mut tasks = Vec::new();
        {
            let mut wfs = ctx.wfs.borrow_mut();
            let w = wfs.get_mut(wf, wgen).ok_or(Err::NotFound)?;
            w.children_done += 1;
            w.children_pending = w.children_pending.saturating_sub(1);
            w.grow(completed);
            deliver_event(ctx, w, t, &mut tasks);
        }
        commit_tasks(ctx, shard, wf, wgen, &tasks);
        drop(lock);
        Ok(())
    }
    .await;
    ctx.m
        .borrow_mut()
        .hist_op(pod, HistApi::RecordChildExecutionCompleted)
        .record(now() - t0, r.as_ref().err().copied());
    r
}

/// DescribeWorkflowExecution (and the history part of QueryWorkflow).
pub async fn describe(
    ctx: &Ctx,
    pod: PodId,
    wf: WfId,
    wgen: u32,
    api: HistApi,
    deadline: Time,
) -> Res<()> {
    let caller = Caller::Api(2, ctx.wf_ns(wf, wgen));
    history_admit(ctx, pod, caller)?;
    let t0 = now();
    let r = async {
        cpu(ctx, pod, ctx.p.costs.history[api.idx()]).await;
        let shard = ctx.wf_shard(wf, wgen).ok_or(Err::NotFound)?;
        shard_ready(ctx, pod, shard, deadline).await?;
        let lock = lock_wf(ctx, wf, wgen, caller, deadline).await?;
        load_ms(ctx, pod, shard, wf, wgen, caller).await?;
        drop(lock);
        Ok(())
    }
    .await;
    ctx.m
        .borrow_mut()
        .hist_op(pod, api)
        .record(now() - t0, r.as_ref().err().copied());
    r
}

/// GetWorkflowExecutionHistory. With `wait_close` it long-polls until the workflow closes or
/// `history.longPollExpirationInterval` passes (returns Ok(false) on expiry).
pub async fn get_history(
    ctx: &Ctx,
    pod: PodId,
    wf: WfId,
    wgen: u32,
    pages: u32,
    wait_close: bool,
    deadline: Time,
) -> Res<bool> {
    let caller = Caller::Api(1, ctx.wf_ns(wf, wgen));
    history_admit(ctx, pod, caller)?;
    let t0 = now();
    let r = async {
        cpu(
            ctx,
            pod,
            ctx.p.costs.history[HistApi::GetWorkflowExecutionHistory.idx()],
        )
        .await;
        let shard = ctx.wf_shard(wf, wgen).ok_or(Err::NotFound)?;
        shard_ready(ctx, pod, shard, deadline).await?;
        let closed = {
            let wfs = ctx.wfs.borrow();
            match wfs.get(wf, wgen) {
                Some(w) => w.status == WfStatus::Closed,
                None => true,
            }
        };
        if wait_close && !closed {
            let (tx, rx) = crate::sim::executor::oneshot();
            let lp = {
                let mut wfs = ctx.wfs.borrow_mut();
                let w = wfs.get_mut(wf, wgen).ok_or(Err::NotFound)?;
                w.history_waiters.push(tx);
                ctx.p.namespaces[w.ns].history_long_poll
            };
            ctx.m.borrow_mut().history_long_polls += 1;
            let wait = lp.min(deadline.saturating_sub(now()));
            match crate::sim::executor::timeout(wait, rx).await {
                Ok(_) => {}
                Err(_) => return Ok(false),
            }
        }
        // a page of events, at the history's mean event size
        let page_bytes = ctx.wfs.borrow().get(wf, wgen).map_or(0.0, |w| {
            f64::from(w.history_events.min(HISTORY_PAGE)) * w.event_bytes()
        });
        for _ in 0..pages.max(1) {
            read_history(ctx, pod, page_bytes, caller, Some(shard)).await?;
            cpu(
                ctx,
                pod,
                ctx.p.costs.history_per_event_read * f64::from(HISTORY_PAGE),
            )
            .await;
        }
        Ok(true)
    }
    .await;
    ctx.m
        .borrow_mut()
        .hist_op(pod, HistApi::GetWorkflowExecutionHistory)
        .record(now() - t0, r.as_ref().err().copied());
    r
}

pub fn api_deadline(d: Time) -> Time {
    deadline_after(d)
}
