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

use crate::sim::executor::{Time, now};
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
    /// activities of the step that failed for good
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
}

#[derive(Clone, Copy, Debug)]
pub struct ActTaskInfo {
    pub wf: WfId,
    pub wgen: u32,
    pub wf_type: usize,
    pub seq: u32,
    pub attempt: u32,
    pub step: usize,
    pub scheduled_at: Time,
    /// when the activity was first scheduled (the poll response's `ScheduledTime`)
    pub first_scheduled_at: Time,
    /// how the activity's attempts go (none: they fail at the step's failure rate)
    pub plan: super::params::AttemptPlan,
}

/// Commands produced by a workflow task.
#[derive(Clone, Debug, Default)]
pub struct Commands {
    /// (step, activity task queue, count)
    pub schedule_activities: Vec<(usize, usize, u32)>,
    pub start_timer: Option<Time>,
    pub start_children: Option<(usize, u32)>,
    pub complete: bool,
    /// close the workflow as failed (with `complete`): an activity failed for good
    pub fail: bool,
    pub markers: u32,
    pub new_prog: ProgState,
    pub sticky_worker: Option<usize>,
    /// scheduler workflows: actions performed in this task
    pub actions_done: u32,
    /// how many of the scheduled activities the worker takes eagerly
    pub eager_activities: u32,
}

pub struct RespondResult {
    pub new_wft: Option<WftInfo>,
    pub eager: Vec<ActTaskInfo>,
}

pub const HISTORY_PAGE: u32 = 256;

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
    let tp = &ctx.p.wf_types[wf_type];
    let payload = tp.payload_bytes;
    // Brand new execution: no contention on its lock (unique IDs), write under the shard sem.
    shard_write(
        ctx,
        pod,
        shard,
        PersistOp::CreateWorkflowExecution,
        true,
        Caller::Api(1, tp.ns),
        deadline,
    )
    .await?;
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
        history_events: 2 + u32::from(eager),
        history_bytes: payload * 2.0,
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
        signals_received: 0,
        signals_consumed: 0,
        activities: Vec::new(),
        next_act_seq: 0,
        children_pending: 0,
        children_done: 0,
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
    });
    Ok((id, wgen, eager_wft))
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
    let (attempt, sticky, scheduled_at) = {
        let wfs = ctx.wfs.borrow();
        let w = wfs.get(wf, wgen).ok_or(Err::NotFound)?;
        match w.wft {
            WftState::Scheduled {
                seq: s,
                attempt,
                sticky,
                at,
            } if s == seq && w.status == WfStatus::Running => (attempt, sticky, at),
            _ => return Err(Err::NotFound),
        }
    };
    // transient workflow task (attempt > 1) is a mutable-state-only write
    let r = shard_write(
        ctx,
        pod,
        shard,
        PersistOp::UpdateWorkflowExecution,
        attempt == 1,
        caller,
        deadline,
    )
    .await;
    if let Err(e) = r {
        evict_ms(ctx, pod, shard, wf, wgen);
        return Err(e);
    }
    let t = now();
    let (info, ns) = {
        let mut wfs = ctx.wfs.borrow_mut();
        let w = wfs.get_mut(wf, wgen).ok_or(Err::NotFound)?;
        w.wft = WftState::Started {
            seq,
            attempt,
            sticky,
            at: t,
        };
        w.history_events += 1;
        let events_in_response = if sticky {
            w.history_events.saturating_sub(w.last_started_event).max(1)
        } else {
            w.history_events.min(HISTORY_PAGE)
        };
        w.last_started_event = w.history_events;
        let (prog, snap) = snapshot(w);
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
            },
            w.ns,
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
    persist(ctx, pod, PersistOp::ReadHistoryBranch, caller, Some(shard)).await?;
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
        w.history_events += 1;
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
    let n_cmds = cmds.schedule_activities.iter().map(|x| x.2).sum::<u32>()
        + u32::from(cmds.start_timer.is_some())
        + cmds.start_children.map(|c| c.1).unwrap_or(0)
        + u32::from(cmds.complete)
        + cmds.markers;
    cpu(
        ctx,
        pod,
        ctx.p.costs.history[HistApi::RespondWorkflowTaskCompleted.idx()]
            + ctx.p.costs.history_per_command * f64::from(n_cmds),
    )
    .await;
    let (wf, wgen) = (info.wf, info.wgen);
    let shard = ctx.wf_shard(wf, wgen).ok_or(Err::NotFound)?;
    shard_ready(ctx, pod, shard, deadline).await?;
    let lock = lock_wf(ctx, wf, wgen, caller, deadline).await?;
    load_ms(ctx, pod, shard, wf, wgen, caller).await?;
    // validate the task is still the started one
    let buffered = {
        let wfs = ctx.wfs.borrow();
        let w = wfs.get(wf, wgen).ok_or(Err::NotFound)?;
        match w.wft {
            WftState::Started { seq, .. } if seq == info.seq && w.status == WfStatus::Running => {}
            _ => return Err(Err::NotFound),
        }
        w.buffered_events
    };
    let eager_n = cmds.eager_activities;
    let inline_new_wft = buffered > 0 && !cmds.complete;
    let r = shard_write(
        ctx,
        pod,
        shard,
        PersistOp::UpdateWorkflowExecution,
        true,
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
    let (ns, wf_type) = {
        let mut wfs = ctx.wfs.borrow_mut();
        let w = wfs.get_mut(wf, wgen).ok_or(Err::NotFound)?;
        let ns = w.ns;
        let wf_type = w.wf_type;
        // WFT completed event + command events
        w.history_events += 1 + n_cmds;
        w.history_bytes += ctx.p.wf_types[wf_type].payload_bytes * f64::from(n_cmds.max(1));
        w.wft = WftState::None;
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
        for &(step, tq, count) in &cmds.schedule_activities {
            for _ in 0..count {
                w.next_act_seq += 1;
                let seq = w.next_act_seq;
                let eager = eager_left > 0;
                if eager {
                    eager_left -= 1;
                }
                let plan = match ctx.p.wf_types[wf_type].steps.get(step) {
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
                    tq,
                    scheduled_at: t,
                    first_scheduled_at: t,
                    started_at: if eager { t } else { 0 },
                    last_heartbeat: 0,
                    timers: 0,
                    hb_timer_at: 0,
                    plan,
                });
                if eager {
                    eager_out.push(ActTaskInfo {
                        wf,
                        wgen,
                        wf_type,
                        seq,
                        attempt: 1,
                        step,
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
        if let Some((child_type, count)) = cmds.start_children {
            w.children_pending += count;
            for i in 0..count {
                tasks.push(TaskSpec::now(
                    TaskType::TransferStartChildExecution,
                    child_type as u32,
                    i,
                ));
            }
        }
        if cmds.complete {
            w.history_events += 1;
            tasks.push(TaskSpec::now(TaskType::TransferCloseExecution, 0, 0));
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
            w.history_events += 2;
            let events_in_response = w.history_events.saturating_sub(w.last_started_event).max(1);
            w.last_started_event = w.history_events;
            let (prog, snap) = snapshot(w);
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
        close_workflow(ctx, wf, wgen, wf_type, parent, cmds.fail);
    }
    commit_tasks(ctx, shard, wf, wgen, &tasks);
    drop(lock);
    if new_wft.is_some() {
        persist(ctx, pod, PersistOp::ReadHistoryBranch, caller, Some(shard)).await?;
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

/// Bookkeeping when a workflow closes, completed or `failed`.
fn close_workflow(
    ctx: &Ctx,
    wf: WfId,
    wgen: u32,
    wf_type: usize,
    _parent: Option<(WfId, u32)>,
    failed: bool,
) {
    let (waiters, hwaiters, start) = {
        let mut wfs = ctx.wfs.borrow_mut();
        let Some(w) = wfs.get_mut(wf, wgen) else {
            return;
        };
        let waiters = std::mem::take(&mut w.close_waiters);
        let hw = std::mem::take(&mut w.history_waiters);
        let st = w.start_time;
        wfs.mark_closed(wf);
        (waiters, hw, st)
    };
    for tx in waiters {
        let _ = tx.send(());
    }
    for tx in hwaiters {
        let _ = tx.send(());
    }
    let mut m = ctx.m.borrow_mut();
    if failed {
        m.wf[wf_type].failed += 1;
    } else {
        m.wf[wf_type].completed += 1;
        m.wf[wf_type].e2e.record(now() - start);
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
        let (step, scheduled_at, first_scheduled_at, plan, wf_type) = {
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
                a.scheduled_at,
                a.first_scheduled_at,
                a.plan,
                w.wf_type,
            )
        };
        // scheduled event from the shard events cache (while holding the lock)
        let ev_key = {
            let wfs = ctx.wfs.borrow();
            wfs.get(wf, wgen)
                .map(|w| w.key.wrapping_mul(1_000_003) ^ u64::from(seq))
                .unwrap_or(0)
        };
        let ev_hit = {
            let shards = ctx.shards.borrow();
            shards[(shard - 1) as usize].events_cache.contains(ev_key)
        };
        {
            let mut m = ctx.m.borrow_mut();
            if ev_hit {
                m.events_cache_hits += 1;
            } else {
                m.events_cache_misses += 1;
            }
        }
        if !ev_hit {
            persist(ctx, pod, PersistOp::ReadHistoryBranch, caller, Some(shard)).await?;
        }
        let r = shard_write(
            ctx,
            pod,
            shard,
            PersistOp::UpdateWorkflowExecution,
            false,
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
        // failure with retry is a mutable-state-only write (server-side retry)
        let r = shard_write(
            ctx,
            pod,
            shard,
            PersistOp::UpdateWorkflowExecution,
            retry.is_none(),
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
                w.history_events += 2;
                w.history_bytes += ctx.p.wf_types[w.wf_type].payload_bytes;
                if gave_up {
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
            false,
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
        let lock = lock_wf(ctx, wf, wgen, caller, deadline).await?;
        load_ms(ctx, pod, shard, wf, wgen, caller).await?;
        {
            let wfs = ctx.wfs.borrow();
            let w = wfs.get(wf, wgen).ok_or(Err::NotFound)?;
            if w.status != WfStatus::Running {
                return Err(Err::NotFound);
            }
        }
        let r = shard_write(
            ctx,
            pod,
            shard,
            PersistOp::UpdateWorkflowExecution,
            true,
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
            w.history_events += 1;
            w.history_bytes += 256.0;
            deliver_event(ctx, w, t, &mut tasks);
        }
        commit_tasks(ctx, shard, wf, wgen, &tasks);
        drop(lock);
        Ok(())
    }
    .await;
    ctx.m
        .borrow_mut()
        .hist_op(pod, HistApi::SignalWorkflowExecution)
        .record(now() - t0, r.as_ref().err().copied());
    r
}

/// RecordChildExecutionCompleted on the parent (from the child's CloseExecution task).
pub async fn record_child_completed(
    ctx: &Ctx,
    pod: PodId,
    wf: WfId,
    wgen: u32,
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
        let r = shard_write(
            ctx,
            pod,
            shard,
            PersistOp::UpdateWorkflowExecution,
            true,
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
            w.history_events += 1;
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
        for _ in 0..pages.max(1) {
            persist(ctx, pod, PersistOp::ReadHistoryBranch, caller, Some(shard)).await?;
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
