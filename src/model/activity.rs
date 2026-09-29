//! Activity timeouts and retries, after Temporal 1.31.0: the timer sequence
//! (`service/history/workflow/timer_sequence.go`), the activity timeout task
//! (`executeActivityTimeoutTask` in `timer_queue_active_task_executor.go`) and `RetryActivity`
//! (`mutable_state_impl.go`, `retry.go`).
//!
//! A workflow keeps at most one activity timer task for its earliest pending timeout: every
//! transaction that changes its activities creates the next one if it is missing
//! (`CreateNextActivityTimer`). When a timer fires, every expired timeout is processed at once.
//! Start-to-close and heartbeat timeouts retry the activity while its retry policy allows;
//! schedule-to-start and schedule-to-close timeouts end it. A heartbeat timer that finds a newer
//! heartbeat re-arms itself, which costs a mutable-state write.

use crate::sim::executor::Time;

use super::params::{ActTimeouts, RetryPolicyP, StepP};
use super::queues::TaskSpec;
use super::types::TaskType;
use super::world::*;

/// Activity timeout kinds, in the bit order of Temporal's `TimerTaskStatus`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum TimeoutKind {
    StartToClose = 0,
    ScheduleToStart = 1,
    ScheduleToClose = 2,
    Heartbeat = 3,
}

impl TimeoutKind {
    pub const ALL: [TimeoutKind; 4] = [
        TimeoutKind::StartToClose,
        TimeoutKind::ScheduleToStart,
        TimeoutKind::ScheduleToClose,
        TimeoutKind::Heartbeat,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            TimeoutKind::StartToClose => "StartToClose",
            TimeoutKind::ScheduleToStart => "ScheduleToStart",
            TimeoutKind::ScheduleToClose => "ScheduleToClose",
            TimeoutKind::Heartbeat => "Heartbeat",
        }
    }

    pub fn idx(self) -> usize {
        self as usize
    }

    fn bit(self) -> u8 {
        1 << self as u8
    }

    /// Start-to-close and heartbeat timeouts end an attempt, which the retry policy may retry;
    /// schedule-to-start and schedule-to-close timeouts end the activity (`RETRY_STATE_TIMEOUT`).
    pub fn retryable(self) -> bool {
        matches!(self, TimeoutKind::StartToClose | TimeoutKind::Heartbeat)
    }
}

/// The timer task's reference to its timeout: the attempt and kind, packed into `r2`.
pub fn encode(attempt: u32, kind: TimeoutKind) -> u32 {
    attempt << 2 | kind as u32
}

pub fn decode(r2: u32) -> (u32, TimeoutKind) {
    (r2 >> 2, TimeoutKind::ALL[(r2 & 3) as usize])
}

/// A pending activity timeout (`TimerSequenceID`).
#[derive(Clone, Copy, Debug)]
pub struct ActTimer {
    pub at: Time,
    pub kind: TimeoutKind,
    pub seq: u32,
    pub attempt: u32,
    /// its timer task exists
    pub created: bool,
}

fn step_params(ctx: &Ctx, wf_type: usize, step: usize) -> Option<(ActTimeouts, RetryPolicyP)> {
    match ctx.p.wf_types[wf_type].steps.get(step) {
        Some(StepP::Activity {
            timeouts, retry, ..
        }) => Some((*timeouts, *retry)),
        _ => None,
    }
}

/// `LoadAndSortActivityTimers`: the timeouts of `acts`, earliest first.
pub fn activity_timers(ctx: &Ctx, wf_type: usize, acts: &[ActInfo]) -> Vec<ActTimer> {
    let mut out = Vec::new();
    for a in acts {
        let Some((t, _)) = step_params(ctx, wf_type, a.step) else {
            continue;
        };
        let started = a.state == ActState::Started;
        let mut push = |at: Time, kind: TimeoutKind| {
            out.push(ActTimer {
                at,
                kind,
                seq: a.seq,
                attempt: a.attempt,
                created: a.timers & kind.bit() != 0,
            })
        };
        if t.schedule_to_close > 0 {
            push(
                a.first_scheduled_at + t.schedule_to_close,
                TimeoutKind::ScheduleToClose,
            );
        }
        if !started && t.schedule_to_start > 0 {
            push(
                a.scheduled_at + t.schedule_to_start,
                TimeoutKind::ScheduleToStart,
            );
        }
        if started && t.start_to_close > 0 {
            push(a.started_at + t.start_to_close, TimeoutKind::StartToClose);
        }
        if started && t.heartbeat > 0 {
            push(
                a.started_at.max(a.last_heartbeat) + t.heartbeat,
                TimeoutKind::Heartbeat,
            );
        }
    }
    out.sort_by_key(|x| (x.at, x.seq, x.kind));
    out
}

/// `CreateNextActivityTimer`: a timer task for the earliest pending timeout of `w`'s
/// activities, unless it already has one. Call at the end of every transaction that changes the
/// activities.
pub fn create_next_timer(ctx: &Ctx, w: &mut Wf, tasks: &mut Vec<TaskSpec>) {
    let timers = activity_timers(ctx, w.wf_type, &w.activities);
    let Some(first) = timers.first().copied() else {
        return;
    };
    if first.created {
        return;
    }
    if let Some(a) = w.activities.iter_mut().find(|a| a.seq == first.seq) {
        a.timers |= first.kind.bit();
        if first.kind == TimeoutKind::Heartbeat {
            a.hb_timer_at = first.at;
        }
    }
    tasks.push(TaskSpec::at(
        TaskType::TimerActivityTimeout,
        first.at,
        first.seq,
        encode(first.attempt, first.kind),
    ));
}

/// What `RetryActivity` decides for an attempt that failed (`timeout` = None) or timed out.
pub enum Next {
    /// the next attempt, after this delay
    Retry(Time),
    /// the activity fails for good
    Fail,
}

pub fn retry_decision(
    ctx: &Ctx,
    wf_type: usize,
    a: &ActInfo,
    timeout: Option<TimeoutKind>,
    now: Time,
) -> Next {
    let Some((t, r)) = step_params(ctx, wf_type, a.step) else {
        return Next::Fail;
    };
    if timeout.is_some_and(|k| !k.retryable()) {
        return Next::Fail;
    }
    let expiration = (t.schedule_to_close > 0).then(|| a.first_scheduled_at + t.schedule_to_close);
    match r.next_delay(a.attempt, now, expiration) {
        Some(d) => Next::Retry(d),
        None => Next::Fail,
    }
}

/// Move `a` to its next attempt, due after `delay` (`UpdateActivityInfoForRetries`: the
/// per-attempt timers are marked for recreation), and return the retry timer task that pushes
/// that attempt to matching.
pub fn schedule_retry(a: &mut ActInfo, now: Time, delay: Time) -> TaskSpec {
    a.state = ActState::Backoff;
    a.attempt += 1;
    a.scheduled_at = now + delay;
    a.started_at = 0;
    a.last_heartbeat = 0;
    a.timers &= TimeoutKind::ScheduleToClose.bit();
    TaskSpec::at(
        TaskType::TimerActivityRetryTimer,
        now + delay,
        a.seq,
        a.attempt,
    )
}

/// The effect of an activity timeout task, computed on a copy of the activities so it is applied
/// only once the write succeeds.
pub struct TimeoutPlan {
    pub activities: Vec<ActInfo>,
    /// retry timer tasks of retried attempts
    pub tasks: Vec<TaskSpec>,
    /// steps of the activities that failed for good
    pub failed_steps: Vec<usize>,
    /// timeouts that fired, by kind
    pub fired: [u64; 4],
}

/// `executeActivityTimeoutTask` for the task `(seq, r2)` firing at `fire_at`, at `now`: `None` when
/// nothing changes (`errNoTimerFired`), so the task writes nothing.
pub fn plan_timeouts(
    ctx: &Ctx,
    w: &Wf,
    seq: u32,
    r2: u32,
    fire_at: Time,
    now: Time,
) -> Option<TimeoutPlan> {
    let mut acts = w.activities.clone();
    let (_, kind) = decode(r2);
    let mut update = false;
    // the activity's current heartbeat timer: clear its mark so the write re-arms it
    if kind == TimeoutKind::Heartbeat
        && let Some(a) = acts.iter_mut().find(|a| a.seq == seq)
        && a.timers & TimeoutKind::Heartbeat.bit() != 0
        && fire_at >= a.hb_timer_at
    {
        a.timers &= !TimeoutKind::Heartbeat.bit();
        update = true;
    }
    let mut plan = TimeoutPlan {
        activities: Vec::new(),
        tasks: Vec::new(),
        failed_steps: Vec::new(),
        fired: [0; 4],
    };
    for tm in activity_timers(ctx, w.wf_type, &acts) {
        if tm.at > now {
            break;
        }
        // an earlier timeout in this pass may have ended the activity or its attempt
        let Some(i) = acts.iter().position(|a| a.seq == tm.seq) else {
            continue;
        };
        if tm.attempt < acts[i].attempt {
            continue;
        }
        plan.fired[tm.kind.idx()] += 1;
        match retry_decision(ctx, w.wf_type, &acts[i], Some(tm.kind), now) {
            Next::Retry(d) => plan.tasks.push(schedule_retry(&mut acts[i], now, d)),
            Next::Fail => {
                plan.failed_steps.push(acts[i].step);
                acts.remove(i);
            }
        }
        update = true;
    }
    update.then(|| {
        plan.activities = acts;
        plan
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timeout_references_round_trip() {
        for kind in TimeoutKind::ALL {
            for attempt in [1, 2, 37] {
                assert_eq!(decode(encode(attempt, kind)), (attempt, kind));
            }
        }
        assert!(TimeoutKind::StartToClose.retryable());
        assert!(TimeoutKind::Heartbeat.retryable());
        assert!(!TimeoutKind::ScheduleToStart.retryable());
        assert!(!TimeoutKind::ScheduleToClose.retryable());
    }

    #[test]
    fn timeouts_are_filled_in_like_the_server() {
        let s = 1_000_000;
        // schedule-to-close bounds the others and fills them in
        let t = ActTimeouts::normalize(None, Some(90 * s), Some(60 * s), Some(5 * s), 10 * s);
        assert_eq!(
            t,
            ActTimeouts {
                schedule_to_start: 60 * s,
                start_to_close: 60 * s,
                schedule_to_close: 60 * s,
                heartbeat: 5 * s,
            }
        );
        // start-to-close alone: no schedule-to-start or schedule-to-close
        let t = ActTimeouts::normalize(None, Some(30 * s), None, Some(60 * s), 10 * s);
        assert_eq!(
            (t.schedule_to_start, t.schedule_to_close, t.heartbeat),
            (0, 0, 30 * s)
        );
        // neither: tempdes's default start-to-close
        let t = ActTimeouts::normalize(Some(5 * s), None, None, None, 10 * s);
        assert_eq!((t.schedule_to_start, t.start_to_close), (5 * s, 10 * s));
    }

    #[test]
    fn retry_policy_backs_off_and_stops() {
        let r = RetryPolicyP {
            initial: 1_000_000,
            coefficient: 2.0,
            max_interval: 5_000_000,
            max_attempts: 4,
        };
        assert_eq!(r.next_delay(1, 0, None), Some(1_000_000));
        assert_eq!(r.next_delay(3, 0, None), Some(4_000_000));
        // attempts are counted with the first one
        assert_eq!(r.next_delay(4, 0, None), None);
        let unlimited = RetryPolicyP {
            max_attempts: 0,
            ..r
        };
        assert_eq!(unlimited.next_delay(10, 0, None), Some(5_000_000));
        // the next attempt may not start after the schedule-to-close deadline
        assert_eq!(unlimited.next_delay(2, 9_000_000, Some(10_000_000)), None);
        assert_eq!(
            unlimited.next_delay(2, 7_000_000, Some(10_000_000)),
            Some(2_000_000)
        );
    }
}
