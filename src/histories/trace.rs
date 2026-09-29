//! One workflow history as a sequence of steps.
//!
//! Commands issued by the same workflow task form one batch: activities scheduled together run
//! in parallel, local activities (`LocalActivity` markers) ran inside that workflow task,
//! children started together run in parallel, as do activities and children started together,
//! and a timer on its own is a sleep. A timer next
//! to activities or children is a timeout guard and is left out. A batch that leaves nothing
//! running, followed by a signal that wakes the workflow, is a wait for signals; a timer in that
//! batch, cancelled by the signal, is the wait's timeout.
//!
//! Only each step's own time is kept: an activity's final attempt from start to close, a
//! workflow task from start to completion. Waits in the cluster (schedule-to-start, throttled
//! dispatch) are recorded separately, for comparison, because a history taken from a busy
//! cluster would otherwise bake its queueing into the workload.

use std::collections::{BTreeMap, HashMap};

use serde_json::Value;

use super::parse::{Event, Kind, duration_us, enum_value, float, get, int, name, path};

/// Timeouts at or above a year are how servers fill in "no timeout" (a 10-year default run
/// timeout, for example); they count as unset.
const UNSET_TIMEOUT_US: u64 = 365 * 86_400 * 1_000_000;

/// How a workflow execution ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Outcome {
    Completed,
    Failed,
    TimedOut,
    Terminated,
    Canceled,
    ContinuedAsNew,
    /// still running when exported
    Open,
}

/// How an activity ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ActOutcome {
    Completed,
    Failed,
    TimedOut,
    Canceled,
    Open,
}

/// Why an activity's retries stopped, as the server records it on the failure or timeout event
/// (`RetryState`, from `RetryActivity` in `service/history/workflow/mutable_state_impl.go`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetryState {
    /// a non-retryable error
    NonRetryable,
    /// the policy's attempts ran out
    MaximumAttempts,
    /// the next attempt would start after schedule-to-close, or a schedule timeout fired
    Timeout,
    /// no retry policy: the first failure ends the activity
    NoPolicy,
    /// the workflow asked to cancel it
    CancelRequested,
    Other,
}

impl RetryState {
    fn parse(v: &Value) -> Option<RetryState> {
        Some(match enum_value(v, "RETRY_STATE_")?.as_str() {
            "nonretryablefailure" => RetryState::NonRetryable,
            "maximumattemptsreached" => RetryState::MaximumAttempts,
            "timeout" => RetryState::Timeout,
            "retrypolicynotset" => RetryState::NoPolicy,
            "cancelrequested" => RetryState::CancelRequested,
            _ => RetryState::Other,
        })
    }
}

/// An activity timeout (`TimeoutType`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimeoutType {
    StartToClose,
    ScheduleToStart,
    ScheduleToClose,
    Heartbeat,
}

impl TimeoutType {
    /// The timeout a failure reports (`timeoutFailureInfo.timeoutType`).
    fn of_failure(failure: &Value) -> Option<TimeoutType> {
        let t = path(failure, &["timeoutFailureInfo", "timeoutType"])?;
        match enum_value(t, "TIMEOUT_TYPE_")?.as_str() {
            "starttoclose" => Some(TimeoutType::StartToClose),
            "scheduletostart" => Some(TimeoutType::ScheduleToStart),
            "scheduletoclose" => Some(TimeoutType::ScheduleToClose),
            "heartbeat" => Some(TimeoutType::Heartbeat),
            _ => None,
        }
    }
}

/// How an activity's attempts ended, for its attempt plan.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Ending {
    /// the last attempt didn't fail: it completed, was cancelled, or is still running
    Completed,
    /// the last attempt failed with a non-retryable error
    NonRetryable,
    /// the attempts kept failing until the policy's attempts or schedule-to-close ran out
    RanOut,
    /// a task waited in its queue past schedule-to-start: the recorded cluster's queueing, not
    /// the activity's
    QueueTimeout,
}

/// An activity's retry policy as recorded when it was scheduled.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Retry {
    pub initial_us: u64,
    pub coefficient: f64,
    pub max_interval_us: u64,
    /// 0 = unlimited
    pub max_attempts: u32,
}

/// Activity timeouts in microseconds; 0 = unset.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timeouts {
    pub schedule_to_start: u64,
    pub start_to_close: u64,
    pub schedule_to_close: u64,
    pub heartbeat: u64,
}

/// One activity of a history.
#[derive(Clone, Debug)]
pub struct Activity {
    pub activity_type: String,
    pub task_queue: String,
    /// the final attempt's number (the only attempt a history records)
    pub attempts: u32,
    /// the final attempt from start to close
    pub run_us: Option<u64>,
    pub outcome: ActOutcome,
    pub timeouts: Timeouts,
    pub retry: Option<Retry>,
    /// why its retries stopped, when it failed for good
    pub retry_state: Option<RetryState>,
    /// the timeout that ended it, when it timed out
    pub timeout_type: Option<TimeoutType>,
    /// its failure is marked non-retryable (`applicationFailureInfo.nonRetryable`)
    pub non_retryable_error: bool,
    /// the timeout that ended the attempt before the last (the started event's `lastFailure`)
    pub last_failure_timeout: Option<TimeoutType>,
    scheduled_us: i64,
    started_us: Option<i64>,
}

impl Activity {
    /// From the first schedule to the final attempt's start: the attempts before it, the retry
    /// intervals between them, and each attempt's wait in the task queue.
    pub fn start_gap_us(&self) -> Option<u64> {
        self.started_us
            .map(|s| s.saturating_sub(self.scheduled_us).max(0) as u64)
    }

    /// How its attempts ended: by the recorded retry state, or without one, by its failure and
    /// retry policy.
    pub fn ending(&self) -> Ending {
        if !matches!(self.outcome, ActOutcome::Failed | ActOutcome::TimedOut) {
            return Ending::Completed;
        }
        if self.timeout_type == Some(TimeoutType::ScheduleToStart) {
            return Ending::QueueTimeout;
        }
        match self.retry_state {
            Some(RetryState::NonRetryable | RetryState::NoPolicy) => Ending::NonRetryable,
            Some(RetryState::MaximumAttempts | RetryState::Timeout) => Ending::RanOut,
            Some(RetryState::CancelRequested) => Ending::Completed,
            _ if self.non_retryable_error => Ending::NonRetryable,
            _ if self.timeout_type == Some(TimeoutType::ScheduleToClose)
                || self
                    .retry
                    .is_some_and(|r| r.max_attempts > 0 && self.attempts >= r.max_attempts) =>
            {
                Ending::RanOut
            }
            // it failed with retries left: the failure ended it
            _ => Ending::NonRetryable,
        }
    }
}

/// A step of a workflow's program.
#[derive(Clone, Debug)]
pub enum Step {
    /// activities scheduled by one workflow task: one, or several in parallel
    Activities(Vec<Activity>),
    /// local activities run inside one workflow task, each taking about `per_activity_us`
    LocalActivities {
        count: u32,
        per_activity_us: u64,
    },
    Timer {
        duration_us: u64,
    },
    /// child workflows started by one workflow task, by type
    Children(Vec<String>),
    /// activities and child workflows started by one workflow task
    Parallel {
        activities: Vec<Activity>,
        children: Vec<String>,
    },
    /// the workflow waited for `count` signals, with an optional timeout
    SignalWait {
        count: u32,
        timeout_us: Option<u64>,
    },
}

impl Step {
    /// The activities it started.
    pub fn activities(&self) -> &[Activity] {
        match self {
            Step::Activities(a) | Step::Parallel { activities: a, .. } => a,
            _ => &[],
        }
    }

    /// The types of the child workflows it started.
    pub fn children(&self) -> &[String] {
        match self {
            Step::Children(c) | Step::Parallel { children: c, .. } => c,
            _ => &[],
        }
    }

    /// The step's shape, to tell apart executions that took different paths.
    pub fn signature(&self) -> String {
        match self {
            Step::Parallel {
                activities,
                children,
            } => format!(
                "{} with {}",
                Step::Activities(activities.clone()).signature(),
                Step::Children(children.clone()).signature()
            ),
            Step::Activities(a) => {
                let mut types: Vec<&str> = a.iter().map(|x| x.activity_type.as_str()).collect();
                types.sort_unstable();
                format!("activity {}", types.join(" + "))
            }
            Step::LocalActivities { count, .. } => format!("local activity ×{count}"),
            Step::Timer { .. } => "timer".into(),
            Step::Children(t) => {
                let mut types: Vec<&str> = t.iter().map(String::as_str).collect();
                types.sort_unstable();
                format!("child {}", types.join(" + "))
            }
            Step::SignalWait { count, .. } => format!("signal ×{count}"),
        }
    }
}

/// A workflow task's `historySizeBytes`: the size of the events written before it, as the
/// server counts it (`ExecutionStats.HistorySize`), with what those events were.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SizeSample {
    pub bytes: u64,
    pub events: u32,
    pub signals: u32,
    /// events that carry a payload where the simulator charges one: the workflow's input,
    /// activities' and children's inputs and results, and local activities' markers
    pub payloads: u32,
    /// events the simulator doesn't model (other markers, search attribute upserts, updates,
    /// ...), whose data counts in `bytes` too
    pub unmodelled: u32,
}

/// One workflow execution read from its history.
#[derive(Clone, Debug)]
pub struct Trace {
    pub workflow_type: String,
    pub task_queue: String,
    /// started as a child of another workflow
    pub is_child: bool,
    pub start_us: i64,
    pub end_us: Option<i64>,
    pub outcome: Outcome,
    pub steps: Vec<Step>,
    /// workflow tasks from start to completion (those running local activities left out)
    pub wft_processing_us: Vec<u64>,
    pub wft_timeouts: u32,
    /// waits in the cluster: first attempts' activity schedule-to-start, and workflow tasks'
    pub activity_queue_wait_us: Vec<u64>,
    pub wft_queue_wait_us: Vec<u64>,
    /// event kinds the importer doesn't model, with counts
    pub skipped: BTreeMap<String, u32>,
    pub notes: Vec<String>,
    /// the history's size before its last workflow task that recorded one
    pub size: Option<SizeSample>,
}

/// What one workflow task's completion issued.
#[derive(Default)]
struct Batch {
    completed_id: i64,
    span_us: u64,
    local_activities: u32,
    activities: Vec<i64>,
    timers: Vec<i64>,
    children: Vec<String>,
}

struct Timer {
    duration_us: u64,
    fired: bool,
    canceled: bool,
}

/// Read one execution from its events.
pub fn trace(events: &[Event]) -> Result<Trace, String> {
    let first = events.first().ok_or("empty history")?;
    if first.kind != Kind::WorkflowStarted {
        return Err("history doesn't start with WorkflowExecutionStarted".into());
    }
    let mut t = Trace {
        workflow_type: name(&first.attrs, "workflowType").ok_or("no workflow type")?,
        task_queue: name(&first.attrs, "taskQueue").unwrap_or_default(),
        is_child: get(&first.attrs, "parentWorkflowExecution").is_some_and(|p| !p.is_null()),
        start_us: first.time_us,
        end_us: None,
        outcome: Outcome::Open,
        steps: Vec::new(),
        wft_processing_us: Vec::new(),
        wft_timeouts: 0,
        activity_queue_wait_us: Vec::new(),
        wft_queue_wait_us: Vec::new(),
        skipped: BTreeMap::new(),
        notes: Vec::new(),
        size: None,
    };
    let id_of = |e: &Event, key: &str| get(&e.attrs, key).and_then(int);
    let mut wft_scheduled: HashMap<i64, i64> = HashMap::new();
    let mut wft_started: HashMap<i64, i64> = HashMap::new();
    let mut wft_scheduled_ids: Vec<i64> = Vec::new();
    let mut batches: Vec<Batch> = Vec::new();
    let mut batch_of: HashMap<i64, usize> = HashMap::new();
    let mut activities: HashMap<i64, Activity> = HashMap::new();
    let mut timers: HashMap<i64, Timer> = HashMap::new();
    let mut signals: Vec<i64> = Vec::new();
    // activities by scheduled event and children by initiated event, with the event that
    // closed them (none yet: still running)
    let mut activity_closed: HashMap<i64, i64> = HashMap::new();
    let mut child_closed: HashMap<i64, i64> = HashMap::new();
    // signals and payload-carrying events so far, for workflow tasks' `historySizeBytes`
    let (mut signals_seen, mut payloads_seen, mut unmodelled_seen) = (0, 0, 0);
    for e in events {
        // a workflow task started with the events before it written: their size, unless the
        // task started with the workflow (eager start) and nothing had been written yet
        if e.kind == Kind::WftStarted
            && e.id > 1
            && let Some(bytes) = get(&e.attrs, "historySizeBytes")
                .and_then(int)
                .filter(|&b| b > 0)
        {
            t.size = Some(SizeSample {
                bytes: bytes as u64,
                events: (e.id - 1) as u32,
                signals: signals_seen,
                payloads: payloads_seen,
                unmodelled: unmodelled_seen,
            });
        }
        match e.kind {
            Kind::Signaled => signals_seen += 1,
            Kind::WorkflowStarted
            | Kind::ActivityScheduled
            | Kind::ActivityCompleted
            | Kind::ChildInitiated
            | Kind::ChildCompleted => payloads_seen += 1,
            Kind::Marker if local_activity(e) => payloads_seen += 1,
            Kind::Marker | Kind::Other => unmodelled_seen += 1,
            _ => {}
        }
        match e.kind {
            Kind::WftScheduled => {
                wft_scheduled.insert(e.id, e.time_us);
                wft_scheduled_ids.push(e.id);
            }
            Kind::WftStarted => {
                wft_started.insert(e.id, e.time_us);
                if let Some(s) = id_of(e, "scheduledEventId").and_then(|s| wft_scheduled.get(&s)) {
                    t.wft_queue_wait_us
                        .push(e.time_us.saturating_sub(*s).max(0) as u64);
                }
            }
            Kind::WftCompleted => {
                let span_us = id_of(e, "startedEventId")
                    .and_then(|s| wft_started.get(&s))
                    .map_or(0, |s| e.time_us.saturating_sub(*s).max(0) as u64);
                batch_of.insert(e.id, batches.len());
                batches.push(Batch {
                    completed_id: e.id,
                    span_us,
                    ..Default::default()
                });
            }
            Kind::WftTimedOut => t.wft_timeouts += 1,
            Kind::ActivityScheduled => {
                let a = &e.attrs;
                let timeout = |k: &str| {
                    get(a, k)
                        .and_then(duration_us)
                        .filter(|&v| v < UNSET_TIMEOUT_US)
                        .unwrap_or(0)
                };
                let retry = get(a, "retryPolicy")
                    .filter(|r| r.is_object())
                    .map(|r| Retry {
                        initial_us: get(r, "initialInterval")
                            .and_then(duration_us)
                            .unwrap_or(1_000_000),
                        coefficient: get(r, "backoffCoefficient").and_then(float).unwrap_or(2.0),
                        max_interval_us: get(r, "maximumInterval")
                            .and_then(duration_us)
                            .unwrap_or(0),
                        max_attempts: get(r, "maximumAttempts").and_then(int).unwrap_or(0).max(0)
                            as u32,
                    });
                activities.insert(
                    e.id,
                    Activity {
                        activity_type: name(a, "activityType").unwrap_or_else(|| "activity".into()),
                        task_queue: name(a, "taskQueue").unwrap_or_default(),
                        attempts: 1,
                        run_us: None,
                        outcome: ActOutcome::Open,
                        timeouts: Timeouts {
                            schedule_to_start: timeout("scheduleToStartTimeout"),
                            start_to_close: timeout("startToCloseTimeout"),
                            schedule_to_close: timeout("scheduleToCloseTimeout"),
                            heartbeat: timeout("heartbeatTimeout"),
                        },
                        retry,
                        retry_state: None,
                        timeout_type: None,
                        non_retryable_error: false,
                        last_failure_timeout: None,
                        scheduled_us: e.time_us,
                        started_us: None,
                    },
                );
                if let Some(&b) =
                    id_of(e, "workflowTaskCompletedEventId").and_then(|w| batch_of.get(&w))
                {
                    batches[b].activities.push(e.id);
                }
            }
            Kind::ActivityStarted => {
                if let Some(a) = id_of(e, "scheduledEventId").and_then(|s| activities.get_mut(&s)) {
                    a.attempts = get(&e.attrs, "attempt").and_then(int).unwrap_or(1).max(1) as u32;
                    a.started_us = Some(e.time_us);
                    a.last_failure_timeout =
                        get(&e.attrs, "lastFailure").and_then(TimeoutType::of_failure);
                    if a.attempts == 1 {
                        t.activity_queue_wait_us
                            .push(e.time_us.saturating_sub(a.scheduled_us).max(0) as u64);
                    }
                }
            }
            Kind::ActivityCompleted
            | Kind::ActivityFailed
            | Kind::ActivityTimedOut
            | Kind::ActivityCanceled => {
                if let Some(s) = id_of(e, "scheduledEventId") {
                    activity_closed.insert(s, e.id);
                }
                if let Some(a) = id_of(e, "scheduledEventId").and_then(|s| activities.get_mut(&s)) {
                    a.outcome = match e.kind {
                        Kind::ActivityCompleted => ActOutcome::Completed,
                        Kind::ActivityFailed => ActOutcome::Failed,
                        Kind::ActivityTimedOut => ActOutcome::TimedOut,
                        _ => ActOutcome::Canceled,
                    };
                    a.run_us = a
                        .started_us
                        .map(|s| e.time_us.saturating_sub(s).max(0) as u64);
                    let failure = get(&e.attrs, "failure");
                    a.retry_state = get(&e.attrs, "retryState").and_then(RetryState::parse);
                    a.timeout_type = failure.and_then(TimeoutType::of_failure);
                    a.non_retryable_error = failure
                        .and_then(|f| path(f, &["applicationFailureInfo", "nonRetryable"]))
                        .and_then(Value::as_bool)
                        .unwrap_or(false);
                }
            }
            Kind::TimerStarted => {
                timers.insert(
                    e.id,
                    Timer {
                        duration_us: get(&e.attrs, "startToFireTimeout")
                            .and_then(duration_us)
                            .unwrap_or(0),
                        fired: false,
                        canceled: false,
                    },
                );
                if let Some(&b) =
                    id_of(e, "workflowTaskCompletedEventId").and_then(|w| batch_of.get(&w))
                {
                    batches[b].timers.push(e.id);
                }
            }
            Kind::TimerFired | Kind::TimerCanceled => {
                if let Some(tm) = id_of(e, "startedEventId").and_then(|s| timers.get_mut(&s)) {
                    tm.fired |= e.kind == Kind::TimerFired;
                    tm.canceled |= e.kind == Kind::TimerCanceled;
                }
            }
            Kind::Marker => {
                if local_activity(e) {
                    if let Some(&b) =
                        id_of(e, "workflowTaskCompletedEventId").and_then(|w| batch_of.get(&w))
                    {
                        batches[b].local_activities += 1;
                    }
                } else {
                    *t.skipped
                        .entry(format!("marker {}", marker_name(e)))
                        .or_default() += 1;
                }
            }
            Kind::ChildInitiated => {
                child_closed.insert(e.id, i64::MAX);
                let child = name(&e.attrs, "workflowType").unwrap_or_else(|| "child".into());
                if let Some(&b) =
                    id_of(e, "workflowTaskCompletedEventId").and_then(|w| batch_of.get(&w))
                {
                    batches[b].children.push(child);
                }
            }
            Kind::Signaled => signals.push(e.id),
            Kind::WorkflowCompleted
            | Kind::WorkflowFailed
            | Kind::WorkflowTimedOut
            | Kind::WorkflowTerminated
            | Kind::WorkflowCanceled
            | Kind::WorkflowContinuedAsNew => {
                t.end_us = Some(e.time_us);
                t.outcome = match e.kind {
                    Kind::WorkflowCompleted => Outcome::Completed,
                    Kind::WorkflowFailed => Outcome::Failed,
                    Kind::WorkflowTimedOut => Outcome::TimedOut,
                    Kind::WorkflowTerminated => Outcome::Terminated,
                    Kind::WorkflowCanceled => Outcome::Canceled,
                    _ => Outcome::ContinuedAsNew,
                };
            }
            Kind::ChildStartFailed
            | Kind::ChildCompleted
            | Kind::ChildFailed
            | Kind::ChildCanceled
            | Kind::ChildTimedOut
            | Kind::ChildTerminated => {
                if let Some(c) = id_of(e, "initiatedEventId").and_then(|i| child_closed.get_mut(&i))
                {
                    *c = e.id;
                }
            }
            Kind::WorkflowStarted
            | Kind::WftFailed
            | Kind::ActivityCancelRequested
            | Kind::ChildStarted => {}
            Kind::Other => *t.skipped.entry(e.type_name.clone()).or_default() += 1,
        }
    }
    wft_scheduled_ids.sort_unstable();

    // whether activities or children issued before `at` were still running then
    let busy_at = |at: i64| {
        activities
            .keys()
            .any(|s| *s < at && activity_closed.get(s).is_none_or(|c| *c > at))
            || child_closed.iter().any(|(i, c)| *i < at && *c > at)
    };
    let mut guards = 0;
    for (i, b) in batches.iter().enumerate() {
        if b.local_activities > 0 {
            t.steps.push(Step::LocalActivities {
                count: b.local_activities,
                per_activity_us: b.span_us / u64::from(b.local_activities),
            });
        } else {
            t.wft_processing_us.push(b.span_us);
        }
        let started: Vec<Activity> = b
            .activities
            .iter()
            .filter_map(|id| activities.get(id).cloned())
            .collect();
        match (started.is_empty(), b.children.is_empty()) {
            (false, false) => t.steps.push(Step::Parallel {
                activities: started,
                children: b.children.clone(),
            }),
            (false, true) => t.steps.push(Step::Activities(started)),
            (true, false) => t.steps.push(Step::Children(b.children.clone())),
            (true, true) => {}
        }
        if !b.activities.is_empty() || !b.children.is_empty() {
            guards += b.timers.len();
            continue;
        }
        // nothing left running but timers: a signal before the next workflow task means the
        // workflow was waiting for it
        let next_wft = wft_scheduled_ids
            .iter()
            .find(|&&s| s > b.completed_id)
            .copied()
            .unwrap_or(i64::MAX);
        let mut woken_by = signals
            .iter()
            .filter(|&&s| s > b.completed_id && s < next_wft)
            .count() as u32;
        if busy_at(b.completed_id) {
            // signals that arrive while earlier work runs are buffered, not waited for
            woken_by = 0;
        }
        let timer = b.timers.first().and_then(|id| timers.get(id));
        let last_batch = i + 1 == batches.len();
        match timer {
            Some(tm) if tm.fired || (!tm.canceled && woken_by == 0) => t.steps.push(Step::Timer {
                duration_us: tm.duration_us,
            }),
            _ if woken_by > 0 => t.steps.push(Step::SignalWait {
                count: woken_by,
                timeout_us: timer.map(|tm| tm.duration_us),
            }),
            Some(tm) if !last_batch => t.steps.push(Step::Timer {
                duration_us: tm.duration_us,
            }),
            _ => {}
        }
        guards += b.timers.len().saturating_sub(1);
    }
    let waited: u32 = t
        .steps
        .iter()
        .map(|s| match s {
            Step::SignalWait { count, .. } => *count,
            _ => 0,
        })
        .sum();
    let buffered = (signals.len() as u32).saturating_sub(waited);
    if buffered > 0 {
        t.notes.push(format!(
            "{buffered} signals arrived while the workflow was busy; they are buffered, not waits"
        ));
    }
    if guards > 0 {
        t.notes.push(format!(
            "{guards} timers started with activities or children were left out as timeout guards"
        ));
    }
    let unfinished = activities
        .values()
        .filter(|a| a.outcome == ActOutcome::Open)
        .count();
    if unfinished > 0 && t.outcome != Outcome::Open {
        t.notes
            .push(format!("{unfinished} activities never closed"));
    }
    Ok(t)
}

fn marker_name(e: &Event) -> &str {
    get(&e.attrs, "markerName")
        .and_then(Value::as_str)
        .unwrap_or("")
}

/// A local activity's marker: Go and Java write `LocalActivity`, the Core-based SDKs
/// `core_local_activity`.
fn local_activity(e: &Event) -> bool {
    matches!(marker_name(e), "LocalActivity" | "core_local_activity")
}
