//! One workflow history as a sequence of steps.
//!
//! Commands issued by the same workflow task form one batch: activities scheduled together run
//! in parallel, local activities (`LocalActivity` markers) ran inside that workflow task,
//! children started together run in parallel, and a timer on its own is a sleep. A timer next
//! to activities or children is a timeout guard and is left out. A batch that leaves nothing
//! running, followed by a signal that wakes the workflow, is a wait for signals; a timer in that
//! batch, cancelled by the signal, is the wait's timeout.
//!
//! Only each step's own time is kept: an activity's final attempt from start to close, a
//! workflow task from start to completion. Waits in the cluster (schedule-to-start, throttled
//! dispatch) are recorded separately, for comparison, because a history taken from a busy
//! cluster would otherwise bake its queueing into the workload.

use std::collections::{BTreeMap, HashMap};

use super::parse::{Event, Kind, duration_us, float, get, int, name};

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
    scheduled_us: i64,
    started_us: Option<i64>,
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
    /// the workflow waited for `count` signals, with an optional timeout
    SignalWait {
        count: u32,
        timeout_us: Option<u64>,
    },
}

impl Step {
    /// The step's shape, to tell apart executions that took different paths.
    pub fn signature(&self) -> String {
        match self {
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
    for e in events {
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
                let marker = get(&e.attrs, "markerName")
                    .and_then(|m| m.as_str())
                    .unwrap_or("");
                // Go and Java write `LocalActivity`; the Core-based SDKs `core_local_activity`
                if matches!(marker, "LocalActivity" | "core_local_activity") {
                    if let Some(&b) =
                        id_of(e, "workflowTaskCompletedEventId").and_then(|w| batch_of.get(&w))
                    {
                        batches[b].local_activities += 1;
                    }
                } else {
                    *t.skipped.entry(format!("marker {marker}")).or_default() += 1;
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
        if !b.activities.is_empty() {
            t.steps.push(Step::Activities(
                b.activities
                    .iter()
                    .filter_map(|id| activities.get(id).cloned())
                    .collect(),
            ));
        }
        if !b.children.is_empty() {
            t.steps.push(Step::Children(b.children.clone()));
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
