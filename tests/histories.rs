//! `tempdes workload import`: histories in both export spellings become steps, pooled
//! programs and a scenario that simulates.

use serde_json::{Value, json};
use tempdes::config::scenario::Scenario;
use tempdes::histories::trace::{Outcome, Step, trace};
use tempdes::histories::{Options, parse::parse_history, program};
use tempdes::report;
use tempdes::run::{self, Overrides};

/// Builds a workflow history the way the CLI or the Web UI export it, with event times in
/// milliseconds after `start_ms`.
struct History {
    events: Vec<Value>,
    /// each event's type, unprefixed
    kinds: Vec<String>,
    /// `EVENT_TYPE_ACTIVITY_TASK_SCHEDULED` rather than `ActivityTaskScheduled`
    prefixed: bool,
    start_ms: f64,
}

impl History {
    fn new(workflow_type: &str, start_ms: f64, prefixed: bool) -> History {
        let mut h = History {
            events: Vec::new(),
            kinds: Vec::new(),
            prefixed,
            start_ms,
        };
        h.event(
            "WorkflowExecutionStarted",
            0.0,
            json!({"workflowType": {"name": workflow_type}, "taskQueue": {"name": "orders"}}),
        );
        h
    }

    fn event(&mut self, kind: &str, at_ms: f64, attrs: Value) -> i64 {
        let id = self.events.len() as i64 + 1;
        let event_type = if self.prefixed {
            let mut s = String::from("EVENT_TYPE");
            for c in kind.chars() {
                if c.is_ascii_uppercase() {
                    s.push('_');
                }
                s.push(c.to_ascii_uppercase());
            }
            s
        } else {
            kind.to_string()
        };
        let key = format!("{}{}EventAttributes", kind[..1].to_lowercase(), &kind[1..]);
        self.kinds.push(kind.to_string());
        let us = ((self.start_ms + at_ms) * 1000.0).round() as i64;
        self.events.push(json!({
            "eventId": id.to_string(),
            "eventTime": format!(
                "2026-09-28T10:{:02}:{:02}.{:06}Z",
                us / 60_000_000,
                us / 1_000_000 % 60,
                us % 1_000_000
            ),
            "eventType": event_type,
            key: attrs,
        }));
        id
    }

    /// A workflow task scheduled at `at_ms` that runs for `span_ms`; returns its completion.
    fn wft(&mut self, at_ms: f64, span_ms: f64) -> i64 {
        let s = self.event("WorkflowTaskScheduled", at_ms, json!({}));
        let st = self.event(
            "WorkflowTaskStarted",
            at_ms + 1.0,
            json!({"scheduledEventId": s.to_string()}),
        );
        self.event(
            "WorkflowTaskCompleted",
            at_ms + 1.0 + span_ms,
            json!({"scheduledEventId": s.to_string(), "startedEventId": st.to_string()}),
        )
    }

    fn schedule(&mut self, wft: i64, activity: &str, at_ms: f64) -> i64 {
        self.event(
            "ActivityTaskScheduled",
            at_ms,
            json!({
                "activityType": {"name": activity},
                "taskQueue": {"name": "orders"},
                "scheduleToCloseTimeout": "315360000s",
                "startToCloseTimeout": "10s",
                "heartbeatTimeout": "0s",
                "workflowTaskCompletedEventId": wft.to_string(),
                "retryPolicy": {"initialInterval": "1s", "backoffCoefficient": 2, "maximumInterval": "100s"}
            }),
        )
    }

    /// The final attempt of activity `scheduled`, from `start_ms` to `end_ms`.
    fn run(&mut self, scheduled: i64, attempt: u32, start_ms: f64, end_ms: f64) {
        let st = self.event(
            "ActivityTaskStarted",
            start_ms,
            json!({"scheduledEventId": scheduled.to_string(), "attempt": attempt}),
        );
        self.event(
            "ActivityTaskCompleted",
            end_ms,
            json!({"scheduledEventId": scheduled.to_string(), "startedEventId": st.to_string()}),
        );
    }

    /// The final attempt of activity `scheduled` failing: started at `start_ms` (`None`: it never
    /// started), with `started` added to its started event, and closed at `end_ms` by `close`
    /// (`ActivityTaskFailed` or `ActivityTaskTimedOut`) with `closed` added.
    #[allow(clippy::too_many_arguments)]
    fn fail(
        &mut self,
        scheduled: i64,
        attempt: u32,
        start_ms: Option<f64>,
        started: Value,
        close: &str,
        end_ms: f64,
        closed: Value,
    ) {
        let mut attrs = json!({"scheduledEventId": scheduled.to_string()});
        if let Some(s) = start_ms {
            let mut a = json!({"scheduledEventId": scheduled.to_string(), "attempt": attempt});
            merge(&mut a, started);
            let st = self.event("ActivityTaskStarted", s, a);
            attrs["startedEventId"] = st.to_string().into();
        }
        merge(&mut attrs, closed);
        self.event(close, end_ms, attrs);
    }

    /// Record on each workflow task started the size of the events before it
    /// (`historySizeBytes`), as the server does, for events of 128 bytes that carry `payload`
    /// bytes of input or result, or 256 bytes for a signal.
    fn record_sizes(&mut self, payload: f64) {
        let mut size = 0.0;
        for (e, kind) in self.events.iter_mut().zip(&self.kinds) {
            if kind == "WorkflowTaskStarted" {
                e["workflowTaskStartedEventAttributes"]["historySizeBytes"] =
                    (size as u64).to_string().into();
            }
            size += 128.0
                + match kind.as_str() {
                    "WorkflowExecutionStarted"
                    | "ActivityTaskScheduled"
                    | "ActivityTaskCompleted"
                    | "StartChildWorkflowExecutionInitiated"
                    | "ChildWorkflowExecutionCompleted"
                    | "MarkerRecorded" => payload,
                    "WorkflowExecutionSignaled" => 256.0,
                    _ => 0.0,
                };
        }
    }

    /// Record the worker processes that started its tasks, as the started events' `identity`:
    /// `workflow` for workflow tasks and `activity` for activities, and the SDK the workflow
    /// worker reports on its first workflow task completion (`sdkMetadata`).
    fn record_workers(&mut self, workflow: &str, activity: &str, sdk: (&str, &str)) {
        let mut reported = false;
        for (e, kind) in self.events.iter_mut().zip(&self.kinds) {
            match kind.as_str() {
                "WorkflowTaskStarted" => {
                    e["workflowTaskStartedEventAttributes"]["identity"] = workflow.into();
                }
                "ActivityTaskStarted" => {
                    e["activityTaskStartedEventAttributes"]["identity"] = activity.into();
                }
                "WorkflowTaskCompleted" if !reported => {
                    e["workflowTaskCompletedEventAttributes"]["sdkMetadata"] =
                        json!({"sdkName": sdk.0, "sdkVersion": sdk.1});
                    reported = true;
                }
                _ => {}
            }
        }
    }

    fn json(&self) -> String {
        json!({"events": self.events}).to_string()
    }
}

fn merge(into: &mut Value, from: Value) {
    if let (Some(a), Value::Object(b)) = (into.as_object_mut(), from) {
        a.extend(b);
    }
}

/// A payment: one Authorize activity scheduled at 10ms, 20ms in the queue before each attempt,
/// failed attempts of 300ms, 1s, 2s, 4s… apart, and a final attempt of 100ms (50ms when it
/// fails). It succeeds on attempt `attempts`, or with `non_retryable`, fails for good there and
/// fails the workflow.
fn payment(start_ms: f64, attempts: u32, non_retryable: bool, prefixed: bool) -> String {
    let mut h = History::new("PaymentWorkflow", start_ms, prefixed);
    let w = h.wft(0.0, 5.0);
    let a = h.schedule(w, "Authorize", 10.0);
    let mut t = 10.0;
    for n in 1..attempts {
        t += 20.0 + 300.0 + 1000.0 * 2f64.powi(n as i32 - 1);
    }
    t += 20.0;
    let last_failure = if attempts > 1 {
        json!({"lastFailure": {"message": "declined", "applicationFailureInfo": {"type": "Busy"}}})
    } else {
        json!({})
    };
    if non_retryable {
        let state = if prefixed {
            "RETRY_STATE_NON_RETRYABLE_FAILURE"
        } else {
            "NonRetryableFailure"
        };
        h.fail(
            a,
            attempts,
            Some(t),
            last_failure,
            "ActivityTaskFailed",
            t + 50.0,
            json!({"retryState": state, "failure": {"message": "card declined", "applicationFailureInfo": {"type": "Declined", "nonRetryable": true}}}),
        );
        h.wft(t + 50.0, 2.0);
        h.event("WorkflowExecutionFailed", t + 53.0, json!({}));
    } else {
        let st = h.event(
            "ActivityTaskStarted",
            t,
            json!({"scheduledEventId": a.to_string(), "attempt": attempts, "lastFailure": last_failure["lastFailure"]}),
        );
        h.event(
            "ActivityTaskCompleted",
            t + 100.0,
            json!({"scheduledEventId": a.to_string(), "startedEventId": st.to_string()}),
        );
        h.wft(t + 100.0, 2.0);
        h.event("WorkflowExecutionCompleted", t + 103.0, json!({}));
    }
    h.json()
}

fn import(histories: &[String]) -> program::Program {
    let traces: Vec<_> = histories
        .iter()
        .map(|h| trace(&parse_history(h).unwrap()).unwrap())
        .collect();
    program::build(
        &traces,
        &Options {
            namespace: "payments".into(),
            rate: Some(10.0),
            ..Default::default()
        },
    )
}

/// An order: charge; publish, check and look up in parallel (the lookup took `lookup_attempts`,
/// 1s and 2s apart after 100ms tries, each attempt waiting 20ms in the cluster); a local
/// activity; a 2s sleep (left out when `sleep` is false); then a shipment child.
fn order(start_ms: f64, lookup_attempts: u32, sleep: bool, prefixed: bool) -> String {
    order_history(start_ms, lookup_attempts, sleep, prefixed).json()
}

fn order_history(start_ms: f64, lookup_attempts: u32, sleep: bool, prefixed: bool) -> History {
    let mut h = History::new("OrderWorkflow", start_ms, prefixed);
    let w = h.wft(0.0, 5.0);
    let charge = h.schedule(w, "Charge", 6.0);
    h.run(charge, 1, 26.0, 226.0);
    let w = h.wft(226.0, 4.0);
    let publish = h.schedule(w, "Publish", 231.0);
    let check = h.schedule(w, "Check", 231.0);
    let lookup = h.schedule(w, "Lookup", 231.0);
    h.run(publish, 1, 251.0, 351.0);
    h.wft(351.0, 2.0);
    h.run(check, 1, 251.0, 451.0);
    h.wft(451.0, 2.0);
    // earlier attempts aren't recorded: 100ms tries with 1s, 2s, 4s… between them
    let backoff: f64 = (1..lookup_attempts)
        .map(|n| 1000.0 * 2f64.powi(n as i32 - 1) + 120.0)
        .sum();
    let last = 251.0 + backoff;
    h.run(lookup, lookup_attempts, last, last + 100.0);
    // the next workflow task runs a local activity (40ms) and starts the sleep
    let w = h.wft(last + 100.0, 42.0);
    h.event(
        "MarkerRecorded",
        last + 143.0,
        json!({"markerName": "LocalActivity", "details": {"data": {}}, "workflowTaskCompletedEventId": w.to_string()}),
    );
    let mut t = last + 143.0;
    let w = if sleep {
        let timer = h.event(
            "TimerStarted",
            t,
            json!({"timerId": "1", "startToFireTimeout": "2s", "workflowTaskCompletedEventId": w.to_string()}),
        );
        t += 2000.0;
        h.event(
            "TimerFired",
            t,
            json!({"timerId": "1", "startedEventId": timer.to_string()}),
        );
        h.wft(t, 3.0)
    } else {
        w
    };
    let child = h.event(
        "StartChildWorkflowExecutionInitiated",
        t + 4.0,
        json!({"workflowType": {"name": "ShipmentWorkflow"}, "taskQueue": {"name": "orders"}, "workflowTaskCompletedEventId": w.to_string()}),
    );
    h.event(
        "ChildWorkflowExecutionCompleted",
        t + 500.0,
        json!({"initiatedEventId": child.to_string()}),
    );
    h.wft(t + 500.0, 3.0);
    h.event("WorkflowExecutionCompleted", t + 504.0, json!({}));
    h
}

fn steps_of(text: &str) -> Vec<Step> {
    trace(&parse_history(text).expect("parses"))
        .expect("traces")
        .steps
}

#[test]
fn an_order_history_becomes_its_steps() {
    for prefixed in [false, true] {
        let text = order(0.0, 4, true, prefixed);
        let t = trace(&parse_history(&text).unwrap()).unwrap();
        assert_eq!(t.workflow_type, "OrderWorkflow");
        assert_eq!(t.outcome, Outcome::Completed);
        let sig: Vec<String> = t.steps.iter().map(Step::signature).collect();
        assert_eq!(
            sig,
            [
                "activity Charge",
                "activity Check + Lookup + Publish",
                "local activity ×1",
                "timer",
                "child ShipmentWorkflow"
            ]
        );
        let Step::Activities(group) = &t.steps[1] else {
            panic!("parallel activities")
        };
        let lookup = group.iter().find(|a| a.activity_type == "Lookup").unwrap();
        assert_eq!(lookup.attempts, 4);
        // the final attempt's own time, not the retries before it
        assert_eq!(lookup.run_us, Some(100_000));
        // the 10-year schedule-to-close servers fill in counts as unset
        assert_eq!(lookup.timeouts.schedule_to_close, 0);
        assert_eq!(lookup.timeouts.start_to_close, 10_000_000);
        // cluster waits are kept apart: 20ms before each first attempt
        assert!(t.activity_queue_wait_us.iter().all(|&w| w == 20_000));
        let Step::Timer { duration_us } = t.steps[3] else {
            panic!("timer")
        };
        assert_eq!(duration_us, 2_000_000);
    }
}

#[test]
fn signal_waits_are_told_from_signals_buffered_during_work() {
    // idle after the first workflow task: the signal wakes the workflow
    let mut h = History::new("CartWorkflow", 0.0, false);
    h.wft(0.0, 2.0);
    h.event(
        "WorkflowExecutionSignaled",
        5000.0,
        json!({"signalName": "add"}),
    );
    h.wft(5000.0, 2.0);
    h.event("WorkflowExecutionCompleted", 5004.0, json!({}));
    let sig: Vec<String> = steps_of(&h.json()).iter().map(Step::signature).collect();
    assert_eq!(sig, ["signal ×1"]);

    // a signal while an activity runs is buffered
    let mut h = History::new("CartWorkflow", 0.0, false);
    let w = h.wft(0.0, 2.0);
    let a = h.schedule(w, "Price", 3.0);
    h.event(
        "WorkflowExecutionSignaled",
        50.0,
        json!({"signalName": "add"}),
    );
    h.wft(50.0, 2.0);
    h.run(a, 1, 10.0, 200.0);
    h.wft(200.0, 2.0);
    h.event("WorkflowExecutionCompleted", 204.0, json!({}));
    let t = trace(&parse_history(&h.json()).unwrap()).unwrap();
    let sig: Vec<String> = t.steps.iter().map(Step::signature).collect();
    assert_eq!(sig, ["activity Price"]);
    assert!(
        t.notes.iter().any(|n| n.contains("buffered")),
        "{:?}",
        t.notes
    );
}

#[test]
fn executions_are_pooled_by_path() {
    // six orders on one path, two of them with a lookup that took four attempts, and one that
    // skipped the sleep
    let mut traces = Vec::new();
    for i in 0..6 {
        let attempts = if i < 2 { 4 } else { 1 };
        traces.push(
            trace(&parse_history(&order(i as f64 * 1000.0, attempts, true, i % 2 == 0)).unwrap())
                .unwrap(),
        );
    }
    traces.push(trace(&parse_history(&order(6000.0, 1, false, false)).unwrap()).unwrap());
    let opts = Options {
        namespace: "orders".into(),
        rate: Some(70.0),
        ..Default::default()
    };
    let p = program::build(&traces, &opts);
    // the three activities started together are members of a parallel step, each with its own
    // settings: the check takes 200ms, the publish 100ms, and 2 of 6 lookups took four attempts
    for want in [
        "      - parallel:\n          # Check: 6 activities in 6 executions\n          - activity:\n              count: 1\n              duration: 200ms\n",
        "          # Publish: 6 activities in 6 executions\n          - activity:\n              count: 1\n              duration: 100ms\n",
        "          # Lookup: 6 activities in 6 executions, 33% retried\n",
        "              attempts: { 1: 0.667, 4: 0.333 }\n",
    ] {
        assert!(p.yaml.contains(want), "{want}\n{}", p.yaml);
    }
    // their earlier attempts ran 100ms: the time before the last attempt less 1s + 2s + 4s of
    // retry intervals and 20ms in the queue per attempt
    assert!(p.yaml.contains("failed_duration: 100ms"), "{}", p.yaml);
    assert!(
        p.summary
            .contains("failed attempts' durations were estimated")
            && p.summary.contains("(2 activities)"),
        "{}",
        p.summary
    );
    // the rarer path (1 of 7) is a type of its own, with its share of the rate
    assert!(p.yaml.contains("type: \"OrderWorkflow~2\""), "{}", p.yaml);
    assert!(
        p.yaml.contains("start_rate: 60/s") && p.yaml.contains("start_rate: 10/s"),
        "{}",
        p.yaml
    );
    // the child's histories weren't given: a stub keeps the scenario valid
    assert!(p.yaml.contains("type: \"ShipmentWorkflow\""), "{}", p.yaml);
    assert!(
        p.summary.contains("none of its histories were given"),
        "{}",
        p.summary
    );

    // a higher threshold folds the rarer path into the common one
    let folded = program::build(
        &traces,
        &Options {
            min_path_share: 0.2,
            ..opts
        },
    );
    assert!(!folded.yaml.contains("OrderWorkflow~2"), "{}", folded.yaml);
    assert!(folded.yaml.contains("start_rate: 70/s"), "{}", folded.yaml);
}

/// A sync: one Fetch activity with a 10s schedule-to-close timeout, scheduled at 10ms, after
/// which the workflow completes. It succeeds after 20ms in the queue and 100ms (`"ok"`), keeps
/// failing until the timeout fires during its third attempt, which started at 3.67s (`"ran
/// out"`), or waits in its queue past its 5s schedule-to-start timeout (`"queued"`).
fn sync(start_ms: f64, ending: &str, prefixed: bool) -> String {
    let mut h = History::new("SyncWorkflow", start_ms, prefixed);
    let w = h.wft(0.0, 5.0);
    let a = h.event(
        "ActivityTaskScheduled",
        10.0,
        json!({
            "activityType": {"name": "Fetch"},
            "taskQueue": {"name": "orders"},
            "scheduleToCloseTimeout": "10s",
            "scheduleToStartTimeout": "5s",
            "startToCloseTimeout": "10s",
            "workflowTaskCompletedEventId": w.to_string(),
            "retryPolicy": {"initialInterval": "1s", "backoffCoefficient": 2, "maximumInterval": "100s"}
        }),
    );
    let spell = |prefix: &str, long: &str, short: &str| {
        if prefixed {
            format!("{prefix}{long}")
        } else {
            short.to_string()
        }
    };
    let end = match ending {
        "ok" => {
            h.run(a, 1, 30.0, 130.0);
            130.0
        }
        "ran out" => {
            h.fail(
                a,
                3,
                Some(3670.0),
                json!({}),
                "ActivityTaskTimedOut",
                10_010.0,
                json!({
                    "retryState": spell("RETRY_STATE_", "TIMEOUT", "Timeout"),
                    "failure": {"timeoutFailureInfo": {"timeoutType": spell("TIMEOUT_TYPE_", "SCHEDULE_TO_CLOSE", "ScheduleToClose")}}
                }),
            );
            10_010.0
        }
        _ => {
            h.fail(
                a,
                1,
                None,
                json!({}),
                "ActivityTaskTimedOut",
                5010.0,
                json!({
                    "retryState": spell("RETRY_STATE_", "TIMEOUT", "Timeout"),
                    "failure": {"timeoutFailureInfo": {"timeoutType": spell("TIMEOUT_TYPE_", "SCHEDULE_TO_START", "ScheduleToStart")}}
                }),
            );
            5010.0
        }
    };
    h.wft(end, 2.0);
    h.event("WorkflowExecutionCompleted", end + 3.0, json!({}));
    h.json()
}

/// Wraps imported `workflows:` in a scenario with a worker fleet, and simulates it.
fn simulate(namespace: &str, workflows: &str) -> report::RunResult {
    let scenario = format!(
        "name: imported\nwarmup: 20s\nduration: 30s\ncluster:\n  num_history_shards: 64\n  replicas: {{ frontend: 1, history: 1, matching: 1, worker: 1 }}\n  persistence: {{ store: postgresql }}\nnamespaces: [ {{ name: {namespace} }} ]\nworkers:\n  - {{ name: w, namespace: {namespace}, task_queue: orders, processes: 2, activity_slots: 200 }}\n{workflows}"
    );
    let sc = Scenario::parse_str(&scenario).unwrap_or_else(|e| panic!("{e}\n{scenario}"));
    let params = run::prepare(&sc, &Overrides::default(), None).expect("parameters resolve");
    let out = run::run_params(params);
    report::analyze(&out.ctx, &out.info, None)
}

#[test]
fn failures_become_attempt_plans() {
    for prefixed in [false, true] {
        // 7 payments authorised at once, 2 on their third attempt, 1 declined on its second
        let histories: Vec<String> = (0..10)
            .map(|i| {
                let start = f64::from(i) * 10_000.0;
                match i {
                    0..7 => payment(start, 1, false, prefixed),
                    7 | 8 => payment(start, 3, false, prefixed),
                    _ => payment(start, 2, true, prefixed),
                }
            })
            .collect();
        let p = import(&histories);
        for want in [
            "30% retried, 10% failed for good",
            "attempts: { 1: 0.7, 3: 0.2 }",
            "non_retryable: { 2: 0.1 }",
            "duration: 100ms",
            // the declined attempt's 50ms, and 300ms for the five attempts before the last ones:
            // the time before the last attempt less 1s, 2s… of retry intervals and 20ms in the
            // queue per attempt
            "failed_duration: 300ms",
        ] {
            assert!(p.yaml.contains(want), "{want}\n{}", p.yaml);
        }
        // the declined payment failed its workflow
        assert!(!p.yaml.contains("on_failure"), "{}", p.yaml);
        assert!(p.summary.contains("(3 activities)"), "{}", p.summary);
        // a tenth of the workflows fail, as planned: not a hotspot
        let r = simulate("payments", &p.yaml);
        let w = &r.workflows[0];
        assert!(
            (w.failed_per_s / w.started_per_s - 0.1).abs() < 0.05,
            "{w:#?}"
        );
        assert_eq!(w.activities_non_retryable, w.activities_failed, "{w:#?}");
        assert!(
            !r.hotspots.iter().any(|h| h.category == "activity-timeouts"),
            "{:?}",
            r.hotspots.iter().map(|h| &h.title).collect::<Vec<_>>()
        );
    }
}

#[test]
fn timeouts_are_told_apart() {
    let histories: Vec<String> = ["ok", "ok", "ok", "ran out", "queued"]
        .iter()
        .enumerate()
        .map(|(i, e)| sync(i as f64 * 20_000.0, e, i % 2 == 0))
        .collect();
    let p = import(&histories);
    for want in [
        // the attempts that ran out of time need a fifth attempt to succeed, and the 1s, 2s,
        // 4s and 8s of retry intervals alone before it pass the 10s schedule-to-close
        "attempts: { 1: 0.75, 5: 0.25 }",
        // the workflows went on after the activity failed
        "on_failure: continue",
        "schedule_to_close_timeout: 10s",
        // the timed-out attempt's 6.34s, and 300ms for each of the two before it
        "failed_duration: { p50: 300ms, p99: 6.34s }",
    ] {
        assert!(p.yaml.contains(want), "{want}\n{}", p.yaml);
    }
    // the queued one is the recorded cluster's queueing: left out of the plans
    assert!(
        p.summary.contains("timed out waiting in a task queue")
            && p.summary.contains("(1 activity)"),
        "{}",
        p.summary
    );
    simulate("payments", &p.yaml);
}

#[test]
fn activities_and_children_started_together_become_a_parallel_step() {
    // a booking reserves two seats and starts its payment and notification children in the
    // same workflow task, then completes when all four are done
    let histories: Vec<String> = (0..4)
        .map(|i| {
            let mut h = History::new("BookingWorkflow", f64::from(i) * 5_000.0, i % 2 == 0);
            let w = h.wft(0.0, 5.0);
            let seats = [h.schedule(w, "Reserve", 6.0), h.schedule(w, "Reserve", 6.0)];
            let mut children = Vec::new();
            for child in ["PaymentWorkflow", "NotifyWorkflow"] {
                children.push(h.event(
                    "StartChildWorkflowExecutionInitiated",
                    6.0,
                    json!({"workflowType": {"name": child}, "taskQueue": {"name": "orders"}, "workflowTaskCompletedEventId": w.to_string()}),
                ));
            }
            for s in seats {
                h.run(s, 1, 26.0, 326.0);
            }
            for (c, at) in children.iter().zip([500.0, 800.0]) {
                h.event(
                    "ChildWorkflowExecutionCompleted",
                    at,
                    json!({"initiatedEventId": c.to_string()}),
                );
            }
            h.wft(800.0, 2.0);
            h.event("WorkflowExecutionCompleted", 803.0, json!({}));
            h.json()
        })
        .collect();
    let t = trace(&parse_history(&histories[0]).unwrap()).unwrap();
    let sig: Vec<String> = t.steps.iter().map(Step::signature).collect();
    assert_eq!(
        sig,
        ["activity Reserve + Reserve with child NotifyWorkflow + PaymentWorkflow"]
    );
    let p = import(&histories);
    let want = "      - parallel:
          # Reserve: 8 activities in 4 executions
          - activity:
              count: 2
              parallel: true
              duration: 300ms
";
    assert!(p.yaml.contains(want), "{want}\n{}", p.yaml);
    for want in [
        "          - child_workflow: { workflow_type: \"NotifyWorkflow\", count: 1 }\n",
        "          - child_workflow: { workflow_type: \"PaymentWorkflow\", count: 1 }\n",
        // the children's histories weren't given: stubs keep the scenario valid
        "  - type: \"PaymentWorkflow\"",
    ] {
        assert!(p.yaml.contains(want), "{want}\n{}", p.yaml);
    }
    // the booking waits for the slowest of the four, the 300ms reservations
    let r = simulate("payments", &p.yaml);
    let w = r
        .workflows
        .iter()
        .find(|w| w.workflow_type == "BookingWorkflow")
        .unwrap();
    assert!(w.completed_per_s > 0.9 * w.started_per_s, "{w:#?}");
    assert!(
        w.e2e.p50_ms > 300.0 && w.e2e.p50_ms < 600.0,
        "{}ms",
        w.e2e.p50_ms
    );
}

#[test]
fn failures_without_a_retry_state_are_judged_by_failure_and_policy() {
    // two activities that failed on the fifth and last attempt their policy allows, with no
    // retry state recorded: one with an error marked non-retryable, one that used up its attempts
    let histories: Vec<String> = [true, false]
        .iter()
        .map(|&non_retryable| {
            let mut h = History::new("LegacyWorkflow", 0.0, false);
            let w = h.wft(0.0, 5.0);
            let a = h.event(
                "ActivityTaskScheduled",
                10.0,
                json!({
                    "activityType": {"name": "Post"},
                    "taskQueue": {"name": "orders"},
                    "startToCloseTimeout": "10s",
                    "workflowTaskCompletedEventId": w.to_string(),
                    "retryPolicy": {"initialInterval": "1s", "backoffCoefficient": 2, "maximumAttempts": 5}
                }),
            );
            h.fail(
                a,
                5,
                Some(15_000.0),
                json!({}),
                "ActivityTaskFailed",
                15_100.0,
                json!({"failure": {"applicationFailureInfo": {"type": "Rejected", "nonRetryable": non_retryable}}}),
            );
            h.wft(15_100.0, 2.0);
            h.event("WorkflowExecutionFailed", 15_103.0, json!({}));
            h.json()
        })
        .collect();
    let p = import(&histories);
    // the used-up one needs a sixth attempt its policy doesn't allow
    for want in [
        "attempts: 6",
        "non_retryable: { 5: 0.5 }",
        "max_attempts: 5",
    ] {
        assert!(p.yaml.contains(want), "{want}\n{}", p.yaml);
    }
    assert!(
        p.summary.contains("no recorded retry state") && p.summary.contains("(2 activities)"),
        "{}",
        p.summary
    );
    simulate("payments", &p.yaml);
}

#[test]
fn payload_sizes_come_from_the_history_sizes_the_server_recorded() {
    // orders whose inputs and results are 4 KiB, with the sizes their workflow tasks recorded
    let sized = |i: u32, payload: f64| {
        let mut h = order_history(f64::from(i) * 1000.0, 1, true, i.is_multiple_of(2));
        h.record_sizes(payload);
        h.json()
    };
    let p = import(&(0..4).map(|i| sized(i, 4096.0)).collect::<Vec<_>>());
    assert!(p.yaml.contains("    payload_bytes: 4.0KiB\n"), "{}", p.yaml);
    assert!(
        p.summary
            .contains("payload_bytes 4.0KiB from the history sizes the server recorded")
            && p.summary.contains("in 4 of 4 histories"),
        "{}",
        p.summary
    );
    let w = simulate("payments", &p.yaml);
    assert!(
        w.workflows
            .iter()
            .any(|w| w.workflow_type == "OrderWorkflow" && w.max_history_bytes > 30_000.0),
        "{:#?}",
        w.workflows
    );
    // tiny payloads come out near nothing, not below
    let p = import(&(0..4).map(|i| sized(i, 0.0)).collect::<Vec<_>>());
    assert!(p.yaml.contains("    payload_bytes: 0B\n"), "{}", p.yaml);
    // histories that recorded no sizes leave payload_bytes at the default
    let p = import(
        &(0..4)
            .map(|i| order(f64::from(i) * 1000.0, 1, true, false))
            .collect::<Vec<_>>(),
    );
    assert!(!p.yaml.contains("    payload_bytes:"), "{}", p.yaml);
    assert!(
        p.summary.contains("payload_bytes left at the default"),
        "{}",
        p.summary
    );
}

#[test]
fn worker_processes_are_counted_per_task_queue_without_naming_them() {
    // orders run by two workflow worker processes and three activity worker processes; the
    // server's own identity isn't a worker. Two payment histories on their own task queue share
    // a workflow worker process with the orders
    let mut histories: Vec<String> = (0..6)
        .map(|i| {
            let mut h = order_history(f64::from(i) * 1000.0, 1, true, false);
            let workflow = if i == 5 {
                "history-service".to_string()
            } else {
                format!("{}@wf-host-{}@", 100 + i % 2, i % 2)
            };
            let activity = format!("{}@act-host-{}@", 200 + i % 3, i % 3);
            h.record_workers(&workflow, &activity, ("temporal-java", "1.29.0"));
            h.json()
        })
        .collect();
    for i in 0..2 {
        let mut h = order_history(f64::from(i) * 1000.0, 1, true, false);
        h.record_workers("100@wf-host-0@", "300@pay-host@", ("temporal-go", "1.34.0"));
        histories.push(h.json().replace("\"orders\"", "\"payments\""));
    }
    let p = import(&histories);
    // a commented fleet per task queue, with the processes seen
    for (queue, processes, ran) in [
        ("orders", 5, "2 ran workflow tasks and 3 ran activities"),
        ("payments", 2, "1 ran workflow tasks and 1 ran activities"),
    ] {
        assert!(
            p.yaml.contains(&format!(
                "#     task_queue: \"{queue}\"\n#     processes: {processes:<13} # {ran}\n"
            )),
            "{}",
            p.yaml
        );
        assert!(
            p.summary
                .contains(&format!("  {queue}: {processes} processes ({ran})")),
            "{}",
            p.summary
        );
    }
    assert!(
        p.summary
            .contains("1 process ran tasks from more than one task queue")
    );
    assert!(
        p.summary
            .contains("workers report temporal-java 1.29.0: tempdes follows the Go SDK")
    );
    // identities name hosts: they are counted, never written
    for text in [&p.yaml, &p.summary] {
        assert!(!text.contains("host") && !text.contains('@'), "{text}");
    }
    // uncommented, the fleets are a scenario's workers
    let mut inside = false;
    let uncommented: Vec<&str> = p
        .yaml
        .lines()
        .map(|l| {
            inside = (inside || l.starts_with("# workers:")) && !l.starts_with("workflows:");
            if inside { &l[2..] } else { l }
        })
        .collect();
    let scenario = format!(
        "name: fleets\nwarmup: 5s\nduration: 10s\ncluster:\n  num_history_shards: 16\n  replicas: {{ frontend: 1, history: 1, matching: 1, worker: 1 }}\n  persistence: {{ store: postgresql }}\nnamespaces: [ {{ name: payments }} ]\n{}",
        uncommented.join("\n")
    );
    let sc = Scenario::parse_str(&scenario).unwrap_or_else(|e| panic!("{e}\n{scenario}"));
    let params = run::prepare(&sc, &Overrides::default(), None).expect("parameters resolve");
    let processes: Vec<(String, u32)> = params
        .fleets
        .iter()
        .filter(|f| !f.system)
        .map(|f| (f.name.clone(), f.processes))
        .collect();
    assert_eq!(
        processes,
        [("orders".to_string(), 5), ("payments".to_string(), 2)]
    );
}

#[test]
fn imported_workloads_simulate() {
    let dir = std::env::temp_dir().join(format!("tempdes-import-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    for i in 0..10 {
        let attempts = if i % 5 == 0 { 3 } else { 1 };
        std::fs::write(
            dir.join(format!("order-{i}.json")),
            order(i as f64 * 500.0, attempts, true, i % 2 == 0),
        )
        .unwrap();
    }
    std::fs::write(dir.join("notes.txt"), "not a history").unwrap();
    let p = tempdes::histories::import(
        std::slice::from_ref(&dir),
        &Options {
            namespace: "orders".into(),
            rate: Some(20.0),
            ..Default::default()
        },
    )
    .expect("imports");
    std::fs::remove_dir_all(&dir).ok();
    let scenario = format!(
        "name: imported\nwarmup: 20s\nduration: 30s\ncluster:\n  num_history_shards: 64\n  replicas: {{ frontend: 1, history: 1, matching: 1, worker: 1 }}\n  persistence: {{ store: postgresql }}\nnamespaces: [ {{ name: orders }} ]\nworkers:\n  - {{ name: w, namespace: orders, task_queue: orders, processes: 2, activity_slots: 200 }}\n{}",
        p.yaml
    );
    let sc = Scenario::parse_str(&scenario).unwrap_or_else(|e| panic!("{e}\n{scenario}"));
    let params = run::prepare(&sc, &Overrides::default(), None).expect("parameters resolve");
    let out = run::run_params(params);
    let r = report::analyze(&out.ctx, &out.info, None);
    let w = r
        .workflows
        .iter()
        .find(|w| w.workflow_type == "OrderWorkflow")
        .unwrap();
    assert!(w.completed_per_s > 15.0, "{w:#?}");
    // 2 of 30 parallel activities took three attempts: about 4 failures per 34 attempts
    let per_s = |api: &str| {
        r.apis
            .iter()
            .find(|a| a.api == api)
            .map_or(0.0, |a| a.per_s)
    };
    let failed = per_s("RespondActivityTaskFailed");
    let completed = per_s("RespondActivityTaskCompleted");
    assert!(
        failed > 0.0 && failed < 0.3 * completed,
        "{failed} failed, {completed} completed"
    );
    assert!(
        !r.hotspots.iter().any(|h| h.category == "throughput"),
        "{:?}",
        r.hotspots.iter().map(|h| &h.title).collect::<Vec<_>>()
    );
}
