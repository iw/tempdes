//! The Prometheus output read during a run, as a scrape, and the Workers' metrics in it.

use std::path::Path;

use tempdes::config::dynamic::{Constraints, DcValue};
use tempdes::config::scenario::Scenario;
use tempdes::model::build;
use tempdes::report::prom::{self, Reading};
use tempdes::run::{self, Overrides};

fn baseline() -> Scenario {
    Scenario::load(&Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/scenarios/baseline.yaml"))
        .expect("scenario loads")
}

fn short() -> Overrides {
    Overrides {
        warmup_s: Some(8.0),
        duration_s: Some(20.0),
        ..Default::default()
    }
}

/// Run `sc` for `seconds` of simulated time and scrape it then.
fn scrape_at(sc: &Scenario, seconds: u64) -> String {
    scrapes(sc, &short(), &[seconds]).remove(0)
}

/// Run `sc` with `ov`, scraping it at each of `seconds` of simulated time.
fn scrapes(sc: &Scenario, ov: &Overrides, seconds: &[u64]) -> Vec<String> {
    let p = run::prepare(sc, ov, None).expect("parameters resolve");
    let (ctx, mut ex) = build::build(p);
    let _rates = build::start(&ctx, &mut ex);
    seconds
        .iter()
        .map(|s| {
            ex.run_until(s * 1_000_000);
            prom::render_as(&ctx, Reading::Scrape)
        })
        .collect()
}

/// The increase per second of the samples of `metric` matching every one of `labels`,
/// between two scrapes `seconds` apart.
fn rate(before: &str, after: &str, seconds: f64, metric: &str, labels: &[&str]) -> f64 {
    let total = |text: &str| -> f64 {
        samples(text, metric)
            .iter()
            .filter(|(l, _)| labels.iter().all(|want| l.contains(want)))
            .map(|(_, v)| v)
            .sum()
    };
    (total(after) - total(before)) / seconds
}

/// `(labels, value)` of every sample of `metric`.
fn samples<'a>(text: &'a str, metric: &str) -> Vec<(&'a str, f64)> {
    text.lines()
        .filter(|l| !l.starts_with('#'))
        .filter_map(|l| {
            let (series, value) = l.rsplit_once(' ')?;
            let labels = series.strip_prefix(metric)?;
            (labels.is_empty() || labels.starts_with('{'))
                .then(|| (labels, value.parse().expect("a number")))
        })
        .collect()
}

#[test]
fn a_scrape_reads_a_run_in_progress() {
    let text = scrape_at(&baseline(), 18);
    assert!(text.contains("scraped_at_seconds=18"), "{}", &text[..200]);

    // Six worker processes with 200 Workflow Task slots each.
    let slots = samples(&text, "temporal_worker_task_slots_available");
    let workflow: Vec<f64> = slots
        .iter()
        .filter(|(l, _)| l.contains("worker_type=\"WorkflowWorker\""))
        .map(|(_, v)| *v)
        .collect();
    assert_eq!(workflow.len(), 6);
    assert!(
        workflow.iter().all(|v| (0.0..=200.0).contains(v)),
        "{workflow:?}"
    );
    assert!(slots.iter().all(|(l, _)| l.contains("namespace=\"orders\"")
        && l.contains("task_queue=\"orders\"")
        && l.contains("instance=\"order-workers-")));

    // Ten Workflow Task pollers each, split between the sticky and the normal queue.
    let pollers = samples(&text, "temporal_num_pollers");
    let polling = |t: &str| -> f64 {
        pollers
            .iter()
            .filter(|(l, _)| l.contains(&format!("poller_type=\"{t}\"")))
            .map(|(_, v)| v)
            .sum()
    };
    assert!(polling("workflow_task") + polling("workflow_sticky_task") <= 60.0);
    assert!(polling("workflow_task") > 0.0);
    assert!(polling("activity_task") > 0.0);

    // The frontend's requests carry their namespace, as Temporal's do.
    let starts = samples(&text, "service_requests");
    assert!(
        starts
            .iter()
            .any(|(l, v)| l.contains("service_name=\"frontend\"")
                && l.contains("operation=\"StartWorkflowExecution\"")
                && l.contains("namespace=\"orders\"")
                && *v > 0.0)
    );
    assert!(
        starts
            .iter()
            .filter(|(l, _)| l.contains("service_name=\"frontend\""))
            .all(|(l, _)| l.contains("namespace="))
    );

    // Every poll is counted as it is sent, so a failure counter's absence means none failed.
    let polls = samples(&text, "temporal_long_request");
    assert!(
        polls
            .iter()
            .any(|(l, v)| l.contains("operation=\"PollWorkflowTaskQueue\"") && *v > 0.0)
    );
    assert!(
        polls
            .iter()
            .any(|(l, v)| l.contains("operation=\"PollActivityTaskQueue\"") && *v > 0.0)
    );

    let succeeded = samples(&text, "temporal_workflow_task_queue_poll_succeed");
    assert!(
        succeeded
            .iter()
            .any(|(l, v)| l.contains("workflow_type=\"OrderWorkflow\"") && *v > 0.0)
    );
    let scheduled = samples(
        &text,
        "temporal_workflow_task_schedule_to_start_latency_bucket",
    );
    assert!(scheduled.iter().any(|(l, v)| l.contains("le=\"+Inf\"")
        && l.contains("workflow_type=\"OrderWorkflow\"")
        && *v > 0.0));

    // Matching's gauges are current: whole tasks, and each partition says how old its backlog is.
    let backlog = samples(&text, "approximate_backlog_count");
    assert!(!backlog.is_empty());
    assert!(backlog.iter().all(|(_, v)| v.fract() == 0.0), "{backlog:?}");
    assert_eq!(
        samples(&text, "approximate_backlog_age_seconds").len(),
        backlog.len()
    );
}

#[test]
fn scarce_slots_show_in_the_workers_metrics() {
    let mut scarce = baseline();
    for fleet in &mut scarce.workers {
        fleet.workflow_slots = 2;
    }
    let free = |text: &str| -> f64 {
        samples(text, "temporal_worker_task_slots_available")
            .iter()
            .filter(|(l, _)| l.contains("worker_type=\"WorkflowWorker\""))
            .map(|(_, v)| v)
            .sum()
    };
    let plenty = scrape_at(&baseline(), 18);
    let starved = scrape_at(&scarce, 18);
    assert!(free(&starved) <= 12.0, "{}", free(&starved));
    assert!(free(&plenty) > free(&starved));
    // Workers without a free slot don't poll, so tasks wait longer in Matching.
    let waiting = |text: &str| -> f64 {
        let count = |m: &str| -> f64 {
            samples(text, m)
                .iter()
                .filter(|(l, _)| l.contains("workflow_type=\"OrderWorkflow\""))
                .map(|(_, v)| v)
                .sum()
        };
        count("temporal_workflow_task_schedule_to_start_latency_sum")
            / count("temporal_workflow_task_schedule_to_start_latency_count")
    };
    // In this run, about a second against about 13 ms.
    assert!(
        waiting(&starved) > 10.0 * waiting(&plenty),
        "{} vs {}",
        waiting(&starved),
        waiting(&plenty)
    );
}

#[test]
fn a_finished_run_still_reads_its_window() {
    let sc = baseline();
    let out = run::run_params(run::prepare(&sc, &short(), None).expect("parameters resolve"));
    let text = prom::render(&out.ctx);
    assert!(text.contains("window_seconds=20"));
    assert_eq!(text, prom::render_as(&out.ctx, Reading::Window));
    assert!(!samples(&text, "temporal_worker_task_slots_available").is_empty());
}

#[test]
fn a_partition_dispatch_limit_holds_without_stalling() {
    // Four partitions at 25 tasks/s each, far below the Workflow Tasks baseline.yaml adds.
    let ov = Overrides {
        dc: vec![(
            "admin.matchingNamespaceTaskqueueToPartitionDispatchRate".into(),
            DcValue::Float(25.0),
            Constraints::default(),
        )],
        warmup_s: Some(8.0),
        duration_s: Some(120.0),
        ..Default::default()
    };
    let s = scrapes(&baseline(), &ov, &[68, 128]);
    let queue = ["taskqueue=\"orders\"", "task_type=\"Workflow\""];
    let total = rate(&s[0], &s[1], 60.0, "poll_success", &queue);
    assert!((90.0..=110.0).contains(&total), "{total} tasks/s");
    // Every partition keeps dispatching at its own limit: a refused dispatch is retried
    // without putting the limiter in debt.
    for partition in 0..4 {
        let label = format!("partition=\"{partition}\"");
        let mut labels = queue.to_vec();
        labels.push(&label);
        let each = rate(&s[0], &s[1], 60.0, "poll_success", &labels);
        assert!(
            (20.0..=30.0).contains(&each),
            "partition {partition}: {each} tasks/s"
        );
    }
    let backlog: f64 = samples(&s[1], "approximate_backlog_count")
        .iter()
        .filter(|(l, _)| queue.iter().all(|q| l.contains(q)))
        .map(|(_, v)| v)
        .sum();
    assert!(backlog > 1_000.0, "{backlog}");
}

const SIGNALLED: &str = r#"
name: signalled
seed: 3
warmup: 8s
duration: 80s
cluster:
  num_history_shards: 64
  replicas: { frontend: 1, history: 1, matching: 1, worker: 1 }
  persistence: { store: postgresql, capacity: 80 }
namespaces:
  - name: orders
workers:
  - { name: order-workers, namespace: orders, task_queue: orders, processes: 2 }
workflows:
  - type: OrderWorkflow
    namespace: orders
    task_queue: orders
    start_rate: 20/s
    steps:
      - activity: { count: 1, duration: { p50: 30ms, p99: 250ms } }
      - timer: 5s
load:
  signals: [ { workflow_type: OrderWorkflow, rate: 0/s } ]
events:
  - { at: 40s, signal_rate: { workflow_type: OrderWorkflow, rate: 50/s }, label: batch job }
"#;

#[test]
fn a_signal_rate_event_changes_the_rate() {
    let sc = Scenario::parse_str(SIGNALLED).expect("scenario");
    let ov = Overrides::default();
    let s = scrapes(&sc, &ov, &[20, 38, 58, 78]);
    let signals = [
        "operation=\"SignalWorkflowExecution\"",
        "namespace=\"orders\"",
    ];
    let before = rate(&s[0], &s[1], 18.0, "service_requests", &signals);
    let after = rate(&s[2], &s[3], 20.0, "service_requests", &signals);
    assert_eq!(before, 0.0, "a load at 0/s sends nothing");
    assert!((40.0..=60.0).contains(&after), "{after} signals/s");

    // An event for a type no signal load sends to is refused.
    let bad = SIGNALLED.replace(
        "signal_rate: { workflow_type: OrderWorkflow",
        "signal_rate: { workflow_type: OrderWorkflow2",
    );
    assert!(run::prepare(&Scenario::parse_str(&bad).expect("scenario"), &ov, None).is_err());
}
