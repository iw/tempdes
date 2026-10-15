//! The Prometheus output read during a run, as a scrape, and the Workers' metrics in it.

use std::path::Path;

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
    let p = run::prepare(sc, &short(), None).expect("parameters resolve");
    let (ctx, mut ex) = build::build(p);
    let _rates = build::start(&ctx, &mut ex);
    ex.run_until(seconds * 1_000_000);
    prom::render_as(&ctx, Reading::Scrape)
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
