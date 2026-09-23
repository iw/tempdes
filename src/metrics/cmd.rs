//! `tempdes metrics …` — help collecting observed Temporal metrics for calibration.

use std::path::PathBuf;
use std::process::ExitCode;

use super::observed::Observations;
use crate::util::units::{fmt_rate, fmt_us};

pub enum Cmd {
    Queries { window: String },
    Template,
    Show { file: PathBuf },
}

/// (observations entry name, labels, what to record, PromQL)
fn queries(w: &str) -> Vec<(&'static str, &'static str, &'static str, String)> {
    let mut q = Vec::new();
    let rate =
        |m: &str, by: &str, filter: &str| format!("sum by ({by}) (rate({m}{{{filter}}}[{w}]))");
    let hq = |m: &str, p: f64, by: &str, filter: &str| {
        format!("histogram_quantile({p}, sum by (le, {by}) (rate({m}_bucket{{{filter}}}[{w}])))")
    };
    q.push((
        "service_requests",
        "service_name=frontend, operation",
        "rate: workload mix (starts, signals, polls, responds)",
        rate("service_requests", "operation", "service_name=\"frontend\""),
    ));
    q.push((
        "service_requests",
        "service_name=history|matching, operation",
        "rate: internal call mix (CPU calibration)",
        rate(
            "service_requests",
            "service_name, operation",
            "service_name=~\"history|matching\"",
        ),
    ));
    q.push((
        "service_latency",
        "service_name, operation",
        "p50/p99: validation of API latency",
        hq(
            "service_latency",
            0.99,
            "service_name, operation",
            "service_name=~\"frontend|history\"",
        ),
    ));
    q.push((
        "persistence_requests",
        "operation",
        "rate: persistence op mix (DB capacity calibration)",
        rate("persistence_requests", "operation", ""),
    ));
    for p in [0.5, 0.9, 0.99] {
        q.push((
            "persistence_latency",
            "operation",
            if p == 0.5 {
                "p50: DB service time"
            } else if p == 0.9 {
                "p90: DB service time"
            } else {
                "p99: DB service time"
            },
            hq("persistence_latency", p, "operation", ""),
        ));
    }
    q.push((
        "task_requests",
        "task_type",
        "rate: history queue task mix",
        rate("task_requests", "task_type", "service_name=\"history\""),
    ));
    q.push((
        "cache_requests / cache_miss",
        "cache_type=mutablestate",
        "rate: mutable state cache hit ratio (validation)",
        format!(
            "sum(rate(cache_miss{{cache_type=\"mutablestate\"}}[{w}])) / sum(rate(cache_requests{{cache_type=\"mutablestate\"}}[{w}]))"
        ),
    ));
    q.push((
        "poll_success / poll_success_sync",
        "",
        "rate: sync match ratio (validation)",
        format!("sum(rate(poll_success_sync[{w}])) / sum(rate(poll_success[{w}]))"),
    ));
    q.push((
        "approximate_backlog_count",
        "taskqueue, task_type",
        "value: task backlog (validation)",
        "sum by (taskqueue, task_type) (approximate_backlog_count)".into(),
    ));
    q.push((
        "numshards_gauge",
        "instance",
        "value: shards per history pod (ring balance)",
        "numshards_gauge".into(),
    ));
    q.push((
        "container_cpu_usage_seconds_total",
        "container",
        "rate: CPU cores per Temporal service (CPU calibration)",
        format!(
            "sum by (container) (rate(container_cpu_usage_seconds_total{{container=~\"temporal-(frontend|history|matching|worker)\"}}[{w}]))"
        ),
    ));
    q.push((
        "service_errors_resource_exhausted",
        "operation, resource_exhausted_cause",
        "rate: throttling (validation)",
        rate(
            "service_errors_resource_exhausted",
            "service_name, operation, resource_exhausted_cause",
            "",
        ),
    ));
    q.push((
        "db_utilization",
        "",
        "value 0..1: database busy fraction (DB capacity calibration; e.g. Aurora DBLoadCPU / vCPUs)",
        "(from your database monitoring)".into(),
    ));
    q
}

const TEMPLATE: &str = r#"# tempdes observations file
# Values read from your Temporal Prometheus metrics (see `tempdes metrics queries`).
# Metric names are Temporal's; `temporal_` prefixes and `_total` / `_milliseconds` suffixes are
# accepted and stripped. Label filters are subset matches.
description: production, weekday peak
window: 15m                      # needed only for entries that give `increase`

# Optional: raw Prometheus scrapes of the Temporal pods (concatenate pods into one file).
# With `before` + `interval`, counters become rates; a single `after` gives histogram shapes.
# prometheus:
#   before: scrape-0900.prom
#   after:  scrape-0915.prom
#   interval: 15m

metrics:
  # --- workload (scales scenario start/signal rates when calibration.workload is on) ---
  - name: service_requests
    labels: { service_name: frontend, operation: StartWorkflowExecution }
    rate: 180/s
  - name: service_requests
    labels: { service_name: frontend, operation: SignalWorkflowExecution }
    rate: 40/s

  # --- persistence latency → database service time per operation ---
  - name: persistence_latency
    labels: { operation: UpdateWorkflowExecution }
    p50: 3.2ms
    p90: 7ms
    p99: 21ms
  - name: persistence_latency
    labels: { operation: CreateWorkflowExecution }
    p50: 4.5ms
    p99: 26ms
  - name: persistence_latency
    labels: { operation: GetWorkflowExecution }
    quantiles: { 0.5: 1.4ms, 0.9: 3ms, 0.99: 9ms }

  # --- database capacity (optional) ---
  - name: persistence_requests
    labels: { operation: UpdateWorkflowExecution }
    rate: 3600/s
  - name: db_utilization
    value: 0.35

  # --- CPU calibration: cores used per service (cAdvisor) ---
  - name: container_cpu_usage_seconds_total
    labels: { container: temporal-history }
    rate: 5.2
  - name: service_requests
    labels: { service_name: history, operation: RespondWorkflowTaskCompleted }
    rate: 900/s

  # --- validation only ---
  - name: service_latency
    labels: { service_name: frontend, operation: StartWorkflowExecution }
    p99: 40ms
  - name: poll_success
    rate: 1500/s
  - name: poll_success_sync
    rate: 1320/s
"#;

pub fn run(cmd: Cmd) -> anyhow::Result<ExitCode> {
    match cmd {
        Cmd::Queries { window } => {
            println!("# PromQL for a tempdes observations file (window {window}).");
            println!("# Record each result under `metrics:` with the given name and labels.\n");
            for (name, labels, what, q) in queries(&window) {
                println!("# {name} {{{labels}}} — {what}");
                println!("{q}\n");
            }
            Ok(ExitCode::SUCCESS)
        }
        Cmd::Template => {
            print!("{TEMPLATE}");
            Ok(ExitCode::SUCCESS)
        }
        Cmd::Show { file } => {
            let o = Observations::load(&file)?;
            println!("{}: {} observations", file.display(), o.items.len());
            let mut items = o.items.clone();
            items.sort_by(|a, b| a.name.cmp(&b.name));
            for it in items.iter().take(400) {
                let labels: Vec<String> =
                    it.labels.iter().map(|(k, v)| format!("{k}={v}")).collect();
                let mut what = Vec::new();
                if let Some(r) = it.rate {
                    what.push(format!("rate {}", fmt_rate(r)));
                }
                if let Some(v) = it.value {
                    what.push(format!("value {v:.4}"));
                }
                for q in [0.5, 0.99] {
                    if let Some(v) = it.quantile_us(q) {
                        what.push(format!("p{} {}", (q * 100.0) as u32, fmt_us(v)));
                    }
                }
                println!(
                    "  {:<40} {{{}}} {}",
                    it.name,
                    labels.join(","),
                    what.join(" · ")
                );
            }
            for n in &o.notes {
                println!("note: {n}");
            }
            Ok(ExitCode::SUCCESS)
        }
    }
}
