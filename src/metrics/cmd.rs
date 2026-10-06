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

/// How a metric is read for an observations file.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Kind {
    /// a counter, recorded as its rate over the window
    Counter,
    /// a gauge, recorded as its value at the end of the window
    Gauge,
    /// a histogram, recorded as these quantiles over the window
    Histogram(&'static [f64]),
    /// cAdvisor's CPU seconds, recorded as cores over the window
    Cores,
}

/// One query of an observations file.
pub struct Spec {
    /// Temporal's metric name, the observations entry's name
    pub name: &'static str,
    pub kind: Kind,
    /// labels the results are grouped by
    pub by: &'static str,
    /// label matchers
    pub filter: &'static str,
    /// labels every result carries, from equality matchers
    pub fixed: &'static [(&'static str, &'static str)],
    pub what: &'static str,
}

/// The default matcher for Temporal's containers in cAdvisor's metrics.
pub const CPU_SELECTOR: &str = r#"container=~"temporal-(frontend|history|matching|worker)""#;

/// The queries whose results make an observations file: `metrics queries` prints them, and
/// `metrics fetch` runs them.
pub const SPECS: &[Spec] = &[
    Spec {
        name: "service_requests",
        kind: Kind::Counter,
        by: "namespace, operation",
        filter: r#"service_name=~"(.+-)?frontend""#,
        fixed: &[("service_name", "frontend")],
        what: "workload mix: starts, signals, polls, responds, per namespace",
    },
    Spec {
        name: "service_requests",
        kind: Kind::Counter,
        by: "service_name, operation",
        filter: r#"service_name=~"(.+-)?(history|matching)""#,
        fixed: &[],
        what: "internal call mix (CPU calibration)",
    },
    Spec {
        name: "service_latency",
        kind: Kind::Histogram(&[0.5, 0.99]),
        by: "service_name, operation",
        filter: r#"service_name=~"(.+-)?(frontend|history)""#,
        fixed: &[],
        what: "API latency (validation)",
    },
    Spec {
        name: "persistence_requests",
        kind: Kind::Counter,
        by: "operation",
        filter: "",
        fixed: &[],
        what: "persistence operation mix (database capacity calibration)",
    },
    Spec {
        name: "persistence_latency",
        kind: Kind::Histogram(&[0.5, 0.9, 0.99]),
        by: "operation",
        filter: "",
        fixed: &[],
        what: "database service time per operation",
    },
    Spec {
        name: "task_requests",
        kind: Kind::Counter,
        by: "task_type",
        filter: r#"service_name=~"(.+-)?history""#,
        fixed: &[],
        what: "history queue task mix",
    },
    Spec {
        name: "cache_requests",
        kind: Kind::Counter,
        by: "",
        filter: r#"cache_type="mutablestate""#,
        fixed: &[("cache_type", "mutablestate")],
        what: "mutable state cache hit ratio (validation)",
    },
    Spec {
        name: "cache_miss",
        kind: Kind::Counter,
        by: "",
        filter: r#"cache_type="mutablestate""#,
        fixed: &[("cache_type", "mutablestate")],
        what: "mutable state cache hit ratio (validation)",
    },
    Spec {
        name: "poll_success",
        kind: Kind::Counter,
        by: "",
        filter: "",
        fixed: &[],
        what: "sync match ratio (validation)",
    },
    Spec {
        name: "poll_success_sync",
        kind: Kind::Counter,
        by: "",
        filter: "",
        fixed: &[],
        what: "sync match ratio (validation)",
    },
    Spec {
        name: "approximate_backlog_count",
        kind: Kind::Gauge,
        by: "namespace, task_type",
        filter: "",
        fixed: &[],
        what: "task backlog (validation, summed)",
    },
    Spec {
        name: "numshards_gauge",
        kind: Kind::Gauge,
        by: "instance",
        filter: "",
        fixed: &[],
        what: "shards per history pod (ring balance)",
    },
    Spec {
        name: "container_cpu_usage_seconds_total",
        kind: Kind::Cores,
        by: "container",
        filter: CPU_SELECTOR,
        fixed: &[],
        what: "CPU cores per Temporal service (CPU calibration)",
    },
    Spec {
        name: "service_errors_resource_exhausted",
        kind: Kind::Counter,
        by: "service_name, operation, resource_exhausted_cause, resource_exhausted_scope",
        filter: "",
        fixed: &[],
        what: "throttling (validation)",
    },
    Spec {
        name: "workflow_success",
        kind: Kind::Counter,
        by: "namespace",
        filter: "",
        fixed: &[],
        what: "completed workflows per namespace (not read by tempdes)",
    },
    Spec {
        name: "workflow_context_cleared",
        kind: Kind::Counter,
        by: "",
        filter: "",
        fixed: &[],
        what: "mutable state cleared after errors, twice per failed write (not read by tempdes)",
    },
];

impl Spec {
    /// The PromQL for `metric` (the spec's name, or the name this Prometheus uses for it) over
    /// `window`: one query, or one per quantile for a histogram.
    pub fn promql(&self, metric: &str, window: &str) -> Vec<(Option<f64>, String)> {
        self.promql_with(metric, window, self.filter)
    }

    /// `promql` with other label matchers.
    pub fn promql_with(
        &self,
        metric: &str,
        window: &str,
        filter: &str,
    ) -> Vec<(Option<f64>, String)> {
        let filter = if filter.is_empty() {
            String::new()
        } else {
            format!("{{{filter}}}")
        };
        // `sum(` or `sum by (labels) (`
        let sum = |extra: &str| match (extra.is_empty(), self.by.is_empty()) {
            (true, true) => "sum(".to_string(),
            (true, false) => format!("sum by ({}) (", self.by),
            (false, true) => format!("sum by ({extra}) ("),
            (false, false) => format!("sum by ({extra}, {}) (", self.by),
        };
        match self.kind {
            Kind::Counter | Kind::Cores => vec![(
                None,
                format!("{}rate({metric}{filter}[{window}]))", sum("")),
            )],
            Kind::Gauge => vec![(None, format!("{}{metric}{filter})", sum("")))],
            Kind::Histogram(qs) => qs
                .iter()
                .map(|&q| {
                    (
                        Some(q),
                        format!(
                            "histogram_quantile({q}, {}rate({metric}{filter}[{window}])))",
                            sum("le")
                        ),
                    )
                })
                .collect(),
        }
    }

    /// What `metrics queries` says to record.
    fn record(&self) -> String {
        let what = match self.kind {
            Kind::Counter => "rate".to_string(),
            Kind::Cores => "rate (cores)".to_string(),
            Kind::Gauge => "value".to_string(),
            Kind::Histogram(qs) => qs
                .iter()
                .map(|q| format!("p{}", (q * 100.0).round()))
                .collect::<Vec<_>>()
                .join("/"),
        };
        format!("{what}: {}", self.what)
    }

    /// The labels each recorded result has.
    fn labels(&self) -> String {
        self.fixed
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .chain(
                self.by
                    .split(',')
                    .map(str::trim)
                    .filter(|l| !l.is_empty())
                    .map(String::from),
            )
            .collect::<Vec<_>>()
            .join(", ")
    }
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
            println!(
                "# Record each result under `metrics:` with the given name and labels, or let"
            );
            println!("# `tempdes metrics fetch` run them. Histograms are `<name>_bucket` here.\n");
            for s in SPECS {
                let metric = match s.kind {
                    Kind::Histogram(_) => format!("{}_bucket", s.name),
                    _ => s.name.to_string(),
                };
                println!("# {} {{{}}} — {}", s.name, s.labels(), s.record());
                for (_, q) in s.promql(&metric, &window) {
                    println!("{q}");
                }
                println!();
            }
            println!("# db_utilization — value 0..1: database busy fraction (database capacity");
            println!(
                "# calibration; e.g. Aurora DBLoadCPU / vCPUs), from your database monitoring"
            );
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
