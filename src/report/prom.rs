//! Export simulated metrics in Prometheus text format using Temporal's metric names and labels
//! (tally reporter conventions: timers are histograms in seconds with Temporal's default
//! buckets, counters have no `_total` suffix). The server's metrics come with the Workers' own,
//! under the Go SDK's names.
//!
//! [`render`] reads a finished run: values are totals over the measurement window, i.e.
//! equivalent to the difference between two scrapes `window` seconds apart, and gauges that
//! vary are their mean over it. [`render_as`] with [`Reading::Scrape`] reads a run in progress
//! as Prometheus would scrape it at that moment: counters and histograms since the warm-up
//! ended, and gauges at their current value.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use crate::model::types::*;
use crate::model::world::{Ctx, TqKind};
use crate::sim::stats::Histogram;

/// Temporal's default millisecond histogram boundaries (common/metrics/config.go).
const MS_BOUNDS: [u64; 19] = [
    1, 2, 5, 10, 20, 50, 100, 200, 500, 1_000, 2_000, 5_000, 10_000, 20_000, 50_000, 100_000,
    200_000, 500_000, 1_000_000,
];

/// When the metrics are read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reading {
    /// After a run, over its measurement window.
    Window,
    /// During a run, as a scrape at the current simulated time.
    Scrape,
}

struct W {
    out: String,
    typed: BTreeMap<String, &'static str>,
}

/// `base` with one more label.
fn with(
    base: &[(&'static str, String)],
    label: &'static str,
    value: String,
) -> Vec<(&'static str, String)> {
    let mut l = base.to_vec();
    l.push((label, value));
    l
}

fn labels(l: &[(&str, String)]) -> String {
    if l.is_empty() {
        return String::new();
    }
    let parts: Vec<String> = l
        .iter()
        .map(|(k, v)| format!("{k}=\"{}\"", v.replace('\\', "\\\\").replace('"', "\\\"")))
        .collect();
    format!("{{{}}}", parts.join(","))
}

impl W {
    fn typ(&mut self, name: &str, t: &'static str) {
        if !self.typed.contains_key(name) {
            let _ = writeln!(self.out, "# TYPE {name} {t}");
            self.typed.insert(name.to_string(), t);
        }
    }

    fn counter(&mut self, name: &str, l: &[(&str, String)], v: f64) {
        self.typ(name, "counter");
        let _ = writeln!(self.out, "{name}{} {v}", labels(l));
    }

    fn gauge(&mut self, name: &str, l: &[(&str, String)], v: f64) {
        self.typ(name, "gauge");
        let _ = writeln!(self.out, "{name}{} {v}", labels(l));
    }

    fn timer(&mut self, name: &str, l: &[(&str, String)], h: &Histogram) {
        if h.is_empty() {
            return;
        }
        self.typ(name, "histogram");
        let bounds_us: Vec<u64> = MS_BOUNDS.iter().map(|ms| ms * 1_000).collect();
        let cum = h.cumulative_at(&bounds_us);
        for (i, ms) in MS_BOUNDS.iter().enumerate() {
            let mut ll: Vec<(&str, String)> = l.to_vec();
            ll.push(("le", format!("{}", *ms as f64 / 1000.0)));
            let _ = writeln!(self.out, "{name}_bucket{} {}", labels(&ll), cum[i]);
        }
        let mut ll: Vec<(&str, String)> = l.to_vec();
        ll.push(("le", "+Inf".into()));
        let _ = writeln!(self.out, "{name}_bucket{} {}", labels(&ll), h.count());
        let _ = writeln!(self.out, "{name}_sum{} {}", labels(l), h.sum() / 1e6);
        let _ = writeln!(self.out, "{name}_count{} {}", labels(l), h.count());
    }
}

/// The metrics of a finished run, over its measurement window.
pub fn render(ctx: &Ctx) -> String {
    render_as(ctx, Reading::Window)
}

pub fn render_as(ctx: &Ctx, reading: Reading) -> String {
    let p = &ctx.p;
    let m = ctx.m.borrow();
    let pods = ctx.pods.borrow();
    let mut w = W {
        out: String::new(),
        typed: BTreeMap::new(),
    };
    match reading {
        Reading::Window => {
            let _ = writeln!(
                w.out,
                "# tempdes simulated metrics (Temporal {} naming) scenario={} window_seconds={}",
                crate::config::dynamic::registry().temporal_version,
                p.name,
                p.duration as f64 / 1e6
            );
            let _ = writeln!(
                w.out,
                "# counters are totals over the window; divide by window_seconds for rates"
            );
        }
        Reading::Scrape => {
            let _ = writeln!(
                w.out,
                "# tempdes simulated metrics (Temporal {} naming) scenario={} scraped_at_seconds={}",
                crate::config::dynamic::registry().temporal_version,
                p.name,
                crate::sim::executor::now() as f64 / 1e6
            );
            let _ = writeln!(
                w.out,
                "# counters count from the end of the warm-up; gauges are current values"
            );
        }
    }

    // service_* per pod
    for (id, pod) in pods.iter().enumerate() {
        let inst = pod.addr.clone();
        let svc = pod.svc.as_str().to_string();
        // (operation, namespace, stats): the frontend's requests carry their namespace
        let ops: Vec<(String, Option<String>, crate::model::metrics::OpStats)> = match pod.svc {
            Service::Frontend => {
                // a history long poll is a GetWorkflowExecutionHistory call in Temporal's metrics
                let mut by_op: BTreeMap<(&str, usize), crate::model::metrics::OpStats> =
                    BTreeMap::new();
                for ((pod_id, ns, api), o) in &m.fe_by_ns {
                    if *pod_id == id {
                        by_op
                            .entry((api.metric_operation(), *ns))
                            .or_default()
                            .merge(o);
                    }
                }
                by_op
                    .into_iter()
                    .map(|((op, ns), o)| (op.to_string(), Some(p.namespaces[ns].name.clone()), o))
                    .collect()
            }
            Service::History => m
                .hist
                .get(id)
                .map(|v| {
                    HistApi::ALL
                        .iter()
                        .zip(v.iter())
                        .map(|(a, o)| (a.as_str().to_string(), None, o.clone()))
                        .collect()
                })
                .unwrap_or_default(),
            Service::Matching => m
                .matching
                .get(id)
                .map(|v| {
                    MatchApi::ALL
                        .iter()
                        .zip(v.iter())
                        .map(|(a, o)| (a.as_str().to_string(), None, o.clone()))
                        .collect()
                })
                .unwrap_or_default(),
            Service::Worker => Vec::new(),
        };
        for (op, ns, o) in ops {
            if o.count == 0 {
                continue;
            }
            let mut l = vec![("service_name", svc.clone()), ("operation", op.clone())];
            if let Some(ns) = ns {
                l.push(("namespace", ns));
            }
            l.push(("instance", inst.clone()));
            w.counter("service_requests", &l, o.count as f64);
            w.timer("service_latency", &l, &o.latency);
            for (e, n) in &o.errors {
                match e {
                    Err::ResourceExhausted(c, s) => {
                        let mut ll = l.to_vec();
                        ll.push(("resource_exhausted_cause", c.as_str().into()));
                        ll.push(("resource_exhausted_scope", s.as_str().into()));
                        w.counter("service_errors_resource_exhausted", &ll, *n as f64);
                    }
                    other => {
                        let mut ll = l.to_vec();
                        ll.push(("error_type", other.label()));
                        w.counter("service_error_with_type", &ll, *n as f64);
                    }
                }
            }
        }
        w.gauge(
            "tempdes_pod_cpu_utilization",
            &[("service_name", svc.clone()), ("instance", inst.clone())],
            pod.cpu.utilization(),
        );
        if pod.svc == Service::History {
            let owned = ctx.shards.borrow().iter().filter(|s| s.owner == id).count();
            w.gauge(
                "numshards_gauge",
                &[("instance", inst.clone())],
                owned as f64,
            );
            if let Some(h) = &pod.hist {
                let l = [
                    ("cache_type", "mutablestate".to_string()),
                    ("operation", "HistoryCacheGetOrCreate".to_string()),
                    ("instance", inst.clone()),
                ];
                w.counter("cache_requests", &l, (h.cache.hits + h.cache.misses) as f64);
                w.counter("cache_miss", &l, h.cache.misses as f64);
                w.gauge(
                    "cache_usage",
                    &l[..1]
                        .iter()
                        .cloned()
                        .chain([("instance", inst.clone())])
                        .collect::<Vec<_>>(),
                    h.cache.len() as f64,
                );
            }
        }
        let pool = pod.db_pool.stats();
        if pool.capacity < 10_000 {
            let l = [("service_name", svc.clone()), ("instance", inst.clone())];
            w.gauge(
                "persistence_sql_max_open_conn",
                &l,
                f64::from(pool.capacity),
            );
            let in_use = match reading {
                Reading::Window => pool.utilization * f64::from(pool.capacity),
                Reading::Scrape => f64::from(pod.db_pool.in_use()),
            };
            w.gauge("persistence_sql_in_use", &l, in_use);
        }
    }

    // persistence
    for op in PersistOp::ALL {
        let o = &m.persist[op.idx()];
        if o.count > 0 {
            let l = [("operation", op.as_str().to_string())];
            w.counter("persistence_requests", &l, o.count as f64);
            w.timer("persistence_latency", &l, &o.latency);
            let rejected = o.error_count();
            if rejected > 0 {
                w.counter(
                    "persistence_errors_resource_exhausted",
                    &[
                        ("operation", op.as_str().to_string()),
                        (
                            "resource_exhausted_cause",
                            ReCause::PersistenceLimit.as_str().into(),
                        ),
                    ],
                    rejected as f64,
                );
            }
        }
        let v = &m.vis_persist[op.idx()];
        if v.count > 0 {
            let l = [("operation", op.as_str().to_string())];
            w.counter("visibility_persistence_requests", &l, v.count as f64);
            w.timer("visibility_persistence_latency", &l, &v.latency);
        }
    }

    // history tasks
    for t in TaskType::ALL {
        let o = &m.tasks[t.idx()];
        if o.count == 0 {
            continue;
        }
        let l = [
            ("task_type", t.as_str().to_string()),
            ("operation", t.as_str().to_string()),
        ];
        w.counter("task_requests", &l, o.count as f64);
        w.timer("task_latency_load", &l, &o.load_latency);
        w.timer("task_latency_schedule", &l, &o.schedule_latency);
        w.timer("task_latency_processing", &l, &o.processing);
        w.timer("task_latency_queue", &l, &o.queue_latency);
        if o.busy_errors > 0 {
            w.counter("task_errors_workflow_busy", &l, o.busy_errors as f64);
        }
        if o.sched_throttled > 0 {
            w.counter("task_scheduler_throttled", &l, o.sched_throttled as f64);
        }
        if o.throttled_errors > 0 {
            w.counter("task_errors_throttled", &l, o.throttled_errors as f64);
        }
    }
    w.timer(
        "history_workflow_execution_cache_latency",
        &[],
        &m.lock_wait,
    );
    w.counter("acquire_lock_failed", &[], m.lock_timeouts as f64);
    w.counter(
        "cache_requests",
        &[
            ("cache_type", "events".into()),
            ("operation", "EventsCacheGetEvent".into()),
        ],
        (m.events_cache_hits + m.events_cache_misses) as f64,
    );
    w.counter(
        "cache_miss",
        &[
            ("cache_type", "events".into()),
            ("operation", "EventsCacheGetEvent".into()),
        ],
        m.events_cache_misses as f64,
    );

    // matching
    let matching = ctx.matching.borrow();
    for pt in &matching.parts {
        let tq = &p.task_queues[pt.tq];
        let partition = match pt.sticky_of {
            Some(_) => "__sticky__".to_string(),
            None => pt.part.to_string(),
        };
        let tq_name = match pt.sticky_of {
            Some(_) => "__sticky__".to_string(),
            None => tq.name.clone(),
        };
        let l = [
            ("namespace", p.namespaces[tq.ns].name.clone()),
            ("taskqueue", tq_name),
            (
                "task_type",
                match pt.kind {
                    TqKind::Workflow => "Workflow".into(),
                    TqKind::Activity => "Activity".into(),
                },
            ),
            ("partition", partition),
            ("instance", pods[pt.host].addr.clone()),
        ];
        let matched = pt.sync_matches + pt.async_matches;
        // A partition that tasks were added to reports its backlog whether or not anything
        // polled it.
        if matched + pt.polls + pt.adds == 0 {
            continue;
        }
        w.counter("poll_success", &l, matched as f64);
        w.counter("poll_success_sync", &l, pt.sync_matches as f64);
        w.counter("poll_timeouts", &l, pt.poll_timeouts as f64);
        w.counter("forwarded", &l, pt.forwarded_tasks as f64);
        w.counter("task_write_throttle_count", &l, pt.write_rejects as f64);
        let backlog = match reading {
            Reading::Window => pt.backlog_gauge.clone().mean(),
            Reading::Scrape => pt.backlog_len() as f64,
        };
        w.gauge("approximate_backlog_count", &l, backlog);
        w.gauge(
            "approximate_backlog_age_seconds",
            &l,
            pt.backlog_head_age() as f64 / 1e6,
        );
        w.timer("asyncmatch_latency", &l, &pt.task_wait);
    }

    // The Workers' metrics, as the Go SDK's tally reporter names them: one series per worker
    // process, told apart by `instance` as Prometheus tells scrape targets apart. The server's
    // own system workers are left out.
    let workers = ctx.workers.borrow();
    for (wk, proc) in workers.iter().enumerate() {
        let f = &p.fleets[proc.fleet];
        if f.system {
            continue;
        }
        let base = [
            ("namespace", p.namespaces[f.ns].name.clone()),
            ("task_queue", p.task_queues[f.tq].name.clone()),
            ("instance", format!("{}-{}", f.name, proc.ordinal)),
        ];
        for (worker_type, slots) in [
            ("WorkflowWorker", &proc.wft_slots),
            ("ActivityWorker", &proc.act_slots),
        ] {
            let l = with(&base, "worker_type", worker_type.into());
            let used = slots.in_use();
            w.gauge(
                "temporal_worker_task_slots_available",
                &l,
                f64::from(slots.capacity().saturating_sub(used)),
            );
            w.gauge("temporal_worker_task_slots_used", &l, f64::from(used));
        }
        for (poller_type, n) in [
            ("workflow_task", proc.pending_regular),
            ("workflow_sticky_task", proc.pending_sticky),
            ("activity_task", proc.pending_activity),
        ] {
            w.gauge(
                "temporal_num_pollers",
                &with(&base, "poller_type", poller_type.into()),
                f64::from(n),
            );
        }
        let s = m.workers.get(wk).cloned().unwrap_or_default();
        for (operation, n) in &s.long_polls {
            w.counter(
                "temporal_long_request",
                &with(&base, "operation", (*operation).into()),
                *n as f64,
            );
        }
        for (wf_type, n) in &s.wft_polls_succeeded {
            w.counter(
                "temporal_workflow_task_queue_poll_succeed",
                &with(&base, "workflow_type", p.wf_types[*wf_type].name.clone()),
                *n as f64,
            );
        }
        w.counter(
            "temporal_workflow_task_queue_poll_empty",
            &base,
            s.wft_polls_empty as f64,
        );
        w.counter(
            "temporal_activity_poll_no_task",
            &base,
            s.act_polls_empty as f64,
        );
        for ((operation, status_code), n) in &s.poll_failures {
            let mut l = with(&base, "operation", (*operation).into());
            l.push(("status_code", (*status_code).into()));
            w.counter("temporal_long_request_failure", &l, *n as f64);
        }
    }

    // workflows
    for (i, t) in p.wf_types.iter().enumerate() {
        let s = &m.wf[i];
        let l = [
            ("namespace", p.namespaces[t.ns].name.clone()),
            ("workflowType", t.name.clone()),
        ];
        w.counter("workflow_success", &l, s.completed as f64);
        w.timer("workflow_schedule_to_close_latency", &l, &s.e2e);
        // Schedule-to-start as the Workers' SDK reports it, recorded where Matching hands the
        // task to a poll.
        let l = [
            ("namespace", p.namespaces[t.ns].name.clone()),
            ("task_queue", p.task_queues[t.tq].name.clone()),
            ("workflow_type", t.name.clone()),
        ];
        w.timer(
            "temporal_workflow_task_schedule_to_start_latency",
            &l,
            &s.wft_sched_to_start,
        );
        w.timer(
            "temporal_activity_schedule_to_start_latency",
            &l,
            &s.act_sched_to_start,
        );
    }
    if !p.schedules.is_empty() {
        w.counter("schedule_action_success", &[], m.schedule_actions as f64);
        w.counter("schedule_rate_limited", &[], m.schedule_rate_limited as f64);
        w.timer("schedule_action_delay", &[], &m.schedule_delay);
    }
    // database (not a Temporal metric)
    w.gauge(
        "tempdes_database_utilization",
        &[],
        ctx.db.borrow().servers.utilization(),
    );
    w.out
}
