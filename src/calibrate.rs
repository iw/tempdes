//! Calibration: use observed Temporal metrics to set model parameters.
//!
//! | Observed metric (Temporal name)                                  | Informs                                    |
//! |------------------------------------------------------------------|--------------------------------------------|
//! | `persistence_latency{operation=X}` (histogram / quantiles)        | DB service time distribution of X          |
//! | `visibility_persistence_latency{operation=X}`                     | visibility store latency                    |
//! | `persistence_requests{operation}` + DB utilisation observation    | database capacity (concurrency)             |
//! | `container_cpu_usage_seconds_total` per service                   | per-service CPU cost scale (pilot run)      |
//! | `service_requests{frontend, StartWorkflowExecution / Signal...}`  | workload start / signal rates (optional)    |
//!
//! CPU usage is taken from `container_cpu_usage_seconds_total` (cAdvisor, rate = cores) with a
//! `container`/`service_name` label naming the Temporal service, or from `cpu_cores{service_name}`.

use crate::metrics::observed::Observations;
use crate::model::params::Params;
use crate::model::types::*;
use crate::util::units::{fmt_rate, fmt_us};

pub fn apply(p: &mut Params, obs: &Observations, persistence_latency: bool, workload: bool) {
    let mut notes = Vec::new();
    // --- persistence latency → DB service times ------------------------------------------------
    for op in PersistOp::ALL {
        if !persistence_latency {
            break;
        }
        let name = if op.is_visibility() {
            "visibility_persistence_latency"
        } else {
            "persistence_latency"
        };
        if let Some(o) = obs.latency(name, &[("operation", op.as_str())])
            && let Some(d) = o.to_dist()
        {
            notes.push(format!(
                "calibrated {} service time from {name}: p50 {} p99 {}",
                op.as_str(),
                fmt_us(d.quantile(0.5)),
                fmt_us(d.quantile(0.99))
            ));
            if op.is_visibility() {
                match op {
                    PersistOp::ListWorkflowExecutions | PersistOp::CountWorkflowExecutions => {
                        p.vis.read = d
                    }
                    _ => p.vis.write = d,
                }
            } else {
                p.db_latency[op.idx()] = d;
            }
        }
    }

    // --- database capacity from observed utilisation -----------------------------------------------
    if let Some(util) = obs
        .value_sum("db_utilization", &[])
        .or_else(|| obs.value_sum("rds_cpu_utilization", &[]).map(|v| v / 100.0))
    {
        // offered DB work = sum over ops of rate x mean service time
        let mut work = 0.0; // busy servers
        for op in PersistOp::ALL {
            if op.is_visibility() {
                continue;
            }
            if let Some(r) = obs.rate("persistence_requests", &[("operation", op.as_str())]) {
                work += r * p.db_latency[op.idx()].mean() / 1e6;
            }
        }
        if work > 0.0 && util > 0.01 {
            let cap = (work / util.min(0.99)).ceil().max(1.0) as u32;
            notes.push(format!(
                "calibrated database capacity to {cap} concurrent operations (observed utilisation {:.0}%, offered {:.1} busy servers)",
                util * 100.0,
                work
            ));
            p.db_capacity = cap;
        }
    }

    // --- workload rates -------------------------------------------------------------------------
    if !workload {
        p.prov.notes.extend(notes);
        return;
    }
    let fe = [
        ("service_name", "frontend"),
        ("operation", "StartWorkflowExecution"),
    ];
    if let Some(obs_rate) = obs.rate("service_requests", &fe) {
        let scen: f64 = p
            .wf_types
            .iter()
            .filter(|t| !t.system_scheduler)
            .map(|t| t.start_rate)
            .sum();
        if scen > 0.0 && obs_rate > 0.0 {
            let k = obs_rate / scen;
            if (k - 1.0).abs() > 0.02 {
                for t in p.wf_types.iter_mut().filter(|t| !t.system_scheduler) {
                    t.start_rate *= k;
                }
                notes.push(format!(
                    "scaled workflow start rates x{k:.2} to match observed StartWorkflowExecution {}",
                    fmt_rate(obs_rate)
                ));
            }
        }
    }
    let sig = [
        ("service_name", "frontend"),
        ("operation", "SignalWorkflowExecution"),
    ];
    if let Some(obs_rate) = obs.rate("service_requests", &sig) {
        let scen: f64 = p.signals.iter().map(|s| s.rate).sum();
        if scen > 0.0 && obs_rate > 0.0 {
            let k = obs_rate / scen;
            for s in &mut p.signals {
                s.rate *= k;
            }
            notes.push(format!(
                "scaled signal rates x{k:.2} to match observed {}",
                fmt_rate(obs_rate)
            ));
        }
    }
    for n in &obs.notes {
        notes.push(n.clone());
    }
    p.prov.notes.extend(notes);
}

/// Observed CPU cores for a service (sum over pods).
pub fn cpu_cores(obs: &Observations, svc: Service) -> Option<f64> {
    let name = svc.as_str();
    let mut total = 0.0;
    let mut any = false;
    for o in &obs.items {
        let is_cpu = matches!(
            o.name.as_str(),
            "container_cpu_usage_seconds" | "container_cpu_usage" | "cpu_cores" | "process_cpu"
        );
        if !is_cpu {
            continue;
        }
        let matches_svc = o.labels.iter().any(|(k, v)| {
            (k == "container"
                || k == "service_name"
                || k == "app_kubernetes_io_component"
                || k == "service")
                && (v == name
                    || v.ends_with(&format!("-{name}"))
                    || v.ends_with(&format!("_{name}")))
        });
        if !matches_svc {
            continue;
        }
        if let Some(r) = o.rate.or(o.value) {
            total += r;
            any = true;
        }
    }
    any.then_some(total)
}
