//! Loading a scenario with overrides and running one simulation.

use std::path::Path;

use anyhow::Context;

use crate::config::dynamic::{Constraints, DcValue, DynamicConfig};
use crate::config::scenario::{ClientLb, Replicas, Scenario};
use crate::metrics::observed::Observations;
use crate::model::build::{self, RunInfo};
use crate::model::params::Params;
use crate::model::world::Ctx;

/// Overrides applied on top of a scenario (CLI flags, sweep cells).
#[derive(Clone, Debug, Default)]
pub struct Overrides {
    pub replicas: Vec<(String, u32)>,
    pub dc: Vec<(String, DcValue, Constraints)>,
    pub duration_s: Option<f64>,
    pub warmup_s: Option<f64>,
    pub seed: Option<u64>,
    pub start_rate_scale: Option<f64>,
    pub client_lb: Option<ClientLb>,
}

impl Overrides {
    pub fn label(&self) -> String {
        let mut parts: Vec<String> = self
            .replicas
            .iter()
            .map(|(s, n)| format!("{s}={n}"))
            .collect();
        parts.extend(self.dc.iter().map(|(k, v, _)| format!("{k}={v}")));
        if let Some(s) = self.start_rate_scale {
            parts.push(format!("load×{s}"));
        }
        if let Some(lb) = self.client_lb {
            parts.push(format!("client_lb={lb}"));
        }
        parts.join(" ")
    }
}

pub fn load_dynamic_config(sc: &Scenario) -> anyhow::Result<DynamicConfig> {
    let mut dc = DynamicConfig::new();
    for f in &sc.dynamic_config_files {
        let path = sc.resolve_path(f);
        let text = std::fs::read_to_string(&path)
            .with_context(|| format!("reading dynamic config {}", path.display()))?;
        dc.merge_yaml_str(&text, &path.display().to_string())?;
    }
    dc.merge_inline(&sc.dynamic_config, "scenario.dynamic_config");
    Ok(dc)
}

pub fn apply_replicas(base: Replicas, ov: &[(String, u32)]) -> anyhow::Result<Replicas> {
    let mut r = base;
    for (svc, n) in ov {
        match svc.as_str() {
            "frontend" => r.frontend = *n,
            "history" => r.history = *n,
            "matching" => r.matching = *n,
            "worker" => r.worker = *n,
            other => anyhow::bail!("unknown service {other:?} (frontend|history|matching|worker)"),
        }
    }
    anyhow::ensure!(
        r.frontend >= 1 && r.history >= 1 && r.matching >= 1 && r.worker >= 1,
        "replica counts must be >= 1"
    );
    Ok(r)
}

/// Observed metrics plus what pilot simulations derived from them.
#[derive(Clone, Debug)]
pub struct Calibration {
    pub obs: Observations,
    pub persistence_latency: bool,
    pub workload: bool,
    /// per-service CPU cost multipliers from the pilot runs
    pub cpu_scale: [Option<f64>; 4],
    /// per persistence operation (indexed by `PersistOp`): the factor on the observed latency
    /// distribution that gives its service time, so that the pilot's latency, queueing
    /// included, matches the observed latency (`None` = not fitted)
    pub persistence_fit: Vec<Option<f64>>,
    pub notes: Vec<String>,
}

/// Pilot runs at most, and the relative error that ends the fit early.
const PILOT_RUNS: usize = 3;
const FIT_TOLERANCE: f64 = 0.03;

/// Build a calibration from observations. Pilot simulations of the *base* configuration (the one
/// the observations came from), at the observed load, derive:
///
/// * CPU cost scales, when CPU usage is observed: observed cores ÷ simulated cores per service;
/// * persistence service times, when persistence latency is observed: production measures the
///   whole call, queueing in the connection pool and the database included, so using it as the
///   service time would count the queueing twice. Each operation's service time is the observed
///   distribution times a factor, adjusted until the pilot's mean latency matches the observed
///   mean (a one-dimensional fit per operation, repeated because the operations share the
///   database).
///
/// Sweeps reuse the results for every cell, and a `--load` multiplier applies on top.
pub fn calibrate(
    sc: &Scenario,
    base: &Overrides,
    obs: Observations,
) -> anyhow::Result<Calibration> {
    use crate::model::types::{PersistOp, Service};
    let flags = sc
        .calibration
        .clone()
        .unwrap_or(crate::config::scenario::CalibrationSpec {
            observations: Vec::new(),
            persistence_latency: true,
            cpu: true,
            workload: true,
        });
    let mut cal = Calibration {
        obs,
        persistence_latency: flags.persistence_latency,
        workload: flags.workload,
        cpu_scale: [None; 4],
        persistence_fit: vec![None; PersistOp::ALL.len()],
        notes: Vec::new(),
    };
    let cpu_targets: Vec<(Service, f64)> = if flags.cpu {
        [Service::Frontend, Service::History, Service::Matching]
            .into_iter()
            .filter_map(|s| crate::calibrate::cpu_cores(&cal.obs, s).map(|c| (s, c)))
            .collect()
    } else {
        Vec::new()
    };
    // the database operations whose observed latency is fitted (visibility stores excluded)
    let latency_targets: Vec<(PersistOp, f64)> = if flags.persistence_latency {
        PersistOp::ALL
            .into_iter()
            .filter(|op| !op.is_visibility())
            .filter_map(|op| {
                crate::calibrate::observed_latency(&cal.obs, op).map(|d| (op, d.mean()))
            })
            .filter(|(_, mean)| *mean > 0.0)
            .collect()
    } else {
        Vec::new()
    };
    if cpu_targets.is_empty() && latency_targets.is_empty() {
        return Ok(cal);
    }
    let mut pilot = base.clone();
    pilot.start_rate_scale = None; // observations belong to the observed load
    pilot.warmup_s = Some(sc.warmup().secs().min(15.0));
    pilot.duration_s = Some(sc.duration.secs().min(30.0));
    // per fitted operation: observed mean, the last pilot's mean and the factor it ran with
    let mut last_errors: Vec<(PersistOp, f64, f64, f64)> = Vec::new();
    for run in 0..PILOT_RUNS {
        let p = prepare(sc, &pilot, Some(&cal))?;
        let out = run_params(p);
        let mut converged = true;
        {
            let pods = out.ctx.pods.borrow();
            for &(svc, observed) in &cpu_targets {
                // demand already includes the scale the pilot ran with
                let simulated: f64 = pods
                    .iter()
                    .filter(|p| p.svc == svc && p.alive)
                    .map(|p| p.cpu.demand_us() / p.cpu.window_us().max(1.0))
                    .sum();
                if simulated > 0.0 {
                    let current = cal.cpu_scale[svc.idx()].unwrap_or(1.0);
                    let k = (current * observed / simulated).clamp(0.05, 20.0);
                    converged &= (k / current - 1.0).abs() < FIT_TOLERANCE;
                    cal.cpu_scale[svc.idx()] = Some(k);
                }
            }
        }
        last_errors.clear();
        {
            let m = out.ctx.m.borrow();
            for &(op, observed) in &latency_targets {
                let o = &m.persist[op.idx()];
                if o.count - o.error_count() < 50 {
                    continue; // too few calls in the pilot to fit
                }
                let simulated = o.latency.mean();
                if simulated <= 0.0 {
                    continue;
                }
                let current = cal.persistence_fit[op.idx()].unwrap_or(1.0);
                // latency can't be shorter than service time: the factor stays at or below 1
                let k = (current * observed / simulated).clamp(0.05, 1.0);
                converged &= (k / current - 1.0).abs() < FIT_TOLERANCE;
                cal.persistence_fit[op.idx()] = Some(k);
                last_errors.push((op, observed, simulated, current));
            }
        }
        if converged || run + 1 == PILOT_RUNS {
            break;
        }
    }
    for (svc, observed) in &cpu_targets {
        if let Some(k) = cal.cpu_scale[svc.idx()] {
            cal.notes.push(format!(
                "calibrated {svc} CPU costs x{k:.2}: pilot simulations of the observed configuration matched production's {observed:.2} cores"
            ));
        }
    }
    for (op, observed, simulated, pilot_k) in last_errors {
        let (Some(k), Some(d)) = (
            cal.persistence_fit[op.idx()],
            crate::calibrate::observed_latency(&cal.obs, op),
        ) else {
            continue;
        };
        let d = d.scaled(k);
        cal.notes.push(format!(
            "calibrated {} service time as the observed latency ×{k:.2}, p50 {} p99 {}: a pilot at ×{pilot_k:.2} measured a mean latency of {}, queueing included, against production's {}",
            op.as_str(),
            crate::util::units::fmt_us(d.quantile(0.5)),
            crate::util::units::fmt_us(d.quantile(0.99)),
            crate::util::units::fmt_us(simulated),
            crate::util::units::fmt_us(observed)
        ));
    }
    Ok(cal)
}

/// Resolve a scenario + overrides (+ calibration) into parameters.
pub fn prepare(sc: &Scenario, ov: &Overrides, cal: Option<&Calibration>) -> anyhow::Result<Params> {
    let mut sc = sc.clone();
    if let Some(d) = ov.duration_s {
        sc.duration = crate::util::units::Dur::from_secs(d);
    }
    if let Some(w) = ov.warmup_s {
        sc.warmup = Some(crate::util::units::Dur::from_secs(w));
    }
    if let Some(s) = ov.seed {
        sc.seed = s;
    }
    if let Some(lb) = ov.client_lb {
        sc.cluster.network.client_lb = lb;
    }
    if let Some(k) = ov.start_rate_scale {
        for w in &mut sc.workflows {
            if let Some(r) = w.start_rate.as_mut() {
                r.0 *= k;
            }
        }
        for s in &mut sc.load.signals {
            s.rate.0 *= k;
        }
    }
    let mut dc = load_dynamic_config(&sc)?;
    for (k, v, c) in &ov.dc {
        dc.set(k, v.clone(), c.clone());
    }
    let replicas = apply_replicas(sc.cluster.replicas, &ov.replicas)?;
    let mut p = Params::build(&sc, dc, replicas)?;
    if let Some(c) = cal {
        let load = ov.start_rate_scale.unwrap_or(1.0);
        crate::calibrate::apply(
            &mut p,
            &c.obs,
            c.persistence_latency,
            c.workload,
            load,
            &c.persistence_fit,
        );
        if !is_observed_load(ov) {
            p.prov.notes.push(format!(
                "load ×{load} applied on top of the calibrated workload; the comparison with observed metrics is skipped"
            ));
        }
        for (i, k) in c.cpu_scale.iter().enumerate() {
            if let Some(k) = k {
                p.costs.scale[i] = *k;
            }
        }
        p.prov.notes.extend(c.notes.iter().cloned());
    }
    Ok(p)
}

/// True when the overrides keep the scenario's (or the observed) load, so simulated metrics can
/// be compared with observed ones.
pub fn is_observed_load(ov: &Overrides) -> bool {
    ov.start_rate_scale.is_none_or(|k| (k - 1.0).abs() < 1e-9)
}

/// Observations to validate a run against: none when a load multiplier makes the simulated
/// workload differ from the observed one.
pub fn validation_obs<'a>(
    ov: &Overrides,
    obs: Option<&'a Observations>,
) -> Option<&'a Observations> {
    obs.filter(|_| is_observed_load(ov))
}

pub struct RunOutput {
    pub ctx: Ctx,
    pub info: RunInfo,
}

pub fn run_params(p: Params) -> RunOutput {
    let (ctx, ex) = build::build(p);
    let info = build::run(&ctx, ex);
    RunOutput { ctx, info }
}

pub fn load_observations(sc: &Scenario, extra: &[String]) -> anyhow::Result<Option<Observations>> {
    let mut files: Vec<std::path::PathBuf> = Vec::new();
    if let Some(c) = &sc.calibration {
        for f in &c.observations {
            files.push(sc.resolve_path(f));
        }
    }
    for f in extra {
        files.push(Path::new(f).to_path_buf());
    }
    if files.is_empty() {
        return Ok(None);
    }
    let mut all = Observations::default();
    for f in files {
        all.merge(Observations::load(&f)?);
    }
    Ok(Some(all))
}
