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

/// Observed metrics plus what a pilot simulation derived from them.
#[derive(Clone, Debug)]
pub struct Calibration {
    pub obs: Observations,
    pub persistence_latency: bool,
    pub workload: bool,
    /// per-service CPU cost multipliers from the pilot run
    pub cpu_scale: [Option<f64>; 4],
    pub notes: Vec<String>,
}

/// Build a calibration from observations. When CPU usage is observed, a pilot simulation of the
/// *base* configuration (the one the observations came from) measures simulated CPU demand
/// and the cost tables are scaled to match; sweeps then reuse the same scales for every cell.
pub fn calibrate(
    sc: &Scenario,
    base: &Overrides,
    obs: Observations,
) -> anyhow::Result<Calibration> {
    use crate::model::types::Service;
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
        notes: Vec::new(),
    };
    let targets: Vec<(Service, f64)> = [Service::Frontend, Service::History, Service::Matching]
        .into_iter()
        .filter_map(|s| crate::calibrate::cpu_cores(&cal.obs, s).map(|c| (s, c)))
        .collect();
    if !flags.cpu || targets.is_empty() {
        return Ok(cal);
    }
    let mut pilot = base.clone();
    pilot.warmup_s = Some(sc.warmup().secs().min(15.0));
    pilot.duration_s = Some(sc.duration.secs().min(30.0));
    let p = prepare(sc, &pilot, Some(&cal))?;
    let out = run_params(p);
    let pods = out.ctx.pods.borrow();
    for (svc, observed) in targets {
        let simulated: f64 = pods
            .iter()
            .filter(|p| p.svc == svc && p.alive)
            .map(|p| {
                p.cpu.demand_us() / p.cpu.window_us().max(1.0) * out.ctx.p.costs.scale[svc.idx()]
            })
            .sum();
        if simulated > 0.0 {
            let k = (observed / simulated).clamp(0.05, 20.0);
            cal.cpu_scale[svc.idx()] = Some(k);
            cal.notes.push(format!(
                "calibrated {svc} CPU costs x{k:.2}: pilot simulation of the observed configuration used {simulated:.2} cores, production {observed:.2}"
            ));
        }
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
        crate::calibrate::apply(&mut p, &c.obs, c.persistence_latency, c.workload);
        for (i, k) in c.cpu_scale.iter().enumerate() {
            if let Some(k) = k {
                p.costs.scale[i] = *k;
            }
        }
        p.prov.notes.extend(c.notes.iter().cloned());
    }
    Ok(p)
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
