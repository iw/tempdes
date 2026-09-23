//! Two-dimensional sweeps: replica counts (rows) × dynamic config values (columns).
//!
//! Axis specs (repeat a flag for a cartesian product):
//! * rows: `history=3,6,9`, `frontend+history=2+3,3+6` (linked values), `load=0.5,1,2`
//! * cols: `history.shardIOConcurrency=1,2,4`, `frontend.namespaceRPS[namespace=orders]=500,1000`,
//!   linked `frontend.rps+history.rps=1000+2000,2000+4000`, or `load=…` / replica keys.
//!
//! Every cell is an independent deterministic simulation (same seed), run in parallel.

use std::path::Path;
use std::process::ExitCode;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use serde::Serialize;

use crate::cli::parse_dc_override;
use crate::config::scenario::Scenario;
use crate::report::{self, RunResult, Severity};
use crate::run::{self, Overrides};
use crate::util::units::{fmt_pct, fmt_rate, fmt_us};

/// One setting applied by an axis value.
#[derive(Clone, Debug, Serialize)]
pub enum Setting {
    Replicas(String, u32),
    Dc(String, String),
    Load(f64),
    ClientLb(crate::config::scenario::ClientLb),
}

impl Setting {
    fn label(&self) -> String {
        match self {
            Setting::Replicas(s, n) => format!("{s}={n}"),
            Setting::Dc(k, v) => format!("{}={v}", short_key(k)),
            Setting::Load(l) => format!("load×{l}"),
            Setting::ClientLb(m) => format!("client_lb={m}"),
        }
    }

    fn apply(&self, ov: &mut Overrides) -> anyhow::Result<()> {
        match self {
            Setting::Replicas(s, n) => {
                ov.replicas.retain(|(x, _)| x != s);
                ov.replicas.push((s.clone(), *n));
            }
            Setting::Dc(k, v) => {
                let (key, val, cons) = parse_dc_override(&format!("{k}={v}"))?;
                ov.dc
                    .retain(|(x, _, c)| !(x.eq_ignore_ascii_case(&key) && *c == cons));
                ov.dc.push((key, val, cons));
            }
            Setting::Load(l) => ov.start_rate_scale = Some(ov.start_rate_scale.unwrap_or(1.0) * l),
            Setting::ClientLb(m) => ov.client_lb = Some(*m),
        }
        Ok(())
    }
}

fn short_key(k: &str) -> String {
    k.to_string()
}

/// Parse one axis spec into its list of values (each value = several settings).
fn parse_axis(spec: &str) -> anyhow::Result<Vec<Vec<Setting>>> {
    // split "keys=values" at the last '=' that is not inside [...]
    let mut depth = 0;
    let mut eq = None;
    for (i, ch) in spec.char_indices() {
        match ch {
            '[' => depth += 1,
            ']' => depth -= 1,
            '=' if depth == 0 => {
                eq = Some(i);
                break;
            }
            _ => {}
        }
    }
    let eq = eq.ok_or_else(|| anyhow::anyhow!("axis spec {spec:?} needs KEY=V1,V2,…"))?;
    let keys: Vec<&str> = spec[..eq].split('+').map(str::trim).collect();
    let values = &spec[eq + 1..];
    let mut out = Vec::new();
    for v in values.split(',') {
        let parts: Vec<&str> = v.split('+').map(str::trim).collect();
        anyhow::ensure!(
            parts.len() == keys.len(),
            "axis {spec:?}: value {v:?} has {} parts for {} keys",
            parts.len(),
            keys.len()
        );
        let mut settings = Vec::new();
        for (k, val) in keys.iter().zip(parts) {
            let lk = k.to_ascii_lowercase();
            let s = match lk.as_str() {
                "frontend" | "history" | "matching" | "worker" => {
                    Setting::Replicas(lk.clone(), val.parse()?)
                }
                "replicas.frontend" | "replicas.history" | "replicas.matching"
                | "replicas.worker" => {
                    Setting::Replicas(lk.trim_start_matches("replicas.").to_string(), val.parse()?)
                }
                "load" => Setting::Load(val.parse()?),
                "client_lb" | "network.client_lb" | "cluster.network.client_lb" => {
                    Setting::ClientLb(val.parse().map_err(|e: String| anyhow::anyhow!(e))?)
                }
                _ => {
                    let base = k.split('[').next().unwrap_or(k);
                    if crate::config::dynamic::registry().get(base).is_none() {
                        let sugg = crate::config::dynamic::registry().suggest(base, 3);
                        anyhow::bail!(
                            "{base:?} is not a Temporal 1.31.0 dynamic config key{}",
                            if sugg.is_empty() {
                                String::new()
                            } else {
                                format!(" (did you mean {}?)", sugg.join(", "))
                            }
                        );
                    }
                    Setting::Dc(k.to_string(), val.to_string())
                }
            };
            settings.push(s);
        }
        out.push(settings);
    }
    Ok(out)
}

/// Cartesian product of several axes.
fn product(axes: &[Vec<Vec<Setting>>]) -> Vec<Vec<Setting>> {
    let mut acc: Vec<Vec<Setting>> = vec![Vec::new()];
    for axis in axes {
        let mut next = Vec::new();
        for prefix in &acc {
            for v in axis {
                let mut p = prefix.clone();
                p.extend(v.iter().cloned());
                next.push(p);
            }
        }
        acc = next;
    }
    acc
}

#[derive(Clone, Debug, Serialize)]
pub struct Cell {
    pub row: usize,
    pub col: usize,
    pub row_label: String,
    pub col_label: String,
    pub completed_per_s: f64,
    pub offered_per_s: f64,
    pub e2e_p99_ms: f64,
    pub start_p99_ms: f64,
    pub wft_s2s_p99_ms: f64,
    pub cpu_max: std::collections::BTreeMap<String, f64>,
    pub db_util: f64,
    pub shard_io_max: f64,
    pub lock_wait_p99_ms: f64,
    pub rejections_per_s: f64,
    pub api_error_rate: f64,
    pub critical: usize,
    pub warning: usize,
    pub top_hotspot: String,
    pub top_category: String,
    pub headline: String,
    pub hotspots: Vec<report::Hotspot>,
    pub error: Option<String>,
}

impl Cell {
    fn from_result(row: usize, col: usize, rl: &str, cl: &str, r: &RunResult) -> Cell {
        let first = r.hotspots.iter().find(|h| h.severity != Severity::Info);
        let total_err: f64 = r
            .apis
            .iter()
            .filter(|a| !a.api.starts_with("Poll"))
            .map(|a| a.error_rate * a.per_s)
            .sum();
        let total_calls: f64 = r
            .apis
            .iter()
            .filter(|a| !a.api.starts_with("Poll"))
            .map(|a| a.per_s)
            .sum();
        Cell {
            row,
            col,
            row_label: rl.to_string(),
            col_label: cl.to_string(),
            completed_per_s: r.total_completed_per_s(),
            offered_per_s: r.total_offered_per_s(),
            e2e_p99_ms: r
                .workflows
                .iter()
                .filter(|w| w.offered_start_rate > 0.0)
                .map(|w| w.e2e.p99_ms)
                .fold(0.0, f64::max),
            start_p99_ms: r
                .apis
                .iter()
                .find(|a| a.api == "StartWorkflowExecution")
                .map(|a| a.latency.p99_ms)
                .unwrap_or(0.0),
            wft_s2s_p99_ms: r
                .workflows
                .iter()
                .map(|w| w.wft_schedule_to_start.p99_ms)
                .fold(0.0, f64::max),
            cpu_max: r
                .services
                .iter()
                .map(|s| (s.service.clone(), s.cpu_max))
                .collect(),
            db_util: r.persistence.utilization,
            shard_io_max: r.history.shard_util_max,
            lock_wait_p99_ms: r.history.lock_wait.p99_ms,
            rejections_per_s: r.limits.iter().map(|l| l.per_s).sum(),
            api_error_rate: if total_calls > 0.0 {
                total_err / total_calls
            } else {
                0.0
            },
            critical: r
                .hotspots
                .iter()
                .filter(|h| h.severity == Severity::Critical)
                .count(),
            warning: r
                .hotspots
                .iter()
                .filter(|h| h.severity == Severity::Warning)
                .count(),
            top_hotspot: first.map(|h| h.title.clone()).unwrap_or_default(),
            top_category: first.map(|h| h.category.clone()).unwrap_or_default(),
            headline: r.headline.clone(),
            hotspots: r.hotspots.iter().take(8).cloned().collect(),
            error: None,
        }
    }

    pub fn status(&self) -> &'static str {
        if self.error.is_some() {
            "ERR"
        } else if self.critical > 0 {
            "CRIT"
        } else if self.warning > 0 {
            "WARN"
        } else {
            "OK"
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct SweepResult {
    pub scenario: String,
    pub temporal_version: String,
    pub rows: Vec<String>,
    pub cols: Vec<String>,
    pub cells: Vec<Cell>,
    pub base: String,
    pub wall_ms: u128,
}

#[allow(clippy::too_many_arguments)]
pub fn cmd_sweep(
    scenario: &Path,
    base: Overrides,
    observed: &[String],
    rows: &[String],
    cols: &[String],
    jobs: Option<usize>,
    json: Option<&Path>,
    csv: Option<&Path>,
    html: Option<&Path>,
) -> anyhow::Result<ExitCode> {
    let sc = Scenario::load(scenario)?;
    let obs = run::load_observations(&sc, observed)?;
    let cal = match obs.clone() {
        Some(o) => {
            eprintln!("calibrating against observed metrics (base configuration)…");
            Some(run::calibrate(&sc, &base, o)?)
        }
        None => None,
    };
    let row_axes: Vec<Vec<Vec<Setting>>> = rows
        .iter()
        .map(|s| parse_axis(s))
        .collect::<anyhow::Result<_>>()?;
    let col_axes: Vec<Vec<Vec<Setting>>> = cols
        .iter()
        .map(|s| parse_axis(s))
        .collect::<anyhow::Result<_>>()?;
    let row_vals = product(&row_axes);
    let col_vals = product(&col_axes);
    let label = |v: &Vec<Setting>| {
        if v.is_empty() {
            "(scenario)".to_string()
        } else {
            v.iter().map(Setting::label).collect::<Vec<_>>().join(" ")
        }
    };
    let row_labels: Vec<String> = row_vals.iter().map(label).collect();
    let col_labels: Vec<String> = col_vals.iter().map(label).collect();
    let n = row_vals.len() * col_vals.len();
    let jobs = jobs
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|x| x.get())
                .unwrap_or(4)
        })
        .clamp(1, n.max(1));
    eprintln!(
        "sweeping {} × {} = {n} simulations on {jobs} threads…",
        row_vals.len(),
        col_vals.len()
    );
    let wall = std::time::Instant::now();
    let next = AtomicUsize::new(0);
    let cells: Mutex<Vec<Option<Cell>>> = Mutex::new(vec![None; n]);
    let done = AtomicUsize::new(0);
    std::thread::scope(|s| {
        for _ in 0..jobs {
            s.spawn(|| {
                loop {
                    let i = next.fetch_add(1, Ordering::SeqCst);
                    if i >= n {
                        break;
                    }
                    let (ri, ci) = (i / col_vals.len(), i % col_vals.len());
                    let mut ov = base.clone();
                    let cell = (|| -> anyhow::Result<Cell> {
                        for st in row_vals[ri].iter().chain(col_vals[ci].iter()) {
                            st.apply(&mut ov)?;
                        }
                        let p = run::prepare(&sc, &ov, cal.as_ref())?;
                        let out = run::run_params(p);
                        let mut r = report::analyze(&out.ctx, &out.info, obs.as_ref());
                        r.label = ov.label();
                        Ok(Cell::from_result(
                            ri,
                            ci,
                            &row_labels[ri],
                            &col_labels[ci],
                            &r,
                        ))
                    })();
                    let cell = cell.unwrap_or_else(|e| Cell {
                        row: ri,
                        col: ci,
                        row_label: row_labels[ri].clone(),
                        col_label: col_labels[ci].clone(),
                        completed_per_s: 0.0,
                        offered_per_s: 0.0,
                        e2e_p99_ms: 0.0,
                        start_p99_ms: 0.0,
                        wft_s2s_p99_ms: 0.0,
                        cpu_max: Default::default(),
                        db_util: 0.0,
                        shard_io_max: 0.0,
                        lock_wait_p99_ms: 0.0,
                        rejections_per_s: 0.0,
                        api_error_rate: 0.0,
                        critical: 0,
                        warning: 0,
                        top_hotspot: String::new(),
                        top_category: String::new(),
                        headline: String::new(),
                        hotspots: Vec::new(),
                        error: Some(format!("{e:#}")),
                    });
                    cells.lock().unwrap()[i] = Some(cell);
                    let d = done.fetch_add(1, Ordering::SeqCst) + 1;
                    eprint!("\r  {d}/{n} done");
                }
            });
        }
    });
    eprintln!();
    let result = SweepResult {
        scenario: sc.name.clone().unwrap_or_else(|| "scenario".into()),
        temporal_version: crate::config::dynamic::registry().temporal_version.clone(),
        rows: row_labels,
        cols: col_labels,
        cells: cells
            .into_inner()
            .unwrap()
            .into_iter()
            .map(|c| c.unwrap())
            .collect(),
        base: base.label(),
        wall_ms: wall.elapsed().as_millis(),
    };
    print!("{}", render_text(&result));
    if let Some(p) = json {
        std::fs::write(p, serde_json::to_string_pretty(&result)?)?;
        eprintln!("wrote {}", p.display());
    }
    if let Some(p) = csv {
        std::fs::write(p, render_csv(&result))?;
        eprintln!("wrote {}", p.display());
    }
    if let Some(p) = html {
        std::fs::write(p, report::html::render_sweep(&result))?;
        eprintln!("wrote {}", p.display());
    }
    Ok(ExitCode::SUCCESS)
}

fn grid(r: &SweepResult, title: &str, f: &dyn Fn(&Cell) -> String) -> String {
    use crate::report::text::Table;
    let mut head: Vec<&str> = vec!["replicas \\ dynamic config"];
    for c in &r.cols {
        head.push(c.as_str());
    }
    let mut t = Table::new(&head);
    for (ri, rl) in r.rows.iter().enumerate() {
        let mut row = vec![rl.clone()];
        for ci in 0..r.cols.len() {
            let cell = &r.cells[ri * r.cols.len() + ci];
            row.push(if cell.error.is_some() {
                "error".into()
            } else {
                f(cell)
            });
        }
        t.row(row);
    }
    format!(
        "\n{title}\n{}",
        t.render(&crate::report::text::Style::new(), 2)
    )
}

pub fn render_text(r: &SweepResult) -> String {
    let mut o = String::new();
    o.push_str(&format!(
        "\ntempdes sweep · Temporal {} · {} · {} cells in {:.1}s{}\n",
        r.temporal_version,
        r.scenario,
        r.cells.len(),
        r.wall_ms as f64 / 1000.0,
        if r.base.is_empty() {
            String::new()
        } else {
            format!(" · base overrides: {}", r.base)
        }
    ));
    o.push_str(&grid(
        r,
        "STATUS (critical/warning hotspot count · top hotspot category)",
        &|c| {
            if c.critical + c.warning == 0 {
                "OK".into()
            } else {
                format!(
                    "{} {}c/{}w {}",
                    c.status(),
                    c.critical,
                    c.warning,
                    c.top_category
                )
            }
        },
    ));
    o.push_str(&grid(r, "COMPLETED WORKFLOWS / OFFERED STARTS", &|c| {
        format!(
            "{} / {}",
            fmt_rate(c.completed_per_s),
            fmt_rate(c.offered_per_s)
        )
    }));
    o.push_str(&grid(r, "WORKFLOW END-TO-END p99", &|c| {
        fmt_us(c.e2e_p99_ms * 1e3)
    }));
    o.push_str(&grid(r, "StartWorkflowExecution p99 (client)", &|c| {
        fmt_us(c.start_p99_ms * 1e3)
    }));
    o.push_str(&grid(
        r,
        "MAX POD CPU  frontend / history / matching",
        &|c| {
            format!(
                "{} / {} / {}",
                fmt_pct(*c.cpu_max.get("frontend").unwrap_or(&0.0)),
                fmt_pct(*c.cpu_max.get("history").unwrap_or(&0.0)),
                fmt_pct(*c.cpu_max.get("matching").unwrap_or(&0.0))
            )
        },
    ));
    o.push_str(&grid(r, "DATABASE BUSY · HOTTEST SHARD IO BUSY", &|c| {
        format!("{} · {}", fmt_pct(c.db_util), fmt_pct(c.shard_io_max))
    }));
    o.push_str(&grid(r, "RATE-LIMIT REJECTIONS /s", &|c| {
        fmt_rate(c.rejections_per_s)
    }));
    o.push_str("\nTOP HOTSPOT PER CELL\n");
    for c in &r.cells {
        let s = match &c.error {
            Some(e) => format!("error: {e}"),
            None if c.top_hotspot.is_empty() => "no hotspots".into(),
            None => format!("{} {}", c.status(), c.top_hotspot),
        };
        o.push_str(&format!("  [{} | {}] {}\n", c.row_label, c.col_label, s));
    }
    o.push('\n');
    o
}

pub fn render_csv(r: &SweepResult) -> String {
    let mut o = String::from(
        "row,col,status,completed_per_s,offered_per_s,e2e_p99_ms,start_p99_ms,wft_s2s_p99_ms,cpu_max_frontend,cpu_max_history,cpu_max_matching,db_util,shard_io_max,lock_wait_p99_ms,rejections_per_s,api_error_rate,critical,warning,top_hotspot\n",
    );
    for c in &r.cells {
        let q = |s: &str| format!("\"{}\"", s.replace('"', "'"));
        o.push_str(&format!(
            "{},{},{},{:.3},{:.3},{:.3},{:.3},{:.3},{:.4},{:.4},{:.4},{:.4},{:.4},{:.3},{:.3},{:.5},{},{},{}\n",
            q(&c.row_label),
            q(&c.col_label),
            c.status(),
            c.completed_per_s,
            c.offered_per_s,
            c.e2e_p99_ms,
            c.start_p99_ms,
            c.wft_s2s_p99_ms,
            c.cpu_max.get("frontend").unwrap_or(&0.0),
            c.cpu_max.get("history").unwrap_or(&0.0),
            c.cpu_max.get("matching").unwrap_or(&0.0),
            c.db_util,
            c.shard_io_max,
            c.lock_wait_p99_ms,
            c.rejections_per_s,
            c.api_error_rate,
            c.critical,
            c.warning,
            q(c.error.as_deref().unwrap_or(&c.top_hotspot))
        ));
    }
    o
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn axis_parsing() {
        let a = parse_axis("history=3,6,9").unwrap();
        assert_eq!(a.len(), 3);
        let b = parse_axis("frontend+history=2+3,3+6").unwrap();
        assert_eq!(b.len(), 2);
        assert_eq!(b[1].len(), 2);
        let c = parse_axis("frontend.namespaceRPS[namespace=orders]=500,1000").unwrap();
        assert!(
            matches!(&c[0][0], Setting::Dc(k, v) if k.contains("[namespace=orders]") && v == "500")
        );
        assert!(parse_axis("history.shardIoConcurency=1,2").is_err());
        let p = product(&[a, b]);
        assert_eq!(p.len(), 6);
        let a = parse_axis("client_lb=pinned,round_robin").unwrap();
        assert_eq!(a.len(), 2);
        assert_eq!(a[1][0].label(), "client_lb=round_robin");
        assert!(parse_axis("client_lb=sticky").is_err());
    }
}
