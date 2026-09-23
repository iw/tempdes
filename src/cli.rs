//! Command line interface.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Args, Parser, Subcommand};

use crate::config::dynamic::{Constraints, DcValue};
use crate::config::scenario::Scenario;
use crate::run::{self, Overrides};

#[derive(Parser)]
#[command(
    name = "tempdes",
    version,
    about = "Discrete-event simulator that surfaces hotspots in Temporal 1.31.0 clusters on EKS",
    long_about = None
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Simulate one configuration and print a hotspot report.
    Run(RunArgs),
    /// Sweep replica counts (rows) × dynamic config values (columns) and compare hotspots.
    Sweep(SweepArgs),
    /// Inspect / validate Temporal 1.31.0 dynamic config.
    Dc {
        #[command(subcommand)]
        cmd: DcCmd,
    },
    /// Work with observed Temporal metrics (calibration inputs).
    Metrics {
        #[command(subcommand)]
        cmd: MetricsCmd,
    },
}

#[derive(Args, Clone)]
pub struct CommonArgs {
    /// Scenario file (YAML).
    scenario: PathBuf,
    /// Override replica counts: `history=6` (repeatable).
    #[arg(long = "replicas", short = 'r', value_name = "SERVICE=N")]
    replicas: Vec<String>,
    /// Override a dynamic config value: `history.shardIOConcurrency=2` or
    /// `frontend.namespaceRPS[namespace=orders]=500` (repeatable).
    #[arg(long = "dc", short = 'd', value_name = "KEY=VALUE")]
    dc: Vec<String>,
    /// Observed metrics (YAML observations or Prometheus text) used for calibration.
    #[arg(long = "observed", short = 'o')]
    observed: Vec<String>,
    /// Measured simulated time in seconds (overrides the scenario).
    #[arg(long)]
    duration: Option<f64>,
    /// Warm-up in seconds (overrides the scenario).
    #[arg(long)]
    warmup: Option<f64>,
    /// Random seed (overrides the scenario).
    #[arg(long)]
    seed: Option<u64>,
    /// Multiply all start and signal rates.
    #[arg(long = "load")]
    load: Option<f64>,
    /// How SDK clients reach the frontends: pinned, round_robin or proxy (overrides
    /// `cluster.network.client_lb`).
    #[arg(long = "client-lb", value_name = "MODE")]
    client_lb: Option<crate::config::scenario::ClientLb>,
}

#[derive(Args)]
struct RunArgs {
    #[command(flatten)]
    common: CommonArgs,
    /// Write the full result as JSON.
    #[arg(long)]
    json: Option<PathBuf>,
    /// Write simulated metrics in Prometheus text format (Temporal metric names).
    #[arg(long)]
    prom: Option<PathBuf>,
    /// Write a self-contained HTML report.
    #[arg(long)]
    html: Option<PathBuf>,
    /// Print extra detail (per pod, per shard, per partition).
    #[arg(long, short = 'v')]
    verbose: bool,
}

#[derive(Args)]
struct SweepArgs {
    #[command(flatten)]
    common: CommonArgs,
    /// Row axis: replica settings, e.g. `history=3,6,9` (repeat for a cartesian product) or
    /// linked tuples `frontend+history=2+3,3+6`.
    #[arg(long = "rows", value_name = "SPEC")]
    rows: Vec<String>,
    /// Column axis: dynamic config values, e.g. `history.shardIOConcurrency=1,2,4`
    /// (repeat for a cartesian product).
    #[arg(long = "cols", value_name = "SPEC")]
    cols: Vec<String>,
    /// Parallel simulations (default: available cores).
    #[arg(long, short = 'j')]
    jobs: Option<usize>,
    /// Write sweep results as JSON.
    #[arg(long)]
    json: Option<PathBuf>,
    /// Write sweep results as CSV.
    #[arg(long)]
    csv: Option<PathBuf>,
    /// Write an HTML heatmap report.
    #[arg(long)]
    html: Option<PathBuf>,
}

#[derive(Subcommand)]
enum DcCmd {
    /// List the dynamic config keys the simulator models (with 1.31.0 defaults).
    Modeled,
    /// Search all 1.31.0 dynamic config keys.
    Search { pattern: String },
    /// Show one key: type, scope, default, description.
    Explain { key: String },
    /// Validate a Temporal dynamic config YAML file against 1.31.0.
    Validate { file: PathBuf },
}

#[derive(Subcommand)]
enum MetricsCmd {
    /// Print the PromQL queries whose results feed an observations file.
    Queries {
        /// Rate window, e.g. 5m.
        #[arg(long, default_value = "5m")]
        window: String,
    },
    /// Print an observations file template.
    Template,
    /// Parse an observations / Prometheus file and show what the simulator will use.
    Show { file: PathBuf },
}

pub fn parse_overrides(c: &CommonArgs) -> anyhow::Result<Overrides> {
    let mut ov = Overrides {
        duration_s: c.duration,
        warmup_s: c.warmup,
        seed: c.seed,
        start_rate_scale: c.load,
        client_lb: c.client_lb,
        ..Default::default()
    };
    for r in &c.replicas {
        let (s, n) = r
            .split_once('=')
            .ok_or_else(|| anyhow::anyhow!("--replicas expects SERVICE=N, got {r:?}"))?;
        ov.replicas
            .push((s.trim().to_ascii_lowercase(), n.trim().parse()?));
    }
    for d in &c.dc {
        ov.dc.push(parse_dc_override(d)?);
    }
    Ok(ov)
}

/// `key=value` or `key[namespace=x,taskQueueName=y,taskType=Activity]=value`
pub fn parse_dc_override(s: &str) -> anyhow::Result<(String, DcValue, Constraints)> {
    let (lhs, value) = s
        .rsplit_once('=')
        .ok_or_else(|| anyhow::anyhow!("--dc expects KEY=VALUE, got {s:?}"))?;
    let (key, cons) = match lhs.find('[') {
        Some(i) => {
            let inner = lhs[i + 1..].trim_end_matches(']');
            let mut c = Constraints::default();
            for kv in inner.split(',') {
                let (k, v) = kv
                    .split_once(':')
                    .or_else(|| kv.split_once('='))
                    .ok_or_else(|| anyhow::anyhow!("bad constraint {kv:?}"))?;
                match k.trim().to_ascii_lowercase().as_str() {
                    "namespace" => c.namespace = Some(v.trim().to_string()),
                    "taskqueuename" | "taskqueue" => c.task_queue_name = Some(v.trim().to_string()),
                    "tasktype" => {
                        c.task_queue_type = crate::config::dynamic::TaskQueueType::parse(
                            &DcValue::Str(v.trim().to_string()),
                        )
                    }
                    "shardid" => c.shard_id = v.trim().parse().ok(),
                    other => anyhow::bail!("unknown constraint {other:?}"),
                }
            }
            (lhs[..i].to_string(), c)
        }
        None => (lhs.to_string(), Constraints::default()),
    };
    Ok((key.trim().to_string(), DcValue::parse_cli(value), cons))
}

pub fn main() -> anyhow::Result<ExitCode> {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Run(a) => cmd_run(a),
        Cmd::Sweep(a) => crate::sweep::cmd_sweep(
            &a.common.scenario,
            parse_overrides(&a.common)?,
            &a.common.observed,
            &a.rows,
            &a.cols,
            a.jobs,
            a.json.as_deref(),
            a.csv.as_deref(),
            a.html.as_deref(),
        ),
        Cmd::Dc { cmd } => crate::dccmd::run(cmd_dc(cmd)),
        Cmd::Metrics { cmd } => crate::metrics::cmd::run(match cmd {
            MetricsCmd::Queries { window } => crate::metrics::cmd::Cmd::Queries { window },
            MetricsCmd::Template => crate::metrics::cmd::Cmd::Template,
            MetricsCmd::Show { file } => crate::metrics::cmd::Cmd::Show { file },
        }),
    }
}

fn cmd_dc(cmd: DcCmd) -> crate::dccmd::Cmd {
    match cmd {
        DcCmd::Modeled => crate::dccmd::Cmd::Modeled,
        DcCmd::Search { pattern } => crate::dccmd::Cmd::Search(pattern),
        DcCmd::Explain { key } => crate::dccmd::Cmd::Explain(key),
        DcCmd::Validate { file } => crate::dccmd::Cmd::Validate(file),
    }
}

fn cmd_run(a: RunArgs) -> anyhow::Result<ExitCode> {
    let sc = Scenario::load(&a.common.scenario)?;
    let ov = parse_overrides(&a.common)?;
    let obs = run::load_observations(&sc, &a.common.observed)?;
    let cal = match obs.clone() {
        Some(o) => {
            eprintln!("calibrating against observed metrics…");
            Some(run::calibrate(&sc, &ov, o)?)
        }
        None => None,
    };
    let params = run::prepare(&sc, &ov, cal.as_ref())?;
    eprintln!(
        "simulating {} ({}s warm-up + {}s)…",
        params.name,
        params.warmup / 1_000_000,
        params.duration / 1_000_000
    );
    let out = run::run_params(params);
    let result = crate::report::analyze(&out.ctx, &out.info, obs.as_ref());
    print!("{}", crate::report::text::render(&result, a.verbose));
    if let Some(p) = &a.json {
        std::fs::write(p, serde_json::to_string_pretty(&result)?)?;
        eprintln!("wrote {}", p.display());
    }
    if let Some(p) = &a.prom {
        std::fs::write(p, crate::report::prom::render(&out.ctx))?;
        eprintln!("wrote {}", p.display());
    }
    if let Some(p) = &a.html {
        std::fs::write(p, crate::report::html::render_run(&result))?;
        eprintln!("wrote {}", p.display());
    }
    let critical = result
        .hotspots
        .iter()
        .any(|h| h.severity == crate::report::Severity::Critical);
    Ok(if critical {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    })
}
