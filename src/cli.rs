//! Command line interface.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Args, Parser, Subcommand};

use crate::config::dynamic::{Constraints, DcValue};
use crate::profile::{self, RunOptions, RunSpec};
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
    /// Simulate one configuration and watch it live in the browser: load, replica counts and
    /// dynamic config can be changed while it runs.
    #[cfg(feature = "ui")]
    Ui(UiArgs),
    /// Save runs as named profiles, kept private outside any repository, and manage them.
    Profile {
        #[command(subcommand)]
        cmd: ProfileCmd,
    },
}

#[derive(Args, Clone)]
pub struct CommonArgs {
    /// Scenario file (YAML).
    #[arg(required_unless_present = "profile", conflicts_with = "profile")]
    scenario: Option<PathBuf>,
    /// Run a saved profile (`tempdes profile list`); the options given here apply on top.
    #[arg(long, value_name = "NAME")]
    profile: Option<String>,
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
    /// Multiply all start and signal rates (with --observed: the calibrated, observed rates).
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
    /// Write the report as Markdown, with GitHub-flavoured tables (`--verbose` applies).
    #[arg(long)]
    md: Option<PathBuf>,
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
    /// Write sweep results as Markdown tables.
    #[arg(long)]
    md: Option<PathBuf>,
}

#[cfg(feature = "ui")]
#[derive(Args)]
struct UiArgs {
    #[command(flatten)]
    common: CommonArgs,
    /// Address to listen on.
    #[arg(long, default_value = "127.0.0.1")]
    host: String,
    /// Port to listen on (0 picks a free one).
    #[arg(long, default_value_t = 3000)]
    port: u16,
    /// Simulated seconds per wall-clock second after warm-up (0 runs as fast as possible).
    /// Warm-up always runs as fast as possible.
    #[arg(long, default_value_t = 1.0)]
    speed: f64,
    /// Open the page in the default browser.
    #[arg(long)]
    open: bool,
}

#[derive(Subcommand)]
enum ProfileCmd {
    /// Save a scenario and run options under a name, e.g.
    /// `tempdes profile save prod cluster.yaml -r history=4 -o observed.yaml`.
    /// With `--profile BASE` instead of a scenario, the new profile starts from BASE.
    Save(Box<SaveArgs>),
    /// List the saved profiles.
    List,
    /// Show a profile's options and files.
    Show { name: String },
    /// Delete a profile.
    #[command(alias = "rm")]
    Remove { name: String },
    /// Print where profiles are stored.
    Dir,
}

#[derive(Args)]
struct SaveArgs {
    /// Profile name: letters, digits, '-', '_' and '.'.
    name: String,
    #[command(flatten)]
    common: CommonArgs,
    /// One line describing the profile.
    #[arg(long)]
    description: Option<String>,
    /// Replace an existing profile with the same name.
    #[arg(long)]
    force: bool,
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
    options(c).overrides()
}

/// The run options given on the command line.
fn options(c: &CommonArgs) -> RunOptions {
    RunOptions {
        observed: c.observed.iter().map(PathBuf::from).collect(),
        replicas: c.replicas.clone(),
        dc: c.dc.clone(),
        load: c.load,
        client_lb: c.client_lb,
        duration: c.duration,
        warmup: c.warmup,
        seed: c.seed,
    }
}

/// The scenario and options of a run: `--profile` (with the command line's options applied on
/// top) or the scenario file given.
pub fn run_spec(c: &CommonArgs) -> anyhow::Result<RunSpec> {
    let mut spec = match (&c.profile, &c.scenario) {
        (Some(name), _) => {
            let entry = profile::Store::open()?.load(name)?;
            eprintln!("using profile {name} ({})", entry.path.display());
            entry.spec
        }
        (None, Some(path)) => RunSpec::for_scenario(path),
        (None, None) => anyhow::bail!("give a scenario file or --profile NAME"),
    };
    spec.options.layer(&options(c));
    Ok(spec)
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
        Cmd::Sweep(a) => {
            let spec = run_spec(&a.common)?;
            crate::sweep::cmd_sweep(
                spec.load_scenario()?,
                spec.options.overrides()?,
                &spec.options.observed_args(),
                &a.rows,
                &a.cols,
                a.jobs,
                a.json.as_deref(),
                a.csv.as_deref(),
                a.html.as_deref(),
                a.md.as_deref(),
            )
        }
        Cmd::Profile { cmd } => cmd_profile(cmd),
        Cmd::Dc { cmd } => crate::dccmd::run(cmd_dc(cmd)),
        Cmd::Metrics { cmd } => crate::metrics::cmd::run(match cmd {
            MetricsCmd::Queries { window } => crate::metrics::cmd::Cmd::Queries { window },
            MetricsCmd::Template => crate::metrics::cmd::Cmd::Template,
            MetricsCmd::Show { file } => crate::metrics::cmd::Cmd::Show { file },
        }),
        #[cfg(feature = "ui")]
        Cmd::Ui(a) => cmd_ui(a),
    }
}

#[cfg(feature = "ui")]
fn cmd_ui(a: UiArgs) -> anyhow::Result<ExitCode> {
    let spec = run_spec(&a.common)?;
    let sc = spec.load_scenario()?;
    let ov = spec.options.overrides()?;
    let obs = run::load_observations(&sc, &spec.options.observed_args())?;
    let cal = match obs {
        Some(o) => {
            eprintln!("calibrating against observed metrics…");
            Some(run::calibrate(&sc, &ov, o)?)
        }
        None => None,
    };
    let params = run::prepare(&sc, &ov, cal.as_ref())?;
    anyhow::ensure!(
        a.speed.is_finite() && a.speed >= 0.0,
        "--speed must be 0 (unlimited) or a positive number"
    );
    crate::ui::serve(
        params,
        &crate::ui::UiOptions {
            host: a.host,
            port: a.port,
            speed: a.speed,
            open: a.open,
        },
    )?;
    Ok(ExitCode::SUCCESS)
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
    let spec = run_spec(&a.common)?;
    let sc = spec.load_scenario()?;
    let ov = spec.options.overrides()?;
    let obs = run::load_observations(&sc, &spec.options.observed_args())?;
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
    let result =
        crate::report::analyze(&out.ctx, &out.info, run::validation_obs(&ov, obs.as_ref()));
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
    if let Some(p) = &a.md {
        std::fs::write(p, crate::report::markdown::render_run(&result, a.verbose))?;
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

fn cmd_profile(cmd: ProfileCmd) -> anyhow::Result<ExitCode> {
    let store = profile::Store::open()?;
    match cmd {
        ProfileCmd::Save(a) => {
            let spec = run_spec(&a.common)?;
            let saved = store.save(&a.name, &spec, a.description, a.force)?;
            println!("saved profile {} in {}", a.name, saved.dir.display());
            for f in &saved.copied {
                println!("  copied  {}", f.display());
            }
            for f in &saved.external {
                println!("  read from its own place on every run: {}", f.display());
            }
            if let Some(root) = profile::untracked_in_git(store.dir()) {
                eprintln!(
                    "warning: the profile store is inside the git working tree {} and git doesn't ignore it, so `git add -A` would commit your profiles",
                    root.display()
                );
            }
            for f in saved.copied.iter().chain(&saved.external) {
                if let Some(root) = profile::untracked_in_git(f) {
                    eprintln!(
                        "note: {} is untracked in the git working tree {} and not ignored, so `git add -A` would commit it. The profile has its own copy: if the file is private, delete it, or list it in {}/.git/info/exclude (a local ignore file that is never pushed).",
                        f.display(),
                        root.display(),
                        root.display()
                    );
                }
            }
            println!("run it with: tempdes run --profile {}", a.name);
        }
        ProfileCmd::List => {
            let profiles = store.list()?;
            if profiles.is_empty() {
                println!("no profiles in {}", store.dir().display());
            }
            let width = profiles.iter().map(|(n, _)| n.len()).max().unwrap_or(0);
            for (name, entry) in &profiles {
                let line = match entry {
                    Ok(e) if e.kind == profile::Kind::Scenario => match e.spec.load_scenario() {
                        Ok(sc) => format!(
                            "{name:<width$}  scenario file{}",
                            sc.name.map(|n| format!(" ({n})")).unwrap_or_default()
                        ),
                        Err(err) => format!(
                            "{name:<width$}  scenario file, doesn't load: {}",
                            first_line(&err)
                        ),
                    },
                    Ok(e) => {
                        let mut line = format!("{name:<width$}  {}", e.spec.options.summary());
                        if let Some(d) = &e.description {
                            line = format!("{line}  # {d}");
                        }
                        line
                    }
                    Err(err) => format!("{name:<width$}  doesn't load: {}", first_line(err)),
                };
                println!("{}", line.trim_end());
            }
        }
        ProfileCmd::Show { name } => {
            let entry = store.load(&name)?;
            let spec = &entry.spec;
            println!("profile {name}  ({})", entry.path.display());
            if entry.kind == profile::Kind::Scenario {
                println!("  kind         scenario file: the scenario is the whole run");
            }
            if let Some(d) = &entry.description {
                println!("  description  {d}");
            }
            if entry.kind == profile::Kind::Saved {
                println!("  scenario     {}", spec.scenario.display());
                println!(
                    "  resolves     relative paths against {}",
                    spec.scenario_dir.display()
                );
                for f in &spec.options.observed {
                    println!("  observed     {}", f.display());
                }
                let options = RunOptions {
                    observed: Vec::new(),
                    ..spec.options.clone()
                };
                let summary = options.summary();
                println!(
                    "  options      {}",
                    if summary.is_empty() {
                        "(none)"
                    } else {
                        &summary
                    }
                );
            }
            match spec.load_scenario() {
                Ok(sc) => {
                    for f in sc.referenced_files() {
                        let path = sc.resolve_path(&f);
                        let state = if path.exists() { "" } else { "  (missing)" };
                        println!("  reads        {}{state}", path.display());
                    }
                }
                Err(err) => println!("  scenario doesn't load: {err:#}"),
            }
            if let Some(note) = profile::readable_by_others(&entry.path) {
                eprintln!("note: {note}");
            }
        }
        ProfileCmd::Remove { name } => {
            let dir = store.remove(&name)?;
            println!("removed profile {name} ({})", dir.display());
        }
        ProfileCmd::Dir => println!("{}", store.dir().display()),
    }
    Ok(ExitCode::SUCCESS)
}

/// The first line of an error, for one-line listings.
fn first_line(err: &anyhow::Error) -> String {
    err.to_string()
        .lines()
        .next()
        .unwrap_or_default()
        .to_string()
}
