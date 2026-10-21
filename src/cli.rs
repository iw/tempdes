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
    /// Build scenario workloads from exported workflow histories.
    Workload {
        #[command(subcommand)]
        cmd: WorkloadCmd,
    },
}

#[derive(Subcommand)]
enum WorkloadCmd {
    /// Infer `workflows:` entries from exported histories (`workload fetch`, `temporal workflow
    /// show --output json`, or the Web UI's download): steps, durations, attempts, retry
    /// policies and timeouts, with waits in the cluster left out, and payload sizes from the
    /// history sizes the server recorded, with a commented worker fleet per task queue.
    /// Payloads are not read.
    Import(ImportArgs),
    /// Fetch histories for `import` from a Temporal frontend's HTTP API: the executions started
    /// in a window, counted by type, and up to `--limit` of each spread over it, without their
    /// payloads. It connects as the Temporal CLI does, from a `temporal.toml` profile and
    /// `TEMPORAL_*` environment variables. Requests go one at a time, at most a listing and five
    /// history reads a second, and wait when the frontend refuses them.
    #[cfg(feature = "fetch")]
    Fetch(Box<WorkloadFetchArgs>),
}

#[cfg(feature = "fetch")]
#[derive(Args)]
struct WorkloadFetchArgs {
    /// End of the window: RFC 3339 (`2026-10-01T22:17:00Z`), `YYYY-MM-DD HH:MM` (UTC) or Unix
    /// seconds.
    #[arg(long)]
    end: String,
    /// Window length: the executions started in it are listed.
    #[arg(long, default_value = "15m")]
    window: String,
    /// List the window this much at a time; fetch.yaml counts each type's starts and closes
    /// in every slice.
    #[arg(long, default_value = "1m")]
    slice: String,
    /// Only this workflow type (repeatable).
    #[arg(long = "type", value_name = "TYPE")]
    types: Vec<String>,
    /// A visibility filter ANDed with the window, e.g. `TaskQueue = 'orders'`.
    #[arg(long)]
    query: Option<String>,
    /// Histories to fetch of each type, spread over the window.
    #[arg(long, default_value_t = 300)]
    limit: usize,
    /// Directory for the histories, created readable only by you [default: histories].
    #[arg(short, long)]
    output: Option<PathBuf>,
    /// Write into this profile's `workload` folder in the store: --output, when relative, is
    /// inside it.
    #[arg(long, value_name = "NAME")]
    profile: Option<String>,
    /// With --profile, a folder of its own in `workload` for this period's histories:
    /// workload/PERIOD/histories. Letters, digits, '-' and '_'.
    #[arg(long, value_name = "PERIOD", requires = "profile")]
    period: Option<String>,
    /// Listing requests a second at most, 1/s or fewer: listings share each frontend's
    /// visibility limit for the namespace (10/s by default) with the Web UI and other clients.
    #[arg(long, value_name = "RATE", value_parser = parse_rate, default_value = "1/s")]
    list_rate: f64,
    /// History reads a second at most, a page of a history each, 5/s or fewer.
    #[arg(long, value_name = "RATE", value_parser = parse_rate, default_value = "5/s")]
    history_rate: f64,
    /// Profile in Temporal's client config file [default: TEMPORAL_PROFILE, else `default`].
    #[arg(long, value_name = "NAME")]
    temporal_profile: Option<String>,
    /// Temporal's client config file [default: TEMPORAL_CONFIG_FILE, else
    /// temporalio/temporal.toml in your config folder, where the Temporal CLI keeps it].
    #[arg(long, value_name = "FILE")]
    temporal_config: Option<PathBuf>,
    /// Frontend address, `host:port` (overrides the profile and TEMPORAL_ADDRESS).
    #[arg(long)]
    address: Option<String>,
    /// Namespace to read (overrides the profile and TEMPORAL_NAMESPACE).
    #[arg(long, short = 'n')]
    namespace: Option<String>,
    /// The frontend's HTTP API [default: the address's host on port 7243, the frontend's
    /// httpPort].
    #[arg(long, value_name = "URL")]
    http_url: Option<String>,
    /// Go through the Temporal CLI, `temporal` or the one at PATH, with its own configuration,
    /// rather than the HTTP API: for a frontend only its gRPC port reaches.
    #[arg(
        long,
        value_name = "PATH",
        num_args = 0..=1,
        default_missing_value = "temporal",
        conflicts_with_all = ["http_url", "temporal_profile", "temporal_config"]
    )]
    cli: Option<PathBuf>,
    /// An argument for every Temporal CLI command, e.g. `--cli-arg=--env=prod` (repeatable).
    #[arg(
        long = "cli-arg",
        value_name = "ARG",
        allow_hyphen_values = true,
        requires = "cli"
    )]
    cli_args: Vec<String>,
    /// Seconds allowed per request, or per Temporal CLI command.
    #[arg(long, default_value_t = 60.0)]
    timeout: f64,
}

#[derive(Args)]
struct ImportArgs {
    /// History files, or directories of `*.json` history files [default with --profile: its
    /// workload/histories].
    #[arg(required_unless_present = "profile")]
    paths: Vec<PathBuf>,
    /// Read and write in this profile's `workload` folder in the store: the paths and --output,
    /// when relative, are inside it, and the output is workflows.yaml unless --output says.
    #[arg(long, value_name = "NAME")]
    profile: Option<String>,
    /// With --profile, read and write in this period's folder in `workload`, as `workload
    /// fetch --period` wrote it: workload/PERIOD/histories into workload/PERIOD/workflows.yaml.
    #[arg(long, value_name = "PERIOD", requires = "profile")]
    period: Option<String>,
    /// Namespace of the imported workflow types.
    #[arg(long, default_value = "default")]
    namespace: String,
    /// Start rate of each imported workflow type (`100/s`), split across its paths. By default
    /// it comes from the counts in a `workload fetch` directory, else it is estimated from the
    /// histories' start times.
    #[arg(long, value_parser = parse_rate)]
    rate: Option<f64>,
    /// Paths (step sequences) that at least this share of a type's executions took become
    /// workflow types of their own; rarer ones are folded into the most common.
    #[arg(long, default_value_t = 0.05)]
    min_path_share: f64,
    /// Write the `workflows:` YAML here instead of to standard output.
    #[arg(long, short = 'o')]
    output: Option<PathBuf>,
}

/// When profile `name` doesn't read the observations in `file` yet, say how to make it.
#[cfg(feature = "fetch")]
fn observed_hint(name: &str, file: &std::path::Path) {
    let Ok(entry) = profile::Store::open().and_then(|s| s.load(name)) else {
        return;
    };
    let listed = entry
        .spec
        .options
        .observed
        .iter()
        .any(|f| same_file(f, file));
    let calibrated = entry.spec.load_scenario().is_ok_and(|sc| {
        sc.calibration.as_ref().is_some_and(|c| {
            c.observations
                .iter()
                .any(|o| same_file(&sc.resolve_path(o), file))
        })
    });
    if !listed && !calibrated {
        eprintln!(
            "profile {name} doesn't read it yet: list it in its profile.yaml as `observed: [{}]`",
            relative_to(&entry.path, file)
        );
    }
}

/// The folder in profile `name` where its fetched and imported files go (`metrics` or
/// `workload`), or the folder of `period` in it, created private if it isn't there yet.
fn profile_area(name: &str, sub: &str, period: Option<&str>) -> anyhow::Result<PathBuf> {
    if let Some(p) = period {
        profile::check_period(p)?;
    }
    let mut dir = profile::Store::open()?.folder(name)?.join(sub);
    profile::create_private_dir(&dir)?;
    if let Some(p) = period {
        dir.push(p);
        profile::create_private_dir(&dir)?;
    }
    eprintln!("using profile {name} ({})", dir.display());
    Ok(dir)
}

/// Whether `a` and `b` are the same file.
fn same_file(a: &std::path::Path, b: &std::path::Path) -> bool {
    match (std::fs::canonicalize(a), std::fs::canonicalize(b)) {
        (Ok(x), Ok(y)) => x == y,
        _ => a == b,
    }
}

/// `file` as a file in `folder` refers to it: relative, with `/` between the parts as YAML
/// takes it on any platform, or as it is when it is elsewhere.
fn relative_to(folder: &std::path::Path, file: &std::path::Path) -> String {
    let real = |p: &std::path::Path| std::fs::canonicalize(p).ok();
    let rel = file
        .strip_prefix(folder)
        .ok()
        .map(PathBuf::from)
        .or_else(|| {
            real(file)?
                .strip_prefix(real(folder)?)
                .ok()
                .map(PathBuf::from)
        });
    match rel {
        Some(rel) => rel
            .components()
            .map(|c| c.as_os_str().to_string_lossy().into_owned())
            .collect::<Vec<_>>()
            .join("/"),
        None => file.display().to_string(),
    }
}

/// When the scenario of profile `name` doesn't take its workflows from `file` yet, say how to
/// make it.
fn workflows_hint(name: &str, file: &std::path::Path) {
    let Ok(entry) = profile::Store::open().and_then(|s| s.load(name)) else {
        return;
    };
    let Ok(sc) = entry.spec.load_scenario() else {
        return;
    };
    let reads = sc
        .referenced_files()
        .iter()
        .any(|f| same_file(&sc.resolve_path(f), file));
    if !reads {
        // a folder's workflows.yaml is named by the folder
        let named = match file.file_name() {
            Some(f) if f == "workflows.yaml" => file.parent().unwrap_or(file),
            _ => file,
        };
        eprintln!(
            "profile {name} doesn't read it yet: set its scenario's `workflows: {}`",
            relative_to(&entry.spec.scenario_dir, named)
        );
    }
}

fn parse_rate(s: &str) -> Result<f64, String> {
    crate::util::units::parse_rate_per_sec(s)
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
    /// Start a profile from a commented prototype: a profile.yaml and a scenario.yaml to edit,
    /// with folders for what `metrics fetch` and `workload fetch` / `import --profile` write.
    Init {
        /// Profile name: letters, digits, '-', '_' and '.'.
        name: String,
        /// One line describing the profile.
        #[arg(long)]
        description: Option<String>,
    },
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
    /// Run those queries against a Prometheus HTTP API and write an observations file for the
    /// window ending at `--end`.
    #[cfg(feature = "fetch")]
    Fetch(FetchArgs),
    /// Print the rate of workflow-starting calls, minute by minute, from a Prometheus HTTP API,
    /// and suggest steady windows for `fetch`.
    #[cfg(feature = "fetch")]
    Scan(ScanArgs),
}

/// Reaching a Prometheus HTTP API.
#[cfg(feature = "fetch")]
#[derive(clap::Args)]
struct PromArgs {
    /// Prometheus base URL, or a Grafana data source proxy URL. A bearer token is read from
    /// PROMETHEUS_BEARER_TOKEN.
    #[arg(long)]
    url: String,
    /// Extra request header (repeatable), e.g. `X-Scope-OrgID: tenant`.
    #[arg(long = "header", value_name = "NAME: VALUE")]
    headers: Vec<String>,
    /// Basic auth user; the password is read from PROMETHEUS_PASSWORD.
    #[arg(long)]
    user: Option<String>,
    /// Trust these CA certificates (PEM) instead of the system's.
    #[arg(long)]
    ca_file: Option<PathBuf>,
    /// Skip TLS certificate verification.
    #[arg(long)]
    insecure: bool,
    /// Seconds allowed per request.
    #[arg(long, default_value_t = 60.0)]
    timeout: f64,
    /// Rewrite a label value, such as a namespace, before anything is written or printed
    /// (repeatable).
    #[arg(long, value_name = "OLD=NEW")]
    rename: Vec<String>,
}

#[cfg(feature = "fetch")]
#[derive(clap::Args)]
struct FetchArgs {
    #[command(flatten)]
    prom: PromArgs,
    /// End of the window: RFC 3339 (`2026-10-01T22:17:00Z`), `YYYY-MM-DD HH:MM` (UTC) or Unix
    /// seconds.
    #[arg(long)]
    end: String,
    /// Window length.
    #[arg(long, default_value = "15m")]
    window: String,
    /// The observations file to write [default: observed.yaml].
    #[arg(short, long)]
    output: Option<PathBuf>,
    /// Write into this profile's `metrics` folder in the store: --output and --baseline-out,
    /// when relative, are inside it.
    #[arg(long, value_name = "NAME")]
    profile: Option<String>,
    /// With --profile, a folder of its own in `metrics` for this period's observations:
    /// metrics/PERIOD/observed.yaml. Letters, digits, '-' and '_'.
    #[arg(long, value_name = "PERIOD", requires = "profile")]
    period: Option<String>,
    /// Also write the window ending here, such as the run's start.
    #[arg(long)]
    baseline_end: Option<String>,
    /// File for the window ending at --baseline-end [default: baseline.yaml beside --output].
    #[arg(long)]
    baseline_out: Option<PathBuf>,
    /// The file's description [default: the window].
    #[arg(long)]
    description: Option<String>,
    /// Label matchers for Temporal's containers in cAdvisor's CPU metrics.
    #[arg(long, default_value = crate::metrics::cmd::CPU_SELECTOR)]
    cpu_selector: String,
    /// The database's busy fraction, 0..1, over the window (e.g. from CloudWatch).
    #[arg(long, conflicts_with = "db_utilization_query")]
    db_utilization: Option<f64>,
    /// PromQL for the database's busy fraction (0..1, or a percentage).
    #[arg(long)]
    db_utilization_query: Option<String>,
}

#[cfg(feature = "fetch")]
#[derive(clap::Args)]
struct ScanArgs {
    #[command(flatten)]
    prom: PromArgs,
    /// Start of the range (times as for `fetch --end`).
    #[arg(long)]
    from: String,
    /// End of the range.
    #[arg(long)]
    to: String,
    /// Window length to suggest.
    #[arg(long, default_value = "15m")]
    window: String,
    /// Resolution.
    #[arg(long, default_value = "1m")]
    step: String,
    /// Window of each rate [default: four scrape intervals, or the step if longer].
    #[arg(long)]
    rate_window: Option<String>,
}

#[cfg(feature = "fetch")]
fn conn(p: &PromArgs) -> crate::metrics::fetch::Conn {
    crate::metrics::fetch::Conn {
        url: p.url.clone(),
        headers: p.headers.clone(),
        user: p.user.clone(),
        ca_file: p.ca_file.clone(),
        insecure: p.insecure,
        timeout_s: p.timeout,
    }
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
        Cmd::Metrics { cmd } => match cmd {
            MetricsCmd::Queries { window } => {
                crate::metrics::cmd::run(crate::metrics::cmd::Cmd::Queries { window })
            }
            MetricsCmd::Template => crate::metrics::cmd::run(crate::metrics::cmd::Cmd::Template),
            MetricsCmd::Show { file } => {
                crate::metrics::cmd::run(crate::metrics::cmd::Cmd::Show { file })
            }
            #[cfg(feature = "fetch")]
            MetricsCmd::Fetch(a) => {
                let output = a.output.unwrap_or_else(|| PathBuf::from("observed.yaml"));
                let (output, baseline_out) = match &a.profile {
                    Some(name) => {
                        let area = profile_area(name, profile::METRICS_DIR, a.period.as_deref())?;
                        (
                            profile::within(&area, &output),
                            a.baseline_out.map(|b| profile::within(&area, &b)),
                        )
                    }
                    None => (output, a.baseline_out),
                };
                crate::metrics::fetch::fetch(&crate::metrics::fetch::FetchArgs {
                    conn: conn(&a.prom),
                    end: a.end,
                    window: a.window,
                    output: output.clone(),
                    baseline_end: a.baseline_end,
                    baseline_out,
                    description: a.description,
                    rename: a.prom.rename,
                    cpu_selector: a.cpu_selector,
                    db_utilization: a.db_utilization,
                    db_utilization_query: a.db_utilization_query,
                })?;
                if let Some(name) = &a.profile {
                    observed_hint(name, &output);
                }
                Ok(ExitCode::SUCCESS)
            }
            #[cfg(feature = "fetch")]
            MetricsCmd::Scan(a) => {
                let report = crate::metrics::fetch::scan(&crate::metrics::fetch::ScanArgs {
                    conn: conn(&a.prom),
                    from: a.from,
                    to: a.to,
                    window: a.window,
                    step: a.step,
                    rate_window: a.rate_window,
                    rename: a.prom.rename,
                })?;
                print!("{report}");
                Ok(ExitCode::SUCCESS)
            }
        },
        #[cfg(feature = "ui")]
        Cmd::Ui(a) => cmd_ui(a),
        Cmd::Workload { cmd } => match cmd {
            WorkloadCmd::Import(a) => cmd_import(a),
            #[cfg(feature = "fetch")]
            WorkloadCmd::Fetch(a) => cmd_workload_fetch(*a),
        },
    }
}

#[cfg(feature = "fetch")]
fn cmd_workload_fetch(a: WorkloadFetchArgs) -> anyhow::Result<ExitCode> {
    use crate::histories::fetch::{Cli, LIST_PER_S, READ_PER_S, Via};
    anyhow::ensure!(
        a.list_rate <= LIST_PER_S,
        "--list-rate can only lower the pace, 1/s at most: listings share each frontend's visibility limit for the namespace (frontend.namespaceRPS.visibility, 10/s by default) with the Web UI and other clients"
    );
    anyhow::ensure!(
        a.history_rate <= READ_PER_S,
        "--history-rate can only lower the pace, 5/s at most"
    );
    let via = match a.cli {
        // the CLI reads its own configuration
        Some(program) => Via::Cli(Cli {
            program,
            namespace: a.namespace,
            address: a.address,
            args: a.cli_args,
        }),
        None => {
            let mut profile = crate::envconfig::load(&crate::envconfig::Choice {
                file: a.temporal_config,
                profile: a.temporal_profile,
            })?;
            if let Some(address) = a.address {
                profile.profile.address = Some(address);
                profile.origin.push_str(", with --address");
            }
            if let Some(ns) = a.namespace {
                profile.profile.namespace = Some(ns);
                profile.origin.push_str(", with --namespace");
            }
            Via::Http {
                profile,
                http_url: a.http_url,
            }
        }
    };
    // import finds them by itself where they go by default
    let found = a.profile.is_some() && a.output.is_none();
    let output = a
        .output
        .unwrap_or_else(|| PathBuf::from(profile::HISTORIES_DIR));
    let output = match &a.profile {
        Some(name) => profile::within(
            &profile_area(name, profile::WORKLOAD_DIR, a.period.as_deref())?,
            &output,
        ),
        None => output,
    };
    let next = match &a.profile {
        Some(name) if found => format!(
            "tempdes workload import --profile {name}{} --namespace <namespace in your scenario>",
            a.period
                .as_ref()
                .map_or(String::new(), |p| format!(" --period {p}"))
        ),
        _ => format!(
            "tempdes workload import {} --namespace <namespace in your scenario> -o workflows.yaml",
            output.display()
        ),
    };
    let summary = crate::histories::fetch::fetch(&crate::histories::fetch::FetchArgs {
        via,
        end: a.end,
        window: a.window,
        slice: a.slice,
        types: a.types,
        query: a.query,
        limit: a.limit,
        output,
        timeout_s: a.timeout,
        pace: crate::histories::fetch::Pace {
            list_per_s: a.list_rate,
            read_per_s: a.history_rate,
            ..Default::default()
        },
    })?;
    eprint!("{summary}");
    eprintln!("next: {next}");
    Ok(ExitCode::SUCCESS)
}

fn cmd_import(a: ImportArgs) -> anyhow::Result<ExitCode> {
    anyhow::ensure!(
        (0.0..=1.0).contains(&a.min_path_share),
        "--min-path-share must be between 0 and 1"
    );
    let opts = crate::histories::Options {
        namespace: a.namespace,
        rate: a.rate,
        min_path_share: a.min_path_share,
        ..Default::default()
    };
    let (paths, output) = match &a.profile {
        Some(name) => {
            let area = profile_area(name, profile::WORKLOAD_DIR, a.period.as_deref())?;
            let paths = if a.paths.is_empty() {
                vec![area.join(profile::HISTORIES_DIR)]
            } else {
                a.paths.iter().map(|p| profile::within(&area, p)).collect()
            };
            let output = a.output.unwrap_or_else(|| PathBuf::from("workflows.yaml"));
            (paths, Some(profile::within(&area, &output)))
        }
        None => (a.paths, a.output),
    };
    let program = crate::histories::import(&paths, &opts)?;
    eprint!("{}", program.summary);
    match output {
        Some(f) => {
            if f.exists() {
                eprintln!(
                    "warning: replacing {}: a scenario that names it reads the new workflows, and edits made to it are lost",
                    f.display()
                );
            }
            // it names your workflow types, activity types and task queues
            profile::write_private(&f, program.yaml.as_bytes())?;
            eprintln!("wrote {}", f.display());
            if let Some(name) = &a.profile {
                workflows_hint(name, &f);
            }
        }
        None => print!("{}", program.yaml),
    }
    Ok(ExitCode::SUCCESS)
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
        ProfileCmd::Init { name, description } => {
            let dir = store.init(&name, description.as_deref())?;
            println!("created profile {name} in {}", dir.display());
            for (file, what) in [
                ("profile.yaml".to_string(), "the run's options".to_string()),
                (
                    "scenario.yaml".to_string(),
                    "the cluster and its workload: an example to edit".to_string(),
                ),
                (
                    format!("{}/", profile::METRICS_DIR),
                    format!("what `tempdes metrics fetch --profile {name}` writes"),
                ),
                (
                    format!("{}/", profile::WORKLOAD_DIR),
                    format!("what `tempdes workload fetch` and `import --profile {name}` write"),
                ),
            ] {
                println!("  {file:<15}{what}");
            }
            if let Some(root) = profile::untracked_in_git(store.dir()) {
                eprintln!(
                    "warning: the profile store is inside the git working tree {} and git doesn't ignore it, so `git add -A` would commit your profiles",
                    root.display()
                );
            }
            println!("check it with: tempdes profile show {name}");
            println!("run it with:   tempdes run --profile {name}");
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
