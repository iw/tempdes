//! `tempdes metrics fetch` and `scan`: observations read from a Prometheus HTTP API.
//!
//! `fetch` runs the queries of [`SPECS`] as instant queries at the end of a window and writes an
//! observations file. `scan` prints the rate of workflow-starting calls minute by minute, to
//! choose that window. Requests go over rustls, trusting the system's certificates.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, anyhow, bail};

use super::cmd::{Kind, SPECS, Spec};

/// The frontend calls that start (or may start) workflows, as `operation` values: the call's
/// name, or a path ending in it.
const START_CALLS: &str = "(.*/)?(StartWorkflowExecution|SignalWithStartWorkflowExecution|\
                           ExecuteMultiOperation|SignalWorkflowExecution)";

/// Temporal's names for a metric under the tally (default) and OpenTelemetry Prometheus
/// exporters, with or without a `temporal_` prefix.
fn variants(kind: Kind) -> &'static [&'static str] {
    match kind {
        Kind::Counter => &["{}", "{}_total", "temporal_{}", "temporal_{}_total"],
        Kind::Gauge => &["{}", "temporal_{}"],
        Kind::Histogram(_) => &[
            "{}_bucket",
            "{}_milliseconds_bucket",
            "{}_seconds_bucket",
            "temporal_{}_bucket",
            "temporal_{}_milliseconds_bucket",
            "temporal_{}_seconds_bucket",
        ],
        Kind::Cores => &["{}"],
    }
}

/// How to reach Prometheus.
pub struct Conn {
    /// base URL, e.g. `https://prometheus.example` or a Grafana data source proxy
    pub url: String,
    /// extra request headers, `Name: value`
    pub headers: Vec<String>,
    /// basic auth user; the password comes from `PROMETHEUS_PASSWORD`
    pub user: Option<String>,
    /// trust these CA certificates (PEM) instead of the system's
    pub ca_file: Option<PathBuf>,
    /// skip TLS certificate verification
    pub insecure: bool,
    pub timeout_s: f64,
}

pub struct FetchArgs {
    pub conn: Conn,
    /// end of the window
    pub end: String,
    /// window length, e.g. `15m`
    pub window: String,
    pub output: PathBuf,
    /// also write the window ending here (e.g. the run's start)
    pub baseline_end: Option<String>,
    pub baseline_out: Option<PathBuf>,
    pub description: Option<String>,
    /// `OLD=NEW` label value rewrites
    pub rename: Vec<String>,
    /// label matchers for Temporal's containers in cAdvisor's metrics
    pub cpu_selector: String,
    pub db_utilization: Option<f64>,
    pub db_utilization_query: Option<String>,
}

pub struct ScanArgs {
    pub conn: Conn,
    pub from: String,
    pub to: String,
    /// the window length to suggest
    pub window: String,
    /// resolution
    pub step: String,
    /// `rate` window; four scrape intervals (or the step, if longer) when not given
    pub rate_window: Option<String>,
    pub rename: Vec<String>,
}

type Labels = BTreeMap<String, String>;
/// Each series' labels and its `(time, value)` samples.
type Matrix = Vec<(Labels, Vec<(f64, f64)>)>;

// --- the Prometheus HTTP API -----------------------------------------------------------------------

struct Prometheus {
    agent: ureq::Agent,
    base: String,
    headers: Vec<(String, String)>,
}

impl Prometheus {
    fn new(c: &Conn) -> anyhow::Result<Self> {
        // rustls's cryptography, for the connection and the platform verifier (an error only
        // means one is installed already)
        let _ = rustls::crypto::ring::default_provider().install_default();
        let roots = match &c.ca_file {
            Some(path) => {
                let pem =
                    std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
                let mut certs = Vec::new();
                for item in ureq::tls::parse_pem(&pem) {
                    if let ureq::tls::PemItem::Certificate(cert) =
                        item.with_context(|| format!("parsing {}", path.display()))?
                    {
                        certs.push(cert);
                    }
                }
                if certs.is_empty() {
                    bail!("no certificates in {}", path.display());
                }
                ureq::tls::RootCerts::new_with_certs(&certs)
            }
            None => ureq::tls::RootCerts::PlatformVerifier,
        };
        let tls = ureq::tls::TlsConfig::builder()
            .provider(ureq::tls::TlsProvider::Rustls)
            .root_certs(roots)
            .disable_verification(c.insecure)
            .build();
        let agent = ureq::Agent::config_builder()
            .timeout_global(Some(Duration::from_secs_f64(c.timeout_s)))
            .http_status_as_error(false)
            .tls_config(tls)
            .build()
            .new_agent();
        let mut headers = Vec::new();
        if let Ok(token) = std::env::var("PROMETHEUS_BEARER_TOKEN")
            && !token.is_empty()
        {
            headers.push(("Authorization".into(), format!("Bearer {token}")));
        }
        if let Some(user) = &c.user {
            let password = std::env::var("PROMETHEUS_PASSWORD").unwrap_or_default();
            let cred = base64(format!("{user}:{password}").as_bytes());
            headers.push(("Authorization".into(), format!("Basic {cred}")));
        }
        for h in &c.headers {
            let (k, v) = h
                .split_once(':')
                .ok_or_else(|| anyhow!("--header needs `Name: value`, got {h:?}"))?;
            headers.push((k.trim().into(), v.trim().into()));
        }
        Ok(Prometheus {
            agent,
            base: c.url.trim_end_matches('/').to_string(),
            headers,
        })
    }

    fn get(&self, path: &str, params: &[(&str, String)]) -> anyhow::Result<serde_json::Value> {
        let mut req = self.agent.get(format!("{}{path}", self.base));
        for (k, v) in params {
            req = req.query(*k, v);
        }
        for (k, v) in &self.headers {
            req = req.header(k, v);
        }
        let mut resp = req
            .call()
            .map_err(|e| anyhow!("cannot reach Prometheus at {}: {e}", self.base))?;
        let status = resp.status();
        let body = resp
            .body_mut()
            .with_config()
            .limit(1 << 30)
            .read_to_string()
            .context("reading the response from Prometheus")?;
        let v: serde_json::Value = serde_json::from_str(&body).map_err(|_| {
            let start: String = body.chars().take(200).collect();
            anyhow!("{path}: HTTP {status}, not a Prometheus API response: {start}")
        })?;
        if v["status"] != "success" {
            bail!(
                "{path}: HTTP {status}: {}: {}",
                v["errorType"].as_str().unwrap_or("error"),
                v["error"].as_str().unwrap_or("no details")
            );
        }
        Ok(v["data"].clone())
    }

    /// Instant query at `t` (Unix seconds): each series' labels and value.
    fn query(&self, q: &str, t: f64) -> anyhow::Result<Vec<(Labels, f64)>> {
        let d = self.get(
            "/api/v1/query",
            &[("query", q.to_string()), ("time", format!("{t:.3}"))],
        )?;
        if d["resultType"] == "scalar" {
            return Ok(vec![(Labels::new(), sample(&d["result"]).1)]);
        }
        Ok(d["result"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|r| (labels(&r["metric"]), sample(&r["value"]).1))
            .collect())
    }

    /// Range query: each series' labels and samples.
    fn query_range(&self, q: &str, start: f64, end: f64, step_s: u64) -> anyhow::Result<Matrix> {
        let d = self.get(
            "/api/v1/query_range",
            &[
                ("query", q.to_string()),
                ("start", format!("{start:.3}")),
                ("end", format!("{end:.3}")),
                ("step", step_s.to_string()),
            ],
        )?;
        Ok(d["result"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|r| {
                let values = r["values"].as_array().into_iter().flatten().map(sample);
                (labels(&r["metric"]), values.collect())
            })
            .collect())
    }
}

/// A `[time, "value"]` sample.
fn sample(v: &serde_json::Value) -> (f64, f64) {
    let t = v[0].as_f64().unwrap_or(f64::NAN);
    let x = v[1]
        .as_str()
        .and_then(|s| s.parse().ok())
        .unwrap_or(f64::NAN);
    (t, x)
}

fn labels(v: &serde_json::Value) -> Labels {
    v.as_object()
        .into_iter()
        .flatten()
        .filter(|(k, _)| *k != "__name__")
        .filter_map(|(k, v)| Some((k.clone(), v.as_str()?.to_string())))
        .collect()
}

fn base64(data: &[u8]) -> String {
    const ABC: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in data.chunks(3) {
        let n = chunk.iter().fold(0u32, |n, &b| n << 8 | u32::from(b)) << (8 * (3 - chunk.len()));
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(char::from(ABC[(n >> (18 - 6 * i) & 63) as usize]));
            } else {
                out.push('=');
            }
        }
    }
    out
}

/// The names this Prometheus has for Temporal's metrics, and its histograms' units.
struct Names<'a> {
    prom: &'a Prometheus,
    t: f64,
    found: BTreeMap<String, Option<String>>,
    ms_per_unit: BTreeMap<String, f64>,
}

impl<'a> Names<'a> {
    fn new(prom: &'a Prometheus, t: f64) -> Self {
        Names {
            prom,
            t,
            found: BTreeMap::new(),
            ms_per_unit: BTreeMap::new(),
        }
    }

    fn resolve(&mut self, s: &Spec) -> anyhow::Result<Option<String>> {
        let key = format!("{}/{:?}", s.name, std::mem::discriminant(&s.kind));
        if let Some(found) = self.found.get(&key) {
            return Ok(found.clone());
        }
        let mut found = None;
        for v in variants(s.kind) {
            let name = v.replace("{}", s.name);
            if !self
                .prom
                .query(&format!("count({name})"), self.t)?
                .is_empty()
            {
                found = Some(name);
                break;
            }
        }
        self.found.insert(key, found.clone());
        Ok(found)
    }

    /// Milliseconds per unit of a histogram's bounds: tally's are seconds, OpenTelemetry's are
    /// in the name.
    fn ms_per_unit(&mut self, metric: &str) -> anyhow::Result<f64> {
        if let Some(&ms) = self.ms_per_unit.get(metric) {
            return Ok(ms);
        }
        let ms = if metric.contains("_milliseconds_") {
            1.0
        } else if metric.contains("_seconds_") {
            1000.0
        } else {
            // bounds that start below a half are seconds (tally: 0.001, 0.002, ...)
            let smallest = self
                .prom
                .query(&format!("count by (le) ({metric})"), self.t)?
                .iter()
                .filter_map(|(l, _)| l.get("le")?.parse::<f64>().ok())
                .filter(|le| le.is_finite() && *le > 0.0)
                .fold(f64::INFINITY, f64::min);
            if smallest < 0.5 { 1000.0 } else { 1.0 }
        };
        self.ms_per_unit.insert(metric.to_string(), ms);
        Ok(ms)
    }
}

// --- observations -----------------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
enum Value {
    Rate(f64),
    Cores(f64),
    Gauge(f64),
    /// (quantile, milliseconds)
    Quantiles(Vec<(f64, f64)>),
}

struct Entry {
    name: &'static str,
    what: &'static str,
    labels: Labels,
    value: Value,
}

struct Observed {
    entries: Vec<Entry>,
    /// metric names used, with each histogram's unit
    names: Vec<String>,
    /// metrics this Prometheus doesn't have
    missing: Vec<String>,
    /// metrics without data in the window
    no_data: Vec<String>,
}

struct Sources<'a> {
    window: &'a str,
    cpu_selector: &'a str,
    db_utilization: Option<f64>,
    db_utilization_query: Option<&'a str>,
}

/// Run the queries for the window ending at `t`.
fn observe(prom: &Prometheus, t: f64, src: &Sources<'_>) -> anyhow::Result<Observed> {
    let mut names = Names::new(prom, t);
    let mut entries = Vec::new();
    let mut missing = Vec::new();
    let mut no_data = Vec::new();
    for s in SPECS {
        let Some(metric) = names.resolve(s)? else {
            missing.push(s.name.to_string());
            continue;
        };
        let filter = if s.kind == Kind::Cores {
            src.cpu_selector
        } else {
            s.filter
        };
        let queries = s.promql_with(&metric, src.window, filter);
        let ms = match s.kind {
            Kind::Histogram(_) => names.ms_per_unit(&metric)?,
            _ => 1.0,
        };
        let mut series: BTreeMap<Labels, Value> = BTreeMap::new();
        for (q, promql) in queries {
            for (mut l, v) in prom.query(&promql, t)? {
                for (k, val) in s.fixed {
                    l.insert(k.to_string(), val.to_string());
                }
                let value = match (s.kind, q) {
                    (Kind::Counter, _) => Value::Rate(v),
                    (Kind::Cores, _) => Value::Cores(v),
                    (Kind::Gauge, _) => Value::Gauge(v),
                    (Kind::Histogram(_), q) => Value::Quantiles(vec![(q.unwrap_or(0.5), v * ms)]),
                };
                match (series.get_mut(&l), value) {
                    (Some(Value::Quantiles(qs)), Value::Quantiles(more)) => qs.extend(more),
                    (_, value) => {
                        series.insert(l, value);
                    }
                }
            }
        }
        if series.is_empty() && !no_data.iter().any(|n| n == s.name) {
            no_data.push(s.name.to_string());
        }
        entries.extend(series.into_iter().map(|(labels, value)| Entry {
            name: s.name,
            what: s.what,
            labels,
            value,
        }));
    }
    let mut db_utilization = src.db_utilization;
    if let Some(q) = src.db_utilization_query {
        match prom.query(q, t)?.first() {
            // a percentage, or a fraction
            Some(&(_, v)) => db_utilization = Some(if v > 1.5 { v / 100.0 } else { v }),
            None => no_data.push("db_utilization".into()),
        }
    }
    if let Some(v) = db_utilization {
        entries.push(Entry {
            name: "db_utilization",
            what: "database busy fraction (database capacity calibration)",
            labels: Labels::new(),
            value: Value::Gauge(v),
        });
    }
    let mut used: Vec<String> = names.found.values().flatten().cloned().collect();
    used.dedup();
    for n in &mut used {
        if let Some(&ms) = names.ms_per_unit.get(n.as_str()) {
            n.push_str(if ms == 1.0 { " (ms)" } else { " (s)" });
        }
    }
    Ok(Observed {
        entries,
        names: used,
        missing,
        no_data,
    })
}

/// Drop empty and non-finite values (a zero rate of a grouped series says nothing), and
/// rewrite label values. Warns about values that contain a renamed one.
fn clean(o: &mut Observed, rename: &[(String, String)]) {
    let grouped: BTreeMap<&str, bool> = SPECS.iter().map(|s| (s.name, !s.by.is_empty())).collect();
    o.entries.retain_mut(|e| {
        let keep = match &mut e.value {
            Value::Quantiles(qs) => {
                qs.retain(|(_, v)| v.is_finite());
                !qs.is_empty()
            }
            Value::Rate(v) | Value::Cores(v) | Value::Gauge(v) => {
                v.is_finite() && (*v != 0.0 || !grouped.get(e.name).copied().unwrap_or(false))
            }
        };
        if keep {
            for (k, v) in e.labels.iter_mut() {
                if let Some((_, new)) = rename.iter().find(|(old, _)| old == v) {
                    *v = new.clone();
                } else if let Some((old, _)) =
                    rename.iter().find(|(old, _)| v.contains(old.as_str()))
                {
                    eprintln!("warning: label {k}={v:?} contains {old:?} but isn't renamed");
                }
            }
        }
        keep
    });
}

fn renames(pairs: &[String]) -> anyhow::Result<Vec<(String, String)>> {
    pairs
        .iter()
        .map(|p| {
            p.split_once('=')
                .filter(|(old, _)| !old.is_empty())
                .map(|(o, n)| (o.to_string(), n.to_string()))
                .ok_or_else(|| anyhow!("--rename needs OLD=NEW, got {p:?}"))
        })
        .collect()
}

/// A number without an exponent, to about four significant digits.
fn num(v: f64) -> String {
    if v == 0.0 {
        return "0".into();
    }
    let s = if v.abs() >= 100.0 {
        format!("{v:.1}")
    } else if v.abs() >= 1.0 {
        format!("{v:.3}")
    } else {
        format!("{v:.6}")
    };
    s.trim_end_matches('0').trim_end_matches('.').to_string()
}

/// The observations file.
fn render(o: &Observed, t: f64, window: &str, description: &str) -> String {
    let quote = |s: &str| serde_json::to_string(s).expect("a string"); // valid YAML too
    let mut out = String::new();
    let _ = writeln!(
        out,
        "# tempdes observations, written by `tempdes metrics fetch`"
    );
    let _ = writeln!(
        out,
        "# instant queries at {}, over the {window} before it",
        iso(t)
    );
    for (title, list) in [
        ("metric names", &o.names),
        ("not found", &o.missing),
        ("no data in the window", &o.no_data),
    ] {
        if !list.is_empty() {
            comment_list(&mut out, title, list);
        }
    }
    let _ = writeln!(out, "description: {}", quote(description));
    let _ = writeln!(out, "window: {window}\n\nmetrics:");
    let mut last = "";
    for e in &o.entries {
        if e.what != last {
            let _ = writeln!(out, "  # {}", e.what);
            last = e.what;
        }
        let _ = writeln!(out, "  - name: {}", e.name);
        if !e.labels.is_empty() {
            let l: Vec<String> = e
                .labels
                .iter()
                .map(|(k, v)| format!("{k}: {}", quote(v)))
                .collect();
            let _ = writeln!(out, "    labels: {{ {} }}", l.join(", "));
        }
        match &e.value {
            Value::Rate(v) => {
                let _ = writeln!(out, "    rate: {}/s", num(*v));
            }
            Value::Cores(v) => {
                let _ = writeln!(out, "    rate: {}", num(*v));
            }
            Value::Gauge(v) => {
                let _ = writeln!(out, "    value: {}", num(*v));
            }
            Value::Quantiles(qs) => {
                let mut qs = qs.clone();
                qs.sort_by(|a, b| a.0.total_cmp(&b.0));
                let q: Vec<String> = qs
                    .iter()
                    .map(|(q, v)| format!("{q}: {}ms", num(*v)))
                    .collect();
                let _ = writeln!(out, "    quantiles: {{ {} }}", q.join(", "));
            }
        }
    }
    out
}

/// `# title: a, b, c`, wrapped at 100 columns.
fn comment_list(out: &mut String, title: &str, items: &[String]) {
    let mut line = format!("# {title}:");
    for (i, item) in items.iter().enumerate() {
        let sep = if i + 1 < items.len() { "," } else { "" };
        if line.len() + item.len() + 2 > 100 {
            let _ = writeln!(out, "{line}");
            line = "#  ".to_string();
        }
        let _ = write!(line, " {item}{sep}");
    }
    let _ = writeln!(out, "{line}");
}

/// Write `text` readable only by its owner, like the profile store's files.
fn write_private(path: &Path, text: &str) -> anyhow::Result<()> {
    use std::io::Write;
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create(true).truncate(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut opts, 0o600);
    let mut f = opts
        .open(path)
        .with_context(|| format!("writing {}", path.display()))?;
    f.write_all(text.as_bytes())?;
    Ok(())
}

pub fn fetch(a: &FetchArgs) -> anyhow::Result<()> {
    window_secs(&a.window)?;
    let rename = renames(&a.rename)?;
    let prom = Prometheus::new(&a.conn)?;
    let mut runs = vec![(parse_time(&a.end)?, a.output.clone(), a.description.clone())];
    if let Some(b) = &a.baseline_end {
        let out = a
            .baseline_out
            .clone()
            .unwrap_or_else(|| a.output.with_file_name("baseline.yaml"));
        let desc = a
            .description
            .as_ref()
            .map(|d| format!("{d}, before the run"));
        runs.push((parse_time(b)?, out, desc));
    }
    let src = Sources {
        window: &a.window,
        cpu_selector: &a.cpu_selector,
        db_utilization: a.db_utilization,
        db_utilization_query: a.db_utilization_query.as_deref(),
    };
    for (t, path, desc) in runs {
        let mut o = observe(&prom, t, &src)?;
        clean(&mut o, &rename);
        let desc = desc.unwrap_or_else(|| format!("{} ending {}", a.window, iso(t)));
        write_private(&path, &render(&o, t, &a.window, &desc))?;
        eprint!("wrote {}: {} entries", path.display(), o.entries.len());
        if !o.missing.is_empty() {
            eprint!("; not found: {}", o.missing.join(", "));
        }
        if !o.no_data.is_empty() {
            eprint!("; no data: {}", o.no_data.join(", "));
        }
        eprintln!();
    }
    Ok(())
}

/// The rate of workflow-starting calls between `--from` and `--to`, and the steadiest windows.
pub fn scan(a: &ScanArgs) -> anyhow::Result<String> {
    let (from, to) = (parse_time(&a.from)?, parse_time(&a.to)?);
    if to <= from {
        bail!("--to must be after --from");
    }
    let wsecs = window_secs(&a.window)?;
    let ssecs = window_secs(&a.step)?.max(1);
    let rename = renames(&a.rename)?;
    let prom = Prometheus::new(&a.conn)?;
    let spec = SPECS
        .iter()
        .find(|s| s.name == "service_requests" && s.fixed == [("service_name", "frontend")])
        .expect("the frontend requests spec");
    let metric = Names::new(&prom, to)
        .resolve(spec)?
        .ok_or_else(|| anyhow!("no service_requests metric in this Prometheus"))?;
    // `rate` needs two samples in its window: four scrape intervals, or the step if longer,
    // like Grafana's `$__rate_interval`
    let mid = (from + to) / 2.0;
    let scrape = scrape_interval(&prom, &metric, spec.filter, mid)?;
    let rate_window = match &a.rate_window {
        Some(w) => prom_duration(window_secs(w)?),
        None => prom_duration(ssecs.max((4.0 * scrape).ceil() as u64)),
    };
    let q = format!(
        "sum by (namespace, operation) (rate({metric}{{{}, operation=~\"{START_CALLS}\"}}[{rate_window}]))",
        spec.filter
    );
    let series = prom.query_range(&q, from, to, ssecs)?;
    if series.is_empty() {
        // say what there is instead, halfway through the range
        let seen = |by: &str, filter: &str| -> anyhow::Result<String> {
            let q = format!("count by ({by}) ({metric}{{{filter}}})");
            let mut v: Vec<String> = prom
                .query(&q, mid)?
                .into_iter()
                .filter_map(|(l, _)| l.get(by).cloned())
                .collect();
            v.sort();
            v.truncate(12);
            Ok(if v.is_empty() {
                "none".into()
            } else {
                v.join(", ")
            })
        };
        let services = seen("service_name", r#"service_name!="""#)?;
        let operations = seen("operation", spec.filter)?;
        bail!(
            "no workflow-starting calls between {} and {} (times without a zone are UTC). At {}, \
             {metric} has service_name {services}; the frontend's operations are {operations}",
            iso(from),
            iso(to),
            iso(mid)
        );
    }
    let mut cols: Vec<(String, BTreeMap<i64, f64>)> = series
        .into_iter()
        .map(|(l, vals)| {
            let ns = l.get("namespace").cloned().unwrap_or_default();
            let ns = rename
                .iter()
                .find(|(old, _)| *old == ns)
                .map_or(ns, |(_, new)| new.clone());
            let op = l
                .get("operation")
                .map_or("", |o| o.rsplit('/').next().unwrap_or(o));
            let op = match op {
                "StartWorkflowExecution" => "Start",
                "SignalWorkflowExecution" => "Signal",
                "SignalWithStartWorkflowExecution" => "SignalWithStart",
                op => op,
            };
            let vals = vals
                .into_iter()
                .filter(|(_, v)| v.is_finite())
                .map(|(t, v)| (t as i64, v))
                .collect();
            (format!("{ns}/{op}"), vals)
        })
        .collect();
    let mean = |m: &BTreeMap<i64, f64>| m.values().sum::<f64>() / m.len().max(1) as f64;
    cols.sort_by(|a, b| mean(&b.1).total_cmp(&mean(&a.1)));
    let grid: Vec<i64> = {
        let mut g: Vec<i64> = cols.iter().flat_map(|c| c.1.keys().copied()).collect();
        g.sort_unstable();
        g.dedup();
        g
    };
    let total: Vec<f64> = grid
        .iter()
        .map(|t| cols.iter().filter_map(|c| c.1.get(t)).sum())
        .collect();
    let (shown, rest) = cols.split_at(cols.len().min(4));
    let width = shown
        .iter()
        .map(|c| c.0.chars().count())
        .max()
        .unwrap_or(0)
        .clamp(8, 40);
    let mut out = format!(
        "rates over {rate_window} (samples every {scrape:.0}s), by namespace and call\n\n{:17} {:>8}",
        "time (UTC)", "all/s"
    );
    for c in shown {
        let _ = write!(out, "  {:>width$}", truncate(&c.0, width));
    }
    if !rest.is_empty() {
        let _ = write!(out, "  {:>8}", "other");
    }
    out.push('\n');
    for (i, t) in grid.iter().enumerate() {
        let _ = write!(out, "{:17} {:8.1}", &iso(*t as f64)[..16], total[i]);
        for c in shown {
            let _ = write!(out, "  {:width$.1}", c.1.get(t).copied().unwrap_or(0.0));
        }
        if !rest.is_empty() {
            let other: f64 = rest.iter().filter_map(|c| c.1.get(t)).sum();
            let _ = write!(out, "  {other:8.1}");
        }
        out.push('\n');
    }
    let n = ((wsecs as f64 / ssecs as f64).round() as usize).max(1);
    let best = steady_windows(&grid, &total, n);
    if best.is_empty() {
        let _ = writeln!(out, "\nno {} window fits between --from and --to", a.window);
        return Ok(out);
    }
    let _ = writeln!(
        out,
        "\nsteadiest {} windows near the highest rate:",
        a.window
    );
    for (end, mean, cv) in best {
        let _ = writeln!(
            out,
            "  --end {}   mean {mean:.1}/s, variation {:.1}%",
            iso(end as f64),
            cv * 100.0
        );
    }
    Ok(out)
}

/// Seconds between scrapes of `metric`'s series, from the most sampled one over the 10 minutes
/// before `t`; 60 when there are too few samples to tell.
fn scrape_interval(prom: &Prometheus, metric: &str, filter: &str, t: f64) -> anyhow::Result<f64> {
    let q = format!("max(count_over_time({metric}{{{filter}}}[10m]))");
    let n = prom.query(&q, t)?.first().map_or(0.0, |s| s.1);
    Ok(if n >= 2.0 { 600.0 / n } else { 60.0 })
}

/// Seconds as a Prometheus duration: `4m`, `90s`.
fn prom_duration(secs: u64) -> String {
    if secs.is_multiple_of(60) {
        format!("{}m", secs / 60)
    } else {
        format!("{secs}s")
    }
}

fn truncate(s: &str, n: usize) -> String {
    if s.chars().count() <= n {
        s.to_string()
    } else {
        let head: String = s.chars().take(n - 1).collect();
        format!("{head}…")
    }
}

/// Windows of `n` samples ending on the grid, the steadiest three among those whose mean is
/// within 10% of the highest: (end, mean, coefficient of variation).
fn steady_windows(grid: &[i64], total: &[f64], n: usize) -> Vec<(i64, f64, f64)> {
    let mut cands: Vec<(i64, f64, f64)> = (n.saturating_sub(1)..grid.len())
        .filter_map(|i| {
            let vals = &total[i + 1 - n..=i];
            let mean = vals.iter().sum::<f64>() / n as f64;
            let var = vals.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / n as f64;
            (mean > 0.0).then(|| (grid[i], mean, var.sqrt() / mean))
        })
        .collect();
    let peak = cands.iter().map(|c| c.1).fold(0.0, f64::max);
    cands.retain(|c| c.1 >= 0.9 * peak);
    cands.sort_by(|a, b| a.2.total_cmp(&b.2));
    cands.truncate(3);
    cands
}

// --- times ------------------------------------------------------------------------------------------

/// A Prometheus duration (`15m`, `1h30m`) in seconds.
fn window_secs(s: &str) -> anyhow::Result<u64> {
    let bad = || anyhow!("not a Prometheus duration: {s:?} (use e.g. 15m)");
    let mut total = 0u64;
    let mut digits = String::new();
    for c in s.chars() {
        if c.is_ascii_digit() {
            digits.push(c);
            continue;
        }
        let unit = match c {
            's' => 1,
            'm' => 60,
            'h' => 3600,
            'd' => 86_400,
            _ => return Err(bad()),
        };
        let n: u64 = digits.parse().map_err(|_| bad())?;
        total += n * unit;
        digits.clear();
    }
    if total == 0 || !digits.is_empty() {
        return Err(bad());
    }
    Ok(total)
}

/// A time as Unix seconds: RFC 3339 (`2026-10-01T22:17:00Z`, `...+01:00`), or
/// `YYYY-MM-DD HH:MM[:SS]` read as UTC, or Unix seconds.
pub fn parse_time(s: &str) -> anyhow::Result<f64> {
    let s = s.trim();
    let bad = || anyhow!("not a time: {s:?} (use e.g. 2026-10-01T22:17:00Z)");
    if !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit() || b == b'.') {
        return s.parse().map_err(|_| bad());
    }
    let b = s.as_bytes();
    let field = |from: usize, to: usize| -> anyhow::Result<i64> {
        s.get(from..to)
            .filter(|f| f.bytes().all(|c| c.is_ascii_digit()))
            .and_then(|f| f.parse().ok())
            .ok_or_else(bad)
    };
    if b.len() < 16 || b[4] != b'-' || b[7] != b'-' || !matches!(b[10], b'T' | b't' | b' ') {
        return Err(bad());
    }
    if b[13] != b':' {
        return Err(bad());
    }
    let (y, mo, d, h, mi) = (
        field(0, 4)?,
        field(5, 7)?,
        field(8, 10)?,
        field(11, 13)?,
        field(14, 16)?,
    );
    let mut rest = &s[16..];
    let mut secs = 0.0;
    if let Some(r) = rest.strip_prefix(':') {
        let end = r
            .find(|c: char| !(c.is_ascii_digit() || c == '.'))
            .unwrap_or(r.len());
        secs = r[..end].parse().map_err(|_| bad())?;
        rest = &r[end..];
    }
    let offset = match rest {
        "" | "Z" | "z" => 0,
        _ => {
            let sign = match rest.as_bytes()[0] {
                b'+' => 1,
                b'-' => -1,
                _ => return Err(bad()),
            };
            let z = rest[1..].replace(':', "");
            if z.len() != 4 || !z.bytes().all(|c| c.is_ascii_digit()) {
                return Err(bad());
            }
            sign * (z[..2].parse::<i64>()? * 3600 + z[2..].parse::<i64>()? * 60)
        }
    };
    if !(1..=12).contains(&mo) || !(1..=31).contains(&d) || h > 23 || mi > 59 || secs >= 61.0 {
        return Err(bad());
    }
    let days = days_from_civil(y, mo, d);
    Ok((days * 86_400 + h * 3600 + mi * 60 - offset) as f64 + secs)
}

/// `2026-10-01T22:17:00Z`
pub fn iso(t: f64) -> String {
    let secs = t.round() as i64;
    let (y, m, d) = civil_from_days(secs.div_euclid(86_400));
    let s = secs.rem_euclid(86_400);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z",
        s / 3600,
        s % 3600 / 60,
        s % 60
    )
}

/// Days since 1970-01-01 of a proleptic Gregorian date (Howard Hinnant's algorithm).
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * ((m + 9) % 12) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (yoe + era * 400 + i64::from(m <= 2), m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn times_parse_in_the_usual_forms() {
        let t = 1_790_892_420.0; // 2026-10-01T22:07:00Z
        assert_eq!(iso(t), "2026-10-01T22:07:00Z");
        for s in [
            "2026-10-01T22:07:00Z",
            "2026-10-01T22:07Z",
            "2026-10-01 22:07",
            "2026-10-01T23:07:00+01:00",
            "2026-10-01T21:37:00-0030",
            "1790892420",
        ] {
            assert_eq!(parse_time(s).unwrap(), t, "{s}");
        }
        assert_eq!(parse_time("2026-10-01T22:07:30.5Z").unwrap(), t + 30.5);
        for s in [
            "2026-13-01T00:00Z",
            "yesterday",
            "2026-10-01",
            "2026-10-01T22:07+1",
        ] {
            assert!(parse_time(s).is_err(), "{s}");
        }
        // dates round-trip across eras and leap days
        for days in [-719_468, -1, 0, 11_016, 20_727, 20_728, 2_932_896] {
            let (y, m, d) = civil_from_days(days);
            assert_eq!(days_from_civil(y, m, d), days);
        }
        assert_eq!(civil_from_days(days_from_civil(2024, 2, 29)), (2024, 2, 29));
        assert_eq!(
            civil_from_days(days_from_civil(2026, 3, 1) - 1),
            (2026, 2, 28)
        );
    }

    #[test]
    fn durations_and_numbers() {
        assert_eq!(window_secs("15m").unwrap(), 900);
        assert_eq!(window_secs("1h30m").unwrap(), 5400);
        assert!(window_secs("15").is_err() && window_secs("15x").is_err());
        assert!(window_secs("").is_err());
        assert_eq!(num(125.0), "125");
        assert_eq!(num(3750.04), "3750");
        assert_eq!(num(12.3456), "12.346");
        assert_eq!(num(0.0012), "0.0012");
        assert_eq!(base64(b"user:pa ss"), "dXNlcjpwYSBzcw==");
        assert_eq!(base64(b"ab"), "YWI=");
    }

    #[test]
    fn durations_print_as_prometheus_reads_them() {
        assert_eq!(prom_duration(240), "4m");
        assert_eq!(prom_duration(90), "90s");
    }

    #[test]
    fn steadiest_windows_avoid_the_ramp() {
        let grid: Vec<i64> = (0..10).map(|i| i * 60).collect();
        let total = [
            0.0, 50.0, 100.0, 120.0, 125.0, 124.0, 126.0, 125.0, 90.0, 10.0,
        ];
        let best = steady_windows(&grid, &total, 3);
        assert_eq!(best[0].0, 360, "{best:?}"); // 125, 124, 126
        assert!(best.iter().all(|b| b.1 >= 0.9 * 125.0));
    }
}
