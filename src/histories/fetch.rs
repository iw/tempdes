//! `tempdes workload fetch`: histories read from a Temporal frontend, for `import`.
//!
//! The executions started in a window are listed (`ListWorkflowExecutions`) one slice of the
//! window at a time, and counted by type: in all and slice by slice, by how they stood when
//! listed, and by how long the closed ones ran. Up to `limit` of each type, spread evenly over
//! the listing, are fetched (`GetWorkflowExecutionHistory`) and written one file per execution,
//! beside a manifest ([`super::Manifest`]) with the counts, from which `import` takes each type's
//! start rate. Payloads (inputs, results, details, memos, headers and search attribute values),
//! and failures' messages and stack traces, are dropped before anything is written.
//!
//! The frontend is reached one of two ways ([`Via`]):
//!   - over its HTTP API, as the Temporal CLI would reach it from a client profile
//!     ([`crate::envconfig`]): the same calls as gRPC, served on the frontend's `httpPort`, 7243
//!     unless the cluster sets another;
//!   - through the Temporal CLI itself (`temporal workflow list`, `workflow show` and `operator
//!     namespace describe`), run once per call with the CLI's own configuration, for a frontend
//!     only its gRPC port reaches.

use std::collections::{BTreeMap, HashSet};
use std::fmt::Write as _;
use std::io::{IsTerminal, Read};
use std::net::ToSocketAddrs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, anyhow, bail};
use serde_json::{Value, json};
use serde_saphyr::FlowSeq;
use ureq::unversioned::resolver::{ResolvedSocketAddrs, Resolver};

use super::{Durations, MANIFEST, Manifest, TypeCount};
use crate::envconfig::{Loaded, Profile};
use crate::metrics::fetch::{iso, parse_time, window_secs};
use crate::util::units::{fmt_rate, fmt_us};

/// The frontend's `httpPort` when the cluster doesn't set another.
pub const HTTP_PORT: u16 = 7243;

/// The executions listed per page: the frontend's most (`frontend.visibilityMaxPageSize`).
const PAGE_SIZE: u32 = 1000;

/// The most slices a window is listed in; a longer window gets longer slices.
const MAX_SLICES: f64 = 1440.0;

pub struct FetchArgs {
    /// how the frontend is reached
    pub via: Via,
    /// end of the window
    pub end: String,
    /// window length, e.g. `15m`
    pub window: String,
    /// the window is listed one slice of this length at a time, e.g. `1m`
    pub slice: String,
    /// only these workflow types
    pub types: Vec<String>,
    /// an extra visibility filter, ANDed with the window
    pub query: Option<String>,
    /// histories to fetch of each type
    pub limit: usize,
    pub output: PathBuf,
    /// seconds allowed per request, or per Temporal CLI command
    pub timeout_s: f64,
}

/// How the frontend is reached.
pub enum Via {
    /// its HTTP API, with a client profile (the command line's overrides applied)
    Http {
        profile: Loaded,
        /// the frontend's HTTP API, when it isn't the profile's host on [`HTTP_PORT`]
        http_url: Option<String>,
    },
    /// the Temporal CLI
    Cli(Cli),
}

/// The Temporal CLI, run with its own configuration: its environment variables and config file.
pub struct Cli {
    /// the `temporal` executable
    pub program: PathBuf,
    /// the namespace (`--namespace`), else the CLI's own
    pub namespace: Option<String>,
    /// the frontend's address (`--address`), else the CLI's own
    pub address: Option<String>,
    /// more arguments for every command, e.g. `--env` and `prod`
    pub args: Vec<String>,
}

/// Fetch the histories; a summary of what was written.
pub fn fetch(a: &FetchArgs) -> anyhow::Result<String> {
    anyhow::ensure!(a.limit > 0, "--limit must be at least 1");
    let end = parse_time(&a.end)?;
    let window_s = window_secs(&a.window)? as f64;
    let start = end - window_s;
    let mut slice_s = (window_secs(&a.slice)? as f64).min(window_s);
    let mut slice_label = if slice_s < window_s {
        a.slice.clone()
    } else {
        a.window.clone()
    };
    let mut notes = Vec::new();
    if window_s / slice_s > MAX_SLICES {
        slice_s = (window_s / MAX_SLICES).ceil();
        slice_label = fmt_us(slice_s * 1e6);
        notes.push(format!(
            "slices of {slice_label} rather than --slice {}, to list the window in at most {MAX_SLICES} of them",
            a.slice
        ));
    }
    let slices = (window_s / slice_s).ceil().max(1.0) as usize;
    let query = visibility_query(start, end, &a.types, a.query.as_deref())?;
    let source: Box<dyn Source + '_> = match &a.via {
        Via::Http { profile, http_url } => {
            let p = &profile.profile;
            let fe = Frontend::new(p, http_url.as_deref(), a.timeout_s)?;
            if fe.base.starts_with("http://")
                && fe.headers.iter().any(|(k, _)| k == "Authorization")
            {
                eprintln!("warning: credentials are going over plain HTTP, unencrypted");
            }
            let connection = format!("at {} ({}), from {}", fe.base, fe.auth(p), profile.origin);
            Box::new(HttpSource {
                fe,
                ns: p.namespace().to_string(),
                connection,
            })
        }
        Via::Cli(cli) => Box::new(CliSource {
            cli,
            timeout: Duration::from_secs_f64(a.timeout_s),
        }),
    };
    // the namespace exists, and its retention says whether the window's histories still do
    let ns = source.namespace()?;
    eprintln!("namespace {} {}", ns.name, ns.connection);
    let progress = std::io::stderr().is_terminal();
    check_output(&a.output, &ns.name, &query)?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0.0, |d| d.as_secs_f64());
    let expired = ns.retention_s.filter(|r| now - end > *r);
    if let Some(r) = expired {
        eprintln!(
            "warning: the window ended {} ago, and {} keeps closed executions for {}: those of the window are likely deleted",
            fmt_span(now - end),
            ns.name,
            fmt_span(r)
        );
    }

    // list and count, slice by slice
    let mut listed: BTreeMap<String, Listed> = a
        .types
        .iter()
        .map(|t| (t.clone(), Listed::new(slices)))
        .collect();
    let slice_of = |t: f64| (((t - start) / slice_s).floor().max(0.0) as usize).min(slices - 1);
    let mut seen: HashSet<String> = HashSet::new();
    let mut n = 0usize;
    for i in 0..slices {
        let s0 = start + i as f64 * slice_s;
        let s1 = (s0 + slice_s).min(end);
        let q = visibility_query(s0, s1, &a.types, a.query.as_deref())?;
        source.list(&ns.name, &q, &mut |e: &Value| {
            let s = |v: &Value| v.as_str().unwrap_or_default().to_string();
            let (wid, rid) = (
                s(&e["execution"]["workflowId"]),
                s(&e["execution"]["runId"]),
            );
            // slices don't overlap, but an execution is never counted twice
            if !rid.is_empty() && !seen.insert(rid.clone()) {
                return;
            }
            n += 1;
            let l = listed
                .entry(s(&e["type"]["name"]))
                .or_insert_with(|| Listed::new(slices));
            l.started += 1;
            if !s(&e["parentExecution"]["workflowId"]).is_empty() {
                l.children += 1;
            }
            let started = e["startTime"].as_str().and_then(|t| parse_time(t).ok());
            l.started_by_slice[started.map_or(i, slice_of)] += 1;
            if let Some(st) = status_name(&s(&e["status"])) {
                *l.status.entry(st).or_default() += 1;
            }
            if let Some(closed) = e["closeTime"].as_str().and_then(|t| parse_time(t).ok()) {
                if let Some(t0) = started {
                    l.durations.push((closed - t0).max(0.0));
                }
                if (start..end).contains(&closed) {
                    l.closed_by_slice[slice_of(closed)] += 1;
                }
            }
            match source.unaddressable(&wid) {
                _ if wid.is_empty() || rid.is_empty() => {
                    l.unaddressable += 1;
                    l.why_skipped = Some("listed without a workflow or run ID");
                }
                Some(why) => {
                    l.unaddressable += 1;
                    l.why_skipped = Some(why);
                }
                None => l.runs.push((wid, rid)),
            }
        })?;
        if progress {
            eprint!("\rlisted {n} executions, slice {}/{slices}", i + 1);
        }
    }
    if progress {
        eprintln!();
    }
    if n == 0 {
        let mut msg = format!(
            "no executions in {} matched {query:?} (times without a zone are UTC)",
            ns.name
        );
        if let Some(r) = ns.retention_s {
            let _ = write!(
                msg,
                ". {} keeps closed executions for {}{}",
                ns.name,
                fmt_span(r),
                if expired.is_some() {
                    ", and the window ended longer ago than that"
                } else {
                    ""
                }
            );
        }
        bail!("{msg}");
    }

    // choose the histories, and record the counts before fetching, so that a fetch that stops
    // part-way can be run again to finish
    let chosen: Vec<Vec<&(String, String)>> = listed
        .values()
        .map(|l| spread(l.runs.len(), a.limit).map(|i| &l.runs[i]).collect())
        .collect();
    let mut manifest = Manifest {
        namespace: ns.name.clone(),
        start: iso(start),
        end: iso(end),
        window_s,
        slice_s,
        query: query.clone(),
        types: listed
            .iter()
            .zip(&chosen)
            .map(|((t, l), runs)| TypeCount {
                name: t.clone(),
                started: l.started,
                children: l.children,
                fetched: runs.len() as u64,
                started_by_slice: Some(FlowSeq(l.started_by_slice.clone())),
                closed_by_slice: Some(FlowSeq(l.closed_by_slice.clone())),
                status: l.status.clone(),
                duration_s: Durations::of(l.durations.clone()),
            })
            .collect(),
    };
    create_output(&a.output)?;
    write_manifest(&a.output, &manifest)?;

    // fetch
    let total: usize = chosen.iter().map(Vec::len).sum();
    let (mut done, mut kept, mut gone) = (0usize, 0usize, 0usize);
    let mut shorthand = false;
    for (count, runs) in manifest.types.iter_mut().zip(&chosen) {
        count.fetched = 0;
        for (wid, rid) in runs {
            done += 1;
            if progress {
                eprint!("\rfetching histories: {done}/{total}");
            }
            let file = a.output.join(format!("{}.json", file_stem(rid)));
            if file.exists() {
                kept += 1;
                count.fetched += 1;
                continue;
            }
            match source.history(&ns.name, wid, rid)? {
                Some((events, short)) => {
                    shorthand |= short;
                    let doc = json!({
                        "workflowId": wid,
                        "runId": rid,
                        "workflowType": count.name,
                        "events": events,
                    });
                    crate::profile::write_private(&file, doc.to_string().as_bytes())?;
                    count.fetched += 1;
                }
                // deleted since it was listed (its retention ran out)
                None => gone += 1,
            }
        }
    }
    if progress {
        eprintln!();
    }
    write_manifest(&a.output, &manifest)?;

    // summary
    let mut out = String::new();
    let width = manifest
        .types
        .iter()
        .map(|t| t.name.len())
        .max()
        .unwrap_or(0);
    let _ = writeln!(
        out,
        "executions started {} to {} ({}), listed {slice_label} at a time:",
        manifest.start, manifest.end, a.window,
    );
    for (t, l) in manifest.types.iter().zip(listed.values()) {
        let own = t.started - t.children;
        let started = match (own, t.children) {
            (_, 0) => format!(
                "{} started ({})",
                t.started,
                fmt_rate(own as f64 / window_s)
            ),
            (0, _) => format!("{} started, all by other workflows", t.started),
            _ => format!(
                "{} started, {} by other workflows ({} without a parent)",
                t.started,
                t.children,
                fmt_rate(own as f64 / window_s)
            ),
        };
        let _ = write!(
            out,
            "  {:<width$}  {started}; {} fetched",
            t.name, t.fetched
        );
        if let Some(why) = l.why_skipped {
            let _ = write!(out, "; {} skipped: {why}", l.unaddressable);
        }
        let _ = writeln!(out);
        let _ = writeln!(out, "  {:<width$}  {}", "", shape(l, start, end, slice_s));
    }
    for note in &notes {
        let _ = writeln!(out, "{note}");
    }
    let _ = write!(
        out,
        "wrote {} histories to {} with {MANIFEST}, readable only by you",
        total - kept - gone,
        a.output.display()
    );
    if kept > 0 {
        let _ = write!(out, " ({kept} already there)");
    }
    let _ = writeln!(out);
    if gone > 0 {
        let _ = writeln!(
            out,
            "{gone} {} deleted between listing and fetching",
            if gone == 1 { "was" } else { "were" }
        );
    }
    if shorthand {
        let _ = writeln!(
            out,
            "the frontend sent payloads in shorthand: the fields that hold them were dropped whole"
        );
    }
    let _ = writeln!(
        out,
        "the files name your workflow types, task queues and worker hosts: keep them private\n\
         next: tempdes workload import {} --namespace <namespace in your scenario> -o workflows.yaml",
        a.output.display()
    );
    Ok(out)
}

/// A type's starts over the window, how its executions stood when listed, and how long the
/// closed ones ran, on one line: `started 10:12-10:34, busiest 10:29 at 126/s; 99812
/// Completed, 188 Failed; ran p50 1.3h, p99 1.4h, max 1.5h`.
fn shape(l: &Listed, start: f64, end: f64, slice_s: f64) -> String {
    let mut parts = Vec::new();
    let at = |i: usize| -> String {
        let t = iso(start + i as f64 * slice_s);
        // to the minute, or to the second for slices shorter than one
        t[11..if slice_s < 60.0 { 19 } else { 16 }].to_string()
    };
    let busy: Vec<usize> = (0..l.started_by_slice.len())
        .filter(|i| l.started_by_slice[*i] > 0)
        .collect();
    if let (Some(&first), Some(&last)) = (busy.first(), busy.last()) {
        let peak = busy
            .iter()
            .copied()
            .max_by_key(|i| (l.started_by_slice[*i], std::cmp::Reverse(*i)))
            .unwrap_or(first);
        let len = slice_s.min(end - (start + peak as f64 * slice_s));
        let rate = fmt_rate(l.started_by_slice[peak] as f64 / len);
        parts.push(if first == last {
            format!("started {} at {rate}", at(first))
        } else {
            format!(
                "started {}-{}, busiest {} at {rate}",
                at(first),
                at(last),
                at(peak)
            )
        });
    }
    if !l.status.is_empty() {
        let mut st: Vec<(&String, &u64)> = l.status.iter().collect();
        st.sort_by(|a, b| b.1.cmp(a.1).then(a.0.cmp(b.0)));
        let st: Vec<String> = st.iter().map(|(k, v)| format!("{v} {k}")).collect();
        parts.push(st.join(", "));
    }
    if let Some(d) = Durations::of(l.durations.clone()) {
        parts.push(format!(
            "ran p50 {}, p99 {}, max {}",
            fmt_us(d.p50 * 1e6),
            fmt_us(d.p99 * 1e6),
            fmt_us(d.max * 1e6)
        ));
    }
    parts.join("; ")
}

/// A listed execution's status as its short name (`Completed`, `ContinuedAsNew`), whichever
/// way the frontend spelled it; None when it gave none.
fn status_name(s: &str) -> Option<String> {
    let s = s.trim().trim_start_matches("WORKFLOW_EXECUTION_STATUS_");
    if s.is_empty() || s.eq_ignore_ascii_case("unspecified") {
        return None;
    }
    if !s.contains('_') && s.chars().any(|c| c.is_ascii_lowercase()) {
        return Some(s.to_string());
    }
    Some(
        s.split('_')
            .filter(|w| !w.is_empty())
            .map(|w| w[..1].to_ascii_uppercase() + &w[1..].to_ascii_lowercase())
            .collect(),
    )
}

/// A span of seconds, in days when it is that long.
fn fmt_span(secs: f64) -> String {
    let days = secs / 86_400.0;
    if days < 1.0 {
        fmt_us(secs * 1e6)
    } else if (days - days.round()).abs() < 0.05 {
        let d = days.round();
        format!("{d:.0} day{}", if d == 1.0 { "" } else { "s" })
    } else {
        format!("{days:.1} days")
    }
}

/// A type's executions in the listing.
struct Listed {
    started: u64,
    /// started by another workflow
    children: u64,
    /// those whose histories can be asked for: (workflow ID, run ID)
    runs: Vec<(String, String)>,
    /// those that can't, and why
    unaddressable: u64,
    why_skipped: Option<&'static str>,
    /// started, and closed, in each slice of the window
    started_by_slice: Vec<u64>,
    closed_by_slice: Vec<u64>,
    /// how they stood when listed
    status: BTreeMap<String, u64>,
    /// seconds from start to close, of those closed
    durations: Vec<f64>,
}

impl Listed {
    fn new(slices: usize) -> Self {
        Listed {
            started: 0,
            children: 0,
            runs: Vec::new(),
            unaddressable: 0,
            why_skipped: None,
            started_by_slice: vec![0; slices],
            closed_by_slice: vec![0; slices],
            status: BTreeMap::new(),
            durations: Vec::new(),
        }
    }
}

/// The visibility query for executions started in `[start, end)`.
fn visibility_query(
    start: f64,
    end: f64,
    types: &[String],
    extra: Option<&str>,
) -> anyhow::Result<String> {
    let mut q = format!(
        "StartTime >= '{}' AND StartTime < '{}'",
        iso(start),
        iso(end)
    );
    if !types.is_empty() {
        for t in types {
            if t.contains(['\'', '"', '\\']) {
                bail!("--type {t:?}: quotes and backslashes aren't supported here; use --query");
            }
        }
        let list: Vec<String> = types.iter().map(|t| format!("'{t}'")).collect();
        let _ = write!(q, " AND WorkflowType IN ({})", list.join(", "));
    }
    if let Some(x) = extra.map(str::trim).filter(|x| !x.is_empty()) {
        let _ = write!(q, " AND ({x})");
    }
    Ok(q)
}

/// `k` of `0..n`, spread evenly: the middle of each of `k` equal parts.
fn spread(n: usize, k: usize) -> impl Iterator<Item = usize> {
    let k = k.min(n);
    (0..k).map(move |i| (2 * i + 1) * n / (2 * k))
}

/// A run ID as a file name: run IDs are UUIDs, but nothing from the server becomes a path
/// unchecked.
fn file_stem(rid: &str) -> String {
    rid.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// `out` is new, empty of histories, or holds an earlier run of the same fetch to finish.
fn check_output(out: &Path, ns: &str, query: &str) -> anyhow::Result<()> {
    let manifest = out.join(MANIFEST);
    if manifest.exists() {
        let m = super::read_manifest(&manifest)?;
        if m.namespace != ns || m.query != query {
            bail!(
                "{} holds histories fetched from {} with {:?}: give another --output",
                out.display(),
                m.namespace,
                m.query
            );
        }
    } else if out.is_dir() {
        let holds_json = std::fs::read_dir(out)
            .with_context(|| format!("reading {}", out.display()))?
            .filter_map(Result::ok)
            .any(|e| {
                e.path()
                    .extension()
                    .is_some_and(|x| x.eq_ignore_ascii_case("json"))
            });
        if holds_json {
            bail!(
                "{} already holds history files, which `import` would read with these: give another --output",
                out.display()
            );
        }
    }
    Ok(())
}

/// Create `out`, private to you, unless it exists.
fn create_output(out: &Path) -> anyhow::Result<()> {
    if out.is_dir() {
        if let Some(note) = crate::profile::readable_by_others(out) {
            eprintln!("note: {note}");
        }
        return Ok(());
    }
    crate::profile::create_private_dir(out)
}

fn write_manifest(out: &Path, m: &Manifest) -> anyhow::Result<()> {
    let yaml = serde_saphyr::to_string(m).map_err(|e| anyhow!("writing {MANIFEST}: {e}"))?;
    let text = format!(
        "# Written by `tempdes workload fetch`: the executions it listed and fetched. `tempdes workload\n\
         # import` takes each type's start rate from these counts. Keep it private.\n{yaml}"
    );
    crate::profile::write_private(&out.join(MANIFEST), text.as_bytes())
}

// --- what is kept -----------------------------------------------------------------------------

/// The fields that hold payloads, dropped whole when the frontend sends payloads in its
/// shorthand (their decoded values), which can't be told apart by their shape.
const PAYLOAD_FIELDS: &[&str] = &[
    "input",
    "result",
    "details",
    "heartbeatDetails",
    "lastHeartbeatDetails",
    "lastCompletionResult",
    "memo",
    "searchAttributes",
    "header",
    "encodedAttributes",
    "userMetadata",
    "args",
    "success",
];

/// Drop from an event what `import` doesn't read and shouldn't leave the cluster: payloads,
/// which `noPayloadShorthand` keeps as `{metadata, data}` objects, and failures' messages and
/// stack traces.
fn strip(v: &mut Value, shorthand: bool) {
    match v {
        Value::Object(m) => {
            if !shorthand
                && m.contains_key("metadata")
                && m.keys()
                    .all(|k| matches!(k.as_str(), "metadata" | "data" | "externalPayloads"))
            {
                m.clear();
                return;
            }
            if shorthand {
                for f in PAYLOAD_FIELDS {
                    m.remove(*f);
                }
            }
            if m.contains_key("stackTrace") || m.keys().any(|k| k.ends_with("FailureInfo")) {
                m.remove("message");
                m.remove("stackTrace");
            }
            for x in m.values_mut() {
                strip(x, shorthand);
            }
        }
        Value::Array(a) => {
            for x in a {
                strip(x, shorthand);
            }
        }
        _ => {}
    }
}

// --- where executions and histories come from -------------------------------------------------

/// A namespace as a [`Source`] reaches it.
struct Namespace {
    name: String,
    /// how long it keeps closed executions, in seconds
    retention_s: Option<f64>,
    /// how it is reached, for the connection line
    connection: String,
}

/// Where executions and histories are read from.
trait Source {
    /// The namespace, which must exist.
    fn namespace(&self) -> anyhow::Result<Namespace>;
    /// Every execution matching `query`, as visibility lists them.
    fn list(&self, ns: &str, query: &str, each: &mut dyn FnMut(&Value)) -> anyhow::Result<()>;
    /// An execution's events with their payloads dropped, and whether the frontend sent the
    /// payloads in shorthand; None when the execution is gone.
    fn history(&self, ns: &str, wid: &str, rid: &str)
    -> anyhow::Result<Option<(Vec<Value>, bool)>>;
    /// Why a workflow ID can't be asked for, when it can't.
    fn unaddressable(&self, _wid: &str) -> Option<&'static str> {
        None
    }
}

/// The frontend's HTTP API.
struct HttpSource {
    fe: Frontend,
    ns: String,
    connection: String,
}

impl Source for HttpSource {
    fn namespace(&self) -> anyhow::Result<Namespace> {
        Ok(Namespace {
            name: self.ns.clone(),
            retention_s: self.fe.retention_s(&self.ns)?,
            connection: self.connection.clone(),
        })
    }

    fn list(&self, ns: &str, query: &str, each: &mut dyn FnMut(&Value)) -> anyhow::Result<()> {
        let mut token: Option<String> = None;
        loop {
            let (execs, next) = self.fe.list(ns, query, token.as_deref())?;
            for e in &execs {
                each(e);
            }
            token = next;
            if token.is_none() {
                return Ok(());
            }
        }
    }

    fn history(
        &self,
        ns: &str,
        wid: &str,
        rid: &str,
    ) -> anyhow::Result<Option<(Vec<Value>, bool)>> {
        match self.fe.history(ns, wid, rid) {
            Ok((mut events, short)) => {
                for e in &mut events {
                    strip(e, short);
                }
                Ok(Some((events, short)))
            }
            Err(e)
                if e.downcast_ref::<ApiError>()
                    .is_some_and(|x| x.status == 404) =>
            {
                Ok(None)
            }
            Err(e) => Err(e),
        }
    }

    fn unaddressable(&self, wid: &str) -> Option<&'static str> {
        // the HTTP API reads the workflow ID from the path, decoded, so an ID with a `/` can't
        // be asked for
        wid.contains('/')
            .then_some("the HTTP API can't ask for workflow IDs containing '/'")
    }
}

/// The Temporal CLI, run once per call.
struct CliSource<'a> {
    cli: &'a Cli,
    timeout: Duration,
}

impl CliSource<'_> {
    /// Run `temporal <args>` with the namespace and address, then the arguments passed
    /// through: its standard output, or, when it fails, its standard error.
    fn run(&self, ns: Option<&str>, args: &[String]) -> anyhow::Result<Result<Vec<u8>, String>> {
        let mut cmd = Command::new(&self.cli.program);
        cmd.args(args);
        if let Some(ns) = ns.or(self.cli.namespace.as_deref()) {
            cmd.arg(format!("--namespace={ns}"));
        }
        if let Some(a) = &self.cli.address {
            cmd.arg(format!("--address={a}"));
        }
        cmd.args(&self.cli.args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = cmd.spawn().map_err(|e| {
            anyhow!(
                "cannot run the Temporal CLI as {}: {e}{}",
                self.cli.program.display(),
                if e.kind() == std::io::ErrorKind::NotFound {
                    " (give its path with --cli PATH)"
                } else {
                    ""
                }
            )
        })?;
        // read both pipes while it runs, so that neither fills up and stalls it
        let mut stdout = child.stdout.take().context("the CLI's standard output")?;
        let mut stderr = child.stderr.take().context("the CLI's standard error")?;
        let out = std::thread::spawn(move || {
            let mut b = Vec::new();
            let _ = stdout.read_to_end(&mut b);
            b
        });
        let err = std::thread::spawn(move || {
            let mut b = Vec::new();
            let _ = stderr.read_to_end(&mut b);
            b
        });
        let deadline = Instant::now() + self.timeout;
        let status = loop {
            if let Some(s) = child.try_wait()? {
                break s;
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                bail!(
                    "`temporal {}` took longer than --timeout ({})",
                    args[..2.min(args.len())].join(" "),
                    fmt_us(self.timeout.as_secs_f64() * 1e6)
                );
            }
            std::thread::sleep(Duration::from_millis(10));
        };
        let out = out.join().unwrap_or_default();
        let err = String::from_utf8_lossy(&err.join().unwrap_or_default())
            .trim()
            .to_string();
        Ok(if status.success() { Ok(out) } else { Err(err) })
    }
}

impl Source for CliSource<'_> {
    fn namespace(&self) -> anyhow::Result<Namespace> {
        let args = ["operator", "namespace", "describe", "--output=json"].map(String::from);
        let out = self.run(None, &args)?.map_err(|e| {
            anyhow!(
                "the Temporal CLI couldn't describe the namespace: {}",
                brief(&e)
            )
        })?;
        let v: Value = serde_json::from_slice(&out)
            .context("reading `temporal operator namespace describe --output json`")?;
        let name = v["namespaceInfo"]["name"]
            .as_str()
            .map(str::to_string)
            .or_else(|| self.cli.namespace.clone())
            .context("the Temporal CLI described a namespace without naming it")?;
        // the arguments passed through are named, never their values, which can be secrets
        let mut connection = format!("through the Temporal CLI ({}", self.cli.program.display());
        if let Some(a) = &self.cli.address {
            let _ = write!(connection, ", at {a}");
        }
        let flags = flag_names(&self.cli.args);
        if !flags.is_empty() {
            let _ = write!(connection, ", with {}", flags.join(" "));
        }
        connection.push(')');
        Ok(Namespace {
            name,
            retention_s: retention_of(&v),
            connection,
        })
    }

    fn list(&self, ns: &str, query: &str, each: &mut dyn FnMut(&Value)) -> anyhow::Result<()> {
        let args = [
            "workflow".into(),
            "list".into(),
            format!("--query={query}"),
            "--output=jsonl".into(),
        ];
        let out = self
            .run(Some(ns), &args)?
            .map_err(|e| anyhow!("`temporal workflow list` failed: {}", brief(&e)))?;
        for v in jsonl(&out).context("reading `temporal workflow list --output jsonl`")? {
            each(&v);
        }
        Ok(())
    }

    fn history(
        &self,
        ns: &str,
        wid: &str,
        rid: &str,
    ) -> anyhow::Result<Option<(Vec<Value>, bool)>> {
        let args = [
            "workflow".into(),
            "show".into(),
            format!("--workflow-id={wid}"),
            format!("--run-id={rid}"),
            "--output=json".into(),
        ];
        let out = match self.run(Some(ns), &args)? {
            Ok(out) => out,
            // deleted since it was listed
            Err(e) if e.to_ascii_lowercase().contains("not found") => return Ok(None),
            Err(e) => bail!("`temporal workflow show` for {wid} failed: {}", brief(&e)),
        };
        let v: Value = serde_json::from_slice(&out)
            .with_context(|| format!("reading `temporal workflow show` for {wid}"))?;
        let mut events = match v.get("events").or_else(|| v.pointer("/history/events")) {
            Some(Value::Array(a)) => a.clone(),
            _ => bail!("`temporal workflow show` for {wid} printed no events"),
        };
        // the CLI may print payloads decoded, as plain JSON: drop them by their shape, and by
        // the fields that hold them
        for e in &mut events {
            strip(e, false);
            strip(e, true);
        }
        Ok(Some((events, false)))
    }
}

/// The values of JSON lines.
fn jsonl(out: &[u8]) -> anyhow::Result<Vec<Value>> {
    out.split(|b| *b == b'\n')
        .enumerate()
        .filter(|(_, line)| !line.trim_ascii().is_empty())
        .map(|(i, line)| {
            serde_json::from_slice(line).with_context(|| format!("line {} isn't JSON", i + 1))
        })
        .collect()
}

/// The gist of the CLI's standard error: its `Error:` line, else its first line, cut short.
fn brief(stderr: &str) -> String {
    let line = stderr
        .lines()
        .find(|l| l.trim_start().starts_with("Error"))
        .or_else(|| stderr.lines().find(|l| !l.trim().is_empty()))
        .unwrap_or("no error message")
        .trim();
    let cut: String = line.chars().take(300).collect();
    if cut.len() < line.len() {
        format!("{cut}...")
    } else {
        cut
    }
}

/// The flags among `args`, without their values: `--api-key=k` is `--api-key`.
fn flag_names(args: &[String]) -> Vec<&str> {
    args.iter()
        .filter(|a| a.starts_with('-'))
        .map(|a| a.split('=').next().unwrap_or(a))
        .collect()
}

/// A described namespace's retention, in seconds.
fn retention_of(v: &Value) -> Option<f64> {
    v["config"]["workflowExecutionRetentionTtl"]
        .as_str()
        .and_then(|d| d.strip_suffix('s')?.parse().ok())
}

// --- the frontend's HTTP API ------------------------------------------------------------------

/// An error the frontend answered with.
#[derive(Debug)]
struct ApiError {
    status: u16,
    message: String,
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HTTP {}: {}", self.status, self.message)
    }
}

impl std::error::Error for ApiError {}

struct Frontend {
    agent: ureq::Agent,
    /// `scheme://host:port`
    base: String,
    headers: Vec<(String, String)>,
}

/// What to say when the frontend can't be reached.
const UNREACHABLE: &str = "the HTTP API is on the frontend's httpPort, 7243 unless the cluster sets another; \
                           give --http-url if it is elsewhere";

impl Frontend {
    fn new(p: &Profile, http_url: Option<&str>, timeout_s: f64) -> anyhow::Result<Self> {
        let ep = endpoint(p, http_url)?;
        let agent = agent(p, &ep, timeout_s)?;
        let mut headers = Vec::new();
        let api_key = p.api_key();
        for (k, v) in &p.grpc_meta {
            match k.as_str() {
                // the API key's wins
                "authorization" if api_key.is_some() => {}
                "authorization" => headers.push(("Authorization".into(), v.clone())),
                // the frontend passes these on to its gRPC handlers as metadata
                _ => headers.push((format!("Grpc-Metadata-{k}"), v.clone())),
            }
        }
        if let Some(k) = api_key {
            headers.push(("Authorization".into(), format!("Bearer {k}")));
        }
        Ok(Frontend {
            agent,
            base: ep.base,
            headers,
        })
    }

    /// How requests are authenticated, for the connection line.
    fn auth(&self, p: &Profile) -> String {
        let mut parts = vec![if self.base.starts_with("https://") {
            "TLS"
        } else {
            "plain HTTP"
        }];
        let tls = p.tls.as_ref().filter(|_| self.base.starts_with("https://"));
        if tls.is_some_and(|t| t.client_cert.is_some()) {
            parts.push("client certificate");
        }
        if p.api_key().is_some() {
            parts.push("API key");
        } else if p.grpc_meta.contains_key("authorization") {
            parts.push("authorization header");
        }
        if tls.is_some_and(|t| t.disable_host_verification) {
            parts.push("not verifying the server");
        }
        parts.join(", ")
    }

    fn get(&self, path: &str, params: &[(&str, String)]) -> anyhow::Result<(Value, String)> {
        let url = format!("{}{path}", self.base);
        let mut wait = Duration::from_millis(500);
        let mut attempt = 0;
        loop {
            attempt += 1;
            let last = attempt == 4;
            let mut req = self.agent.get(&url);
            for (k, v) in params {
                req = req.query(*k, v);
            }
            for (k, v) in &self.headers {
                req = req.header(k, v);
            }
            let mut resp = match req.call() {
                Ok(r) => r,
                Err(ureq::Error::Timeout(_) | ureq::Error::Io(_)) if !last => {
                    std::thread::sleep(wait);
                    wait *= 2;
                    continue;
                }
                Err(e) => {
                    // rustls's errors arrive as I/O errors, so tell them by their text
                    let text = e.to_string();
                    let hint = if ["certificate", "alert", "handshake"]
                        .iter()
                        .any(|w| text.contains(w))
                    {
                        "check the profile's tls table: server_ca_cert, server_name, client_cert \
                         and client_key"
                    } else {
                        UNREACHABLE
                    };
                    bail!(
                        "cannot reach the frontend's HTTP API at {}: {text} ({hint})",
                        self.base
                    )
                }
            };
            let status = resp.status().as_u16();
            // busy: try again
            if matches!(status, 429 | 502 | 503 | 504) && !last {
                std::thread::sleep(wait);
                wait *= 2;
                continue;
            }
            let content_type = resp
                .headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default()
                .to_string();
            let body = resp
                .body_mut()
                .with_config()
                .limit(1 << 30)
                .read_to_string()
                .context("reading the frontend's response")?;
            let v: Value = serde_json::from_str(&body).map_err(|_| {
                let start: String = body.chars().take(200).collect();
                anyhow!(
                    "{path}: HTTP {status} from {}, not a Temporal HTTP API response ({UNREACHABLE}): {start}",
                    self.base
                )
            })?;
            if status != 200 {
                let message = v["message"].as_str().unwrap_or("no details").to_string();
                let hint = match status {
                    401 | 403 => {
                        ": check the profile's api_key, its grpc_meta authorization or its client certificate"
                    }
                    _ => "",
                };
                return Err(anyhow::Error::new(ApiError { status, message })
                    .context(format!("{path}{hint}")));
            }
            return Ok((v, content_type));
        }
    }

    /// The namespace's retention (`DescribeNamespace`), in seconds.
    fn retention_s(&self, ns: &str) -> anyhow::Result<Option<f64>> {
        let (v, _) = self
            .get(&format!("/api/v1/namespaces/{}", enc(ns)), &[])
            .map_err(|e| match e.downcast_ref::<ApiError>() {
                Some(x) if x.status == 404 => {
                    anyhow!("namespace {ns} doesn't exist at {}", self.base)
                }
                _ => e,
            })?;
        Ok(retention_of(&v))
    }

    /// A page of executions matching `query` (`ListWorkflowExecutions`), and the next page's
    /// token.
    fn list(
        &self,
        ns: &str,
        query: &str,
        token: Option<&str>,
    ) -> anyhow::Result<(Vec<Value>, Option<String>)> {
        let mut params = vec![
            ("query", query.to_string()),
            ("pageSize", PAGE_SIZE.to_string()),
        ];
        if let Some(t) = token {
            params.push(("nextPageToken", t.to_string()));
        }
        let (v, _) = self.get(
            &format!("/api/v1/namespaces/{}/workflows", enc(ns)),
            &params,
        )?;
        Ok((
            v["executions"].as_array().cloned().unwrap_or_default(),
            next_token(&v),
        ))
    }

    /// An execution's events, page by page (`GetWorkflowExecutionHistory`), and whether the
    /// frontend sent their payloads in shorthand.
    fn history(&self, ns: &str, wid: &str, rid: &str) -> anyhow::Result<(Vec<Value>, bool)> {
        let path = format!(
            "/api/v1/namespaces/{}/workflows/{}/history",
            enc(ns),
            enc(wid)
        );
        let mut events = Vec::new();
        let mut shorthand = false;
        let mut token: Option<String> = None;
        loop {
            // payloads as `{metadata, data}`, which `strip` recognises
            let mut params = vec![
                ("execution.runId", rid.to_string()),
                ("noPayloadShorthand", String::new()),
            ];
            if let Some(t) = &token {
                params.push(("nextPageToken", t.clone()));
            }
            let (v, content_type) = self.get(&path, &params)?;
            shorthand |= !content_type.contains("no-payload-shorthand");
            events.extend(
                v["history"]["events"]
                    .as_array()
                    .cloned()
                    .unwrap_or_default(),
            );
            token = next_token(&v);
            if token.is_none() {
                return Ok((events, shorthand));
            }
        }
    }
}

fn next_token(v: &Value) -> Option<String> {
    v["nextPageToken"]
        .as_str()
        .filter(|t| !t.is_empty())
        .map(str::to_string)
}

/// A path segment, percent-encoded.
fn enc(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(char::from(b));
        } else {
            let _ = write!(out, "%{b:02X}");
        }
    }
    out
}

/// Where requests go.
#[derive(Debug, PartialEq)]
struct Endpoint {
    /// `scheme://host:port[/prefix]`
    base: String,
    /// with TLS verifying a `server_name` other than the host, the base names the server and
    /// connections go to this `host:port` instead
    connect_to: Option<String>,
}

/// The frontend's HTTP API: `--http-url`, else the profile's host on [`HTTP_PORT`], over TLS
/// when the profile has it.
fn endpoint(p: &Profile, http_url: Option<&str>) -> anyhow::Result<Endpoint> {
    let (scheme, authority, prefix) = match http_url {
        Some(u) => {
            let u = u.trim().trim_end_matches('/');
            let (scheme, rest) = u
                .split_once("://")
                .filter(|(s, _)| matches!(*s, "http" | "https"))
                .ok_or_else(|| anyhow!("--http-url needs http:// or https://, got {u:?}"))?;
            let (authority, prefix) = rest.split_at(rest.find('/').unwrap_or(rest.len()));
            (
                scheme.to_string(),
                authority.to_string(),
                prefix.to_string(),
            )
        }
        None => {
            let address = p.address().trim().trim_end_matches('/');
            let (tls_scheme, hostport) = match address.split_once("://") {
                Some((s, rest)) => (s == "https", rest),
                None => (false, address),
            };
            let (host, _grpc_port) = split_host_port(hostport);
            let scheme = if p.tls_enabled() || tls_scheme {
                "https"
            } else {
                "http"
            };
            (
                scheme.to_string(),
                format!("{host}:{HTTP_PORT}"),
                String::new(),
            )
        }
    };
    let (host, port) = split_host_port(&authority);
    if host.is_empty() {
        bail!("no host in {authority:?}");
    }
    let port = port.unwrap_or(if scheme == "https" { 443 } else { 80 });
    let server_name = p
        .tls
        .as_ref()
        .and_then(|t| t.server_name.as_deref())
        .filter(|n| scheme == "https" && !n.is_empty() && !n.eq_ignore_ascii_case(host));
    Ok(match server_name {
        Some(name) => Endpoint {
            base: format!("{scheme}://{name}:{port}{prefix}"),
            connect_to: Some(format!("{host}:{port}")),
        },
        None => Endpoint {
            base: format!("{scheme}://{host}:{port}{prefix}"),
            connect_to: None,
        },
    })
}

/// `host:port`, `host` or `[v6]:port`: the host (brackets kept) and the port.
fn split_host_port(s: &str) -> (&str, Option<u16>) {
    if s.starts_with('[') {
        return match s.find(']') {
            Some(i) => (
                &s[..=i],
                s[i + 1..].strip_prefix(':').and_then(|p| p.parse().ok()),
            ),
            None => (s, None),
        };
    }
    match s.rsplit_once(':') {
        Some((h, p)) if !h.contains(':') => match p.parse() {
            Ok(port) => (h, Some(port)),
            Err(_) => (s, None),
        },
        _ => (s, None),
    }
}

fn agent(p: &Profile, ep: &Endpoint, timeout_s: f64) -> anyhow::Result<ureq::Agent> {
    use ureq::tls::{ClientCert, PemItem, RootCerts, TlsConfig, TlsProvider};
    // rustls's cryptography (an error only means one is installed already)
    let _ = rustls::crypto::ring::default_provider().install_default();
    let tls = p.tls.clone().unwrap_or_default();
    let pem =
        |src: &crate::envconfig::Source, what: &str| -> anyhow::Result<Vec<PemItem<'static>>> {
            let bytes = src.read()?;
            ureq::tls::parse_pem(&bytes)
                .collect::<Result<Vec<_>, _>>()
                .with_context(|| format!("reading the profile's {what}"))
        };
    let roots = match &tls.server_ca_cert {
        Some(src) => {
            let certs: Vec<_> = pem(src, "server CA certificate")?
                .into_iter()
                .filter_map(|i| match i {
                    PemItem::Certificate(c) => Some(c),
                    _ => None,
                })
                .collect();
            if certs.is_empty() {
                bail!("the profile's server CA certificate holds no certificates");
            }
            RootCerts::new_with_certs(&certs)
        }
        None => RootCerts::PlatformVerifier,
    };
    let client = match (&tls.client_cert, &tls.client_key) {
        (Some(cert), Some(key)) => {
            let chain: Vec<_> = pem(cert, "client certificate")?
                .into_iter()
                .filter_map(|i| match i {
                    PemItem::Certificate(c) => Some(c),
                    _ => None,
                })
                .collect();
            let key = pem(key, "client key")?
                .into_iter()
                .find_map(|i| match i {
                    PemItem::PrivateKey(k) => Some(k),
                    _ => None,
                })
                .ok_or_else(|| anyhow!("the profile's client key holds no private key"))?;
            if chain.is_empty() {
                bail!("the profile's client certificate holds no certificates");
            }
            Some(ClientCert::new_with_certs(&chain, key))
        }
        (None, None) => None,
        _ => bail!("the profile gives a client certificate or key without the other: give both"),
    };
    let tls_config = TlsConfig::builder()
        .provider(TlsProvider::Rustls)
        .root_certs(roots)
        .client_cert(client)
        .disable_verification(tls.disable_host_verification)
        .build();
    let config = ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_secs_f64(timeout_s)))
        .http_status_as_error(false)
        .tls_config(tls_config)
        .build();
    Ok(match &ep.connect_to {
        Some(target) => ureq::Agent::with_parts(
            config,
            ureq::unversioned::transport::DefaultConnector::default(),
            ConnectTo(target.clone()),
        ),
        None => config.new_agent(),
    })
}

/// Resolves every request to one `host:port`, so that TLS verifies the URL's host (a profile's
/// `server_name`) while the connection goes to the profile's address.
#[derive(Debug)]
struct ConnectTo(String);

impl Resolver for ConnectTo {
    fn resolve(
        &self,
        _uri: &ureq::http::Uri,
        _config: &ureq::config::Config,
        _timeout: ureq::unversioned::transport::NextTimeout,
    ) -> Result<ResolvedSocketAddrs, ureq::Error> {
        let mut out = self.empty();
        for addr in self.0.to_socket_addrs()?.take(16) {
            out.push(addr);
        }
        if out.is_empty() {
            return Err(ureq::Error::HostNotFound);
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envconfig::Tls;

    #[test]
    fn the_http_api_is_on_the_profiles_host() {
        let mut p = Profile::default();
        let base = |p: &Profile, url: Option<&str>| endpoint(p, url).unwrap();
        assert_eq!(base(&p, None).base, "http://localhost:7243");
        p.address = Some("temporal-frontend.temporal:7233".into());
        assert_eq!(
            base(&p, None).base,
            "http://temporal-frontend.temporal:7243"
        );
        p.address = Some("[::1]:7233".into());
        assert_eq!(base(&p, None).base, "http://[::1]:7243");
        // TLS from the profile, or the address's scheme
        p.address = Some("temporal.example.com:7233".into());
        p.api_key = Some("k".into());
        assert_eq!(base(&p, None).base, "https://temporal.example.com:7243");
        p.api_key = None;
        p.address = Some("https://temporal.example.com:7233".into());
        assert_eq!(base(&p, None).base, "https://temporal.example.com:7243");
        // --http-url wins, path prefix and all
        assert_eq!(
            base(&p, Some("http://127.0.0.1:9000/temporal/")).base,
            "http://127.0.0.1:9000/temporal"
        );
        assert_eq!(
            base(&p, Some("https://gw.example")).base,
            "https://gw.example:443"
        );
        assert!(endpoint(&p, Some("gw.example:7243")).is_err());
    }

    #[test]
    fn a_server_name_is_verified_while_connecting_to_the_address() {
        let p = Profile {
            address: Some("127.0.0.1:7233".into()),
            tls: Some(Tls {
                server_name: Some("frontend.example.com".into()),
                ..Tls::default()
            }),
            ..Profile::default()
        };
        assert_eq!(
            endpoint(&p, None).unwrap(),
            Endpoint {
                base: "https://frontend.example.com:7243".into(),
                connect_to: Some("127.0.0.1:7243".into()),
            }
        );
        // without TLS the name doesn't matter
        let plain = Profile {
            tls: Some(Tls {
                disabled: Some(true),
                server_name: Some("frontend.example.com".into()),
                ..Tls::default()
            }),
            ..p
        };
        assert_eq!(endpoint(&plain, None).unwrap().connect_to, None);
    }

    #[test]
    fn the_query_takes_the_window_and_the_types() {
        let q = visibility_query(1_790_892_000.0, 1_790_892_900.0, &[], None).unwrap();
        assert_eq!(
            q,
            "StartTime >= '2026-10-01T22:00:00Z' AND StartTime < '2026-10-01T22:15:00Z'"
        );
        let q = visibility_query(
            0.0,
            60.0,
            &["A".into(), "B".into()],
            Some("TaskQueue = 'q'"),
        )
        .unwrap();
        assert!(
            q.ends_with(" AND WorkflowType IN ('A', 'B') AND (TaskQueue = 'q')"),
            "{q}"
        );
        assert!(visibility_query(0.0, 60.0, &["it's".into()], None).is_err());
    }

    #[test]
    fn samples_spread_over_the_listing() {
        assert_eq!(spread(10, 3).collect::<Vec<_>>(), [1, 5, 8]);
        assert_eq!(spread(3, 5).collect::<Vec<_>>(), [0, 1, 2]);
        assert_eq!(spread(1000, 300).count(), 300);
        assert_eq!(spread(0, 3).count(), 0);
    }

    #[test]
    fn payloads_and_failure_text_are_dropped() {
        let payload = json!({"metadata": {"encoding": "anNvbi9wbGFpbg=="}, "data": "c2VjcmV0"});
        let mut e = json!({
            "eventId": "5",
            "activityTaskScheduledEventAttributes": {
                "activityType": {"name": "Charge"},
                "input": {"payloads": [payload.clone()]},
                "header": {"fields": {"traceparent": payload.clone()}},
            },
            "activityTaskFailedEventAttributes": {
                "failure": {
                    "message": "card 4111 declined",
                    "stackTrace": "at charge()",
                    "applicationFailureInfo": {"type": "Declined", "nonRetryable": true,
                        "details": {"payloads": [payload.clone()]}},
                    "cause": {"message": "inner", "serverFailureInfo": {}},
                },
                "retryState": "RETRY_STATE_NON_RETRYABLE_FAILURE",
            },
        });
        strip(&mut e, false);
        let text = e.to_string();
        for gone in ["c2VjcmV0", "4111", "at charge()", "inner"] {
            assert!(!text.contains(gone), "{gone} in {text}");
        }
        for kept in [
            "Charge",
            "Declined",
            "nonRetryable",
            "RETRY_STATE_NON_RETRYABLE_FAILURE",
        ] {
            assert!(text.contains(kept), "{kept} not in {text}");
        }
        // in shorthand, the payloads' fields go whole
        let mut e = json!({"workflowExecutionStartedEventAttributes": {
            "workflowType": {"name": "Order"}, "input": [{"card": "4111"}], "memo": {"fields": {"k": "v"}}
        }});
        strip(&mut e, true);
        assert_eq!(
            e,
            json!({"workflowExecutionStartedEventAttributes": {"workflowType": {"name": "Order"}}})
        );
    }

    #[test]
    fn spans_read_in_days() {
        assert_eq!(fmt_span(2_592_000.0), "30 days");
        assert_eq!(fmt_span(86_400.0), "1 day");
        assert_eq!(fmt_span(530_000.0), "6.1 days");
        assert_eq!(fmt_span(5400.0), "1.5h");
    }

    #[test]
    fn statuses_read_by_their_short_names() {
        let s = |x: &str| status_name(x);
        assert_eq!(
            s("WORKFLOW_EXECUTION_STATUS_COMPLETED").as_deref(),
            Some("Completed")
        );
        assert_eq!(
            s("WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW").as_deref(),
            Some("ContinuedAsNew")
        );
        assert_eq!(
            s("WORKFLOW_EXECUTION_STATUS_TIMED_OUT").as_deref(),
            Some("TimedOut")
        );
        assert_eq!(s("Running").as_deref(), Some("Running"));
        assert_eq!(s("WORKFLOW_EXECUTION_STATUS_UNSPECIFIED"), None);
        assert_eq!(s(""), None);
    }

    #[test]
    fn arguments_passed_to_the_cli_are_named_without_their_values() {
        let args: Vec<String> = ["--env", "prod", "--api-key=s3cret", "-n", "x"]
            .map(String::from)
            .into();
        assert_eq!(flag_names(&args), ["--env", "--api-key", "-n"]);
    }

    #[test]
    fn cli_output_is_read_line_by_line() {
        let v = jsonl(b"{\"a\":1}\n\n{\"b\":2}\n").unwrap();
        assert_eq!(v, [json!({"a": 1}), json!({"b": 2})]);
        let err = jsonl(b"{\"a\":1}\nnot json\n").unwrap_err().to_string();
        assert!(err.contains("line 2"), "{err}");
        // cobra prints its usage after the error: the error is what matters
        let stderr = "Error: unknown flag: --nope\nUsage:\n  temporal workflow list [flags]\n";
        assert_eq!(brief(stderr), "Error: unknown flag: --nope");
        assert_eq!(brief("no such namespace\nmore"), "no such namespace");
    }

    #[test]
    fn a_types_shape_reads_on_one_line() {
        let mut l = Listed::new(4);
        l.started_by_slice = vec![0, 6, 30, 0];
        l.status = BTreeMap::from([("Completed".into(), 34), ("Failed".into(), 2)]);
        l.durations = vec![60.0; 36];
        // four minutes from 10:00
        let start = parse_time("2026-10-20T10:00:00Z").unwrap();
        assert_eq!(
            shape(&l, start, start + 240.0, 60.0),
            "started 10:01-10:02, busiest 10:02 at 0.500/s; 34 Completed, 2 Failed; ran p50 1.0m, p99 1.0m, max 1.0m"
        );
        // all in one slice
        l.started_by_slice = vec![0, 0, 36, 0];
        assert!(
            shape(&l, start, start + 240.0, 60.0).starts_with("started 10:02 at 0.600/s;"),
            "{}",
            shape(&l, start, start + 240.0, 60.0)
        );
    }

    #[test]
    fn path_segments_are_encoded() {
        assert_eq!(enc("order-1_a.b~"), "order-1_a.b~");
        assert_eq!(enc("a b/ç"), "a%20b%2F%C3%A7");
        assert_eq!(file_stem("0b9c-../x"), "0b9c-___x");
    }
}
