//! Workflow programs from traces: the scenario's `workflows:` entries.
//!
//! Executions of a type that took the same steps (the same path) are pooled: each step's
//! durations, attempts, retry policy and timeouts come from all of them. A path followed by at
//! least `min_path_share` of the executions becomes a workflow type of its own, with that share
//! of the start rate; rarer paths are folded into the most common one.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use crate::util::units::{fmt_rate, fmt_us};

use super::trace::{ActOutcome, Activity, Outcome, Step, Trace};

/// Options of an import.
#[derive(Clone, Debug)]
pub struct Options {
    /// namespace of the imported workflow types
    pub namespace: String,
    /// start rate of each top-level workflow type; `None` estimates it from the start times
    pub rate: Option<f64>,
    /// paths followed by at least this share of a type's executions become types of their own
    pub min_path_share: f64,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            namespace: "default".into(),
            rate: None,
            min_path_share: 0.05,
        }
    }
}

/// An import's result: the `workflows:` YAML and a readable summary.
#[derive(Clone, Debug)]
pub struct Program {
    pub yaml: String,
    pub summary: String,
}

/// Build the workflow programs of `traces`.
pub fn build(traces: &[Trace], opts: &Options) -> Program {
    let mut by_type: BTreeMap<&str, Vec<&Trace>> = BTreeMap::new();
    for t in traces {
        by_type.entry(t.workflow_type.as_str()).or_default().push(t);
    }
    // types started by other imported workflows get no start rate of their own
    let mut child_types: Vec<&str> = Vec::new();
    for t in traces {
        for s in &t.steps {
            if let Step::Children(c) = s {
                child_types.extend(c.iter().map(String::as_str));
            }
        }
    }
    let mut yaml = String::new();
    let mut summary = String::new();
    let (first, last) = traces.iter().fold((i64::MAX, i64::MIN), |(a, b), t| {
        (a.min(t.start_us), b.max(t.start_us))
    });
    let _ = writeln!(
        yaml,
        "# Imported by `tempdes workload import` from {} histories started over {}.\n\
         # Payloads were not read. Durations are each step's own time, without waits in the\n\
         # cluster. Add a worker fleet for every task queue named here.\nworkflows:",
        traces.len(),
        fmt_us((last - first).max(0) as f64)
    );
    let mut types: Vec<(&str, Vec<&Trace>)> = by_type.into_iter().collect();
    // top-level types first
    types.sort_by_key(|(name, v)| (is_child(name, v, &child_types), *name));
    let imported: Vec<&str> = types.iter().map(|(name, _)| *name).collect();
    for (name, runs) in types {
        let child = is_child(name, &runs, &child_types);
        workflow_type(name, &runs, child, opts, &mut yaml, &mut summary);
    }
    // a child whose own histories weren't exported still needs a type
    child_types.sort_unstable();
    child_types.dedup();
    for name in child_types.iter().filter(|c| !imported.contains(c)) {
        let task_queue = traces
            .iter()
            .find(|t| {
                t.steps
                    .iter()
                    .any(|s| matches!(s, Step::Children(c) if c.iter().any(|x| x == name)))
            })
            .map_or("", |t| t.task_queue.as_str());
        let _ = writeln!(
            yaml,
            "  # {name}: started as a child, but none of its histories were given; add them, or its steps\n  - type: {}\n    namespace: {}\n    task_queue: {}\n    steps: []",
            quote(name),
            quote(&opts.namespace),
            quote(task_queue)
        );
        let _ = writeln!(
            summary,
            "{name}: started as a child, but none of its histories were given: imported as a stub with no steps"
        );
    }
    Program { yaml, summary }
}

fn is_child(name: &str, runs: &[&Trace], child_types: &[&str]) -> bool {
    child_types.contains(&name) || runs.iter().all(|t| t.is_child)
}

/// The entries of one workflow type, one per kept path.
fn workflow_type(
    name: &str,
    runs: &[&Trace],
    child: bool,
    opts: &Options,
    yaml: &mut String,
    summary: &mut String,
) {
    let n = runs.len();
    // paths, most common first
    let mut paths: BTreeMap<Vec<String>, Vec<&Trace>> = BTreeMap::new();
    for t in runs {
        paths
            .entry(t.steps.iter().map(Step::signature).collect())
            .or_default()
            .push(t);
    }
    let mut paths: Vec<(Vec<String>, Vec<&Trace>)> = paths.into_iter().collect();
    paths.sort_by(|a, b| b.1.len().cmp(&a.1.len()).then_with(|| a.0.cmp(&b.0)));
    let kept = paths
        .iter()
        .take_while(|(_, v)| v.len() as f64 / n as f64 >= opts.min_path_share)
        .count()
        .max(1);
    let folded: usize = paths[kept..].iter().map(|(_, v)| v.len()).sum();

    // start rate
    let top: Vec<&&Trace> = runs.iter().filter(|t| !t.is_child).collect();
    let (rate, rate_note) = match opts.rate {
        Some(r) => (r, format!("{} (--rate)", fmt_rate(r))),
        None if top.len() >= 3 && start_span_s(&top) >= 1.0 => {
            let span_s = start_span_s(&top);
            let r = (top.len() - 1) as f64 / span_s;
            (
                r,
                format!(
                    "{} estimated from {} starts over {}: right only if the histories are every execution in that time; set --rate or calibrate with -o",
                    fmt_rate(r),
                    top.len(),
                    fmt_us(span_s * 1e6)
                ),
            )
        }
        None => (
            1.0,
            "1/s placeholder (too few histories to estimate it): set --rate".into(),
        ),
    };

    // summary
    let mut outcomes: BTreeMap<Outcome, usize> = BTreeMap::new();
    for t in runs {
        *outcomes.entry(t.outcome).or_default() += 1;
    }
    let _ = writeln!(
        summary,
        "{name}: {n} histories ({})",
        outcomes
            .iter()
            .map(|(o, c)| format!("{c} {}", format!("{o:?}").to_lowercase()))
            .collect::<Vec<_>>()
            .join(", ")
    );
    let mut run_times: Vec<f64> = runs
        .iter()
        .filter_map(|t| t.end_us.map(|e| (e - t.start_us).max(0) as f64))
        .collect();
    if !run_times.is_empty() {
        let _ = writeln!(
            summary,
            "  run time as recorded, waits in the cluster included: p50 {}, p99 {}",
            fmt_us(quantile(&mut run_times, 0.5)),
            fmt_us(quantile(&mut run_times, 0.99))
        );
    }
    let mut act_wait: Vec<f64> = runs
        .iter()
        .flat_map(|t| t.activity_queue_wait_us.iter().map(|&v| v as f64))
        .collect();
    let mut wft_wait: Vec<f64> = runs
        .iter()
        .flat_map(|t| t.wft_queue_wait_us.iter().map(|&v| v as f64))
        .collect();
    let mut waits = Vec::new();
    for (what, v) in [
        ("activity schedule-to-start", &mut act_wait),
        ("workflow task schedule-to-start", &mut wft_wait),
    ] {
        if !v.is_empty() {
            waits.push(format!(
                "{what} p50 {} p99 {}",
                fmt_us(quantile(v, 0.5)),
                fmt_us(quantile(v, 0.99))
            ));
        }
    }
    if !waits.is_empty() {
        let _ = writeln!(
            summary,
            "  waits in the cluster, left out of the workload: {}",
            waits.join("; ")
        );
    }
    if !child {
        let _ = writeln!(summary, "  start rate: {rate_note}");
    }
    let mut notes: BTreeMap<String, usize> = BTreeMap::new();
    let mut skipped: BTreeMap<String, u32> = BTreeMap::new();
    for t in runs {
        for note in &t.notes {
            *notes.entry(note.clone()).or_default() += 1;
        }
        for (k, v) in &t.skipped {
            *skipped.entry(k.clone()).or_default() += v;
        }
    }

    for (k, (sig, path_runs)) in paths.iter().take(kept).enumerate() {
        let type_name = if k == 0 {
            name.to_string()
        } else {
            format!("{name}~{}", k + 1)
        };
        let share = (path_runs.len() + if k == 0 { folded } else { 0 }) as f64 / n as f64;
        let _ = writeln!(
            summary,
            "  {} path{}: {} of {n} histories{} — {}",
            type_name,
            if k == 0 { " (most common)" } else { "" },
            path_runs.len(),
            if k == 0 && folded > 0 {
                format!(", plus {folded} on rarer paths folded in")
            } else {
                String::new()
            },
            if sig.is_empty() {
                "no steps".to_string()
            } else {
                sig.join(" → ")
            }
        );
        entry(
            &type_name,
            path_runs,
            (!child).then_some(rate * share),
            opts,
            yaml,
            &mut notes,
        );
    }
    for (note, count) in &notes {
        let _ = writeln!(summary, "  note: {note} ({count} histories)");
    }
    if !skipped.is_empty() {
        let _ = writeln!(
            summary,
            "  not modelled: {}",
            skipped
                .iter()
                .map(|(k, v)| format!("{v} {k}"))
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
}

/// One `workflows:` entry from the executions that took the same path.
fn entry(
    type_name: &str,
    runs: &[&Trace],
    rate: Option<f64>,
    opts: &Options,
    yaml: &mut String,
    notes: &mut BTreeMap<String, usize>,
) {
    let task_queue = most_common(runs.iter().map(|t| t.task_queue.clone())).unwrap_or_default();
    let _ = writeln!(yaml, "  - type: {}", quote(type_name));
    let _ = writeln!(yaml, "    namespace: {}", quote(&opts.namespace));
    let _ = writeln!(yaml, "    task_queue: {}", quote(&task_queue));
    if let Some(r) = rate {
        let _ = writeln!(yaml, "    start_rate: {}", rate_yaml(r));
    }
    let mut wft: Vec<f64> = runs
        .iter()
        .flat_map(|t| t.wft_processing_us.iter().map(|&v| v as f64))
        .collect();
    if let Some(d) = dist(&mut wft) {
        let _ = writeln!(yaml, "    wft_processing: {d}");
    }
    let steps = runs.first().map_or(0, |t| t.steps.len());
    if steps == 0 {
        let _ = writeln!(yaml, "    steps: []");
        return;
    }
    let _ = writeln!(yaml, "    steps:");
    for i in 0..steps {
        let at: Vec<&Step> = runs.iter().map(|t| &t.steps[i]).collect();
        match at[0] {
            Step::Activities(group) => {
                let acts: Vec<&Activity> = at
                    .iter()
                    .flat_map(|s| match s {
                        Step::Activities(a) => a.iter().collect::<Vec<_>>(),
                        _ => Vec::new(),
                    })
                    .collect();
                // did a workflow go on after an activity of this step failed for good?
                let (mut went_on, mut stopped) = (0, 0);
                for (t, s) in runs.iter().zip(&at) {
                    if let Step::Activities(a) = s
                        && a.iter().any(|x| failed(x.outcome))
                    {
                        if i + 1 < t.steps.len() || t.outcome == Outcome::Completed {
                            went_on += 1;
                        } else {
                            stopped += 1;
                        }
                    }
                }
                activity_step(
                    group,
                    &acts,
                    &task_queue,
                    went_on > stopped,
                    at.len(),
                    yaml,
                    notes,
                );
            }
            Step::LocalActivities { count, .. } => {
                let mut d: Vec<f64> = at
                    .iter()
                    .filter_map(|s| match s {
                        Step::LocalActivities {
                            per_activity_us, ..
                        } => Some(*per_activity_us as f64),
                        _ => None,
                    })
                    .collect();
                let _ = writeln!(
                    yaml,
                    "      - local_activity: {{ count: {count}, duration: {} }}",
                    dist(&mut d).unwrap_or_else(|| "1ms".into())
                );
            }
            Step::Timer { .. } => {
                let mut d: Vec<f64> = at
                    .iter()
                    .filter_map(|s| match s {
                        Step::Timer { duration_us } => Some(*duration_us as f64),
                        _ => None,
                    })
                    .collect();
                let _ = writeln!(
                    yaml,
                    "      - timer: {}",
                    dist(&mut d).unwrap_or_else(|| "1s".into())
                );
            }
            Step::Children(types) => {
                let child = most_common(types.iter().cloned()).unwrap_or_default();
                if types.iter().any(|t| *t != child) {
                    *notes
                        .entry("children of several types started together were imported as the most common type".into())
                        .or_default() += 1;
                }
                let _ = writeln!(
                    yaml,
                    "      - child_workflow: {{ workflow_type: {}, count: {} }}",
                    quote(&child),
                    types.len()
                );
            }
            Step::SignalWait { count, .. } => {
                let timeout = most_common(at.iter().map(|s| match s {
                    Step::SignalWait { timeout_us, .. } => *timeout_us,
                    _ => None,
                }))
                .flatten();
                let _ = match timeout {
                    Some(t) => writeln!(
                        yaml,
                        "      - wait_signal: {{ count: {count}, timeout: {} }}",
                        dur(t as f64)
                    ),
                    None => writeln!(yaml, "      - wait_signal: {{ count: {count} }}"),
                };
            }
        }
    }
}

fn failed(o: ActOutcome) -> bool {
    matches!(o, ActOutcome::Failed | ActOutcome::TimedOut)
}

/// An activity step pooled over its executions.
fn activity_step(
    group: &[Activity],
    acts: &[&Activity],
    workflow_tq: &str,
    continue_on_failure: bool,
    executions: usize,
    yaml: &mut String,
    notes: &mut BTreeMap<String, usize>,
) {
    let count = group.len();
    let mut types: Vec<&str> = group.iter().map(|a| a.activity_type.as_str()).collect();
    types.sort_unstable();
    types.dedup();
    let retried = acts.iter().filter(|a| a.attempts > 1).count();
    let _ = writeln!(
        yaml,
        "      # {}: {} activities in {executions} executions{}",
        types.join(", "),
        acts.len(),
        if retried > 0 {
            format!(", {} retried", pct(retried, acts.len()))
        } else {
            String::new()
        }
    );
    if types.len() > 1 {
        *notes
            .entry(
                "parallel activities of different types were pooled into one distribution".into(),
            )
            .or_default() += 1;
    }
    let _ = writeln!(yaml, "      - activity:");
    let _ = writeln!(yaml, "          count: {count}");
    if count > 1 {
        let _ = writeln!(yaml, "          parallel: true");
    }
    let mut runs: Vec<f64> = acts
        .iter()
        .filter_map(|a| a.run_us.map(|v| v as f64))
        .collect();
    let _ = writeln!(
        yaml,
        "          duration: {}",
        dist(&mut runs).unwrap_or_else(|| "1s".into())
    );
    // attempts: one that failed for good because its policy ran out needed more than it had
    let retry = most_common(acts.iter().map(|a| a.retry.map(RetryKey::from))).flatten();
    let mut plan: BTreeMap<u32, usize> = BTreeMap::new();
    let mut unexplained = 0;
    for a in acts {
        let exhausted = a
            .retry
            .is_some_and(|r| r.max_attempts > 0 && a.attempts >= r.max_attempts);
        let n = if failed(a.outcome) && exhausted {
            a.attempts + 1
        } else {
            if failed(a.outcome) {
                unexplained += 1;
            }
            a.attempts
        };
        *plan.entry(n).or_default() += 1;
    }
    if unexplained > 0 {
        *notes
            .entry("activities that failed without using up their retries (non-retryable errors, timeouts) were imported as succeeding on their last attempt".into())
            .or_default() += 1;
    }
    match plan.len() {
        1 if plan.contains_key(&1) => {}
        1 => {
            let _ = writeln!(yaml, "          attempts: {}", plan.keys().next().unwrap());
        }
        _ => {
            let total: usize = plan.values().sum();
            let shares: Vec<String> = plan
                .iter()
                .map(|(k, v)| format!("{k}: {}", num(*v as f64 / total as f64)))
                .collect();
            let _ = writeln!(yaml, "          attempts: {{ {} }}", shares.join(", "));
        }
    }
    if let Some(r) = retry {
        let _ = writeln!(
            yaml,
            "          retry_initial: {}",
            dur(r.initial_us as f64)
        );
        let _ = writeln!(
            yaml,
            "          backoff_coefficient: {}",
            num(r.coefficient_milli as f64 / 1000.0)
        );
        if r.max_interval_us > 0 {
            let _ = writeln!(
                yaml,
                "          max_interval: {}",
                dur(r.max_interval_us as f64)
            );
        }
        if r.max_attempts > 0 {
            let _ = writeln!(yaml, "          max_attempts: {}", r.max_attempts);
        }
    }
    let timeouts = most_common(acts.iter().map(|a| a.timeouts)).unwrap_or_default();
    for (key, v) in [
        ("schedule_to_start_timeout", timeouts.schedule_to_start),
        ("start_to_close_timeout", timeouts.start_to_close),
        ("schedule_to_close_timeout", timeouts.schedule_to_close),
        ("heartbeat_timeout", timeouts.heartbeat),
    ] {
        if v > 0 {
            let _ = writeln!(yaml, "          {key}: {}", dur(v as f64));
        }
    }
    if timeouts.heartbeat > 0 {
        // histories don't record heartbeats; the Go SDK sends at most one per 0.8 × the timeout
        let _ = writeln!(
            yaml,
            "          heartbeat: {}   # assumed: the SDK's throttle, 0.8 × heartbeat_timeout",
            dur(timeouts.heartbeat as f64 * 0.8)
        );
    }
    if continue_on_failure {
        let _ = writeln!(yaml, "          on_failure: continue");
    }
    let tq = most_common(acts.iter().map(|a| a.task_queue.clone())).unwrap_or_default();
    if !tq.is_empty() && tq != workflow_tq {
        let _ = writeln!(yaml, "          task_queue: {}", quote(&tq));
    }
}

/// A retry policy as a comparable key.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct RetryKey {
    initial_us: u64,
    coefficient_milli: u64,
    max_interval_us: u64,
    max_attempts: u32,
}

impl From<super::trace::Retry> for RetryKey {
    fn from(r: super::trace::Retry) -> Self {
        RetryKey {
            initial_us: r.initial_us,
            coefficient_milli: (r.coefficient * 1000.0).round().max(0.0) as u64,
            max_interval_us: r.max_interval_us,
            max_attempts: r.max_attempts,
        }
    }
}

/// The most common value (the smallest among ties, so output is deterministic).
fn most_common<T: Ord>(values: impl Iterator<Item = T>) -> Option<T> {
    let mut counts: BTreeMap<T, usize> = BTreeMap::new();
    for v in values {
        *counts.entry(v).or_default() += 1;
    }
    let max = counts.values().copied().max()?;
    counts.into_iter().find(|(_, c)| *c == max).map(|(v, _)| v)
}

/// The value at quantile `q` (sorts `v`); 0 when empty.
fn quantile(v: &mut [f64], q: f64) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    v.sort_by(f64::total_cmp);
    v[((v.len() - 1) as f64 * q).round() as usize]
}

/// A duration distribution for the scenario, from samples in microseconds: a constant when
/// they agree, `{ p50, p99 }` for a few, quantiles for many.
fn dist(v: &mut [f64]) -> Option<String> {
    if v.is_empty() {
        return None;
    }
    v.sort_by(f64::total_cmp);
    let (lo, hi) = (v[0].max(1.0), v[v.len() - 1].max(1.0));
    if hi <= lo * 1.001 || v.len() == 1 {
        return Some(dur(quantile(v, 0.5)));
    }
    if v.len() < 20 {
        let p50 = quantile(v, 0.5).max(1.0);
        return Some(if hi > p50 * 1.001 {
            format!("{{ p50: {}, p99: {} }}", dur(p50), dur(hi))
        } else {
            dur(p50)
        });
    }
    let mut points: Vec<(f64, f64)> = Vec::new();
    for q in [0.1, 0.5, 0.9, 0.99] {
        if q == 0.1 && v.len() < 50 {
            continue;
        }
        let x = quantile(v, q).max(1.0);
        // strictly increasing, or the shape is flat there
        if points.last().is_none_or(|&(_, last)| x > last * 1.001) {
            points.push((q, x));
        }
    }
    if points.len() < 2 {
        return Some(dur(quantile(v, 0.5)));
    }
    Some(format!(
        "{{ quantiles: {{ {} }} }}",
        points
            .iter()
            .map(|(q, x)| format!("{q}: {}", dur(*x)))
            .collect::<Vec<_>>()
            .join(", ")
    ))
}

/// A duration the scenario parser reads: `850us`, `12.5ms`, `3.2s`.
fn dur(us: f64) -> String {
    let us = us.max(1.0);
    if us >= 1e6 {
        format!("{}s", num(us / 1e6))
    } else if us >= 1e3 {
        format!("{}ms", num(us / 1e3))
    } else {
        format!("{}us", us.round())
    }
}

/// Seconds between the first and the last start.
fn start_span_s(runs: &[&&Trace]) -> f64 {
    let (a, b) = runs.iter().fold((i64::MAX, i64::MIN), |(a, b), t| {
        (a.min(t.start_us), b.max(t.start_us))
    });
    (b - a).max(0) as f64 / 1e6
}

/// A rate the scenario parser reads, in a unit that keeps its precision.
fn rate_yaml(per_s: f64) -> String {
    if per_s >= 1.0 {
        format!("{}/s", num(per_s))
    } else if per_s * 60.0 >= 1.0 {
        format!("{}/min", num(per_s * 60.0))
    } else {
        format!("{}/h", num(per_s * 3600.0))
    }
}

/// A number with at most three decimals and no trailing zeros.
fn num(x: f64) -> String {
    let s = format!("{x:.3}");
    let s = s.trim_end_matches('0').trim_end_matches('.');
    if s.is_empty() || s == "-" {
        "0".into()
    } else {
        s.to_string()
    }
}

fn pct(part: usize, whole: usize) -> String {
    format!("{:.0}%", 100.0 * part as f64 / whole.max(1) as f64)
}

/// A double-quoted YAML string.
fn quote(s: &str) -> String {
    format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\""))
}
