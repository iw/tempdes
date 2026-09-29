//! Workflow programs from traces: the scenario's `workflows:` entries.
//!
//! Executions of a type that took the same steps (the same path) are pooled: each step's
//! durations, attempts, retry policy and timeouts come from all of them. A path followed by at
//! least `min_path_share` of the executions becomes a workflow type of its own, with that share
//! of the start rate; rarer paths are folded into the most common one.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use crate::util::units::{fmt_rate, fmt_us};

use super::trace::{ActOutcome, Activity, Ending, Outcome, Retry, Step, TimeoutType, Trace};

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
            child_types.extend(s.children().iter().map(String::as_str));
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
                    .any(|s| s.children().iter().any(|x| x == name))
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
    let mut notes = Notes::new();
    let mut skipped: BTreeMap<String, u32> = BTreeMap::new();
    for t in runs {
        for text in &t.notes {
            note(&mut notes, text, Unit::Histories, 1);
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
    for ((text, unit), n) in &notes {
        let _ = writeln!(summary, "  note: {text} ({})", unit.count(*n));
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
    notes: &mut Notes,
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
    // the typical wait of a first attempt in its queue, for steps without one
    let mut waits: Vec<f64> = runs
        .iter()
        .flat_map(|t| t.activity_queue_wait_us.iter().map(|&v| v as f64))
        .collect();
    let type_wait = quantile(&mut waits, 0.5) as u64;
    let steps = runs.first().map_or(0, |t| t.steps.len());
    if steps == 0 {
        let _ = writeln!(yaml, "    steps: []");
        return;
    }
    let _ = writeln!(yaml, "    steps:");
    for i in 0..steps {
        let at: Vec<&Step> = runs.iter().map(|t| &t.steps[i]).collect();
        match at[0] {
            Step::Activities(_) | Step::Children(_) | Step::Parallel { .. } => {
                started_together(runs, &at, i, &task_queue, type_wait, yaml, notes);
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

/// Step `i` of `runs` (`at`), which started activities, child workflows or both, pooled over its
/// executions: an `activity` or `child_workflow` step when all it started is of one type, else a
/// `parallel` step with a member per type.
fn started_together(
    runs: &[&Trace],
    at: &[&Step],
    i: usize,
    workflow_tq: &str,
    type_wait: u64,
    yaml: &mut String,
    notes: &mut Notes,
) {
    let first = at[0];
    let mut activity_types: Vec<&str> = first
        .activities()
        .iter()
        .map(|a| a.activity_type.as_str())
        .collect();
    activity_types.sort_unstable();
    activity_types.dedup();
    let mut child_types: Vec<&str> = first.children().iter().map(String::as_str).collect();
    child_types.sort_unstable();
    child_types.dedup();
    let pre = if activity_types.len() + child_types.len() > 1 {
        let _ = writeln!(yaml, "      - parallel:");
        "          "
    } else {
        "      "
    };
    for ty in activity_types {
        let group: Vec<Activity> = first
            .activities()
            .iter()
            .filter(|a| a.activity_type == ty)
            .cloned()
            .collect();
        let acts: Vec<&Activity> = at
            .iter()
            .flat_map(|s| s.activities())
            .filter(|a| a.activity_type == ty)
            .collect();
        // did a workflow go on after an activity of this type failed for good?
        let (mut went_on, mut stopped) = (0, 0);
        for (t, s) in runs.iter().zip(at) {
            if s.activities()
                .iter()
                .any(|a| a.activity_type == ty && failed(a.outcome))
            {
                if i + 1 < t.steps.len() || t.outcome == Outcome::Completed {
                    went_on += 1;
                } else {
                    stopped += 1;
                }
            }
        }
        activity_step(
            &group,
            &acts,
            workflow_tq,
            went_on > stopped,
            at.len(),
            type_wait,
            pre,
            yaml,
            notes,
        );
    }
    for ty in child_types {
        let count = first.children().iter().filter(|c| *c == ty).count();
        let _ = writeln!(
            yaml,
            "{pre}- child_workflow: {{ workflow_type: {}, count: {count} }}",
            quote(ty)
        );
    }
}

/// What a summary note counts.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Unit {
    Histories,
    Activities,
}

impl Unit {
    fn count(self, n: usize) -> String {
        let (one, many) = match self {
            Unit::Histories => ("history", "histories"),
            Unit::Activities => ("activity", "activities"),
        };
        format!("{n} {}", if n == 1 { one } else { many })
    }
}

/// Summary notes by text and what they count.
type Notes = BTreeMap<(String, Unit), usize>;

fn note(notes: &mut Notes, text: &str, unit: Unit, n: usize) {
    *notes.entry((text.to_string(), unit)).or_default() += n;
}

/// The retry policy the server gives an activity that sets none
/// (`history.defaultActivityRetryPolicy`).
const DEFAULT_RETRY: Retry = Retry {
    initial_us: 1_000_000,
    coefficient: 2.0,
    max_interval_us: 100_000_000,
    max_attempts: 0,
};

/// The interval before the attempt after `attempt` (`nextBackoffInterval`,
/// `service/history/workflow/retry.go`): the initial interval grown by the coefficient per
/// attempt, up to the maximum interval (none when 0).
fn retry_interval_us(r: &Retry, attempt: u32) -> u64 {
    let v = r.initial_us as f64 * r.coefficient.powi(attempt.saturating_sub(1) as i32);
    match r.max_interval_us {
        0 => v.min(u64::MAX as f64) as u64,
        max => v.min(max as f64) as u64,
    }
}

/// An attempt that an activity whose attempts kept failing until its policy or its
/// schedule-to-close timeout stopped them can't reach: one past the policy's attempts, or the
/// first that the retry intervals alone would start after schedule-to-close.
fn beyond_reach(a: &Activity) -> u32 {
    let r = a.retry.unwrap_or(DEFAULT_RETRY);
    let mut n = a.attempts + 1;
    if r.max_attempts > 0 {
        n = n.max(r.max_attempts + 1);
    } else if a.timeouts.schedule_to_close > 0 {
        let (mut waited, mut k) = (0u64, 1u32);
        while waited <= a.timeouts.schedule_to_close && k < 10_000 {
            waited = waited.saturating_add(retry_interval_us(&r, k));
            k += 1;
        }
        n = n.max(k);
    }
    n
}

/// The run times of an activity's attempts before its last, which a history doesn't record,
/// estimated from `gap`, the time from the first schedule to the last attempt's start: less the
/// retry intervals of its policy and a typical queue wait (`wait`) per attempt, shared equally.
/// The one before the last ran to its start-to-close timeout when the last attempt's
/// `lastFailure` says it timed out.
fn earlier_attempts_us(a: &Activity, gap: u64, wait: u64) -> Vec<u64> {
    let mut left = a.attempts.saturating_sub(1);
    if left == 0 {
        return Vec::new();
    }
    let r = a.retry.unwrap_or(DEFAULT_RETRY);
    let intervals = (1..a.attempts)
        .map(|i| retry_interval_us(&r, i))
        .fold(0u64, u64::saturating_add);
    let waits = u64::from(a.attempts).saturating_mul(wait);
    let mut total = gap.saturating_sub(intervals.saturating_add(waits));
    let mut out = Vec::new();
    let stc = a.timeouts.start_to_close;
    if a.last_failure_timeout == Some(TimeoutType::StartToClose) && stc > 0 {
        out.push(stc);
        total = total.saturating_sub(stc);
        left -= 1;
    }
    if left > 0 {
        out.extend(std::iter::repeat_n(total / u64::from(left), left as usize));
    }
    out
}

/// An activity step pooled over its executions, written at indentation `pre`; `type_wait` is
/// the typical queue wait of the workflow type's first attempts.
#[allow(clippy::too_many_arguments)]
fn activity_step(
    group: &[Activity],
    acts: &[&Activity],
    workflow_tq: &str,
    continue_on_failure: bool,
    executions: usize,
    type_wait: u64,
    pre: &str,
    yaml: &mut String,
    notes: &mut Notes,
) {
    let count = group.len();
    let mut types: Vec<&str> = group.iter().map(|a| a.activity_type.as_str()).collect();
    types.sort_unstable();
    types.dedup();
    let retried = acts.iter().filter(|a| a.attempts > 1).count();
    let failed_for_good = acts
        .iter()
        .filter(|a| matches!(a.ending(), Ending::NonRetryable | Ending::RanOut))
        .count();
    let mut shares = String::new();
    for (n, what) in [(retried, "retried"), (failed_for_good, "failed for good")] {
        if n > 0 {
            let _ = write!(shares, ", {} {what}", pct(n, acts.len()));
        }
    }
    let _ = writeln!(
        yaml,
        "{pre}# {}: {} activities in {executions} executions{shares}",
        types.join(", "),
        acts.len(),
    );
    let _ = writeln!(yaml, "{pre}- activity:");
    let _ = writeln!(yaml, "{pre}    count: {count}");
    if count > 1 {
        let _ = writeln!(yaml, "{pre}    parallel: true");
    }
    // plans: the attempt that succeeds, or fails with a non-retryable error; one whose attempts
    // kept failing until its policy or schedule-to-close stopped them needs one it can't reach
    let mut succeeds: BTreeMap<u32, usize> = BTreeMap::new();
    let mut rejected: BTreeMap<u32, usize> = BTreeMap::new();
    // own run times: final attempts that succeeded; failed attempts, the final one when it
    // failed and the ones before it estimated
    let (mut runs, mut failed_runs): (Vec<f64>, Vec<f64>) = (Vec::new(), Vec::new());
    let mut first_waits: Vec<f64> = acts
        .iter()
        .filter(|a| a.attempts == 1)
        .filter_map(|a| a.start_gap_us().map(|v| v as f64))
        .collect();
    let wait = if first_waits.is_empty() {
        type_wait
    } else {
        quantile(&mut first_waits, 0.5) as u64
    };
    let (mut queue_timeouts, mut estimated, mut no_state) = (0, 0, 0);
    for a in acts {
        match a.ending() {
            Ending::Completed => {
                *succeeds.entry(a.attempts).or_default() += 1;
                runs.extend(a.run_us.map(|v| v as f64));
            }
            Ending::NonRetryable => {
                *rejected.entry(a.attempts).or_default() += 1;
                failed_runs.extend(a.run_us.map(|v| v as f64));
            }
            Ending::RanOut => {
                *succeeds.entry(beyond_reach(a)).or_default() += 1;
                failed_runs.extend(a.run_us.map(|v| v as f64));
            }
            Ending::QueueTimeout => {
                queue_timeouts += 1;
                continue;
            }
        }
        if failed(a.outcome) && a.retry_state.is_none() {
            no_state += 1;
        }
        if a.attempts > 1
            && let Some(gap) = a.start_gap_us()
        {
            failed_runs.extend(
                earlier_attempts_us(a, gap, wait)
                    .into_iter()
                    .map(|v| v as f64),
            );
            estimated += 1;
        }
    }
    for (n, text) in [
        (
            queue_timeouts,
            "activities that timed out waiting in a task queue (schedule-to-start) were left out of the attempt plans: that queueing is the recorded cluster's",
        ),
        (
            estimated,
            "failed attempts' durations were estimated from the time between scheduling and the last attempt's start, less the retry intervals and typical queue waits",
        ),
        (
            no_state,
            "failed activities with no recorded retry state were judged by their failure and retry policy",
        ),
    ] {
        if n > 0 {
            note(notes, text, Unit::Activities, n);
        }
    }
    let failed_dist = dist(&mut failed_runs);
    let _ = writeln!(
        yaml,
        "{pre}    duration: {}",
        dist(&mut runs)
            .or_else(|| failed_dist.clone())
            .unwrap_or_else(|| "1s".into())
    );
    if let Some(d) = failed_dist {
        let _ = writeln!(yaml, "{pre}    failed_duration: {d}");
    }
    let planned = succeeds.values().sum::<usize>() + rejected.values().sum::<usize>();
    let shares_of = |m: &BTreeMap<u32, usize>| {
        m.iter()
            .map(|(k, v)| format!("{k}: {}", share(*v as f64 / planned as f64)))
            .collect::<Vec<_>>()
            .join(", ")
    };
    // one count applies to every activity the non-retryable shares leave
    match succeeds.len() {
        0 => {}
        1 => {
            let k = *succeeds.keys().next().unwrap();
            if k > 1 {
                let _ = writeln!(yaml, "{pre}    attempts: {k}");
            }
        }
        _ => {
            let _ = writeln!(yaml, "{pre}    attempts: {{ {} }}", shares_of(&succeeds));
        }
    }
    if !rejected.is_empty() {
        let _ = writeln!(
            yaml,
            "{pre}    non_retryable: {{ {} }}",
            shares_of(&rejected)
        );
    }
    let retry = most_common(acts.iter().map(|a| a.retry.map(RetryKey::from))).flatten();
    if let Some(r) = retry {
        let _ = writeln!(yaml, "{pre}    retry_initial: {}", dur(r.initial_us as f64));
        let _ = writeln!(
            yaml,
            "{pre}    backoff_coefficient: {}",
            num(r.coefficient_milli as f64 / 1000.0)
        );
        if r.max_interval_us > 0 {
            let _ = writeln!(
                yaml,
                "{pre}    max_interval: {}",
                dur(r.max_interval_us as f64)
            );
        }
        if r.max_attempts > 0 {
            let _ = writeln!(yaml, "{pre}    max_attempts: {}", r.max_attempts);
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
            let _ = writeln!(yaml, "{pre}    {key}: {}", dur(v as f64));
        }
    }
    if timeouts.heartbeat > 0 {
        // histories don't record heartbeats; the Go SDK sends at most one per 0.8 × the timeout
        let _ = writeln!(
            yaml,
            "{pre}    heartbeat: {}   # assumed: the SDK's throttle, 0.8 × heartbeat_timeout",
            dur(timeouts.heartbeat as f64 * 0.8)
        );
    }
    if continue_on_failure {
        let _ = writeln!(yaml, "{pre}    on_failure: continue");
    }
    let tq = most_common(acts.iter().map(|a| a.task_queue.clone())).unwrap_or_default();
    if !tq.is_empty() && tq != workflow_tq {
        let _ = writeln!(yaml, "{pre}    task_queue: {}", quote(&tq));
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

/// A share with three significant digits, so small ones aren't rounded to zero.
fn share(x: f64) -> String {
    if x <= 0.0 {
        return "0".into();
    }
    let decimals = (2 - x.log10().floor() as i32).max(0) as usize;
    let s = format!("{x:.decimals$}");
    let s = s.trim_end_matches('0').trim_end_matches('.');
    s.to_string()
}

fn pct(part: usize, whole: usize) -> String {
    format!("{:.0}%", 100.0 * part as f64 / whole.max(1) as f64)
}

/// A double-quoted YAML string.
fn quote(s: &str) -> String {
    format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\""))
}
