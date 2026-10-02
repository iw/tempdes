//! Terminal rendering of a run result.

use std::fmt::Write as _;
use std::io::IsTerminal;

use super::*;
use crate::util::units::{fmt_pct, fmt_rate, fmt_us};

pub struct Style {
    on: bool,
}

impl Default for Style {
    fn default() -> Self {
        Self::new()
    }
}

impl Style {
    pub fn new() -> Self {
        Style {
            on: std::io::stdout().is_terminal() && std::env::var_os("NO_COLOR").is_none(),
        }
    }
    fn wrap(&self, code: &str, s: &str) -> String {
        if self.on {
            format!("\x1b[{code}m{s}\x1b[0m")
        } else {
            s.to_string()
        }
    }
    fn bold(&self, s: &str) -> String {
        self.wrap("1", s)
    }
    fn dim(&self, s: &str) -> String {
        self.wrap("2", s)
    }
    fn sev(&self, s: Severity) -> String {
        match s {
            Severity::Critical => self.wrap("1;31", "CRITICAL"),
            Severity::Warning => self.wrap("1;33", "WARNING "),
            Severity::Info => self.wrap("1;36", "INFO    "),
        }
    }
}

fn ms(v: f64) -> String {
    fmt_us(v * 1e3)
}

/// Simple aligned table.
pub struct Table {
    head: Vec<String>,
    rows: Vec<Vec<String>>,
    right: Vec<bool>,
}

impl Table {
    pub fn new(head: &[&str]) -> Self {
        Table {
            head: head.iter().map(|s| s.to_string()).collect(),
            rows: Vec::new(),
            right: head.iter().enumerate().map(|(i, _)| i > 0).collect(),
        }
    }

    pub fn row(&mut self, cells: Vec<String>) {
        self.rows.push(cells);
    }

    pub fn render(&self, st: &Style, indent: usize) -> String {
        let n = self.head.len();
        let mut w = vec![0usize; n];
        for (i, h) in self.head.iter().enumerate() {
            w[i] = h.chars().count();
        }
        for r in &self.rows {
            for (i, c) in r.iter().enumerate().take(n) {
                w[i] = w[i].max(c.chars().count());
            }
        }
        let pad = " ".repeat(indent);
        #[allow(clippy::needless_range_loop)]
        let fmt_row = |cells: &[String]| -> String {
            let mut s = pad.clone();
            for i in 0..n {
                let c = cells.get(i).map(String::as_str).unwrap_or("");
                let len = c.chars().count();
                let fill = " ".repeat(w[i].saturating_sub(len));
                if self.right[i] {
                    s.push_str(&fill);
                    s.push_str(c);
                } else {
                    s.push_str(c);
                    s.push_str(&fill);
                }
                if i + 1 < n {
                    s.push_str("  ");
                }
            }
            s.trim_end().to_string()
        };
        let mut out = String::new();
        out.push_str(&st.dim(&fmt_row(&self.head)));
        out.push('\n');
        for r in &self.rows {
            out.push_str(&fmt_row(r));
            out.push('\n');
        }
        out
    }
}

fn wrap_text(s: &str, width: usize, indent: usize) -> String {
    let pad = " ".repeat(indent);
    let mut out = String::new();
    let mut line = String::new();
    for word in s.split_whitespace() {
        if line.chars().count() + word.chars().count() + 1 > width && !line.is_empty() {
            out.push_str(&pad);
            out.push_str(&line);
            out.push('\n');
            line.clear();
        }
        if !line.is_empty() {
            line.push(' ');
        }
        line.push_str(word);
    }
    if !line.is_empty() {
        out.push_str(&pad);
        out.push_str(&line);
        out.push('\n');
    }
    out
}

pub fn render(r: &RunResult, verbose: bool) -> String {
    let st = Style::new();
    let mut o = String::new();
    let rep = &r.config.replicas;
    let _ = writeln!(
        o,
        "\n{}  {}",
        st.bold(&format!(
            "tempdes · Temporal {} · {}",
            r.temporal_version, r.scenario
        )),
        st.dim(&r.label)
    );
    let _ = writeln!(
        o,
        "  replicas frontend={} history={} matching={} worker={} · {} shards · {} (capacity {}) · client LB {} · simulated {:.0}s after {:.0}s warm-up in {:.1}s",
        rep.get("frontend").unwrap_or(&0),
        rep.get("history").unwrap_or(&0),
        rep.get("matching").unwrap_or(&0),
        rep.get("worker").unwrap_or(&0),
        r.config.num_history_shards,
        r.persistence.store,
        r.persistence.capacity,
        r.config.client_lb,
        r.duration_s,
        r.warmup_s,
        r.wall_ms as f64 / 1000.0
    );
    let _ = writeln!(
        o,
        "\n{}",
        st.bold(wrap_text(&r.headline, 110, 2).trim_end())
    );

    // --- hotspots -----------------------------------------------------------------------------
    let _ = writeln!(o, "\n{}", st.bold("HOTSPOTS"));
    if r.hotspots.is_empty() {
        let _ = writeln!(o, "  none detected at the configured thresholds");
    }
    let limit = if verbose { usize::MAX } else { 12 };
    for (i, h) in r.hotspots.iter().take(limit).enumerate() {
        let _ = writeln!(
            o,
            "\n  {} {}  {}",
            st.sev(h.severity),
            st.dim(&format!("#{:<2} {}", i + 1, h.category)),
            st.bold(&h.title)
        );
        o.push_str(&wrap_text(&h.detail, 104, 13));
        for e in &h.evidence {
            let _ = writeln!(o, "             · {e}");
        }
        if !h.metrics.is_empty() {
            let _ = writeln!(
                o,
                "             {} {}",
                st.dim("watch:"),
                h.metrics.join(", ")
            );
        }
        for k in &h.knobs {
            let _ = writeln!(
                o,
                "             {} {} = {}  {}",
                st.dim("knob:"),
                k.key,
                k.current,
                st.dim(&format!("— {}", k.hint))
            );
        }
    }
    if r.hotspots.len() > limit {
        let _ = writeln!(o, "\n  … {} more (use --verbose)", r.hotspots.len() - limit);
    }

    // --- workflows ----------------------------------------------------------------------------
    let _ = writeln!(o, "\n{}", st.bold("WORKFLOWS"));
    let mut t = Table::new(&[
        "type",
        "offered",
        "started",
        "completed",
        "failed",
        "e2e p50",
        "e2e p99",
        "WFT s2s p99",
        "act s2s p99",
        "sticky hit",
        "WFT t/o",
    ]);
    for w in &r.workflows {
        if w.started_per_s == 0.0 && w.offered_start_rate == 0.0 && w.completed_per_s == 0.0 {
            continue;
        }
        t.row(vec![
            w.workflow_type.clone(),
            fmt_rate(w.offered_start_rate),
            fmt_rate(w.started_per_s),
            fmt_rate(w.completed_per_s),
            fmt_rate(w.failed_per_s),
            ms(w.e2e.p50_ms),
            ms(w.e2e.p99_ms),
            ms(w.wft_schedule_to_start.p99_ms),
            ms(w.activity_schedule_to_start.p99_ms),
            fmt_pct(w.sticky_hit_ratio),
            w.wft_timeouts.to_string(),
        ]);
    }
    o.push_str(&t.render(&st, 2));
    for line in r.workflows.iter().filter_map(|w| w.with_start_summary()) {
        let _ = writeln!(o, "  {line}");
    }

    // --- APIs ---------------------------------------------------------------------------------
    let _ = writeln!(
        o,
        "\n{}",
        st.bold("CLIENT-OBSERVED API LATENCY (incl. SDK retries)")
    );
    let mut t = Table::new(&["api", "rate", "p50", "p95", "p99", "errors"]);
    for a in &r.apis {
        t.row(vec![
            a.api.clone(),
            fmt_rate(a.per_s),
            ms(a.latency.p50_ms),
            ms(a.latency.p95_ms),
            ms(a.latency.p99_ms),
            if a.errors.is_empty() {
                "-".into()
            } else {
                a.errors
                    .iter()
                    .map(|(k, v)| format!("{k}:{v}"))
                    .collect::<Vec<_>>()
                    .join(" ")
            },
        ]);
    }
    o.push_str(&t.render(&st, 2));

    // --- pods ---------------------------------------------------------------------------------
    let _ = writeln!(o, "\n{}", st.bold("PODS"));
    let mut t = Table::new(&[
        "pod",
        "cpu",
        "cpu wait p99",
        "req/s",
        "owns",
        "persist/s",
        "db pool",
        "pool wait p99",
        "cache hit",
        "top limit",
        "rejected",
    ]);
    for s in &r.services {
        for p in &s.pods {
            if !p.alive && !verbose {
                continue;
            }
            let owns = match s.service.as_str() {
                "history" => format!("{} shards", p.owned),
                "matching" => format!("{} parts", p.owned),
                "frontend" if r.config.client_lb == "proxy" => "via proxy".to_string(),
                "frontend" => format!("{} conns", p.owned),
                _ => format!("{} ns-workers", p.owned),
            };
            t.row(vec![
                format!("{}{}", p.name, if p.alive { "" } else { " (removed)" }),
                fmt_pct(p.cpu_util),
                ms(p.cpu_wait_p99_ms),
                fmt_rate(p.requests_per_s),
                owns,
                fmt_rate(p.persistence_per_s),
                if p.db_pool_size >= 10_000 {
                    "-".into()
                } else {
                    format!("{}/{}", fmt_pct(p.db_pool_util), p.db_pool_size)
                },
                ms(p.db_pool_wait_p99_ms),
                p.cache_hit_ratio.map(fmt_pct).unwrap_or_else(|| "-".into()),
                p.top_limit()
                    .filter(|(_, u)| *u >= 0.005)
                    .map(|(n, u)| format!("{n} {}", fmt_pct(u)))
                    .unwrap_or_else(|| "-".into()),
                p.rejections.to_string(),
            ]);
        }
    }
    o.push_str(&t.render(&st, 2));

    // --- persistence --------------------------------------------------------------------------
    let _ = writeln!(
        o,
        "\n{}  {} busy, queue wait p99 {}",
        st.bold("DATABASE"),
        fmt_pct(r.persistence.utilization),
        ms(r.persistence.queue_wait.p99_ms)
    );
    let mut t = Table::new(&["operation", "rate", "p50", "p99", "rejected"]);
    for op in r.persistence.ops.iter().take(if verbose { 64 } else { 10 }) {
        t.row(vec![
            op.op.clone(),
            fmt_rate(op.per_s),
            ms(op.latency.p50_ms),
            ms(op.latency.p99_ms),
            op.rejected.to_string(),
        ]);
    }
    o.push_str(&t.render(&st, 2));

    // --- history ------------------------------------------------------------------------------
    let h = &r.history;
    let _ = writeln!(
        o,
        "\n{}  shard IO busy p50 {} / p90 {} / max {} (concurrency {}) · write skew {:.1}x · lock wait p99 {} · busy-workflow timeouts {} · MS cache hit {}",
        st.bold("HISTORY"),
        fmt_pct(h.shard_util_p50),
        fmt_pct(h.shard_util_p90),
        fmt_pct(h.shard_util_max),
        h.shard_io_concurrency,
        h.shard_writes_max_over_mean,
        ms(h.lock_wait.p99_ms),
        h.lock_timeouts,
        fmt_pct(h.cache_hit_ratio)
    );
    let ts = &h.task_scheduler;
    if ts.mode != "off" {
        let _ = writeln!(
            o,
            "  task scheduler limiter {} · pod limit {:.0}–{:.0} tasks/s · {:.0}/s {} ({:.2} per task run)",
            if ts.mode == "shadow" {
                "in shadow mode (counts only)"
            } else {
                "on"
            },
            ts.pod_qps_min,
            ts.pod_qps_max,
            ts.throttled_per_s,
            if ts.mode == "shadow" {
                "would be held back"
            } else {
                "held back"
            },
            ts.throttled_per_task
        );
    }
    let mut t = Table::new(&[
        "hot shard",
        "owner",
        "IO busy",
        "IO wait p99",
        "writes/s",
        "queued tasks",
    ]);
    for s in h.hot_shards.iter().take(if verbose { 20 } else { 5 }) {
        t.row(vec![
            s.shard.to_string(),
            s.owner.clone(),
            fmt_pct(s.io_util),
            ms(s.io_wait_p99_ms),
            fmt_rate(s.writes_per_s),
            s.pending_tasks.to_string(),
        ]);
    }
    o.push_str(&t.render(&st, 2));
    if !h.hot_workflows.is_empty() && h.hot_workflows[0].util > 0.05 {
        let mut t = Table::new(&["hot workflow lock", "shard", "busy", "wait p99", "timeouts"]);
        for l in h.hot_workflows.iter().take(5) {
            t.row(vec![
                l.workflow.clone(),
                l.shard.to_string(),
                fmt_pct(l.util),
                ms(l.wait_p99_ms),
                l.timeouts.to_string(),
            ]);
        }
        o.push_str(&t.render(&st, 2));
    }
    let mut t = Table::new(&[
        "history task",
        "rate",
        "no-op",
        "load p99",
        "sched p99",
        "proc p99",
        "e2e p99",
        "attempts",
        "retries busy/thr",
    ]);
    for tk in &h.tasks {
        t.row(vec![
            tk.task_type.clone(),
            fmt_rate(tk.per_s),
            fmt_pct(tk.noop_fraction),
            ms(tk.load.p99_ms),
            ms(tk.schedule.p99_ms),
            ms(tk.processing.p99_ms),
            ms(tk.queue.p99_ms),
            format!("{:.2}", tk.mean_attempts),
            format!("{}/{}", tk.busy_workflow_retries, tk.throttled_retries),
        ]);
    }
    o.push_str(&t.render(&st, 2));

    // --- matching -----------------------------------------------------------------------------
    let m = &r.matching;
    let _ = writeln!(
        o,
        "\n{}  sync match {} · backlog mean {:.0} (max {:.0})",
        st.bold("MATCHING"),
        fmt_pct(m.sync_match_ratio),
        m.backlog_total_mean,
        m.backlog_total_max
    );
    let mut parts: Vec<&PartitionResult> = m
        .partitions
        .iter()
        .filter(|p| verbose || !p.partition.starts_with("sticky"))
        .collect();
    parts.sort_by(|a, b| {
        (b.adds_per_s + b.backlog_mean * 100.0)
            .partial_cmp(&(a.adds_per_s + a.backlog_mean * 100.0))
            .unwrap()
    });
    let mut t = Table::new(&[
        "task queue",
        "type",
        "part",
        "host",
        "adds/s",
        "polls/s",
        "sync",
        "backlog",
        "pollers",
        "dispatch p99",
        "fwd t/p",
    ]);
    for p in parts.iter().take(if verbose { 64 } else { 12 }) {
        t.row(vec![
            p.task_queue.clone(),
            p.kind.clone(),
            p.partition.clone(),
            p.host.clone(),
            fmt_rate(p.adds_per_s),
            fmt_rate(p.polls_per_s),
            fmt_pct(p.sync_match_ratio),
            format!("{:.0}", p.backlog_mean),
            format!("{:.1}", p.pollers_mean),
            ms(p.task_wait.p99_ms),
            format!("{}/{}", p.forwarded_tasks, p.forwarded_polls),
        ]);
    }
    o.push_str(&t.render(&st, 2));

    // --- limits -------------------------------------------------------------------------------
    if !r.limits.is_empty() {
        let _ = writeln!(o, "\n{}", st.bold("RATE LIMIT REJECTIONS"));
        let mut t = Table::new(&["limiter", "where", "rejected", "rate"]);
        for l in r.limits.iter().take(if verbose { 100 } else { 12 }) {
            t.row(vec![
                l.limiter.clone(),
                l.place.clone(),
                l.rejected.to_string(),
                fmt_rate(l.per_s),
            ]);
        }
        o.push_str(&t.render(&st, 2));
    }

    if let Some(s) = &r.schedules {
        let _ = writeln!(
            o,
            "\n{}  {} actions/s · delay p50 {} p99 {} · rate-limited {}",
            st.bold("SCHEDULES"),
            fmt_rate(s.actions_per_s),
            ms(s.action_delay.p50_ms),
            ms(s.action_delay.p99_ms),
            s.rate_limited
        );
    }

    if !r.validation.is_empty() {
        let _ = writeln!(o, "\n{}", st.bold("VALIDATION AGAINST OBSERVED METRICS"));
        let mut t = Table::new(&["metric", "observed", "simulated", "sim/obs"]);
        for v in &r.validation {
            let f = |x: f64| {
                if v.unit == "/s" {
                    fmt_rate(x)
                } else if v.unit == "ms" {
                    ms(x)
                } else if v.unit.is_empty() {
                    format!("{x:.3}")
                } else {
                    format!("{x:.2} {}", v.unit)
                }
            };
            t.row(vec![
                v.metric.clone(),
                f(v.observed),
                f(v.simulated),
                format!("{:.2}", v.ratio),
            ]);
        }
        o.push_str(&t.render(&st, 2));
    }

    if !r.warnings.is_empty() {
        let _ = writeln!(o, "\n{}", st.bold("CONFIGURATION WARNINGS"));
        for w in &r.warnings {
            o.push_str(&wrap_text(&format!("! {w}"), 110, 2));
        }
    }
    let shown_notes: Vec<&String> = r
        .notes
        .iter()
        .filter(|n| verbose || !n.starts_with("dynamic config ") || n.contains("->"))
        .collect();
    if !shown_notes.is_empty() {
        let _ = writeln!(o, "\n{}", st.bold("NOTES"));
        for n in shown_notes {
            o.push_str(&wrap_text(&format!("· {n}"), 110, 2));
        }
    }
    o.push('\n');
    o
}
