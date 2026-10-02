//! Markdown rendering of run and sweep results, with GitHub-flavoured tables, for pull requests,
//! issues and docs. The content follows the text report section by section.

use std::fmt::Write as _;

use super::*;
use crate::sweep::SweepResult;
use crate::util::units::{fmt_pct, fmt_rate, fmt_us};

fn ms(v: f64) -> String {
    fmt_us(v * 1e3)
}

/// Text for a table cell: pipes escaped and line breaks flattened, so the row stays intact.
fn cell(s: &str) -> String {
    s.replace('|', "\\|").replace(['\r', '\n'], " ")
}

/// Inline code, for metric names and settings.
fn code(s: &str) -> String {
    if s.contains('`') {
        format!("`` {s} ``")
    } else {
        format!("`{s}`")
    }
}

/// A GitHub-flavoured table. The first `text_cols` columns are left-aligned and the rest,
/// numbers, right-aligned.
fn table(head: &[&str], rows: &[Vec<String>], text_cols: usize) -> String {
    let mut o = String::from("|");
    for h in head {
        let _ = write!(o, " {} |", cell(h));
    }
    o.push_str("\n|");
    for i in 0..head.len() {
        o.push_str(if i < text_cols { " --- |" } else { " ---: |" });
    }
    o.push('\n');
    for r in rows {
        o.push('|');
        for i in 0..head.len() {
            let _ = write!(o, " {} |", cell(r.get(i).map_or("", String::as_str)));
        }
        o.push('\n');
    }
    o.push('\n');
    o
}

fn severity(s: Severity) -> &'static str {
    match s {
        Severity::Critical => "Critical",
        Severity::Warning => "Warning",
        Severity::Info => "Info",
    }
}

/// A run's report. `verbose` shows what the text report's `--verbose` shows: every hotspot,
/// removed pods, sticky partitions and longer tables.
pub fn render_run(r: &RunResult, verbose: bool) -> String {
    let mut o = String::new();
    let rep = &r.config.replicas;
    let _ = writeln!(o, "# tempdes: {}\n", r.scenario);
    let _ = writeln!(
        o,
        "Temporal {}{}. Replicas: frontend {}, history {}, matching {}, worker {}. {} history shards, {} (capacity {}), client LB {}. {:.0}s measured after {:.0}s of warm-up, simulated in {:.1}s.\n",
        r.temporal_version,
        if r.label.is_empty() {
            String::new()
        } else {
            format!(", overrides {}", code(&r.label))
        },
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
    let _ = writeln!(o, "**{}**\n", r.headline.trim());

    // --- hotspots -----------------------------------------------------------------------------
    o.push_str("## Hotspots\n\n");
    if r.hotspots.is_empty() {
        o.push_str("None detected at the configured thresholds.\n\n");
    }
    let limit = if verbose { usize::MAX } else { 12 };
    for (i, h) in r.hotspots.iter().take(limit).enumerate() {
        let _ = writeln!(
            o,
            "### {}. {} · {} · {}\n",
            i + 1,
            severity(h.severity),
            h.category,
            h.title
        );
        let _ = writeln!(o, "{}\n", h.detail.trim());
        for e in &h.evidence {
            let _ = writeln!(o, "- {e}");
        }
        if !h.evidence.is_empty() {
            o.push('\n');
        }
        if !h.metrics.is_empty() {
            let watch: Vec<String> = h.metrics.iter().map(|m| code(m)).collect();
            let _ = writeln!(o, "**Watch:** {}\n", watch.join(", "));
        }
        if !h.knobs.is_empty() {
            o.push_str("**Settings:**\n\n");
            for k in &h.knobs {
                let _ = writeln!(o, "- {} = {}: {}", code(&k.key), k.current, k.hint);
            }
            o.push('\n');
        }
    }
    if r.hotspots.len() > limit {
        let _ = writeln!(
            o,
            "… {} more (use `--verbose`).\n",
            r.hotspots.len() - limit
        );
    }

    // --- workflows ----------------------------------------------------------------------------
    o.push_str("## Workflows\n\n");
    let rows: Vec<Vec<String>> = r
        .workflows
        .iter()
        .filter(|w| {
            w.started_per_s != 0.0 || w.offered_start_rate != 0.0 || w.completed_per_s != 0.0
        })
        .map(|w| {
            vec![
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
            ]
        })
        .collect();
    o.push_str(&table(
        &[
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
            "WFT timeouts",
        ],
        &rows,
        1,
    ));
    let with_start: Vec<String> = r
        .workflows
        .iter()
        .filter_map(|w| w.with_start_summary())
        .collect();
    if !with_start.is_empty() {
        for line in with_start {
            let _ = writeln!(o, "- {line}");
        }
        o.push('\n');
    }

    // --- APIs ---------------------------------------------------------------------------------
    o.push_str("## API latency (client-observed, including SDK retries)\n\n");
    let rows: Vec<Vec<String>> = r
        .apis
        .iter()
        .map(|a| {
            vec![
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
                        .map(|(k, v)| format!("{k}: {v}"))
                        .collect::<Vec<_>>()
                        .join(", ")
                },
            ]
        })
        .collect();
    o.push_str(&table(
        &["API", "rate", "p50", "p95", "p99", "errors"],
        &rows,
        1,
    ));

    // --- pods ---------------------------------------------------------------------------------
    o.push_str("## Pods\n\n");
    let mut rows = Vec::new();
    for s in &r.services {
        for p in &s.pods {
            if !p.alive && !verbose {
                continue;
            }
            let owns = match s.service.as_str() {
                "history" => format!("{} shards", p.owned),
                "matching" => format!("{} partitions", p.owned),
                "frontend" if r.config.client_lb == "proxy" => "via proxy".to_string(),
                "frontend" => format!("{} connections", p.owned),
                _ => format!("{} namespace workers", p.owned),
            };
            rows.push(vec![
                format!("{}{}", p.name, if p.alive { "" } else { " (removed)" }),
                fmt_pct(p.cpu_util),
                ms(p.cpu_wait_p99_ms),
                fmt_rate(p.requests_per_s),
                owns,
                fmt_rate(p.persistence_per_s),
                if p.db_pool_size >= 10_000 {
                    "-".into()
                } else {
                    format!("{} of {}", fmt_pct(p.db_pool_util), p.db_pool_size)
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
    o.push_str(&table(
        &[
            "pod",
            "CPU",
            "CPU wait p99",
            "requests/s",
            "owns",
            "persistence/s",
            "DB pool",
            "pool wait p99",
            "cache hit",
            "top limit",
            "rejected",
        ],
        &rows,
        1,
    ));

    // --- persistence --------------------------------------------------------------------------
    o.push_str("## Database\n\n");
    let _ = writeln!(
        o,
        "{} busy, queue wait p99 {}.\n",
        fmt_pct(r.persistence.utilization),
        ms(r.persistence.queue_wait.p99_ms)
    );
    let rows: Vec<Vec<String>> = r
        .persistence
        .ops
        .iter()
        .take(if verbose { 64 } else { 10 })
        .map(|op| {
            vec![
                op.op.clone(),
                fmt_rate(op.per_s),
                ms(op.latency.p50_ms),
                ms(op.latency.p99_ms),
                op.rejected.to_string(),
            ]
        })
        .collect();
    o.push_str(&table(
        &["operation", "rate", "p50", "p99", "rejected"],
        &rows,
        1,
    ));

    // --- history ------------------------------------------------------------------------------
    let h = &r.history;
    o.push_str("## History\n\n");
    let _ = writeln!(
        o,
        "Shard IO busy p50 {}, p90 {}, max {} (concurrency {}). Write skew {:.1}×. Lock wait p99 {}, {} busy-workflow timeouts. Mutable-state cache hit {}.\n",
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
            "Task scheduler limiter {}: pod limit {:.0}–{:.0} tasks/s, {:.0}/s {} ({:.2} per task run).\n",
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
    let rows: Vec<Vec<String>> = h
        .hot_shards
        .iter()
        .take(if verbose { 20 } else { 5 })
        .map(|s| {
            vec![
                s.shard.to_string(),
                s.owner.clone(),
                fmt_pct(s.io_util),
                ms(s.io_wait_p99_ms),
                fmt_rate(s.writes_per_s),
                s.pending_tasks.to_string(),
            ]
        })
        .collect();
    o.push_str(&table(
        &[
            "hot shard",
            "owner",
            "IO busy",
            "IO wait p99",
            "writes/s",
            "queued tasks",
        ],
        &rows,
        2,
    ));
    if !h.hot_workflows.is_empty() && h.hot_workflows[0].util > 0.05 {
        let rows: Vec<Vec<String>> = h
            .hot_workflows
            .iter()
            .take(5)
            .map(|l| {
                vec![
                    l.workflow.clone(),
                    l.shard.to_string(),
                    fmt_pct(l.util),
                    ms(l.wait_p99_ms),
                    l.timeouts.to_string(),
                ]
            })
            .collect();
        o.push_str(&table(
            &["hot workflow lock", "shard", "busy", "wait p99", "timeouts"],
            &rows,
            1,
        ));
    }
    let rows: Vec<Vec<String>> = h
        .tasks
        .iter()
        .map(|t| {
            vec![
                t.task_type.clone(),
                fmt_rate(t.per_s),
                fmt_pct(t.noop_fraction),
                ms(t.load.p99_ms),
                ms(t.schedule.p99_ms),
                ms(t.processing.p99_ms),
                ms(t.queue.p99_ms),
                format!("{:.2}", t.mean_attempts),
                format!("{} / {}", t.busy_workflow_retries, t.throttled_retries),
            ]
        })
        .collect();
    o.push_str(&table(
        &[
            "history task",
            "rate",
            "no-op",
            "load p99",
            "schedule p99",
            "processing p99",
            "e2e p99",
            "attempts",
            "retries busy / throttled",
        ],
        &rows,
        1,
    ));

    // --- matching -----------------------------------------------------------------------------
    let m = &r.matching;
    o.push_str("## Matching\n\n");
    let _ = writeln!(
        o,
        "Sync match {}. Backlog mean {:.0}, max {:.0}.\n",
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
        (b.adds_per_s + b.backlog_mean * 100.0).total_cmp(&(a.adds_per_s + a.backlog_mean * 100.0))
    });
    let rows: Vec<Vec<String>> = parts
        .iter()
        .take(if verbose { 64 } else { 12 })
        .map(|p| {
            vec![
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
                format!("{} / {}", p.forwarded_tasks, p.forwarded_polls),
            ]
        })
        .collect();
    o.push_str(&table(
        &[
            "task queue",
            "type",
            "partition",
            "host",
            "adds/s",
            "polls/s",
            "sync",
            "backlog",
            "pollers",
            "dispatch p99",
            "forwarded tasks / polls",
        ],
        &rows,
        4,
    ));

    // --- limits and the rest ------------------------------------------------------------------
    if !r.limits.is_empty() {
        o.push_str("## Rate-limit rejections\n\n");
        let rows: Vec<Vec<String>> = r
            .limits
            .iter()
            .take(if verbose { 100 } else { 12 })
            .map(|l| {
                vec![
                    l.limiter.clone(),
                    l.place.clone(),
                    l.rejected.to_string(),
                    fmt_rate(l.per_s),
                ]
            })
            .collect();
        o.push_str(&table(&["limiter", "where", "rejected", "rate"], &rows, 2));
    }
    if let Some(s) = &r.schedules {
        o.push_str("## Schedules\n\n");
        let _ = writeln!(
            o,
            "{} actions/s. Action delay p50 {}, p99 {}. {} rate-limited.\n",
            fmt_rate(s.actions_per_s),
            ms(s.action_delay.p50_ms),
            ms(s.action_delay.p99_ms),
            s.rate_limited
        );
    }
    if !r.validation.is_empty() {
        o.push_str("## Validation against observed metrics\n\n");
        let rows: Vec<Vec<String>> = r
            .validation
            .iter()
            .map(|v| {
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
                vec![
                    v.metric.clone(),
                    f(v.observed),
                    f(v.simulated),
                    format!("{:.2}", v.ratio),
                ]
            })
            .collect();
        o.push_str(&table(
            &["metric", "observed", "simulated", "simulated ÷ observed"],
            &rows,
            1,
        ));
    }
    if !r.warnings.is_empty() {
        o.push_str("## Configuration warnings\n\n");
        for w in &r.warnings {
            let _ = writeln!(o, "- {w}");
        }
        o.push('\n');
    }
    let notes: Vec<&String> = r
        .notes
        .iter()
        .filter(|n| verbose || !n.starts_with("dynamic config ") || n.contains("->"))
        .collect();
    if !notes.is_empty() {
        o.push_str("## Notes\n\n");
        for n in notes {
            let _ = writeln!(o, "- {}", n.trim());
        }
        o.push('\n');
    }
    while o.ends_with("\n\n") {
        o.pop();
    }
    o
}

/// A sweep: one table per view, then each cell's top hotspot.
pub fn render_sweep(r: &SweepResult) -> String {
    let mut o = String::new();
    let _ = writeln!(o, "# tempdes sweep: {}\n", r.scenario);
    let _ = writeln!(
        o,
        "Temporal {}. {} cells, simulated in {:.1}s{}.\n",
        r.temporal_version,
        r.cells.len(),
        r.wall_ms as f64 / 1000.0,
        if r.base.is_empty() {
            String::new()
        } else {
            format!(", with base overrides {}", code(&r.base))
        }
    );
    let mut head: Vec<&str> = vec!["replicas ↓ · dynamic config →"];
    head.extend(r.cols.iter().map(String::as_str));
    for (_, title, f) in crate::sweep::views() {
        let _ = writeln!(o, "## {title}\n");
        let rows: Vec<Vec<String>> = r
            .rows
            .iter()
            .enumerate()
            .map(|(ri, label)| {
                let mut row = vec![label.clone()];
                for ci in 0..r.cols.len() {
                    let c = &r.cells[ri * r.cols.len() + ci];
                    row.push(if c.error.is_some() {
                        "error".into()
                    } else {
                        f(c)
                    });
                }
                row
            })
            .collect();
        o.push_str(&table(&head, &rows, 1));
    }
    o.push_str("## Top hotspot per cell\n\n");
    let rows: Vec<Vec<String>> = r
        .cells
        .iter()
        .map(|c| {
            vec![
                c.row_label.clone(),
                c.col_label.clone(),
                crate::sweep::top_hotspot(c),
            ]
        })
        .collect();
    o.push_str(&table(
        &["replicas", "dynamic config", "top hotspot"],
        &rows,
        3,
    ));
    while o.ends_with("\n\n") {
        o.pop();
    }
    o
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sweep::Cell;

    /// Every row of every table has as many cells as its header.
    fn assert_tables_are_well_formed(md: &str) {
        let mut header_cells = None;
        for line in md.lines() {
            if !line.starts_with('|') {
                header_cells = None;
                continue;
            }
            let cells = line.replace("\\|", "").matches('|').count();
            match header_cells {
                None => header_cells = Some(cells),
                Some(n) => assert_eq!(cells, n, "ragged row: {line}"),
            }
        }
    }

    #[test]
    fn cells_keep_their_row_intact() {
        assert_eq!(cell("a|b\nc"), "a\\|b c");
        assert_eq!(code("x"), "`x`");
        assert_eq!(code("a`b"), "`` a`b ``");
        let t = table(&["name", "rate"], &[vec!["p|q".into(), "5/s".into()]], 1);
        assert_eq!(t, "| name | rate |\n| --- | ---: |\n| p\\|q | 5/s |\n\n");
        assert_tables_are_well_formed(&t);
    }

    #[test]
    fn sweep_has_a_table_per_view() {
        let cell = |row: usize, col: usize, critical: usize| Cell {
            row,
            col,
            row_label: format!("history={}", row + 3),
            col_label: format!("history.shardIOConcurrency={}", col + 1),
            completed_per_s: 140.0,
            offered_per_s: 150.0,
            e2e_p99_ms: 2500.0,
            start_p99_ms: 20.0,
            wft_s2s_p99_ms: 30.0,
            cpu_max: Default::default(),
            db_util: 0.4,
            shard_io_max: 0.5,
            lock_wait_p99_ms: 3.0,
            rejections_per_s: 0.0,
            api_error_rate: 0.0,
            critical,
            warning: 0,
            top_hotspot: if critical > 0 {
                "database 100% busy".into()
            } else {
                String::new()
            },
            top_category: if critical > 0 {
                "database".into()
            } else {
                String::new()
            },
            headline: String::new(),
            hotspots: Vec::new(),
            error: None,
        };
        let r = SweepResult {
            scenario: "orders-baseline".into(),
            temporal_version: "1.31.0".into(),
            rows: vec!["history=3".into(), "history=4".into()],
            cols: vec![
                "history.shardIOConcurrency=1".into(),
                "history.shardIOConcurrency=2".into(),
            ],
            cells: vec![cell(0, 0, 1), cell(0, 1, 0), cell(1, 0, 0), cell(1, 1, 0)],
            base: String::new(),
            wall_ms: 1500,
        };
        let md = render_sweep(&r);
        assert!(md.starts_with("# tempdes sweep: orders-baseline\n"));
        for (_, title, _) in crate::sweep::views() {
            assert!(md.contains(&format!("## {title}\n")), "missing {title}");
        }
        assert!(
            md.contains("| history=3 | CRIT 1c/0w database | OK |"),
            "{md}"
        );
        assert!(md.contains("CRIT database 100% busy"));
        assert!(md.contains("no hotspots"));
        assert_tables_are_well_formed(&md);
        assert!(md.ends_with('\n') && !md.ends_with("\n\n"));
    }
}
