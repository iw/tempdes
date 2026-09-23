//! Hotspot detection rules. Each rule looks at one class of contention point, rates it against
//! the report thresholds, explains the likely cause, and lists the Temporal metrics that would
//! show it in production plus the knobs (EKS replicas, dynamic config) that change it.

use super::*;
use crate::metrics::observed::Observations;
use crate::util::units::{fmt_pct, fmt_rate, fmt_us};

struct Ctx2<'a> {
    ctx: &'a Ctx,
    r: &'a RunResult,
    warn: f64,
    crit: f64,
}

impl Ctx2<'_> {
    fn knob(&self, key: &str, hint: &str) -> Knob {
        let current = self
            .r
            .config
            .effective_dynamic_config
            .get(key)
            .cloned()
            .unwrap_or_else(|| self.ctx.p.dc.describe(key));
        let modeled = crate::model::params::MODELED_KEYS
            .iter()
            .any(|m| m.eq_ignore_ascii_case(key));
        Knob {
            key: key.to_string(),
            current,
            hint: if modeled {
                hint.to_string()
            } else {
                format!("{hint} (not simulated — validate in a test cluster)")
            },
        }
    }

    fn replicas(&self, svc: &str, hint: &str) -> Knob {
        Knob {
            key: format!("replicas.{svc}"),
            current: self
                .r
                .config
                .replicas
                .get(svc)
                .map(|n| n.to_string())
                .unwrap_or_default(),
            hint: hint.to_string(),
        }
    }

    fn infra(&self, key: &str, current: String, hint: &str) -> Knob {
        Knob {
            key: key.to_string(),
            current,
            hint: hint.to_string(),
        }
    }

    fn sev_util(&self, u: f64) -> Option<Severity> {
        if u >= self.crit {
            Some(Severity::Critical)
        } else if u >= self.warn {
            Some(Severity::Warning)
        } else {
            None
        }
    }
}

fn ms(v: f64) -> String {
    fmt_us(v * 1e3)
}

#[allow(clippy::too_many_arguments)]
fn hs(
    severity: Severity,
    category: &str,
    resource: String,
    title: String,
    detail: String,
    evidence: Vec<String>,
    metrics: &[&str],
    knobs: Vec<Knob>,
    score: f64,
) -> Hotspot {
    Hotspot {
        severity,
        category: category.into(),
        resource,
        title,
        detail,
        evidence,
        metrics: metrics.iter().map(|s| s.to_string()).collect(),
        knobs,
        score,
    }
}

pub fn detect(ctx: &Ctx, r: &RunResult) -> Vec<Hotspot> {
    let c = Ctx2 {
        ctx,
        r,
        warn: ctx.p.report.warn_utilization,
        crit: ctx.p.report.critical_utilization,
    };
    let mut out = Vec::new();
    throughput(&c, &mut out);
    cpu(&c, &mut out);
    database(&c, &mut out);
    shards(&c, &mut out);
    locks(&c, &mut out);
    queues(&c, &mut out);
    matching(&c, &mut out);
    limits(&c, &mut out);
    headroom(&c, &mut out);
    caches(&c, &mut out);
    schedules(&c, &mut out);
    api_latency(&c, &mut out);
    movement(&c, &mut out);
    causal(&c, &mut out);
    out.sort_by(|a, b| {
        a.severity.cmp(&b.severity).then(
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal),
        )
    });
    out
}

fn throughput(c: &Ctx2<'_>, out: &mut Vec<Hotspot>) {
    for w in &c.r.workflows {
        // Poisson arrivals: ignore shortfalls within 3σ of arrival noise unless starts failed
        let expected = w.offered_start_rate * c.r.duration_s;
        let observed = w.started_per_s * c.r.duration_s;
        let noise = 3.0 * expected.sqrt();
        let short_enough = expected - observed > noise.max(0.03 * expected);
        if w.offered_start_rate > 0.0 && (short_enough || w.start_failures as f64 > 0.01 * expected)
        {
            let short = 1.0 - w.started_per_s / w.offered_start_rate;
            out.push(hs(
                Severity::Critical,
                "throughput",
                w.workflow_type.clone(),
                format!(
                    "{} starts fall short of offered load ({} of {})",
                    w.workflow_type,
                    fmt_rate(w.started_per_s),
                    fmt_rate(w.offered_start_rate)
                ),
                format!(
                    "{:.0}% of offered starts did not complete successfully within the SDK retry window ({} start failures). See the rate-limit and saturation hotspots below for the cause.",
                    short * 100.0,
                    w.start_failures
                ),
                vec![],
                &[
                    "service_requests{operation=\"StartWorkflowExecution\"}",
                    "service_errors_resource_exhausted",
                ],
                vec![],
                100.0 + short * 100.0,
            ));
        }
        if w.wft_timeouts > 0 {
            let rate = w.wft_timeouts as f64 / c.r.duration_s;
            let frac = w.wft_timeouts as f64 / (w.wft_per_s * c.r.duration_s).max(1.0);
            out.push(hs(
                if frac > 0.01 { Severity::Critical } else { Severity::Warning },
                "workflow-tasks",
                w.workflow_type.clone(),
                format!("{} workflow task timeouts ({})", w.workflow_type, fmt_rate(rate)),
                "Workflow tasks timed out: sticky schedule-to-start expired (worker's sticky queue not polled fast enough) or a started task was never completed (RecordWorkflowTaskStarted succeeded but the poll response was lost / too slow). Each timeout adds history events and a retry on the normal queue.".into(),
                vec![format!(
                    "workflow task schedule-to-start p99 {}",
                    ms(w.wft_schedule_to_start.p99_ms)
                )],
                &["task_requests{task_type=\"TimerActiveTaskWorkflowTaskTimeout\"}", "workflow_task_attempt"],
                vec![
                    c.knob("history.defaultWorkflowTaskTimeout", "longer timeout hides, not fixes, slow dispatch"),
                    c.infra("workers.*.workflow_pollers", "scenario".into(), "more pollers/slots so sticky queues are drained"),
                ],
                60.0 + frac * 100.0,
            ));
        }
        let s2s = w.wft_schedule_to_start.p99_ms;
        if s2s > 1000.0 {
            let sev = if s2s > 5000.0 {
                Severity::Critical
            } else {
                Severity::Warning
            };
            out.push(hs(
                sev,
                "workers",
                w.workflow_type.clone(),
                format!("{}: workflow task schedule-to-start p99 {}", w.workflow_type, ms(s2s)),
                "Workflow tasks wait for pollers. Either SDK workers are poller/slot bound or matching dispatch is slow (backlogged partitions, forwarding). Compare with the matching partition table.".into(),
                vec![
                    format!("activity schedule-to-start p99 {}", ms(w.activity_schedule_to_start.p99_ms)),
                    format!("sticky cache hit {}", fmt_pct(w.sticky_hit_ratio)),
                ],
                &[
                    "temporal_workflow_task_schedule_to_start_latency (SDK)",
                    "asyncmatch_latency",
                    "approximate_backlog_count",
                ],
                if w.workflow_type == crate::model::params::SCHEDULER_WF_TYPE {
                    vec![
                        c.knob("worker.perNamespaceWorkerCount", "run the namespace's system worker on more worker-service pods"),
                        c.knob("worker.perNamespaceWorkerOptions", "SDK options (pollers/slots) of the per-namespace worker"),
                        c.replicas("worker", "worker-service pods hosting per-namespace workers"),
                    ]
                } else {
                    vec![
                        c.infra("workers.*.workflow_pollers / workflow_slots", "scenario".into(), "SDK MaxConcurrentWorkflowTaskPollers / ExecutionSize"),
                        c.knob("matching.numTaskqueueReadPartitions", "more partitions spread polls across matching hosts"),
                    ]
                },
                40.0 + s2s / 100.0,
            ));
        }
        let as2s = w.activity_schedule_to_start.p99_ms;
        if as2s > 1000.0 {
            let sev = if as2s > 10_000.0 {
                Severity::Critical
            } else {
                Severity::Warning
            };
            out.push(hs(
                sev,
                "workers",
                w.workflow_type.clone(),
                format!("{}: activity schedule-to-start p99 {}", w.workflow_type, ms(as2s)),
                "Activity tasks queue in matching: activity workers lack pollers/slots for the offered rate, or dispatch is limited per partition.".into(),
                vec![],
                &["temporal_activity_schedule_to_start_latency (SDK)", "approximate_backlog_count{task_type=\"Activity\"}"],
                vec![c.infra("workers.*.activity_pollers / activity_slots", "scenario".into(), "SDK MaxConcurrentActivityTaskPollers / ExecutionSize")],
                35.0 + as2s / 200.0,
            ));
        }
    }
}

fn cpu(c: &Ctx2<'_>, out: &mut Vec<Hotspot>) {
    for s in &c.r.services {
        let svc = s.service.as_str();
        if svc == "worker" && c.ctx.p.schedules.is_empty() {
            continue;
        }
        let pods: Vec<&PodResult> = s.pods.iter().filter(|p| p.alive).collect();
        let Some(hot) = pods
            .iter()
            .max_by(|a, b| a.cpu_util.partial_cmp(&b.cpu_util).unwrap())
        else {
            continue;
        };
        let owned_mean =
            pods.iter().map(|p| p.owned as f64).sum::<f64>() / pods.len().max(1) as f64;
        let cause = match svc {
            "history" => format!(
                "{} owns {} shards (mean {:.0}); shard placement comes from the ringpop hash ring ({} points/host).",
                hot.name, hot.owned, owned_mean, c.ctx.p.k.ringpop_replica_points
            ),
            "frontend" => format!(
                "{} holds {} client connections (mean {:.1}); SDK connections stick to one frontend until GOAWAY at frontend.keepAliveMaxConnectionAge.",
                hot.name, hot.owned, owned_mean
            ),
            "matching" => format!(
                "{} hosts {} task queue partitions (mean {:.1}); partitions are placed by hashing their routing key.",
                hot.name, hot.owned, owned_mean
            ),
            _ => String::new(),
        };
        if let Some(sev) = c.sev_util(hot.cpu_util) {
            let mut knobs = vec![
                c.replicas(svc, "add pods to spread shards/partitions/connections"),
                c.infra(
                    &format!("resources.{svc}.cpu"),
                    format!("{}", c.r.config.cpu.get(svc).copied().unwrap_or(0.0)),
                    "raise the container CPU limit (GOMAXPROCS)",
                ),
            ];
            match svc {
                "frontend" => knobs.push(c.knob(
                    "frontend.keepAliveMaxConnectionAge",
                    "shorter age rebalances connections sooner",
                )),
                "history" => knobs.push(c.knob(
                    "system.ringpopReplicaPoints",
                    "more points smooth shard placement (restart required)",
                )),
                "matching" => knobs.push(c.knob(
                    "matching.numTaskqueueReadPartitions",
                    "more partitions spread a hot task queue over hosts",
                )),
                _ => {}
            }
            out.push(hs(
                sev,
                "cpu",
                hot.name.clone(),
                format!("{svc} CPU {} on {} ({} cores)", fmt_pct(hot.cpu_util), hot.name, hot.cpu_cores),
                format!(
                    "CPU queueing adds latency to every request on this pod (CPU wait p99 {}). Service mean {} vs max {} (imbalance {:.2}x). {cause}",
                    ms(hot.cpu_wait_p99_ms),
                    fmt_pct(s.cpu_mean),
                    fmt_pct(s.cpu_max),
                    s.imbalance
                ),
                vec![format!("{} requests/s on the pod", fmt_rate(hot.requests_per_s))],
                &["container_cpu_usage_seconds_total (cAdvisor)", "service_latency", "service_pending_requests"],
                knobs,
                50.0 + hot.cpu_util * 50.0,
            ));
        } else if s.imbalance >= 1.35 && s.cpu_max >= 0.35 && pods.len() > 1 {
            out.push(hs(
                Severity::Info,
                "imbalance",
                svc.to_string(),
                format!("{svc} load imbalance {:.2}x (max {} vs mean {})", s.imbalance, fmt_pct(s.cpu_max), fmt_pct(s.cpu_mean)),
                format!("{cause} The busiest pod will saturate first: effective capacity is set by the max, not the mean."),
                vec![],
                &["container_cpu_usage_seconds_total", "numshards_gauge"],
                vec![c.replicas(svc, "more pods reduce the relative variance")],
                10.0 * s.imbalance,
            ));
        }
    }
}

fn database(c: &Ctx2<'_>, out: &mut Vec<Hotspot>) {
    let p = &c.r.persistence;
    if let Some(sev) = c.sev_util(p.utilization) {
        let top: Vec<String> = p
            .ops
            .iter()
            .take(4)
            .map(|o| {
                format!(
                    "{} {} (p99 {})",
                    o.op,
                    fmt_rate(o.per_s),
                    ms(o.latency.p99_ms)
                )
            })
            .collect();
        out.push(hs(
            sev,
            "database",
            format!("{} ({} concurrent ops)", p.store, p.capacity),
            format!("database {} busy", fmt_pct(p.utilization)),
            format!(
                "The persistence store is near capacity: requests queue in the database (queue wait p99 {}), which inflates persistence_latency, holds shard IO semaphores and workflow locks longer, and cascades into API latency.",
                ms(p.queue_wait.p99_ms)
            ),
            top,
            &["persistence_latency", "persistence_requests", "DB CPU / Aurora DBLoad"],
            vec![
                c.infra("cluster.persistence.capacity", p.capacity.to_string(), "larger instance / more Cassandra nodes"),
                c.knob("history.persistenceMaxQPS", "cap per-host QPS to protect the database (sheds load as ResourceExhausted)"),
            ],
            70.0 + p.utilization * 30.0,
        ));
    }
    for s in &c.r.services {
        let mut bad: Vec<&PodResult> = s
            .pods
            .iter()
            .filter(|p| p.alive && p.db_pool_size < 10_000)
            .filter(|p| p.db_pool_util >= c.warn || p.db_pool_wait_p99_ms > 5.0)
            .collect();
        if bad.is_empty() {
            continue;
        }
        bad.sort_by(|a, b| b.db_pool_util.partial_cmp(&a.db_pool_util).unwrap());
        let worst = bad[0];
        let max_wait = bad
            .iter()
            .map(|p| p.db_pool_wait_p99_ms)
            .fold(0.0, f64::max);
        let bursty = bad.iter().all(|p| p.db_pool_util < c.warn);
        let sev = if !bursty && (worst.db_pool_util >= c.crit || max_wait > 50.0) {
            Severity::Critical
        } else if bursty {
            if max_wait > 50.0 {
                Severity::Warning
            } else {
                Severity::Info
            }
        } else {
            Severity::Warning
        };
        let title = if bursty {
            format!(
                "{}: bursty DB connection pool exhaustion (avg use {}, wait p99 up to {})",
                s.service,
                fmt_pct(worst.db_pool_util),
                ms(max_wait)
            )
        } else {
            format!(
                "{}: DB connection pools saturated on {} of {} pods (max {} of {} conns, wait p99 up to {})",
                s.service,
                bad.len(),
                s.pods.iter().filter(|p| p.alive).count(),
                fmt_pct(worst.db_pool_util),
                worst.db_pool_size,
                ms(max_wait)
            )
        };
        let detail = if bursty {
            "Pools are mostly idle but run out during bursts (e.g. many timers firing at the same instant), so calls briefly queue for a connection. The wait shows up in persistence_latency, not in database metrics.".to_string()
        } else {
            "Persistence calls wait for a free SQL connection before reaching the database; this latency is invisible in database metrics but shows up in persistence_latency and holds shard IO semaphores and workflow locks longer.".to_string()
        };
        out.push(hs(
            sev,
            "connection-pool",
            format!("{} pods", s.service),
            title,
            detail,
            bad.iter()
                .take(6)
                .map(|p| {
                    format!(
                        "{}: {} of {} conns in use, wait p99 {}, {} persistence ops",
                        p.name,
                        fmt_pct(p.db_pool_util),
                        p.db_pool_size,
                        ms(p.db_pool_wait_p99_ms),
                        fmt_rate(p.persistence_per_s)
                    )
                })
                .collect(),
            &["persistence_sql_in_use", "persistence_sql_open_conn", "persistence_latency"],
            vec![c.infra(
                &format!("cluster.persistence.max_conns.{}", s.service),
                worst.db_pool_size.to_string(),
                "static config persistence.datastores.*.sql.maxConns (check DB max_connections × pods)",
            )],
            if bursty { 12.0 + max_wait / 10.0 } else { 45.0 + worst.db_pool_util * 40.0 },
        ));
    }
}

fn shards(c: &Ctx2<'_>, out: &mut Vec<Hotspot>) {
    let h = &c.r.history;
    let Some(top) = h.hot_shards.first() else {
        return;
    };
    let cass = c.ctx.p.store == crate::config::scenario::StoreKind::Cassandra;
    let skewed = h.shard_writes_max_over_mean >= 3.0;
    if top.io_util < c.warn {
        if top.io_wait_p99_ms > 50.0 {
            out.push(hs(
                Severity::Info,
                "shard",
                format!("shard {}", top.shard),
                format!("bursty shard write contention (shard {} wait p99 {} at {} average IO use)", top.shard, ms(top.io_wait_p99_ms), fmt_pct(top.io_util)),
                "Shards are idle on average but many writes arrive together (e.g. timers or schedules firing at the same instant) and queue on the shard IO semaphore.".into(),
                vec![],
                &["service_latency{service_name=\"history\"}", "persistence_latency"],
                vec![c.knob("history.shardIOConcurrency", "parallel writes per shard (SQL only)")],
                8.0,
            ));
        }
        return;
    }
    if let Some(sev) = c.sev_util(top.io_util) {
        // list the shards that are actually busy; the rest of the top-N is padding
        let list: Vec<String> = h
            .hot_shards
            .iter()
            .take(5)
            .filter(|s| s.io_util >= 0.25 * top.io_util)
            .map(|s| {
                format!(
                    "shard {} on {}: IO {} busy, wait p99 {}, {} writes",
                    s.shard,
                    s.owner,
                    fmt_pct(s.io_util),
                    ms(s.io_wait_p99_ms),
                    fmt_rate(s.writes_per_s)
                )
            })
            .collect();
        let driver = h
            .hot_workflows
            .iter()
            .find(|l| l.shard == top.shard && l.util >= 0.5);
        let detail = if let Some(d) = driver {
            format!(
                "Shard {} is busy because workflow {} writes to it continuously (its lock is {} busy). Writes of one workflow are already serialised by its lock, so the shard IO semaphore has no queue (wait p99 {}): raising history.shardIOConcurrency or adding history pods will not help — the fix is fewer writes per workflow (batch signals, split the entity) or a faster database write.",
                top.shard,
                d.workflow,
                fmt_pct(d.util),
                ms(top.io_wait_p99_ms)
            )
        } else if skewed {
            format!(
                "Writes are skewed: the busiest shard takes {:.1}x the mean. All persistence writes of a shard are serialised by the shard IO semaphore (history.shardIOConcurrency={}), so a shard with hot workflow IDs saturates on its own regardless of replica count.",
                h.shard_writes_max_over_mean, h.shard_io_concurrency
            )
        } else {
            format!(
                "Shards are uniformly busy (p50 {}, p90 {}). Each shard serialises its writes (history.shardIOConcurrency={}); per-shard write throughput ≈ concurrency / write latency, so total write capacity ≈ {} shards × concurrency / latency.",
                fmt_pct(h.shard_util_p50),
                fmt_pct(h.shard_util_p90),
                h.shard_io_concurrency,
                h.num_shards
            )
        };
        let mut knobs = vec![];
        if cass {
            knobs.push(c.infra(
                "history.shardIOConcurrency",
                "1 (forced for Cassandra)".into(),
                "not effective on Cassandra: reduce write latency or use more shards",
            ));
        } else {
            knobs.push(c.knob(
                "history.shardIOConcurrency",
                "allow parallel writes per shard (SQL stores only)",
            ));
        }
        knobs.push(c.infra(
            "numHistoryShards",
            h.num_shards.to_string(),
            "static at cluster creation; more shards need a new cluster/migration",
        ));
        if skewed {
            knobs.push(c.infra(
                "workload",
                "hot workflow IDs".into(),
                "spread entity workflows / signals over more IDs",
            ));
        }
        out.push(hs(
            sev,
            "shard",
            format!("shard {}", top.shard),
            format!(
                "hot history shard {} ({} IO busy, wait p99 {})",
                top.shard,
                fmt_pct(top.io_util),
                ms(top.io_wait_p99_ms)
            ),
            detail,
            list,
            &[
                "persistence_latency{operation=\"UpdateWorkflowExecution\"}",
                "service_latency{service_name=\"history\"}",
                "persistence_shard_rps",
            ],
            knobs,
            // a shard saturated by a single lock-serialised workflow ranks below that lock
            if driver.is_some() {
                50.0
            } else {
                60.0 + top.io_util * 40.0
            },
        ));
    }
}

fn locks(c: &Ctx2<'_>, out: &mut Vec<Hotspot>) {
    let h = &c.r.history;
    let hot = h.hot_workflows.first();
    let hot_util = hot.map(|l| l.util).unwrap_or(0.0);
    if h.lock_timeouts > 0 || hot_util >= 0.5 {
        let sev = if hot_util >= c.crit || h.lock_timeouts as f64 / c.r.duration_s > 1.0 {
            Severity::Critical
        } else {
            Severity::Warning
        };
        let list: Vec<String> = h
            .hot_workflows
            .iter()
            .take(5)
            .filter(|l| l.util >= 0.1 || l.timeouts > 0)
            .map(|l| {
                format!(
                    "{} (shard {}): lock {} busy, wait p99 {}, {} busy-workflow timeouts",
                    l.workflow,
                    l.shard,
                    fmt_pct(l.util),
                    ms(l.wait_p99_ms),
                    l.timeouts
                )
            })
            .collect();
        out.push(hs(
            sev,
            "workflow-lock",
            hot.map(|l| l.workflow.clone()).unwrap_or_else(|| "workflows".into()),
            format!(
                "workflow lock contention (lock wait p99 {}, {} BUSY_WORKFLOW timeouts)",
                ms(h.lock_wait.p99_ms),
                h.lock_timeouts
            ),
            "Every API call and history task on a workflow serialises on its mutable-state lock for the whole persistence write. Queue tasks give up after history.cacheNonUserContextLockTimeout and retry (immediately up to 10 times, then with backoff); API callers wait until their deadline. A workflow receiving more updates/s than 1/(lock hold time) is a hard hotspot.".into(),
            list,
            &[
                "history_workflow_execution_cache_latency",
                "acquire_lock_failed",
                "task_errors_workflow_busy",
                "service_errors_resource_exhausted{resource_exhausted_cause=\"RESOURCE_EXHAUSTED_CAUSE_BUSY_WORKFLOW\"}",
            ],
            vec![
                c.knob("history.cacheNonUserContextLockTimeout", "longer waits reduce retries but hold scheduler workers"),
                c.knob(
                    "history.taskSchedulerEnableExecutionQueueScheduler",
                    "1.31: sequential per-workflow queues for contended workflows (fewer busy-workflow errors)",
                ),
                c.infra("workload", "signal/update fan-in".into(), "shard entity workflows or batch signals"),
            ],
            55.0 + hot_util * 40.0,
        ));
    }
}

fn queues(c: &Ctx2<'_>, out: &mut Vec<Hotspot>) {
    let h = &c.r.history;
    for t in &h.tasks {
        let cat = if t.task_type.starts_with("Transfer") {
            "transfer"
        } else if t.task_type.starts_with("Timer") {
            "timer"
        } else {
            "visibility"
        };
        if t.schedule.p99_ms > 200.0 {
            let sev = if t.schedule.p99_ms > 2000.0 {
                Severity::Critical
            } else {
                Severity::Warning
            };
            out.push(hs(
                sev,
                "history-queue",
                t.task_type.clone(),
                format!("{}: waits {} (p99) for a {cat} scheduler worker", t.task_type, ms(t.schedule.p99_ms)),
                format!("The host-level {cat} task scheduler is saturated: all workers are busy (tasks hold a worker through lock waits, persistence and — for transfer tasks — the matching AddTask round trip including RecordTaskStarted on sync match)."),
                vec![format!("processing p99 {}", ms(t.processing.p99_ms))],
                &["task_latency_schedule", "task_latency_processing", "dynamic_worker_pool_scheduler_active_workers"],
                vec![
                    c.knob(&format!("history.{cat}ProcessorSchedulerWorkerCount"), "more workers per host"),
                    c.replicas("history", "spread shards over more hosts"),
                ],
                30.0 + t.schedule.p99_ms / 100.0,
            ));
        }
        if t.load.p99_ms > 1000.0 && cat != "timer" {
            out.push(hs(
                if t.load.p99_ms > 10_000.0 { Severity::Critical } else { Severity::Warning },
                "history-queue",
                t.task_type.clone(),
                format!("{}: {} (p99) from commit to load", t.task_type, ms(t.load.p99_ms)),
                format!("Queue readers are falling behind: per-shard reads are limited by history.{cat}ProcessorMaxPollRPS and paused when history.queuePendingTasksMaxCount tasks are in memory; each read fetches at most history.{cat}TaskBatchSize tasks."),
                vec![],
                &["task_latency_load", "task_latency_queue", "queue_reader_count"],
                vec![
                    c.knob(&format!("history.{cat}ProcessorMaxPollRPS"), "reads per shard per second"),
                    c.knob(&format!("history.{cat}TaskBatchSize"), "tasks per read"),
                    c.knob("history.queuePendingTasksMaxCount", "in-memory pending task cap per shard queue"),
                ],
                25.0 + t.load.p99_ms / 500.0,
            ));
        }
        let retries = t.busy_workflow_retries + t.throttled_retries;
        if retries as f64 / c.r.duration_s > 1.0 {
            let causes: Vec<String> = t
                .throttled_by
                .iter()
                .map(|(k, v)| format!("{k}: {v}"))
                .collect();
            let mut knobs = Vec::new();
            if t.throttled_by.contains_key("RPS_LIMIT") {
                if t.task_type.contains("WorkflowTask") || t.task_type.contains("ActivityTask") {
                    knobs.push(c.knob(
                        "matching.rps",
                        "AddTask calls rejected by the matching host limit",
                    ));
                } else {
                    knobs.push(c.knob("history.rps", "history host limit"));
                }
            }
            if t.throttled_by.contains_key("PERSISTENCE_LIMIT") {
                knobs.push(c.knob("history.persistenceMaxQPS", "per-host persistence QPS"));
            }
            if t.throttled_by.contains_key("SYSTEM_OVERLOADED") {
                knobs.push(c.knob(
                    "matching.outstandingTaskAppendsThreshold",
                    "matching task writer buffer",
                ));
            }
            if t.busy_workflow_retries > 0 {
                knobs.push(c.knob(
                    "history.cacheNonUserContextLockTimeout",
                    "busy-workflow retries: lock wait limit for tasks",
                ));
            }
            out.push(hs(
                Severity::Warning,
                "history-queue",
                t.task_type.clone(),
                format!("{}: {} task retries ({} busy workflow, {} throttled)", t.task_type, fmt_rate(retries as f64 / c.r.duration_s), t.busy_workflow_retries, t.throttled_retries),
                "Retried tasks consume scheduler capacity and delay progress; throttled retries back off for seconds (max(1s·1.1ⁿ, 3s·1.5ⁿ⁻¹)).".into(),
                {
                    let mut e = vec![format!("mean attempts {:.2}", t.mean_attempts)];
                    if !causes.is_empty() {
                        e.push(format!("throttling causes: {}", causes.join(", ")));
                    }
                    e
                },
                &["task_errors_workflow_busy", "task_errors_throttled", "task_attempt"],
                knobs,
                20.0 + retries as f64 / c.r.duration_s,
            ));
        }
    }
    if let Some(vis) = h
        .tasks
        .iter()
        .find(|t| t.task_type.starts_with("Visibility"))
        && vis.queue.p99_ms > 5000.0
    {
        out.push(hs(
                Severity::Warning,
                "visibility",
                "visibility queue".into(),
                format!("visibility updates lag {} (p99)", ms(vis.queue.p99_ms)),
                "List/Count results trail the workflow state; the visibility queue or the Elasticsearch bulk processor is saturated.".into(),
                vec![],
                &["task_latency_queue{task_type=~\"Visibility.*\"}", "elasticsearch_bulk_processor_request_latency"],
                vec![
                    c.knob("worker.ESProcessorNumOfWorkers", "concurrent bulk requests per host"),
                    c.knob("worker.ESProcessorBulkActions", "documents per bulk"),
                    c.knob("history.visibilityProcessorSchedulerWorkerCount", "visibility task workers"),
                ],
                20.0,
            ));
    }
}

fn matching(c: &Ctx2<'_>, out: &mut Vec<Hotspot>) {
    let m = &c.r.matching;
    // backlogged partitions
    let mut backlogged: Vec<&PartitionResult> = m
        .partitions
        .iter()
        .filter(|p| p.backlog_mean >= 5.0 && p.task_wait.p99_ms > 1000.0)
        .collect();
    backlogged.sort_by(|a, b| b.backlog_mean.partial_cmp(&a.backlog_mean).unwrap());
    if let Some(top) = backlogged.first() {
        let sev = if top.task_wait.p99_ms > 10_000.0 {
            Severity::Critical
        } else {
            Severity::Warning
        };
        out.push(hs(
            sev,
            "matching-backlog",
            format!("{} {} partition {}", top.task_queue, top.kind, top.partition),
            format!(
                "task backlog on {} {} (mean {:.0}, max {:.0}, dispatch wait p99 {})",
                top.task_queue,
                top.kind,
                top.backlog_mean,
                top.backlog_max,
                ms(top.task_wait.p99_ms)
            ),
            "Tasks are written to persistence because no poller was waiting. Backlogged partitions also disable sync match and forwarding until the head is younger than matching.backlogNegligibleAge, adding CreateTasks/GetTasks load.".into(),
            backlogged
                .iter()
                .take(5)
                .map(|p| format!("{} {} p{} on {}: backlog mean {:.0}, pollers {:.1}", p.task_queue, p.kind, p.partition, p.host, p.backlog_mean, p.pollers_mean))
                .collect(),
            &["approximate_backlog_count", "approximate_backlog_age_seconds", "asyncmatch_latency", "persistence_requests{operation=\"CreateTasks\"}"],
            vec![
                c.infra("workers.*", "pollers/slots/processes".into(), "add worker capacity for this task queue"),
                c.knob("matching.numTaskqueueReadPartitions", "spread dispatch over more partitions/hosts"),
            ],
            40.0 + top.backlog_mean.log10().max(0.0) * 10.0,
        ));
    }
    let rejects: u64 = m.partitions.iter().map(|p| p.write_rejects).sum();
    if rejects > 0 {
        out.push(hs(
            Severity::Critical,
            "matching-backlog",
            "task writer".into(),
            format!("{rejects} AddTask rejections: matching task writer buffer full"),
            "More than matching.outstandingTaskAppendsThreshold appends were queued behind CreateTasks on a partition; AddTask fails with SYSTEM_OVERLOADED and history retries the transfer task.".into(),
            vec![],
            &["task_write_throttle_count", "persistence_latency{operation=\"CreateTasks\"}"],
            vec![
                c.knob("matching.outstandingTaskAppendsThreshold", "buffer size"),
                c.knob("matching.maxTaskBatchSize", "tasks per CreateTasks"),
                c.knob("matching.numTaskqueueWritePartitions", "more writers in parallel"),
            ],
            65.0,
        ));
    }
    // partitions per host imbalance
    if m.partitions_per_host.len() > 1 {
        let counts: Vec<f64> = m.partitions_per_host.iter().map(|x| x.1 as f64).collect();
        let mean = counts.iter().sum::<f64>() / counts.len() as f64;
        let max = counts.iter().cloned().fold(0.0, f64::max);
        let min = counts.iter().cloned().fold(f64::MAX, f64::min);
        if mean > 0.0 && (max / mean >= 1.6 || min == 0.0) {
            out.push(hs(
                Severity::Info,
                "matching-placement",
                "matching hosts".into(),
                format!(
                    "uneven task queue partition placement across matching hosts (max {max:.0}, mean {mean:.1}, min {min:.0})"
                ),
                "Partitions are placed by hashing routing keys on the ringpop ring, independently of each other; with few partitions some hosts get several and others none.".into(),
                m.partitions_per_host.iter().map(|(h, n)| format!("{h}: {n} partitions")).collect(),
                &["loaded_task_queue_partition_count"],
                vec![
                    c.knob("matching.spreadRoutingBatchSize", "1.31 option to spread partitions of a queue over distinct hosts"),
                    c.knob("matching.numTaskqueueReadPartitions", "more partitions average out placement"),
                ],
                8.0,
            ));
        }
    }
}

fn limits(c: &Ctx2<'_>, out: &mut Vec<Hotspot>) {
    use std::collections::BTreeMap;
    let mut by: BTreeMap<String, (u64, Vec<String>)> = BTreeMap::new();
    for l in &c.r.limits {
        let e = by.entry(l.limiter.clone()).or_default();
        e.0 += l.rejected;
        e.1.push(format!(
            "{}: {} rejected ({})",
            l.place,
            l.rejected,
            fmt_rate(l.per_s)
        ));
    }
    for (limiter, (n, places)) in by {
        let per_s = n as f64 / c.r.duration_s;
        let sev = if per_s >= 1.0 {
            Severity::Critical
        } else {
            Severity::Warning
        };
        let (detail, metrics, knobs): (String, Vec<&str>, Vec<Knob>) = match limiter.as_str() {
            "frontend.namespaceRPS" => (
                "Per-frontend namespace rate limit. It is a priority limiter: Start/Signal/Respond (P1) reserve tokens from lower priorities, so polls (P4) are rejected first. With a global limit the share is global / #frontends, so a frontend holding more SDK connections throttles while others are idle.".into(),
                vec!["service_errors_resource_exhausted{resource_exhausted_cause=\"RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT\",resource_exhausted_scope=\"RESOURCE_EXHAUSTED_SCOPE_NAMESPACE\"}"],
                vec![
                    c.knob("frontend.namespaceRPS", "per-instance namespace RPS"),
                    c.knob("frontend.globalNamespaceRPS", "cluster-wide; divided by #frontends"),
                    c.knob("frontend.namespaceBurstRatio", "burst = RPS × ratio"),
                    c.replicas("frontend", "with a global limit more frontends don't add capacity, but spread connections"),
                ],
            ),
            "frontend.rps" => (
                "Per-frontend host rate limit (all namespaces). Priority limiter: polls starve first.".into(),
                vec!["service_errors_resource_exhausted{resource_exhausted_scope=\"RESOURCE_EXHAUSTED_SCOPE_SYSTEM\"}", "host_rps_limit"],
                vec![c.knob("frontend.rps", "per-instance RPS"), c.knob("frontend.globalRPS", "cluster-wide"), c.replicas("frontend", "more frontends add host capacity")],
            ),
            "frontend.namespaceCount" => (
                "Concurrent long-running requests (polls, queries) per namespace per API exceed the per-frontend quota. SDK pollers across all workers count here; the busiest frontend (most connections) hits it first.".into(),
                vec!["service_errors_resource_exhausted{resource_exhausted_cause=\"RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT\"}", "service_pending_requests"],
                vec![c.knob("frontend.namespaceCount", "per instance per API"), c.knob("frontend.globalNamespaceCount", "cluster-wide / #frontends")],
            ),
            "frontend.namespaceRPS.visibility" => (
                "List/Count visibility queries are limited to 10/s per frontend per namespace by default.".into(),
                vec!["service_errors_resource_exhausted{operation=\"ListWorkflowExecutions\"}"],
                vec![c.knob("frontend.namespaceRPS.visibility", "per instance"), c.knob("frontend.globalNamespaceRPS.visibility", "cluster-wide")],
            ),
            "history.rps" => (
                "History host handler rate limit; applies to every RPC including queue-driven calls. Rejections are retried once by the internal client, then surface to the caller.".into(),
                vec!["service_errors_resource_exhausted{service_name=\"history\"}"],
                vec![c.knob("history.rps", "per history host"), c.replicas("history", "more hosts")],
            ),
            "matching.rps" => (
                "Matching host handler rate limit (AddTask, polls, forwarded calls). A host with several hot partitions hits it first.".into(),
                vec!["service_errors_resource_exhausted{service_name=\"matching\"}"],
                vec![
                    c.knob("matching.rps", "per matching host"),
                    c.replicas("matching", "more hosts"),
                    c.knob("matching.numTaskqueueReadPartitions", "spread a hot queue"),
                ],
            ),
            l if l.ends_with(".persistenceMaxQPS") => (
                "Persistence priority rate limiter on the pod: calls fail immediately with PERSISTENCE_LIMIT (no waiting). Queue tasks back off for seconds; API calls surface ResourceExhausted.".into(),
                vec!["persistence_errors_resource_exhausted", "task_errors_throttled"],
                vec![c.knob(l, "per-host persistence QPS"), c.knob(&l.replace("persistenceMaxQPS", "persistenceGlobalMaxQPS"), "cluster-wide / #hosts")],
            ),
            "matching.outstandingTaskAppendsThreshold" => (
                "Matching task writer buffer overflow.".into(),
                vec!["task_write_throttle_count"],
                vec![c.knob("matching.outstandingTaskAppendsThreshold", "buffer size")],
            ),
            _ => ("Rate limited.".into(), vec![], vec![]),
        };
        out.push(Hotspot {
            severity: sev,
            category: "rate-limit".into(),
            resource: limiter.clone(),
            title: format!("{limiter}: {n} rejections ({})", fmt_rate(per_s)),
            detail,
            evidence: places.into_iter().take(6).collect(),
            metrics: metrics.into_iter().map(String::from).collect(),
            knobs,
            score: 50.0 + per_s.min(1000.0) / 10.0,
        });
    }
}

fn caches(c: &Ctx2<'_>, out: &mut Vec<Hotspot>) {
    let h = &c.r.history;
    if h.cache_hit_ratio < 0.8 {
        out.push(hs(
            if h.cache_hit_ratio < 0.5 { Severity::Warning } else { Severity::Info },
            "cache",
            "mutable state cache".into(),
            format!("history mutable-state cache hit ratio {}", fmt_pct(h.cache_hit_ratio)),
            "Cache misses reload mutable state with GetWorkflowExecution (extra DB reads and CPU). Note 1.31 does not populate the cache on StartWorkflowExecution, so the first task of every workflow misses.".into(),
            vec![],
            &["cache_miss{cache_type=\"mutablestate\"}", "cache_requests", "persistence_requests{operation=\"GetWorkflowExecution\"}"],
            vec![c.knob("history.hostLevelCacheMaxSize", "entries per host"), c.replicas("history", "more hosts → more total cache")],
            12.0,
        ));
    }
    for w in &c.r.workflows {
        if w.sticky_tasks > 100 && w.sticky_hit_ratio < 0.8 {
            out.push(hs(
                Severity::Warning,
                "sticky-cache",
                w.workflow_type.clone(),
                format!("{}: sticky cache hit {} on sticky tasks", w.workflow_type, fmt_pct(w.sticky_hit_ratio)),
                format!("Workers evict workflows before their next task and must replay full history ({} history pages fetched), increasing worker CPU and GetWorkflowExecutionHistory load.", w.history_pages_fetched),
                vec![],
                &["temporal_sticky_cache_miss (SDK)", "service_requests{operation=\"GetWorkflowExecutionHistory\"}"],
                vec![c.infra("workers.*.sticky_cache_size", "scenario".into(), "SDK WorkerCacheSize")],
                15.0,
            ));
        }
    }
}

fn schedules(c: &Ctx2<'_>, out: &mut Vec<Hotspot>) {
    if let Some(s) = &c.r.schedules
        && (s.rate_limited > 0 || s.action_delay.p99_ms > 5000.0)
    {
        let sev = if s.action_delay.p99_ms > 30_000.0 {
            Severity::Critical
        } else {
            Severity::Warning
        };
        out.push(hs(
                sev,
                "schedules",
                "scheduler workflows".into(),
                format!(
                    "schedule actions delayed up to {} (p99), {} rate-limited",
                    ms(s.action_delay.p99_ms),
                    s.rate_limited
                ),
                "Schedules in a namespace share one start-rate token bucket per worker-service host (worker.schedulerNamespaceStartWorkflowRPS × host share). All per-namespace system work runs on worker.perNamespaceWorkerCount hosts, so aligned schedules (e.g. all at :00) queue behind each other.".into(),
                vec![format!("{} actions/s", fmt_rate(s.actions_per_s))],
                &["schedule_action_delay", "schedule_rate_limited", "schedule_action_success"],
                vec![
                    c.knob("worker.schedulerNamespaceStartWorkflowRPS", "per-namespace start rate"),
                    c.knob("worker.perNamespaceWorkerCount", "spread per-namespace workers over more worker-service pods"),
                    c.replicas("worker", "worker-service pods"),
                ],
                30.0 + s.action_delay.p99_ms / 1000.0,
            ));
    }
}

fn api_latency(c: &Ctx2<'_>, out: &mut Vec<Hotspot>) {
    let slo = c.ctx.p.report.api_p99_slo.0 / 1e3;
    for a in &c.r.apis {
        if a.api.starts_with("Poll") || a.api == "GetWorkflowExecutionHistory" {
            continue;
        }
        if a.latency.p99_ms > slo {
            out.push(hs(
                if a.latency.p99_ms > 4.0 * slo { Severity::Critical } else { Severity::Warning },
                "api-latency",
                a.api.clone(),
                format!("{} p99 {} exceeds the {} objective", a.api, ms(a.latency.p99_ms), ms(slo)),
                "Client-observed latency including SDK retries and backoff. See saturation / rate-limit hotspots for the cause.".into(),
                vec![format!("p50 {}, error rate {}", ms(a.latency.p50_ms), fmt_pct(a.error_rate))],
                &["service_latency", "temporal_request_latency (SDK)"],
                vec![],
                20.0 + a.latency.p99_ms / slo,
            ));
        }
        if a.error_rate > 0.001 {
            out.push(hs(
                if a.error_rate > 0.01 {
                    Severity::Critical
                } else {
                    Severity::Warning
                },
                "api-errors",
                a.api.clone(),
                format!(
                    "{} failing for clients: {} of calls",
                    a.api,
                    fmt_pct(a.error_rate)
                ),
                format!("Errors after SDK retries: {:?}", a.errors),
                vec![],
                &["service_errors", "service_errors_resource_exhausted"],
                vec![],
                40.0 + a.error_rate * 100.0,
            ));
        }
    }
}

fn movement(c: &Ctx2<'_>, out: &mut Vec<Hotspot>) {
    let h = &c.r.history;
    if h.shard_moves > 0 && h.shard_unavailable.p99_ms > 200.0 {
        out.push(hs(
            Severity::Warning,
            "membership",
            "history shard movement".into(),
            format!(
                "{} shard moves; requests blocked up to {} (p99) during shard acquisition",
                h.shard_moves,
                ms(h.shard_unavailable.p99_ms)
            ),
            "When history membership changes, moved shards are closed on the old owner and acquired by the new one (GetOrCreateShard + UpdateShard, history.acquireShardConcurrency at a time). Requests wait meanwhile and the new owner's mutable-state cache starts cold.".into(),
            vec![],
            &["acquire_shards_latency", "sharditem_acquisition_latency", "service_errors_shard_ownership_lost", "membership_changed_count"],
            vec![
                c.knob("history.acquireShardConcurrency", "parallel shard acquisitions per host"),
                c.knob("history.shutdownDrainDuration", "drain before stopping a pod"),
                c.knob("history.alignMembershipChange", "batch membership changes"),
            ],
            25.0,
        ));
    }
}

/// One-line summary of the run.
pub fn headline(r: &RunResult) -> String {
    let crit = r
        .hotspots
        .iter()
        .filter(|h| h.severity == Severity::Critical)
        .count();
    let warn = r
        .hotspots
        .iter()
        .filter(|h| h.severity == Severity::Warning)
        .count();
    // highest utilisation resource for headroom
    let mut worst = ("database".to_string(), r.persistence.utilization);
    for s in &r.services {
        for p in &s.pods {
            if p.alive && p.cpu_util > worst.1 && s.service != "worker" {
                worst = (format!("{} CPU", p.name), p.cpu_util);
            }
        }
    }
    if let Some(sh) = r.history.hot_shards.first()
        && sh.io_util > worst.1
    {
        worst = (format!("shard {} IO", sh.shard), sh.io_util);
    }
    for s in &r.services {
        for p in s.pods.iter().filter(|p| p.alive) {
            for (lim, u) in &p.limit_util {
                if *u > worst.1 {
                    worst = (format!("{lim} on {}", p.name), *u);
                }
            }
        }
    }
    let headroom = if worst.1 > 0.01 {
        1.0 / worst.1
    } else {
        f64::INFINITY
    };
    match r.hotspots.iter().find(|h| h.severity != Severity::Info) {
        // the busiest resource is usually the top hotspot itself; only name it when it differs
        Some(top) if worst.0.split(" on ").all(|part| top.title.contains(part)) => {
            format!(
                "{crit} critical / {warn} warning hotspots — top: {}.",
                top.title
            )
        }
        Some(top) => format!(
            "{crit} critical / {warn} warning hotspots — top: {}. Busiest resource: {} at {}.",
            top.title,
            worst.0,
            fmt_pct(worst.1)
        ),
        None => format!(
            "No hotspots at this load. Busiest resource: {} at {} (≈{:.1}x headroom before it saturates, assuming linear scaling).",
            worst.0,
            fmt_pct(worst.1),
            headroom
        ),
    }
}

/// Compare simulated values with observed metrics.
pub fn validate(ctx: &Ctx, r: &RunResult, obs: &Observations) -> Vec<ValidationRow> {
    let mut rows = Vec::new();
    let mut push = |metric: String, observed: f64, simulated: f64, unit: &str| {
        if observed.is_finite() && observed > 0.0 {
            rows.push(ValidationRow {
                metric,
                observed,
                simulated,
                unit: unit.into(),
                ratio: simulated / observed,
            });
        }
    };
    let m = ctx.m.borrow();
    let dur = r.duration_s;
    for api in Api::ALL {
        let f = [("service_name", "frontend"), ("operation", api.as_str())];
        let sim = m.fe_total(api);
        if let Some(rate) = obs.rate("service_requests", &f) {
            push(
                format!("service_requests{{frontend,{}}}", api.as_str()),
                rate,
                sim.count as f64 / dur,
                "/s",
            );
        }
        if let Some(l) = obs.latency("service_latency", &f)
            && let Some(q) = l.quantile_us(0.99)
        {
            push(
                format!("service_latency p99{{frontend,{}}}", api.as_str()),
                q / 1e3,
                sim.latency.quantile(0.99) as f64 / 1e3,
                "ms",
            );
        }
    }
    for api in HistApi::ALL {
        let f = [("service_name", "history"), ("operation", api.as_str())];
        let sim = m.hist_total(api);
        if let Some(l) = obs.latency("service_latency", &f)
            && let Some(q) = l.quantile_us(0.99)
        {
            push(
                format!("service_latency p99{{history,{}}}", api.as_str()),
                q / 1e3,
                sim.latency.quantile(0.99) as f64 / 1e3,
                "ms",
            );
        }
    }
    for op in PersistOp::ALL {
        let f = [("operation", op.as_str())];
        let sim = &m.persist[op.idx()];
        if let Some(rate) = obs.rate("persistence_requests", &f) {
            push(
                format!("persistence_requests{{{}}}", op.as_str()),
                rate,
                sim.count as f64 / dur,
                "/s",
            );
        }
        if let Some(l) = obs.latency("persistence_latency", &f)
            && let Some(q) = l.quantile_us(0.99)
        {
            push(
                format!("persistence_latency p99{{{}}}", op.as_str()),
                q / 1e3,
                sim.latency.quantile(0.99) as f64 / 1e3,
                "ms",
            );
        }
    }
    // cache hit ratio
    let req = obs
        .rate("cache_requests", &[("cache_type", "mutablestate")])
        .or_else(|| obs.value_sum("cache_requests", &[("cache_type", "mutablestate")]));
    let miss = obs
        .rate("cache_miss", &[("cache_type", "mutablestate")])
        .or_else(|| obs.value_sum("cache_miss", &[("cache_type", "mutablestate")]));
    if let (Some(req), Some(miss)) = (req, miss)
        && req > 0.0
    {
        push(
            "mutable state cache hit ratio".into(),
            1.0 - miss / req,
            r.history.cache_hit_ratio,
            "",
        );
    }
    // sync match ratio
    let ps = obs
        .rate("poll_success", &[])
        .or_else(|| obs.value_sum("poll_success", &[]));
    let pss = obs
        .rate("poll_success_sync", &[])
        .or_else(|| obs.value_sum("poll_success_sync", &[]));
    if let (Some(ps), Some(pss)) = (ps, pss)
        && ps > 0.0
    {
        push(
            "sync match ratio (poll_success_sync/poll_success)".into(),
            pss / ps,
            r.matching.sync_match_ratio,
            "",
        );
    }
    for svc in [Service::Frontend, Service::History, Service::Matching] {
        if let Some(obs_cpu) = crate::calibrate::cpu_cores(obs, svc) {
            let sim: f64 = r
                .services
                .iter()
                .find(|s| s.service == svc.as_str())
                .map(|s| {
                    s.pods
                        .iter()
                        .filter(|p| p.alive)
                        .map(|p| p.cpu_util * p.cpu_cores)
                        .sum()
                })
                .unwrap_or(0.0);
            push(format!("{} CPU cores", svc.as_str()), obs_cpu, sim, "cores");
        }
    }
    if let Some(b) = obs.value_sum("approximate_backlog_count", &[]) {
        push(
            "approximate_backlog_count (sum)".into(),
            b.max(1e-9),
            r.matching.backlog_total_mean,
            "",
        );
    }
    rows
}

/// Re-rank symptoms below their causes and cross-reference them.
fn causal(c: &Ctx2<'_>, out: &mut [Hotspot]) {
    // 1. rate limits rejecting polls → worker schedule-to-start and matching backlog are symptoms
    let poll_rejects: u64 =
        c.r.apis
            .iter()
            .filter(|a| a.api.starts_with("Poll"))
            .flat_map(|a| a.errors.iter())
            .filter(|(k, _)| k.starts_with("ResourceExhausted"))
            .map(|(_, v)| *v)
            .sum();
    if poll_rejects > 0 {
        let symptom_max = out
            .iter()
            .filter(|h| {
                matches!(
                    h.category.as_str(),
                    "workers" | "matching-backlog" | "workflow-tasks"
                )
            })
            .map(|h| h.score)
            .fold(0.0, f64::max);
        let mut limiters = Vec::new();
        for h in out.iter_mut().filter(|h| h.category == "rate-limit") {
            if h.resource.starts_with("frontend.") || h.resource == "matching.rps" {
                h.score = h.score.max(symptom_max + 1.0);
                h.severity = Severity::Critical;
                h.evidence.insert(
                    0,
                    format!("{poll_rejects} SDK polls were rejected; rejected pollers back off 1–10s, so tasks wait in matching although workers have free slots"),
                );
                limiters.push(h.resource.clone());
            }
        }
        if !limiters.is_empty() {
            for h in out
                .iter_mut()
                .filter(|h| matches!(h.category.as_str(), "workers" | "matching-backlog"))
            {
                h.detail = format!(
                    "Likely caused by throttled polls ({}): see that hotspot first. {}",
                    limiters.join(", "),
                    h.detail
                );
            }
        }
    }
    // 2. a saturated database slows every write and read: it is the root cause of the
    //    shard, lock, pool, latency and dispatch symptoms (unless polls are being throttled)
    if c.r.persistence.utilization >= c.crit {
        let symptom_max = out
            .iter()
            .filter(|h| h.category != "database")
            .filter(|h| poll_rejects == 0 || h.category != "rate-limit")
            .map(|h| h.score)
            .fold(0.0, f64::max);
        for h in out.iter_mut().filter(|h| h.category == "database") {
            h.score = h.score.max(symptom_max + 1.0);
        }
        for h in out.iter_mut().filter(|h| {
            matches!(
                h.category.as_str(),
                "shard" | "connection-pool" | "workers" | "matching-backlog" | "workflow-lock"
            )
        }) {
            h.detail = format!(
                "The database is saturated ({} busy), which lengthens every write. {}",
                fmt_pct(c.r.persistence.utilization),
                h.detail
            );
        }
    }
}

/// Rate limiters close to their limit (before rejections start).
fn headroom(c: &Ctx2<'_>, out: &mut Vec<Hotspot>) {
    use std::collections::BTreeMap;
    let rejected: std::collections::BTreeSet<String> =
        c.r.limits.iter().map(|l| l.limiter.clone()).collect();
    let mut by: BTreeMap<String, Vec<(String, f64)>> = BTreeMap::new();
    for s in &c.r.services {
        for p in s.pods.iter().filter(|p| p.alive) {
            for (lim, u) in &p.limit_util {
                if *u >= c.warn {
                    by.entry(lim.clone())
                        .or_default()
                        .push((p.name.clone(), *u));
                }
            }
        }
    }
    for (lim, mut pods) in by {
        let base = lim.split('[').next().unwrap_or(&lim).to_string();
        if rejected.contains(&base) {
            continue; // already reported with its rejections
        }
        pods.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        let worst = pods[0].1;
        let key = base.clone();
        out.push(Hotspot {
            severity: Severity::Warning,
            category: "headroom".into(),
            resource: lim.clone(),
            title: format!("{lim} at {} of its limit on {}", fmt_pct(worst), pods[0].0),
            detail: "Offered requests are close to this pod's rate limit; a modest load increase (or a burst) will start ResourceExhausted rejections here before CPU or the database saturate. Temporal limiters are token buckets (burst 2x the rate for frontend/history/matching RPS, frontend.namespaceBurstRatio for namespaces, system.persistenceQPSBurstRatio for persistence), so short spikes pass but sustained load does not.".into(),
            evidence: pods.iter().take(6).map(|(n, u)| format!("{n}: {} of limit", fmt_pct(*u))).collect(),
            metrics: vec!["service_requests".into(), "service_errors_resource_exhausted".into()],
            knobs: vec![c.knob(&key, "raise the limit, or add pods to spread the load")],
            score: 20.0 + worst * 20.0,
        });
    }
}
