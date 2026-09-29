//! Result analysis: turn the simulated cluster's state and metrics into a structured result
//! with ranked hotspots, each tied to the Temporal metrics that would show it in production and
//! the dynamic config / replica knobs that influence it.

pub mod html;
pub mod markdown;
pub mod prom;
pub mod rules;
pub mod text;

use std::collections::BTreeMap;

use serde::Serialize;

use crate::metrics::observed::Observations;
use crate::model::build::RunInfo;
use crate::model::metrics::Sample;
use crate::model::types::*;
use crate::model::world::{Ctx, TqKind, WfStatus};
use crate::sim::stats::Histogram;

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Severity {
    Critical,
    Warning,
    Info,
}

impl Severity {
    pub fn as_str(self) -> &'static str {
        match self {
            Severity::Critical => "CRITICAL",
            Severity::Warning => "WARNING",
            Severity::Info => "INFO",
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct Knob {
    pub key: String,
    pub current: String,
    pub hint: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct Hotspot {
    pub severity: Severity,
    pub category: String,
    pub resource: String,
    pub title: String,
    pub detail: String,
    pub evidence: Vec<String>,
    /// Temporal metrics that would show this in production
    pub metrics: Vec<String>,
    pub knobs: Vec<Knob>,
    /// ranking score (higher = worse)
    pub score: f64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct Lat {
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub max_ms: f64,
    pub mean_ms: f64,
}

impl Lat {
    pub fn of(h: &Histogram) -> Lat {
        Lat {
            p50_ms: h.quantile(0.5) as f64 / 1e3,
            p95_ms: h.quantile(0.95) as f64 / 1e3,
            p99_ms: h.quantile(0.99) as f64 / 1e3,
            max_ms: h.max() as f64 / 1e3,
            mean_ms: h.mean() / 1e3,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct WorkflowResult {
    pub workflow_type: String,
    pub namespace: String,
    pub offered_start_rate: f64,
    pub started_per_s: f64,
    pub completed_per_s: f64,
    pub start_failures: u64,
    pub e2e: Lat,
    pub wft_schedule_to_start: Lat,
    pub activity_schedule_to_start: Lat,
    pub wft_per_s: f64,
    pub wft_timeouts: u64,
    pub sticky_hit_ratio: f64,
    pub sticky_tasks: u64,
    pub nonsticky_wfts: u64,
    pub sticky_worker_unavailable: u64,
    pub history_pages_fetched: u64,
    pub activities_per_s: f64,
    pub activity_failures: u64,
    pub signals_per_s: f64,
    pub signals_failed: u64,
    pub eager_starts: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct ApiResult {
    pub api: String,
    pub per_s: f64,
    pub latency: Lat,
    pub error_rate: f64,
    pub errors: BTreeMap<String, u64>,
}

#[derive(Clone, Debug, Serialize)]
pub struct PodResult {
    pub service: String,
    pub name: String,
    pub addr: String,
    pub alive: bool,
    pub cpu_cores: f64,
    pub cpu_util: f64,
    pub cpu_wait_p99_ms: f64,
    pub requests_per_s: f64,
    pub persistence_per_s: f64,
    pub db_pool_size: u32,
    pub db_pool_util: f64,
    pub db_pool_wait_p99_ms: f64,
    /// history: owned shards; matching: hosted partitions; frontend: client connections
    pub owned: u64,
    pub cache_hit_ratio: Option<f64>,
    pub scheduler_util: Option<[f64; 3]>,
    pub scheduler_wait_p99_ms: Option<[f64; 3]>,
    pub rejections: u64,
    /// offered requests / effective rate limit, per limiter on this pod
    pub limit_util: Vec<(String, f64)>,
    /// the pod's effective persistence limit in calls/s (history: its share of a cluster-wide
    /// limit, by shard ownership); 0 when unlimited
    pub persistence_qps_limit: f64,
}

impl PodResult {
    /// The rate limiter closest to its limit on this pod.
    pub fn top_limit(&self) -> Option<(&str, f64)> {
        self.limit_util
            .iter()
            .max_by(|a, b| a.1.total_cmp(&b.1))
            .map(|(n, u)| (n.as_str(), *u))
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct ServiceResult {
    pub service: String,
    pub replicas: usize,
    pub cpu_mean: f64,
    pub cpu_max: f64,
    pub imbalance: f64,
    pub pods: Vec<PodResult>,
}

#[derive(Clone, Debug, Serialize)]
pub struct PersistOpResult {
    pub op: String,
    pub per_s: f64,
    pub latency: Lat,
    pub rejected: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct PersistenceResult {
    pub store: String,
    pub capacity: u32,
    pub utilization: f64,
    pub queue_wait: Lat,
    pub ops: Vec<PersistOpResult>,
    pub conn_wait: BTreeMap<String, Lat>,
    pub visibility_utilization: f64,
    pub visibility_ops: Vec<PersistOpResult>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ShardResult {
    pub shard: u32,
    pub owner: String,
    pub io_util: f64,
    pub io_wait_p99_ms: f64,
    pub io_queue_max: usize,
    pub writes_per_s: f64,
    pub pending_tasks: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct LockResult {
    pub workflow: String,
    pub shard: u32,
    pub util: f64,
    pub wait_p99_ms: f64,
    pub timeouts: u64,
    pub acquisitions: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct TaskResult {
    pub task_type: String,
    pub per_s: f64,
    pub noop_fraction: f64,
    pub load: Lat,
    pub schedule: Lat,
    pub processing: Lat,
    pub queue: Lat,
    pub mean_attempts: f64,
    pub busy_workflow_retries: u64,
    pub throttled_retries: u64,
    pub throttled_by: BTreeMap<String, u64>,
    pub other_retries: u64,
    /// the task scheduler limiter's refusals (`task_scheduler_throttled`) per second
    pub sched_throttled_per_s: f64,
}

/// The history task scheduler's rate limiter (`history.taskSchedulerEnableRateLimiter`).
#[derive(Clone, Debug, Serialize)]
pub struct TaskSchedulerResult {
    /// `off`, `shadow` (counts refusals, holds nothing back) or `on`
    pub mode: String,
    /// refusals per second (`task_scheduler_throttled`): would-be refusals in shadow mode
    pub throttled_per_s: f64,
    /// refusals per task run; above 1 when refused tasks are retried and refused again
    pub throttled_per_task: f64,
    /// the pod limit in tasks/s, across history pods
    pub pod_qps_min: f64,
    pub pod_qps_max: f64,
    /// refusals by a namespace bucket and by the pod bucket
    pub refused_by_namespace: u64,
    pub refused_by_pod: u64,
    /// the history pod with the most refusals, and its refusals per second
    pub busiest_pod: Option<(String, f64)>,
}

#[derive(Clone, Debug, Serialize)]
pub struct HistoryResult {
    pub num_shards: u32,
    pub shard_io_concurrency: u32,
    pub hot_shards: Vec<ShardResult>,
    pub shard_util_p50: f64,
    pub shard_util_p90: f64,
    pub shard_util_max: f64,
    pub shard_writes_max_over_mean: f64,
    pub shard_io_wait: Lat,
    pub lock_wait: Lat,
    pub lock_timeouts: u64,
    pub hot_workflows: Vec<LockResult>,
    pub cache_hit_ratio: f64,
    pub events_cache_hit_ratio: f64,
    pub tasks: Vec<TaskResult>,
    pub task_scheduler: TaskSchedulerResult,
    pub shards_per_pod: Vec<(String, u64)>,
    pub shard_moves: u64,
    pub shard_unavailable: Lat,
}

#[derive(Clone, Debug, Serialize)]
pub struct PartitionResult {
    pub task_queue: String,
    pub kind: String,
    pub partition: String,
    pub host: String,
    pub adds_per_s: f64,
    pub polls_per_s: f64,
    pub sync_match_ratio: f64,
    pub backlog_mean: f64,
    pub backlog_max: f64,
    pub backlog_now: u64,
    pub pollers_mean: f64,
    pub task_wait: Lat,
    pub forwarded_tasks: u64,
    pub forwarded_polls: u64,
    pub write_rejects: u64,
    pub poll_timeouts: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct MatchingResult {
    pub sync_match_ratio: f64,
    pub partitions: Vec<PartitionResult>,
    pub partitions_per_host: Vec<(String, u64)>,
    pub backlog_total_mean: f64,
    pub backlog_total_max: f64,
}

#[derive(Clone, Debug, Serialize)]
pub struct LimitResult {
    pub limiter: String,
    pub place: String,
    pub rejected: u64,
    pub per_s: f64,
}

#[derive(Clone, Debug, Serialize)]
pub struct ScheduleResult {
    pub actions_per_s: f64,
    pub rate_limited: u64,
    pub action_delay: Lat,
}

#[derive(Clone, Debug, Serialize)]
pub struct ValidationRow {
    pub metric: String,
    pub observed: f64,
    pub simulated: f64,
    pub unit: String,
    pub ratio: f64,
}

#[derive(Clone, Debug, Serialize)]
pub struct ConfigSummary {
    pub replicas: BTreeMap<String, u32>,
    pub cpu: BTreeMap<String, f64>,
    pub num_history_shards: u32,
    pub store: String,
    pub db_capacity: u32,
    pub max_conns: BTreeMap<String, u32>,
    pub effective_dynamic_config: BTreeMap<String, String>,
    pub cpu_cost_scale: BTreeMap<String, f64>,
    /// how SDK clients reach the frontends (`cluster.network.client_lb`)
    pub client_lb: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct RunResult {
    pub scenario: String,
    pub temporal_version: String,
    pub label: String,
    pub warmup_s: f64,
    pub duration_s: f64,
    pub wall_ms: u128,
    pub sim_steps: u64,
    pub config: ConfigSummary,
    pub workflows: Vec<WorkflowResult>,
    pub apis: Vec<ApiResult>,
    pub services: Vec<ServiceResult>,
    pub persistence: PersistenceResult,
    pub history: HistoryResult,
    pub matching: MatchingResult,
    pub limits: Vec<LimitResult>,
    pub schedules: Option<ScheduleResult>,
    pub hotspots: Vec<Hotspot>,
    pub headline: String,
    /// history pod -> [(shard id, IO utilisation)] for the shard map
    pub shard_map: Vec<(String, Vec<(u32, f64)>)>,
    pub samples: Vec<Sample>,
    pub validation: Vec<ValidationRow>,
    pub notes: Vec<String>,
    pub warnings: Vec<String>,
}

impl RunResult {
    /// Highest severity among hotspots.
    pub fn worst(&self) -> Option<Severity> {
        self.hotspots.iter().map(|h| h.severity).min()
    }

    pub fn total_completed_per_s(&self) -> f64 {
        self.workflows.iter().map(|w| w.completed_per_s).sum()
    }

    pub fn total_offered_per_s(&self) -> f64 {
        self.workflows.iter().map(|w| w.offered_start_rate).sum()
    }
}

fn pod_name(svc: Service, ordinal: usize) -> String {
    format!("temporal-{}-{ordinal}", svc.as_str())
}

fn quantile_of(v: &mut [f64], q: f64) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let i = ((v.len() - 1) as f64 * q).round() as usize;
    v[i]
}

pub fn analyze(ctx: &Ctx, info: &RunInfo, obs: Option<&Observations>) -> RunResult {
    analyze_window(ctx, info, obs, ctx.p.duration as f64 / 1e6)
}

/// [`analyze`] with rates computed over `measured_s` seconds of measurement rather than the
/// scenario's full duration: the live view calls this part-way through a run, with the time
/// elapsed since the warm-up reset.
pub fn analyze_window(
    ctx: &Ctx,
    info: &RunInfo,
    obs: Option<&Observations>,
    measured_s: f64,
) -> RunResult {
    let p = &ctx.p;
    let dur = measured_s.max(1e-9);
    let m = ctx.m.borrow();

    // --- workflows -------------------------------------------------------------------------
    let mut workflows = Vec::new();
    for (i, t) in p.wf_types.iter().enumerate() {
        let w = &m.wf[i];
        let sticky_total = w.sticky_hits + w.sticky_misses;
        workflows.push(WorkflowResult {
            workflow_type: t.name.clone(),
            namespace: p.namespaces[t.ns].name.clone(),
            offered_start_rate: t.start_rate,
            started_per_s: w.started as f64 / dur,
            completed_per_s: w.completed as f64 / dur,
            start_failures: w.start_failed,
            e2e: Lat::of(&w.e2e),
            wft_schedule_to_start: Lat::of(&w.wft_sched_to_start),
            activity_schedule_to_start: Lat::of(&w.act_sched_to_start),
            wft_per_s: w.wft_completed as f64 / dur,
            wft_timeouts: w.wft_timeouts,
            sticky_hit_ratio: if sticky_total > 0 {
                w.sticky_hits as f64 / sticky_total as f64
            } else {
                1.0
            },
            sticky_tasks: sticky_total,
            nonsticky_wfts: w.nonsticky_wfts,
            sticky_worker_unavailable: w.sticky_unavailable,
            history_pages_fetched: w.history_pages_fetched,
            activities_per_s: w.activities_completed as f64 / dur,
            activity_failures: w.activity_failures,
            signals_per_s: w.signals_sent as f64 / dur,
            signals_failed: w.signals_failed,
            eager_starts: w.eager_starts,
        });
    }

    // --- client-observed APIs -------------------------------------------------------------
    let mut apis = Vec::new();
    for api in Api::ALL {
        let o = &m.client[api.idx()];
        if o.count == 0 {
            continue;
        }
        apis.push(ApiResult {
            api: api.as_str().into(),
            per_s: o.count as f64 / dur,
            latency: Lat::of(&o.latency),
            error_rate: o.error_count() as f64 / o.count as f64,
            errors: o.errors.iter().map(|(e, n)| (e.label(), *n)).collect(),
        });
    }

    // --- pods / services ----------------------------------------------------------------------
    let pods = ctx.pods.borrow();
    let shards = ctx.shards.borrow();
    let matching = ctx.matching.borrow();
    let mut per_pod_rejections: BTreeMap<String, u64> = BTreeMap::new();
    for ((_, place), n) in &m.rejections {
        let addr = place.split_whitespace().next().unwrap_or("").to_string();
        *per_pod_rejections.entry(addr).or_default() += n;
    }
    let mut services = Vec::new();
    for svc in Service::ALL {
        let mut prs = Vec::new();
        for (id, pod) in pods.iter().enumerate() {
            if pod.svc != svc {
                continue;
            }
            let reqs: u64 = match svc {
                Service::Frontend => {
                    m.fe.get(id)
                        .map(|v| v.iter().map(|o| o.count).sum())
                        .unwrap_or(0)
                }
                Service::History => m
                    .hist
                    .get(id)
                    .map(|v| v.iter().map(|o| o.count).sum())
                    .unwrap_or(0),
                Service::Matching => m
                    .matching
                    .get(id)
                    .map(|v| v.iter().map(|o| o.count).sum())
                    .unwrap_or(0),
                Service::Worker => 0,
            };
            let owned = match svc {
                Service::History => shards.iter().filter(|s| s.owner == id).count() as u64,
                Service::Matching => {
                    matching.parts.iter().filter(|pt| pt.host == id).count() as u64
                }
                Service::Frontend => pod
                    .fe
                    .as_ref()
                    .map(|f| u64::from(f.connections))
                    .unwrap_or(0),
                Service::Worker => ctx
                    .workers
                    .borrow()
                    .iter()
                    .filter(|w| w.host_pod == Some(id))
                    .count() as u64,
            };
            let pool = pod.db_pool.stats();
            let (cache, sched, sched_wait) = match &pod.hist {
                Some(h) => {
                    let s: Vec<_> = h.schedulers.iter().map(|x| x.stats()).collect();
                    (
                        Some(h.cache.hit_ratio()),
                        Some([s[0].utilization, s[1].utilization, s[2].utilization]),
                        Some([
                            s[0].wait_p99_us as f64 / 1e3,
                            s[1].wait_p99_us as f64 / 1e3,
                            s[2].wait_p99_us as f64 / 1e3,
                        ]),
                    )
                }
                None => (None, None, None),
            };
            // rate-limit headroom: offered rate vs the pod's effective limit
            let mut limit_util: Vec<(String, f64)> = Vec::new();
            let rps_rate = pod.rps_limiter.rate();
            if rps_rate > 0.0 && svc != Service::Worker {
                let name = match svc {
                    Service::Frontend => "frontend.rps",
                    Service::History => "history.rps",
                    _ => "matching.rps",
                };
                limit_util.push((name.into(), reqs as f64 / dur / rps_rate));
            }
            let pq = pod.persist_limiter.rate();
            if pq > 0.0 {
                // calls the limiter charges (not AppendHistoryNodes), rejected ones included
                let n = m.persist_limited_by_pod.get(id).copied().unwrap_or(0) as f64;
                limit_util.push((format!("{}.persistenceMaxQPS", svc.as_str()), n / dur / pq));
            }
            if let Some(fe) = &pod.fe {
                for (ni, lim) in fe.ns_limiters.iter().enumerate() {
                    let r = lim.rate();
                    let n = m.fe_ns_requests.get(&(id, ni, false)).copied().unwrap_or(0) as f64;
                    if r > 0.0 && n > 0.0 {
                        limit_util.push((
                            format!("frontend.namespaceRPS[{}]", p.namespaces[ni].name),
                            n / dur / r,
                        ));
                    }
                    let rv = fe.ns_vis_limiters[ni].rate();
                    let nv = m.fe_ns_requests.get(&(id, ni, true)).copied().unwrap_or(0) as f64;
                    if rv > 0.0 && nv > 0.0 {
                        limit_util.push((
                            format!(
                                "frontend.namespaceRPS.visibility[{}]",
                                p.namespaces[ni].name
                            ),
                            nv / dur / rv,
                        ));
                    }
                }
            }
            prs.push(PodResult {
                limit_util,
                persistence_qps_limit: pq.max(0.0),
                service: svc.as_str().into(),
                name: pod_name(svc, pod.ordinal),
                addr: pod.addr.clone(),
                alive: pod.alive,
                cpu_cores: pod.cpu.capacity(),
                cpu_util: pod.cpu.utilization(),
                cpu_wait_p99_ms: pod.cpu.wait.quantile(0.99) as f64 / 1e3,
                requests_per_s: reqs as f64 / dur,
                persistence_per_s: m.persist_by_pod.get(id).copied().unwrap_or(0) as f64 / dur,
                db_pool_size: pool.capacity,
                db_pool_util: pool.utilization,
                db_pool_wait_p99_ms: pool.wait_p99_us as f64 / 1e3,
                owned,
                cache_hit_ratio: cache,
                scheduler_util: sched,
                scheduler_wait_p99_ms: sched_wait,
                rejections: per_pod_rejections.get(&pod.addr).copied().unwrap_or(0),
            });
        }
        let alive: Vec<&PodResult> = prs.iter().filter(|p| p.alive).collect();
        let n = alive.len().max(1) as f64;
        let mean = alive.iter().map(|p| p.cpu_util).sum::<f64>() / n;
        let max = alive.iter().map(|p| p.cpu_util).fold(0.0, f64::max);
        services.push(ServiceResult {
            service: svc.as_str().into(),
            replicas: alive.len(),
            cpu_mean: mean,
            cpu_max: max,
            imbalance: if mean > 0.0 { max / mean } else { 1.0 },
            pods: prs,
        });
    }

    // --- persistence ----------------------------------------------------------------------------
    let db = ctx.db.borrow();
    let mut ops = Vec::new();
    let mut vis_ops = Vec::new();
    for op in PersistOp::ALL {
        let o = &m.persist[op.idx()];
        if o.count > 0 {
            ops.push(PersistOpResult {
                op: op.as_str().into(),
                per_s: (o.count - o.error_count()) as f64 / dur,
                latency: Lat::of(&o.latency),
                rejected: o.error_count(),
            });
        }
        let v = &m.vis_persist[op.idx()];
        if v.count > 0 {
            vis_ops.push(PersistOpResult {
                op: op.as_str().into(),
                per_s: v.count as f64 / dur,
                latency: Lat::of(&v.latency),
                rejected: 0,
            });
        }
    }
    ops.sort_by(|a, b| b.per_s.partial_cmp(&a.per_s).unwrap());
    let mut conn_wait = BTreeMap::new();
    for svc in Service::ALL {
        let h = &m.persist_conn_wait[svc.idx()];
        if h.count() > 0 {
            conn_wait.insert(svc.as_str().to_string(), Lat::of(h));
        }
    }
    let persistence = PersistenceResult {
        store: format!("{:?}", p.store).to_ascii_lowercase(),
        capacity: p.db_capacity,
        utilization: db.servers.utilization(),
        queue_wait: Lat::of(&db.servers.wait),
        ops,
        conn_wait,
        visibility_utilization: db.vis_servers.utilization(),
        visibility_ops: vis_ops,
    };

    // --- history ---------------------------------------------------------------------------------
    let mut shard_map: Vec<(String, Vec<(u32, f64)>)> = Vec::new();
    for (id, pod) in pods.iter().enumerate() {
        if pod.svc != Service::History || !pod.alive {
            continue;
        }
        let list: Vec<(u32, f64)> = shards
            .iter()
            .filter(|s| s.owner == id)
            .map(|s| (s.id, s.io_sem.stats().utilization))
            .collect();
        shard_map.push((
            format!("{} ({})", pod_name(Service::History, pod.ordinal), pod.addr),
            list,
        ));
    }
    let mut shard_rows: Vec<ShardResult> = shards
        .iter()
        .map(|s| {
            let st = s.io_sem.stats();
            ShardResult {
                shard: s.id,
                owner: pods[s.owner].addr.clone(),
                io_util: st.utilization,
                io_wait_p99_ms: st.wait_p99_us as f64 / 1e3,
                io_queue_max: st.max_queue,
                writes_per_s: s.writes as f64 / dur,
                pending_tasks: s
                    .queues
                    .iter()
                    .map(|q| u64::from(q.pending) + q.backlog() as u64)
                    .sum(),
            }
        })
        .collect();
    let mut utils: Vec<f64> = shard_rows.iter().map(|s| s.io_util).collect();
    let util_p50 = quantile_of(&mut utils, 0.5);
    let util_p90 = quantile_of(&mut utils, 0.9);
    let util_max = utils.last().copied().unwrap_or(0.0);
    let writes_mean =
        shard_rows.iter().map(|s| s.writes_per_s).sum::<f64>() / shard_rows.len().max(1) as f64;
    let writes_max = shard_rows
        .iter()
        .map(|s| s.writes_per_s)
        .fold(0.0, f64::max);
    shard_rows.sort_by(|a, b| b.io_util.partial_cmp(&a.io_util).unwrap());
    let top = p.report.top.max(1);
    let hot_shards: Vec<ShardResult> = shard_rows.into_iter().take(top).collect();

    let mut locks: Vec<LockResult> = Vec::new();
    {
        let wfs = ctx.wfs.borrow();
        for (id, w) in wfs.slots.iter().enumerate() {
            let Some(w) = w else { continue };
            if w.status != WfStatus::Running && !w.entity {
                continue;
            }
            let st = w.lock.stats();
            if st.acquisitions < 2 {
                continue;
            }
            locks.push(LockResult {
                workflow: format!("{}-{} (#{id})", p.wf_types[w.wf_type].name, w.key),
                shard: w.shard,
                // lock busy time over the whole measurement window
                util: (st.busy_us / (dur * 1e6)).min(1.0),
                wait_p99_ms: st.wait_p99_us as f64 / 1e3,
                timeouts: st.timeouts,
                acquisitions: st.acquisitions,
            });
        }
    }
    locks.sort_by(|a, b| b.util.partial_cmp(&a.util).unwrap());
    locks.truncate(top);

    let (hits, misses) = pods
        .iter()
        .filter_map(|p| p.hist.as_ref())
        .fold((0u64, 0u64), |a, h| {
            (a.0 + h.cache.hits, a.1 + h.cache.misses)
        });
    let mut tasks = Vec::new();
    for t in TaskType::ALL {
        let o = &m.tasks[t.idx()];
        if o.count == 0 {
            continue;
        }
        tasks.push(TaskResult {
            task_type: t.as_str().into(),
            per_s: o.count as f64 / dur,
            noop_fraction: o.noop as f64 / o.count as f64,
            load: Lat::of(&o.load_latency),
            schedule: Lat::of(&o.schedule_latency),
            processing: Lat::of(&o.processing),
            queue: Lat::of(&o.queue_latency),
            mean_attempts: o.attempts.mean(),
            busy_workflow_retries: o.busy_errors,
            throttled_retries: o.throttled_errors,
            throttled_by: o
                .throttled_by
                .iter()
                .map(|(c, n)| {
                    (
                        c.as_str()
                            .trim_start_matches("RESOURCE_EXHAUSTED_CAUSE_")
                            .to_string(),
                        *n,
                    )
                })
                .collect(),
            other_retries: o.other_errors,
            sched_throttled_per_s: o.sched_throttled as f64 / dur,
        });
    }
    let task_scheduler = {
        let hist: Vec<_> = pods
            .iter()
            .filter(|p| p.alive)
            .filter_map(|p| p.hist.as_ref().map(|h| (p, h)))
            .collect();
        let throttled: u64 = m.tasks.iter().map(|t| t.sched_throttled).sum();
        let runs: u64 = m.tasks.iter().map(|t| t.count).sum();
        let qps = hist.iter().map(|(_, h)| h.sched_limiter.host_rate());
        TaskSchedulerResult {
            mode: match (p.k.task_sched_enabled, p.k.task_sched_shadow) {
                (false, _) => "off",
                (true, true) => "shadow",
                (true, false) => "on",
            }
            .into(),
            throttled_per_s: throttled as f64 / dur,
            throttled_per_task: if runs > 0 {
                throttled as f64 / runs as f64
            } else {
                0.0
            },
            pod_qps_min: qps.clone().fold(f64::INFINITY, f64::min),
            pod_qps_max: qps.fold(0.0, f64::max),
            refused_by_namespace: hist.iter().map(|(_, h)| h.sched_limiter.refused_ns).sum(),
            refused_by_pod: hist.iter().map(|(_, h)| h.sched_limiter.refused_host).sum(),
            busiest_pod: hist
                .iter()
                .max_by_key(|(_, h)| h.sched_throttled)
                .filter(|(_, h)| h.sched_throttled > 0)
                .map(|(p, h)| (p.addr.clone(), h.sched_throttled as f64 / dur)),
        }
    };
    let shards_per_pod: Vec<(String, u64)> = pods
        .iter()
        .enumerate()
        .filter(|(_, p)| p.svc == Service::History && p.alive)
        .map(|(id, p)| {
            (
                p.addr.clone(),
                shards.iter().filter(|s| s.owner == id).count() as u64,
            )
        })
        .collect();
    let history = HistoryResult {
        num_shards: p.num_shards,
        shard_io_concurrency: p.k.shard_io_concurrency,
        hot_shards,
        shard_util_p50: util_p50,
        shard_util_p90: util_p90,
        shard_util_max: util_max,
        shard_writes_max_over_mean: if writes_mean > 0.0 {
            writes_max / writes_mean
        } else {
            1.0
        },
        shard_io_wait: Lat::of(&m.shard_io_wait),
        lock_wait: Lat::of(&m.lock_wait),
        lock_timeouts: m.lock_timeouts,
        task_scheduler,
        hot_workflows: locks,
        cache_hit_ratio: if hits + misses > 0 {
            hits as f64 / (hits + misses) as f64
        } else {
            1.0
        },
        events_cache_hit_ratio: {
            let t = m.events_cache_hits + m.events_cache_misses;
            if t > 0 {
                m.events_cache_hits as f64 / t as f64
            } else {
                1.0
            }
        },
        tasks,
        shards_per_pod,
        shard_moves: m.shard_moves,
        shard_unavailable: Lat::of(&m.shard_unavailable_waits),
    };

    // --- matching --------------------------------------------------------------------------------
    let mut parts = Vec::new();
    let (mut sync, mut asyncm) = (0u64, 0u64);
    let mut backlog_mean_total = 0.0;
    let mut backlog_max_total = 0.0;
    for pt in matching.parts.iter() {
        sync += pt.sync_matches;
        asyncm += pt.async_matches;
        let mut bg = pt.backlog_gauge.clone();
        let bmean = bg.mean();
        backlog_mean_total += bmean;
        backlog_max_total += bg.max();
        let mut pg = pt.pollers_gauge.clone();
        let tq = &p.task_queues[pt.tq];
        let tq_name = format!("{}/{}", p.namespaces[tq.ns].name, tq.name);
        let label = match pt.sticky_of {
            Some(wk) => format!("sticky(worker {wk})"),
            None => pt.part.to_string(),
        };
        let matched = pt.sync_matches + pt.async_matches;
        parts.push(PartitionResult {
            task_queue: tq_name,
            kind: match pt.kind {
                TqKind::Workflow => "Workflow".into(),
                TqKind::Activity => "Activity".into(),
            },
            partition: label,
            host: pods[pt.host].addr.clone(),
            adds_per_s: pt.adds as f64 / dur,
            polls_per_s: pt.polls as f64 / dur,
            sync_match_ratio: if matched > 0 {
                pt.sync_matches as f64 / matched as f64
            } else {
                1.0
            },
            backlog_mean: bmean,
            backlog_max: bg.max(),
            backlog_now: pt.backlog_len(),
            pollers_mean: pg.mean(),
            task_wait: Lat::of(&pt.task_wait),
            forwarded_tasks: pt.forwarded_tasks,
            forwarded_polls: pt.forwarded_polls,
            write_rejects: pt.write_rejects,
            poll_timeouts: pt.poll_timeouts,
        });
    }
    let partitions_per_host: Vec<(String, u64)> = pods
        .iter()
        .enumerate()
        .filter(|(_, p)| p.svc == Service::Matching && p.alive)
        .map(|(id, p)| {
            (
                p.addr.clone(),
                matching
                    .parts
                    .iter()
                    .filter(|pt| pt.host == id && pt.sticky_of.is_none())
                    .count() as u64,
            )
        })
        .collect();
    let matching_res = MatchingResult {
        sync_match_ratio: if sync + asyncm > 0 {
            sync as f64 / (sync + asyncm) as f64
        } else {
            1.0
        },
        partitions: parts,
        partitions_per_host,
        backlog_total_mean: backlog_mean_total,
        backlog_total_max: backlog_max_total,
    };

    // --- limits ------------------------------------------------------------------------------------
    let mut limits: Vec<LimitResult> = m
        .rejections
        .iter()
        .map(|((l, place), n)| LimitResult {
            limiter: l.clone(),
            place: place.clone(),
            rejected: *n,
            per_s: *n as f64 / dur,
        })
        .collect();
    limits.sort_by_key(|l| std::cmp::Reverse(l.rejected));

    let schedules = (!p.schedules.is_empty()).then(|| ScheduleResult {
        actions_per_s: m.schedule_actions as f64 / dur,
        rate_limited: m.schedule_rate_limited,
        action_delay: Lat::of(&m.schedule_delay),
    });

    let mut effective = p.effective_dc.clone();
    effective.retain(|_, v| !v.is_empty());
    let config = ConfigSummary {
        replicas: Service::ALL
            .iter()
            .map(|s| (s.as_str().to_string(), ctx.n_live(*s) as u32))
            .collect(),
        cpu: Service::ALL
            .iter()
            .map(|s| (s.as_str().to_string(), p.cpu[s.idx()]))
            .collect(),
        num_history_shards: p.num_shards,
        store: format!("{:?}", p.store).to_ascii_lowercase(),
        db_capacity: p.db_capacity,
        max_conns: Service::ALL
            .iter()
            .map(|s| (s.as_str().to_string(), p.max_conns[s.idx()]))
            .collect(),
        effective_dynamic_config: effective,
        cpu_cost_scale: Service::ALL
            .iter()
            .map(|s| (s.as_str().to_string(), p.costs.scale[s.idx()]))
            .collect(),
        client_lb: p.client_lb.as_str().to_string(),
    };

    let samples = m.samples.clone();
    let mut notes = p.prov.notes.clone();
    notes.extend(m.notes.iter().cloned());
    let warnings = p.prov.warnings.clone();
    drop(m);
    drop(pods);
    drop(shards);
    drop(matching);
    drop(db);

    let mut result = RunResult {
        scenario: p.name.clone(),
        temporal_version: crate::config::dynamic::registry().temporal_version.clone(),
        label: String::new(),
        warmup_s: p.warmup as f64 / 1e6,
        duration_s: dur,
        wall_ms: info.wall_ms,
        sim_steps: info.polls,
        config,
        workflows,
        apis,
        services,
        persistence,
        history,
        matching: matching_res,
        limits,
        schedules,
        hotspots: Vec::new(),
        headline: String::new(),
        shard_map,
        samples,
        validation: Vec::new(),
        notes,
        warnings,
    };
    result.hotspots = rules::detect(ctx, &result);
    result.headline = rules::headline(&result);
    if let Some(o) = obs {
        result.validation = rules::validate(ctx, &result, o);
    }
    result
}
