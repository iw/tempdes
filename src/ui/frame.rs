//! One frame of the live view: the state of the simulated cluster over the last sampling
//! interval.
//!
//! A frame is computed by differencing the simulator's cumulative counters and histograms
//! between two snapshots, so its rates, utilisations and quantiles describe the interval just
//! simulated rather than the average since warm-up (which is what the report gives). Every
//! counter the simulator resets at the end of warm-up is handled by treating a value smaller
//! than its predecessor as a fresh count.

use std::collections::BTreeMap;

use serde::Serialize;

use crate::model::metrics::{OpStats, WfStats};
use crate::model::types::*;
use crate::model::world::Ctx;
use crate::sim::executor::{Time, now};
use crate::sim::stats::Histogram;

/// Latency quantiles over an interval, in milliseconds, with the number of observations.
#[derive(Clone, Debug, Default, Serialize)]
pub struct Quantiles {
    pub p50_ms: f64,
    pub p99_ms: f64,
    pub n: u64,
}

impl Quantiles {
    fn of(h: &Histogram) -> Self {
        Quantiles {
            p50_ms: h.quantile(0.5) as f64 / 1e3,
            p99_ms: h.quantile(0.99) as f64 / 1e3,
            n: h.count(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Phase {
    /// Before the warm-up reset: the cluster fills up from empty.
    Warmup,
    /// After the reset: what the report measures.
    Measuring,
}

/// The rate limiter closest to its limit on a pod or service.
#[derive(Clone, Debug, Serialize)]
pub struct Limit {
    pub name: String,
    /// offered requests divided by the effective limit; above 1 the limiter rejects
    pub util: f64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct Workload {
    pub offered_per_s: f64,
    pub started_per_s: f64,
    pub completed_per_s: f64,
    pub running: u64,
    pub start_failed_per_s: f64,
    pub signals_per_s: f64,
    pub signals_failed_per_s: f64,
    pub activities_per_s: f64,
}

#[derive(Clone, Debug, Serialize)]
pub struct ServiceFrame {
    pub service: &'static str,
    pub replicas: usize,
    pub cpu_max: f64,
    pub cpu_mean: f64,
    pub req_per_s: f64,
    pub rejected_per_s: f64,
    pub persistence_per_s: f64,
    pub limit: Option<Limit>,
}

#[derive(Clone, Debug, Serialize)]
pub struct PodFrame {
    pub service: &'static str,
    pub name: String,
    pub addr: String,
    pub alive: bool,
    /// CPU utilisation over the interval
    pub cpu: f64,
    pub req_per_s: f64,
    pub rejected_per_s: f64,
    pub persistence_per_s: f64,
    pub pool_util: f64,
    /// history: owned shards; matching: hosted partitions; frontend: client connections;
    /// worker: per-namespace worker processes
    pub owned: u64,
    pub cache_hit: Option<f64>,
    pub scheduler_util: Option<f64>,
    pub limit: Option<Limit>,
}

/// Traffic between two nodes of the topology. Flows come in a fixed order (see [`FLOWS`]).
#[derive(Clone, Debug, Serialize)]
pub struct Flow {
    pub id: &'static str,
    pub from: &'static str,
    pub to: &'static str,
    pub per_s: f64,
    pub rejected_per_s: f64,
    /// the limiter that rejects on this edge
    pub limiter: &'static str,
}

/// Flow ids in the order they appear in [`Frame::flows`].
pub const FLOWS: [&str; 10] = [
    "clients-frontend",
    "workers-frontend",
    "frontend-history",
    "frontend-matching",
    "history-matching",
    "matching-history",
    "history-persistence",
    "matching-persistence",
    "frontend-persistence",
    "worker-frontend",
];

#[derive(Clone, Debug, Serialize)]
pub struct LimitFrame {
    pub limiter: String,
    pub rejected_per_s: f64,
    /// pods (or partitions) rejecting
    pub places: usize,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct PersistenceFrame {
    pub util: f64,
    pub visibility_util: f64,
    pub ops_per_s: f64,
    pub rejected_per_s: f64,
    pub queue_wait: Quantiles,
    /// SQL connection pool utilisation per service
    pub pool_util: BTreeMap<String, f64>,
    pub pool_wait_p99_ms: BTreeMap<String, f64>,
    /// busiest operations: (operation, per second, p99 ms)
    pub top_ops: Vec<(String, f64, f64)>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct HistoryFrame {
    pub shard_io_max: f64,
    pub shard_io_p90: f64,
    pub hottest_shard: u32,
    pub hottest_shard_owner: String,
    pub shards_unavailable: u32,
    pub shard_io_wait: Quantiles,
    pub lock_wait: Quantiles,
    pub lock_timeouts_per_s: f64,
    pub cache_hit: f64,
    pub events_cache_hit: f64,
    /// transfer / visibility tasks persisted or loaded but not yet executed
    pub pending_tasks: u64,
    pub timers_pending: u64,
    pub tasks_per_s: f64,
    pub busy_retries_per_s: f64,
    pub throttled_retries_per_s: f64,
    pub scheduler_util_max: f64,
    pub shard_moves: u64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct MatchingFrame {
    pub backlog: u64,
    pub backlog_max_partition: u64,
    pub sync_match_ratio: f64,
    pub adds_per_s: f64,
    pub polls_per_s: f64,
    pub pollers_waiting: u64,
    pub dispatch: Quantiles,
    pub forwarded_per_s: f64,
    pub write_rejects_per_s: f64,
    pub poll_timeouts_per_s: f64,
    pub partitions: usize,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct WorkersFrame {
    pub processes: usize,
    pub wft_slot_util: f64,
    pub act_slot_util: f64,
    pub outstanding_polls: u64,
    pub sticky_hit_ratio: f64,
    pub nonsticky_per_s: f64,
    pub wft_per_s: f64,
    pub wft_timeouts_per_s: f64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct LatencyFrame {
    pub start: Quantiles,
    pub signal: Quantiles,
    pub respond_wft: Quantiles,
    pub e2e: Quantiles,
    pub wft_schedule_to_start: Quantiles,
    pub activity_schedule_to_start: Quantiles,
    pub api_errors_per_s: f64,
    /// client-observed errors by kind
    pub errors: Vec<(String, f64)>,
}

#[derive(Clone, Debug, Serialize)]
pub struct EventNote {
    pub t: f64,
    pub text: String,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct AnalysisSummary {
    pub seq: u64,
    pub critical: usize,
    pub warning: usize,
    pub headline: String,
    pub measured_s: f64,
}

/// Which history pod owns each shard (sent when it changes).
#[derive(Clone, Debug, Serialize)]
pub struct ShardOwners {
    pub pods: Vec<String>,
    /// index into `pods` per shard, in shard order
    pub owner: Vec<u16>,
}

#[derive(Clone, Debug, Serialize)]
pub struct Frame {
    pub seq: u64,
    pub run: u32,
    /// simulated seconds since the start of the run
    pub t: f64,
    /// length of the sampled interval in simulated seconds
    pub dt: f64,
    pub phase: Phase,
    pub warmup_s: f64,
    pub measured_s: f64,
    pub paused: bool,
    /// requested simulated seconds per wall second; 0 means as fast as possible
    pub speed: f64,
    /// achieved simulated seconds per wall second
    pub actual_speed: f64,
    pub load_scale: f64,
    pub workload: Workload,
    pub services: Vec<ServiceFrame>,
    pub pods: Vec<PodFrame>,
    pub flows: Vec<Flow>,
    pub limits: Vec<LimitFrame>,
    pub rejected_per_s: f64,
    pub persistence: PersistenceFrame,
    pub history: HistoryFrame,
    pub matching: MatchingFrame,
    pub workers: WorkersFrame,
    pub latency: LatencyFrame,
    /// interval IO utilisation per shard, 0..=200
    pub shard_heat: Vec<u8>,
    pub shard_owners: Option<ShardOwners>,
    /// notes the simulator added since the previous frame (timeline events, scaling)
    pub events: Vec<EventNote>,
    pub analysis: AnalysisSummary,
}

/// What the engine knows that the simulation state does not.
pub struct Meta {
    pub seq: u64,
    pub run: u32,
    pub paused: bool,
    pub speed: f64,
    pub actual_speed: f64,
    pub load_scale: f64,
    pub offered_per_s: f64,
    pub analysis: AnalysisSummary,
}

// --- snapshots ---------------------------------------------------------------------------------

#[derive(Clone, Default)]
struct PartCounters {
    adds: u64,
    polls: u64,
    sync: u64,
    asynchronous: u64,
    poll_timeouts: u64,
    write_rejects: u64,
    forwarded: u64,
}

/// Cumulative counters at one instant.
struct Snapshot {
    t: Time,
    notes: usize,
    cpu_done: Vec<f64>,
    pool_busy: Vec<f64>,
    rps_offered: Vec<u64>,
    persist_limited: Vec<u64>,
    persist_by_pod: Vec<u64>,
    fe_ops: Vec<Vec<u64>>,
    hist_ops: Vec<Vec<u64>>,
    match_ops: Vec<Vec<u64>>,
    fe_ns_offered: BTreeMap<(PodId, usize, bool), u64>,
    cache: Vec<(u64, u64)>,
    sched_busy: Vec<[f64; 3]>,
    db_done: f64,
    vis_done: f64,
    db_wait: Histogram,
    rejections: BTreeMap<(String, String), u64>,
    client: Vec<OpStats>,
    persist: Vec<OpStats>,
    conn_wait: [Histogram; 4],
    wf: Vec<WfStats>,
    tasks: Vec<(u64, u64, u64)>,
    lock_wait: Histogram,
    lock_timeouts: u64,
    shard_io_wait: Histogram,
    events_cache: (u64, u64),
    shard_busy: Vec<f64>,
    parts: Vec<PartCounters>,
    task_wait: Histogram,
    worker_wft_busy: Vec<f64>,
    worker_act_busy: Vec<f64>,
    schedule_actions: u64,
}

impl Snapshot {
    fn capture(ctx: &Ctx) -> Snapshot {
        let pods = ctx.pods.borrow();
        let m = ctx.m.borrow();
        let db = ctx.db.borrow();
        let shards = ctx.shards.borrow();
        let matching = ctx.matching.borrow();
        let workers = ctx.workers.borrow();
        let counts = |v: &Vec<Vec<OpStats>>| -> Vec<Vec<u64>> {
            v.iter()
                .map(|ops| ops.iter().map(|o| o.count).collect())
                .collect()
        };
        let mut task_wait = Histogram::default();
        for p in matching.parts.iter() {
            task_wait.merge(&p.task_wait);
        }
        Snapshot {
            t: now(),
            notes: m.notes.len(),
            cpu_done: pods.iter().map(|p| p.cpu.done_abs()).collect(),
            pool_busy: pods.iter().map(|p| p.db_pool.busy_us()).collect(),
            rps_offered: pods
                .iter()
                .map(|p| p.rps_limiter.total_allowed() + p.rps_limiter.total_rejected())
                .collect(),
            persist_limited: m.persist_limited_by_pod.clone(),
            persist_by_pod: m.persist_by_pod.clone(),
            fe_ops: counts(&m.fe),
            hist_ops: counts(&m.hist),
            match_ops: counts(&m.matching),
            fe_ns_offered: m.fe_ns_requests.clone(),
            cache: pods
                .iter()
                .map(|p| {
                    p.hist
                        .as_ref()
                        .map(|h| (h.cache.hits, h.cache.misses))
                        .unwrap_or((0, 0))
                })
                .collect(),
            sched_busy: pods
                .iter()
                .map(|p| match &p.hist {
                    Some(h) => [
                        h.schedulers[0].busy_us(),
                        h.schedulers[1].busy_us(),
                        h.schedulers[2].busy_us(),
                    ],
                    None => [0.0; 3],
                })
                .collect(),
            db_done: db.servers.done_abs(),
            vis_done: db.vis_servers.done_abs(),
            db_wait: db.servers.wait.clone(),
            rejections: m.rejections.clone(),
            client: m.client.clone(),
            persist: m.persist.clone(),
            conn_wait: m.persist_conn_wait.clone(),
            wf: m.wf.clone(),
            tasks: m
                .tasks
                .iter()
                .map(|t| (t.count, t.busy_errors, t.throttled_errors))
                .collect(),
            lock_wait: m.lock_wait.clone(),
            lock_timeouts: m.lock_timeouts,
            shard_io_wait: m.shard_io_wait.clone(),
            events_cache: (m.events_cache_hits, m.events_cache_misses),
            shard_busy: shards.iter().map(|s| s.io_sem.busy_us()).collect(),
            parts: matching
                .parts
                .iter()
                .map(|p| PartCounters {
                    adds: p.adds,
                    polls: p.polls,
                    sync: p.sync_matches,
                    asynchronous: p.async_matches,
                    poll_timeouts: p.poll_timeouts,
                    write_rejects: p.write_rejects,
                    forwarded: p.forwarded_tasks + p.forwarded_polls,
                })
                .collect(),
            task_wait,
            worker_wft_busy: workers.iter().map(|w| w.wft_slots.busy_us()).collect(),
            worker_act_busy: workers.iter().map(|w| w.act_slots.busy_us()).collect(),
            schedule_actions: m.schedule_actions,
        }
    }
}

/// Difference of two cumulative counts, treating a decrease as a reset (end of warm-up).
fn d(now: u64, last: u64) -> u64 {
    if now >= last { now - last } else { now }
}

fn df(now: f64, last: f64) -> f64 {
    if now >= last { now - last } else { now }
}

fn at<T: Copy + Default>(v: &[T], i: usize) -> T {
    v.get(i).copied().unwrap_or_default()
}

fn ratio(num: f64, den: f64) -> f64 {
    if den > 0.0 {
        (num / den).clamp(0.0, 1.0)
    } else {
        0.0
    }
}

fn pod_name(svc: Service, ordinal: usize) -> String {
    format!("temporal-{}-{ordinal}", svc.as_str())
}

/// Produces frames by differencing successive snapshots of one run.
pub struct Sampler {
    prev: Snapshot,
    owners: Vec<PodId>,
}

impl Sampler {
    pub fn new(ctx: &Ctx) -> Self {
        Sampler {
            prev: Snapshot::capture(ctx),
            owners: Vec::new(),
        }
    }

    /// Start the next interval now, keeping the note cursor. Call this right after the
    /// warm-up reset: a frame spanning the reset would count only what happened after it.
    pub fn resync(&mut self, ctx: &Ctx) {
        let notes = self.prev.notes;
        self.prev = Snapshot::capture(ctx);
        self.prev.notes = notes;
    }

    /// The frame for the interval since the previous call (or since construction / the last
    /// [`resync`](Self::resync)).
    #[allow(clippy::too_many_lines)]
    pub fn frame(&mut self, ctx: &Ctx, meta: &Meta) -> Frame {
        let cur = Snapshot::capture(ctx);
        let prev = &self.prev;
        let p = &ctx.p;
        let dt_us = cur.t.saturating_sub(prev.t).max(1) as f64;
        let dt = dt_us / 1e6;
        let per_s = |n: u64| n as f64 / dt;

        let pods = ctx.pods.borrow();
        let shards = ctx.shards.borrow();
        let matching = ctx.matching.borrow();
        let workers = ctx.workers.borrow();
        let m = ctx.m.borrow();

        // --- rejections by limiter and by pod ---------------------------------------------
        let mut rej_by_limiter: BTreeMap<String, (u64, usize)> = BTreeMap::new();
        let mut rej_by_place: BTreeMap<String, u64> = BTreeMap::new();
        for ((limiter, place), n) in &cur.rejections {
            let dn = d(
                *n,
                prev.rejections
                    .get(&(limiter.clone(), place.clone()))
                    .copied()
                    .unwrap_or(0),
            );
            if dn == 0 {
                continue;
            }
            let e = rej_by_limiter.entry(limiter.clone()).or_default();
            e.0 += dn;
            e.1 += 1;
            let addr = place.split_whitespace().next().unwrap_or("").to_string();
            *rej_by_place.entry(addr).or_default() += dn;
        }
        let rej_of =
            |limiter: &str| -> f64 { per_s(rej_by_limiter.get(limiter).map(|e| e.0).unwrap_or(0)) };
        let rej_prefix = |prefix: &str| -> f64 {
            per_s(
                rej_by_limiter
                    .iter()
                    .filter(|(k, _)| k.starts_with(prefix))
                    .map(|(_, e)| e.0)
                    .sum(),
            )
        };
        let mut limits: Vec<LimitFrame> = rej_by_limiter
            .iter()
            .map(|(k, (n, places))| LimitFrame {
                limiter: k.clone(),
                rejected_per_s: per_s(*n),
                places: *places,
            })
            .collect();
        limits.sort_by(|a, b| b.rejected_per_s.total_cmp(&a.rejected_per_s));
        let rejected_per_s = limits.iter().map(|l| l.rejected_per_s).sum();

        // --- pods and services ---------------------------------------------------------------
        let ops_delta = |cur: &[Vec<u64>], prev: &[Vec<u64>], pod: PodId| -> u64 {
            cur.get(pod)
                .map(|v| {
                    v.iter()
                        .enumerate()
                        .map(|(i, n)| {
                            d(
                                *n,
                                prev.get(pod).and_then(|q| q.get(i)).copied().unwrap_or(0),
                            )
                        })
                        .sum()
                })
                .unwrap_or(0)
        };
        let op_delta = |cur: &[Vec<u64>], prev: &[Vec<u64>], pod: PodId, i: usize| -> u64 {
            let n = cur.get(pod).and_then(|v| v.get(i)).copied().unwrap_or(0);
            d(
                n,
                prev.get(pod).and_then(|v| v.get(i)).copied().unwrap_or(0),
            )
        };
        let mut pod_frames = Vec::with_capacity(pods.len());
        let mut fe_api = [0u64; Api::ALL.len()];
        let mut hist_api = [0u64; HistApi::ALL.len()];
        let mut match_api = [0u64; MatchApi::ALL.len()];
        let mut persist_by_svc = [0u64; 4];
        let mut pool_busy_by_svc = [0.0f64; 4];
        let mut pool_cap_by_svc = [0.0f64; 4];
        for (id, pod) in pods.iter().enumerate() {
            let svc = pod.svc;
            let reqs = match svc {
                Service::Frontend => {
                    for (i, n) in fe_api.iter_mut().enumerate() {
                        *n += op_delta(&cur.fe_ops, &prev.fe_ops, id, i);
                    }
                    ops_delta(&cur.fe_ops, &prev.fe_ops, id)
                }
                Service::History => {
                    for (i, n) in hist_api.iter_mut().enumerate() {
                        *n += op_delta(&cur.hist_ops, &prev.hist_ops, id, i);
                    }
                    ops_delta(&cur.hist_ops, &prev.hist_ops, id)
                }
                Service::Matching => {
                    for (i, n) in match_api.iter_mut().enumerate() {
                        *n += op_delta(&cur.match_ops, &prev.match_ops, id, i);
                    }
                    ops_delta(&cur.match_ops, &prev.match_ops, id)
                }
                Service::Worker => 0,
            };
            let persist = d(at(&cur.persist_by_pod, id), at(&prev.persist_by_pod, id));
            persist_by_svc[svc.idx()] += persist;
            let cpu = ratio(
                at(&cur.cpu_done, id) - at(&prev.cpu_done, id),
                dt_us * f64::from(pod.cpu.servers()),
            );
            let pool_busy = df(at(&cur.pool_busy, id), at(&prev.pool_busy, id));
            let pool_cap = f64::from(pod.db_pool.capacity());
            if pod.alive {
                pool_busy_by_svc[svc.idx()] += pool_busy;
                pool_cap_by_svc[svc.idx()] += pool_cap;
            }
            // the limiter closest to its limit
            let mut limit: Option<Limit> = None;
            let mut consider = |name: String, util: f64| {
                if limit.as_ref().is_none_or(|l| util > l.util) {
                    limit = Some(Limit { name, util });
                }
            };
            let rps = pod.rps_limiter.rate();
            if rps > 0.0 && svc != Service::Worker {
                let name = match svc {
                    Service::Frontend => "frontend.rps",
                    Service::History => "history.rps",
                    _ => "matching.rps",
                };
                let offered = d(at(&cur.rps_offered, id), at(&prev.rps_offered, id));
                consider(name.into(), per_s(offered) / rps);
            }
            let pq = pod.persist_limiter.rate();
            if pq > 0.0 {
                let offered = d(at(&cur.persist_limited, id), at(&prev.persist_limited, id));
                consider(
                    format!("{}.persistenceMaxQPS", svc.as_str()),
                    per_s(offered) / pq,
                );
            }
            if let Some(fe) = &pod.fe {
                for (ni, lim) in fe.ns_limiters.iter().enumerate() {
                    let r = lim.rate();
                    if r <= 0.0 {
                        continue;
                    }
                    let key = (id, ni, false);
                    let offered = d(
                        cur.fe_ns_offered.get(&key).copied().unwrap_or(0),
                        prev.fe_ns_offered.get(&key).copied().unwrap_or(0),
                    );
                    if offered > 0 {
                        consider(
                            format!("frontend.namespaceRPS[{}]", p.namespaces[ni].name),
                            per_s(offered) / r,
                        );
                    }
                }
            }
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
                Service::Worker => workers.iter().filter(|w| w.host_pod == Some(id)).count() as u64,
            };
            let cache_hit = pod.hist.as_ref().map(|_| {
                let (h, mi) = at(&cur.cache, id);
                let (ph, pm) = at(&prev.cache, id);
                let (dh, dm) = (d(h, ph), d(mi, pm));
                if dh + dm > 0 {
                    dh as f64 / (dh + dm) as f64
                } else {
                    1.0
                }
            });
            let scheduler_util = pod.hist.as_ref().map(|h| {
                let cb = at(&cur.sched_busy, id);
                let pb = at(&prev.sched_busy, id);
                (0..3)
                    .map(|c| {
                        ratio(
                            df(cb[c], pb[c]),
                            dt_us * f64::from(h.schedulers[c].capacity()),
                        )
                    })
                    .fold(0.0, f64::max)
            });
            pod_frames.push(PodFrame {
                service: svc.as_str(),
                name: pod_name(svc, pod.ordinal),
                addr: pod.addr.clone(),
                alive: pod.alive,
                cpu,
                req_per_s: per_s(reqs),
                rejected_per_s: per_s(rej_by_place.get(&pod.addr).copied().unwrap_or(0)),
                persistence_per_s: per_s(persist),
                pool_util: ratio(pool_busy, dt_us * pool_cap),
                owned,
                cache_hit,
                scheduler_util,
                limit,
            });
        }
        let services: Vec<ServiceFrame> = Service::ALL
            .iter()
            .map(|&svc| {
                let live: Vec<&PodFrame> = pod_frames
                    .iter()
                    .filter(|pf| pf.alive && pf.service == svc.as_str())
                    .collect();
                let n = live.len().max(1) as f64;
                let limit = live
                    .iter()
                    .filter_map(|pf| pf.limit.clone())
                    .max_by(|a, b| a.util.total_cmp(&b.util));
                ServiceFrame {
                    service: svc.as_str(),
                    replicas: live.len(),
                    cpu_max: live.iter().map(|pf| pf.cpu).fold(0.0, f64::max),
                    cpu_mean: live.iter().map(|pf| pf.cpu).sum::<f64>() / n,
                    req_per_s: live.iter().map(|pf| pf.req_per_s).sum(),
                    rejected_per_s: live.iter().map(|pf| pf.rejected_per_s).sum(),
                    persistence_per_s: per_s(persist_by_svc[svc.idx()]),
                    limit,
                }
            })
            .collect();

        // --- flows ---------------------------------------------------------------------------
        let api_sum = |apis: &[Api]| -> u64 { apis.iter().map(|a| fe_api[a.idx()]).sum() };
        let hist_sum = |apis: &[HistApi]| -> u64 { apis.iter().map(|a| hist_api[a.idx()]).sum() };
        let match_sum =
            |apis: &[MatchApi]| -> u64 { apis.iter().map(|a| match_api[a.idx()]).sum() };
        let client_apis = [
            Api::StartWorkflowExecution,
            Api::SignalWorkflowExecution,
            Api::QueryWorkflow,
            Api::DescribeWorkflowExecution,
            Api::PollWorkflowExecutionHistory,
            Api::ListWorkflowExecutions,
            Api::CountWorkflowExecutions,
        ];
        let worker_apis = [
            Api::PollWorkflowTaskQueue,
            Api::PollActivityTaskQueue,
            // workers fetch history to replay after a sticky cache miss
            Api::GetWorkflowExecutionHistory,
            Api::RespondWorkflowTaskCompleted,
            Api::RespondActivityTaskCompleted,
            Api::RespondActivityTaskFailed,
            Api::RecordActivityTaskHeartbeat,
        ];
        let started_by_matching = [
            HistApi::RecordWorkflowTaskStarted,
            HistApi::RecordActivityTaskStarted,
        ];
        let hist_from_frontend: u64 = HistApi::ALL
            .iter()
            .filter(|a| !started_by_matching.contains(a))
            .map(|a| hist_api[a.idx()])
            .sum();
        let flow = |id: &'static str,
                    from: &'static str,
                    to: &'static str,
                    n: u64,
                    rejected_per_s: f64,
                    limiter: &'static str| Flow {
            id,
            from,
            to,
            per_s: per_s(n),
            rejected_per_s,
            limiter,
        };
        let mut flows = vec![
            flow(
                "clients-frontend",
                "clients",
                "frontend",
                api_sum(&client_apis),
                rej_prefix("frontend."),
                "frontend.namespaceRPS / frontend.rps / frontend.namespaceCount",
            ),
            flow(
                "workers-frontend",
                "workers",
                "frontend",
                api_sum(&worker_apis),
                0.0,
                "",
            ),
            flow(
                "frontend-history",
                "frontend",
                "history",
                hist_from_frontend,
                rej_of("history.rps"),
                "history.rps",
            ),
            flow(
                "frontend-matching",
                "frontend",
                "matching",
                match_sum(&[
                    MatchApi::PollWorkflowTaskQueue,
                    MatchApi::PollActivityTaskQueue,
                    MatchApi::QueryWorkflow,
                ]),
                rej_of("matching.rps"),
                "matching.rps",
            ),
            flow(
                "history-matching",
                "history",
                "matching",
                match_sum(&[MatchApi::AddWorkflowTask, MatchApi::AddActivityTask]),
                rej_of("matching.outstandingTaskAppendsThreshold"),
                "matching.outstandingTaskAppendsThreshold",
            ),
            flow(
                "matching-history",
                "matching",
                "history",
                hist_sum(&started_by_matching),
                0.0,
                "",
            ),
            flow(
                "history-persistence",
                "history",
                "persistence",
                persist_by_svc[Service::History.idx()],
                rej_of("history.persistenceMaxQPS"),
                "history.persistenceMaxQPS",
            ),
            flow(
                "matching-persistence",
                "matching",
                "persistence",
                persist_by_svc[Service::Matching.idx()],
                rej_of("matching.persistenceMaxQPS"),
                "matching.persistenceMaxQPS",
            ),
            flow(
                "frontend-persistence",
                "frontend",
                "persistence",
                persist_by_svc[Service::Frontend.idx()],
                rej_of("frontend.persistenceMaxQPS"),
                "frontend.persistenceMaxQPS",
            ),
        ];
        // scheduler workflows on the worker service start workflows through the frontend
        flows.push(flow(
            "worker-frontend",
            "worker",
            "frontend",
            d(cur.schedule_actions, prev.schedule_actions),
            0.0,
            "",
        ));

        // --- workload --------------------------------------------------------------------------
        let mut workload = Workload {
            offered_per_s: meta.offered_per_s,
            running: ctx.wfs.borrow().running() as u64,
            ..Default::default()
        };
        let mut e2e = Histogram::default();
        let mut wft_s2s = Histogram::default();
        let mut act_s2s = Histogram::default();
        let (mut sticky_hits, mut sticky_misses, mut nonsticky, mut wft_done, mut wft_timeouts) =
            (0u64, 0u64, 0u64, 0u64, 0u64);
        for (i, w) in cur.wf.iter().enumerate() {
            let pw = prev.wf.get(i);
            let g = |f: fn(&WfStats) -> u64| d(f(w), pw.map(f).unwrap_or(0));
            workload.started_per_s += per_s(g(|w| w.started));
            workload.completed_per_s += per_s(g(|w| w.completed));
            workload.start_failed_per_s += per_s(g(|w| w.start_failed));
            workload.signals_per_s += per_s(g(|w| w.signals_sent));
            workload.signals_failed_per_s += per_s(g(|w| w.signals_failed));
            workload.activities_per_s += per_s(g(|w| w.activities_completed));
            sticky_hits += g(|w| w.sticky_hits);
            sticky_misses += g(|w| w.sticky_misses);
            nonsticky += g(|w| w.nonsticky_wfts);
            wft_done += g(|w| w.wft_completed);
            wft_timeouts += g(|w| w.wft_timeouts);
            let empty = WfStats::default();
            let pw = pw.unwrap_or(&empty);
            e2e.merge(&w.e2e.since(&pw.e2e));
            wft_s2s.merge(&w.wft_sched_to_start.since(&pw.wft_sched_to_start));
            act_s2s.merge(&w.act_sched_to_start.since(&pw.act_sched_to_start));
        }

        // --- latency ---------------------------------------------------------------------------
        let client_q = |api: Api| -> Quantiles {
            let c = &cur.client[api.idx()];
            match prev.client.get(api.idx()) {
                Some(pc) => Quantiles::of(&c.latency.since(&pc.latency)),
                None => Quantiles::of(&c.latency),
            }
        };
        let mut errors: BTreeMap<String, u64> = BTreeMap::new();
        for (i, c) in cur.client.iter().enumerate() {
            for (e, n) in &c.errors {
                let pn = prev
                    .client
                    .get(i)
                    .and_then(|pc| pc.errors.get(e))
                    .copied()
                    .unwrap_or(0);
                let dn = d(*n, pn);
                if dn > 0 {
                    *errors.entry(e.label()).or_default() += dn;
                }
            }
        }
        let mut errors: Vec<(String, f64)> =
            errors.into_iter().map(|(k, n)| (k, per_s(n))).collect();
        errors.sort_by(|a, b| b.1.total_cmp(&a.1));
        errors.truncate(5);
        let latency = LatencyFrame {
            start: client_q(Api::StartWorkflowExecution),
            signal: client_q(Api::SignalWorkflowExecution),
            respond_wft: client_q(Api::RespondWorkflowTaskCompleted),
            e2e: Quantiles::of(&e2e),
            wft_schedule_to_start: Quantiles::of(&wft_s2s),
            activity_schedule_to_start: Quantiles::of(&act_s2s),
            api_errors_per_s: errors.iter().map(|e| e.1).sum(),
            errors,
        };

        // --- persistence -----------------------------------------------------------------------
        let db = ctx.db.borrow();
        let mut top_ops: Vec<(String, f64, f64)> = PersistOp::ALL
            .iter()
            .filter_map(|op| {
                let c = &cur.persist[op.idx()];
                let pc = &prev.persist[op.idx()];
                let n = d(c.count, pc.count);
                (n > 0).then(|| {
                    (
                        op.as_str().to_string(),
                        per_s(n),
                        c.latency.since(&pc.latency).quantile(0.99) as f64 / 1e3,
                    )
                })
            })
            .collect();
        top_ops.sort_by(|a, b| b.1.total_cmp(&a.1));
        let ops_per_s = top_ops.iter().map(|o| o.1).sum();
        top_ops.truncate(6);
        let mut pool_util = BTreeMap::new();
        let mut pool_wait = BTreeMap::new();
        for svc in Service::ALL {
            if pool_cap_by_svc[svc.idx()] > 0.0 {
                pool_util.insert(
                    svc.as_str().to_string(),
                    ratio(
                        pool_busy_by_svc[svc.idx()],
                        dt_us * pool_cap_by_svc[svc.idx()],
                    ),
                );
            }
            let h = cur.conn_wait[svc.idx()].since(&prev.conn_wait[svc.idx()]);
            if h.count() > 0 {
                pool_wait.insert(svc.as_str().to_string(), h.quantile(0.99) as f64 / 1e3);
            }
        }
        let persistence = PersistenceFrame {
            util: ratio(
                cur.db_done - prev.db_done,
                dt_us * f64::from(db.servers.servers()),
            ),
            visibility_util: ratio(
                cur.vis_done - prev.vis_done,
                dt_us * f64::from(db.vis_servers.servers()),
            ),
            ops_per_s,
            rejected_per_s: rej_prefix("frontend.persistenceMaxQPS")
                + rej_of("history.persistenceMaxQPS")
                + rej_of("matching.persistenceMaxQPS")
                + rej_of("worker.persistenceMaxQPS"),
            queue_wait: Quantiles::of(&cur.db_wait.since(&prev.db_wait)),
            pool_util,
            pool_wait_p99_ms: pool_wait,
            top_ops,
        };

        // --- history ---------------------------------------------------------------------------
        let cap_io = f64::from(p.k.shard_io_concurrency.max(1));
        let mut shard_heat = Vec::with_capacity(shards.len());
        let mut utils: Vec<f64> = Vec::with_capacity(shards.len());
        let (mut hottest, mut hottest_util, mut unavailable) = (0u32, -1.0f64, 0u32);
        let t_now = now();
        for (i, s) in shards.iter().enumerate() {
            let cap = f64::from(s.io_sem.capacity()).max(cap_io.min(1.0));
            let u = ratio(
                df(at(&cur.shard_busy, i), at(&prev.shard_busy, i)),
                dt_us * cap,
            );
            utils.push(u);
            shard_heat.push((u * 200.0).round() as u8);
            if u > hottest_util {
                hottest_util = u;
                hottest = s.id;
            }
            if s.available_at > t_now {
                unavailable += 1;
            }
        }
        let shard_io_p90 = {
            let mut v = utils.clone();
            v.sort_by(f64::total_cmp);
            if v.is_empty() {
                0.0
            } else {
                v[((v.len() - 1) as f64 * 0.9).round() as usize]
            }
        };
        let (mut pending, mut timers) = (0u64, 0u64);
        for s in shards.iter() {
            pending += u64::from(s.queues[0].pending)
                + s.queues[0].unloaded.len() as u64
                + u64::from(s.queues[2].pending)
                + s.queues[2].unloaded.len() as u64;
            timers += u64::from(s.queues[1].pending) + s.queues[1].timers.len() as u64;
        }
        let (mut tasks_n, mut busy, mut throttled) = (0u64, 0u64, 0u64);
        for (i, (n, b, th)) in cur.tasks.iter().enumerate() {
            let (pn, pb, pt) = at(&prev.tasks, i);
            tasks_n += d(*n, pn);
            busy += d(*b, pb);
            throttled += d(*th, pt);
        }
        let owners: Vec<PodId> = shards.iter().map(|s| s.owner).collect();
        let shard_owners = (owners != self.owners).then(|| {
            let mut pod_ids: Vec<PodId> = owners.clone();
            pod_ids.sort_unstable();
            pod_ids.dedup();
            let index = |id: PodId| pod_ids.iter().position(|&x| x == id).unwrap_or(0) as u16;
            ShardOwners {
                pods: pod_ids
                    .iter()
                    .map(|&id| pod_name(pods[id].svc, pods[id].ordinal))
                    .collect(),
                owner: owners.iter().map(|&o| index(o)).collect(),
            }
        });
        self.owners = owners;
        let history = HistoryFrame {
            shard_io_max: hottest_util.max(0.0),
            shard_io_p90,
            hottest_shard: hottest,
            hottest_shard_owner: shards
                .get((hottest as usize).saturating_sub(1))
                .map(|s| pod_name(Service::History, pods[s.owner].ordinal))
                .unwrap_or_default(),
            shards_unavailable: unavailable,
            shard_io_wait: Quantiles::of(&cur.shard_io_wait.since(&prev.shard_io_wait)),
            lock_wait: Quantiles::of(&cur.lock_wait.since(&prev.lock_wait)),
            lock_timeouts_per_s: per_s(d(cur.lock_timeouts, prev.lock_timeouts)),
            cache_hit: {
                let (h, mi): (u64, u64) = pods
                    .iter()
                    .enumerate()
                    .filter(|(_, pd)| pd.hist.is_some())
                    .fold((0, 0), |a, (i, _)| {
                        let (ch, cm) = at(&cur.cache, i);
                        let (ph, pm) = at(&prev.cache, i);
                        (a.0 + d(ch, ph), a.1 + d(cm, pm))
                    });
                if h + mi > 0 {
                    h as f64 / (h + mi) as f64
                } else {
                    1.0
                }
            },
            events_cache_hit: {
                let h = d(cur.events_cache.0, prev.events_cache.0);
                let mi = d(cur.events_cache.1, prev.events_cache.1);
                if h + mi > 0 {
                    h as f64 / (h + mi) as f64
                } else {
                    1.0
                }
            },
            pending_tasks: pending,
            timers_pending: timers,
            tasks_per_s: per_s(tasks_n),
            busy_retries_per_s: per_s(busy),
            throttled_retries_per_s: per_s(throttled),
            scheduler_util_max: pod_frames
                .iter()
                .filter_map(|pf| pf.scheduler_util)
                .fold(0.0, f64::max),
            shard_moves: m.shard_moves,
        };

        // --- matching --------------------------------------------------------------------------
        let (mut adds, mut polls, mut sync, mut asynchronous, mut timeouts, mut wrej, mut fwd) =
            (0u64, 0u64, 0u64, 0u64, 0u64, 0u64, 0u64);
        for (i, c) in cur.parts.iter().enumerate() {
            let pc = prev.parts.get(i).cloned().unwrap_or_default();
            adds += d(c.adds, pc.adds);
            polls += d(c.polls, pc.polls);
            sync += d(c.sync, pc.sync);
            asynchronous += d(c.asynchronous, pc.asynchronous);
            timeouts += d(c.poll_timeouts, pc.poll_timeouts);
            wrej += d(c.write_rejects, pc.write_rejects);
            fwd += d(c.forwarded, pc.forwarded);
        }
        let backlog: u64 = matching.parts.iter().map(|pt| pt.backlog_len()).sum();
        let matching_frame = MatchingFrame {
            backlog,
            backlog_max_partition: matching
                .parts
                .iter()
                .map(|pt| pt.backlog_len())
                .max()
                .unwrap_or(0),
            sync_match_ratio: if sync + asynchronous > 0 {
                sync as f64 / (sync + asynchronous) as f64
            } else {
                1.0
            },
            adds_per_s: per_s(adds),
            polls_per_s: per_s(polls),
            pollers_waiting: matching
                .parts
                .iter()
                .map(|pt| pt.pollers.len() as u64)
                .sum(),
            dispatch: Quantiles::of(&cur.task_wait.since(&prev.task_wait)),
            forwarded_per_s: per_s(fwd),
            write_rejects_per_s: per_s(wrej),
            poll_timeouts_per_s: per_s(timeouts),
            partitions: matching
                .parts
                .iter()
                .filter(|pt| pt.sticky_of.is_none())
                .count(),
        };

        // --- workers ---------------------------------------------------------------------------
        let (mut wft_busy, mut wft_cap, mut act_busy, mut act_cap, mut outstanding) =
            (0.0f64, 0.0f64, 0.0f64, 0.0f64, 0u64);
        for (i, w) in workers.iter().enumerate() {
            if p.fleets[w.fleet].system {
                continue;
            }
            wft_busy += df(at(&cur.worker_wft_busy, i), at(&prev.worker_wft_busy, i));
            wft_cap += f64::from(w.wft_slots.capacity());
            act_busy += df(at(&cur.worker_act_busy, i), at(&prev.worker_act_busy, i));
            act_cap += f64::from(w.act_slots.capacity());
            outstanding += u64::from(w.pending_sticky + w.pending_regular);
        }
        let workers_frame = WorkersFrame {
            processes: workers.iter().filter(|w| !p.fleets[w.fleet].system).count(),
            wft_slot_util: ratio(wft_busy, dt_us * wft_cap),
            act_slot_util: ratio(act_busy, dt_us * act_cap),
            outstanding_polls: outstanding,
            sticky_hit_ratio: if sticky_hits + sticky_misses > 0 {
                sticky_hits as f64 / (sticky_hits + sticky_misses) as f64
            } else {
                1.0
            },
            nonsticky_per_s: per_s(nonsticky),
            wft_per_s: per_s(wft_done),
            wft_timeouts_per_s: per_s(wft_timeouts),
        };

        // --- events ----------------------------------------------------------------------------
        let t = cur.t as f64 / 1e6;
        // notes carry their own `t=12.0s` prefix; indented follow-ups belong to the same event
        let mut event_t = t;
        let events: Vec<EventNote> = m
            .notes
            .iter()
            .skip(prev.notes)
            .map(|n| {
                let text = n.trim();
                let text = match text
                    .strip_prefix("t=")
                    .and_then(|rest| rest.split_once("s "))
                    .and_then(|(num, rest)| num.parse::<f64>().ok().map(|at| (at, rest)))
                {
                    Some((at, rest)) => {
                        event_t = at;
                        rest.trim()
                    }
                    None => text,
                };
                EventNote {
                    t: event_t,
                    text: text.to_string(),
                }
            })
            .collect();

        let warmup_s = p.warmup as f64 / 1e6;
        let frame = Frame {
            seq: meta.seq,
            run: meta.run,
            t,
            dt,
            phase: if cur.t < p.warmup {
                Phase::Warmup
            } else {
                Phase::Measuring
            },
            warmup_s,
            measured_s: (t - warmup_s).max(0.0),
            paused: meta.paused,
            speed: meta.speed,
            actual_speed: meta.actual_speed,
            load_scale: meta.load_scale,
            workload,
            services,
            pods: pod_frames,
            flows,
            limits,
            rejected_per_s,
            persistence,
            history,
            matching: matching_frame,
            workers: workers_frame,
            latency,
            shard_heat,
            shard_owners,
            events,
            analysis: meta.analysis.clone(),
        };
        drop(db);
        self.prev = cur;
        frame
    }
}
