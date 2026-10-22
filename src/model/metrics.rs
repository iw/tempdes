//! Metric collection during a run. Organised by Temporal metric concepts so the report and the
//! Prometheus export can use Temporal's names (`service_latency`, `persistence_latency`,
//! `task_latency_schedule`, `poll_success_sync`, ...).

use std::collections::BTreeMap;

use crate::sim::executor::Time;
use crate::sim::stats::Histogram;

use super::types::*;

#[derive(Clone, Debug, Default)]
pub struct OpStats {
    pub count: u64,
    pub errors: BTreeMap<Err, u64>,
    pub latency: Histogram,
}

impl OpStats {
    pub fn record(&mut self, lat_us: u64, err: Option<Err>) {
        self.count += 1;
        self.latency.record(lat_us);
        if let Some(e) = err {
            *self.errors.entry(e).or_default() += 1;
        }
    }

    pub fn error_count(&self) -> u64 {
        self.errors.values().sum()
    }

    pub fn merge(&mut self, o: &OpStats) {
        self.count += o.count;
        for (k, v) in &o.errors {
            *self.errors.entry(*k).or_default() += v;
        }
        self.latency.merge(&o.latency);
    }
}

#[derive(Clone, Debug, Default)]
pub struct TaskStats {
    pub count: u64,
    /// runs, completed or failed (`task_requests`, one per `Execute`)
    pub runs: u64,
    pub noop: u64,
    pub load_latency: Histogram,
    /// each run's wait from the submission that got it into the scheduler to its start
    /// (`task_latency_schedule`), recorded as the run ends, as `Execute` records it: the
    /// limiter's hold-back is left out, as in Temporal
    pub schedule_latency: Histogram,
    /// how long the enforced task scheduler rate limiter held each submission back, zero when it
    /// admitted it at once (not a Temporal metric). Holds still open are in
    /// [`Metrics::open_holds`].
    pub held_back: Histogram,
    /// each run's wait for a worker of the host scheduler's pool, the part of the scheduling
    /// wait that busy workers make: a run on a workflow's execution queue waits for that
    /// queue's workers instead (not a Temporal metric)
    pub pool_wait: Histogram,
    pub processing: Histogram,
    pub queue_latency: Histogram,
    pub attempts: Histogram,
    pub busy_errors: u64,
    pub throttled_errors: u64,
    /// throttled retries by cause (RPS limit of the called service vs persistence limit ...)
    pub throttled_by: BTreeMap<ReCause, u64>,
    pub other_errors: u64,
    /// refusals by the task scheduler's rate limiter (`task_scheduler_throttled`)
    pub sched_throttled: u64,
    /// runs on a workflow's execution queue (execution queue scheduler)
    pub exec_queue_runs: u64,
}

#[derive(Clone, Debug, Default)]
pub struct WfStats {
    pub started: u64,
    pub start_failed: u64,
    pub completed: u64,
    /// workflows that closed as failed (an activity failed for good)
    pub failed: u64,
    /// workflows the server terminated as their history grew over `limit.historySize.error`
    pub terminated: u64,
    /// workflows whose history grew over `limit.historySize.warn`, and the largest history
    pub over_size_warn: u64,
    pub max_history_bytes: f64,
    pub e2e: Histogram,
    pub wft_completed: u64,
    pub wft_timeouts: u64,
    pub wft_sched_to_start: Histogram,
    pub act_sched_to_start: Histogram,
    pub activities_completed: u64,
    /// failed attempts (RespondActivityTaskFailed)
    pub activity_failures: u64,
    /// activity timeouts that fired, by kind (start-to-close, schedule-to-start,
    /// schedule-to-close, heartbeat), whether retried or not
    pub activity_timeouts: [u64; 4],
    /// activities that failed for good: a timeout that is not retried, retries used up, or a
    /// non-retryable error
    pub activities_failed: u64,
    /// of those, activities that failed with a non-retryable error
    pub activities_non_retryable: u64,
    pub sticky_hits: u64,
    pub sticky_misses: u64,
    /// workflow tasks delivered through the normal queue (first task, after sticky timeouts)
    pub nonsticky_wfts: u64,
    pub sticky_unavailable: u64,
    pub history_pages_fetched: u64,
    pub signals_sent: u64,
    pub signals_failed: u64,
    pub eager_starts: u64,
    /// signal- or update-with-start calls that found the workflow running and only signaled or
    /// updated it
    pub with_start_existing: u64,
    /// update-with-start updates that completed, and the caller's wait for them (from the
    /// call to the outcome, polls included)
    pub updates_completed: u64,
    pub update_latency: Histogram,
    /// update-with-start calls sent again because the update was still waiting when the last
    /// one returned
    pub update_resends: u64,
}

#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct Sample {
    pub t: f64,
    pub started: u64,
    pub completed: u64,
    pub cpu: [Vec<f64>; 4],
    pub db_util: f64,
    pub backlog: u64,
    pub history_pending: u64,
    pub rejections: u64,
    pub api_errors: u64,
    pub running: u64,
}

/// What a worker process's SDK counts, as the Go SDK's metrics count it.
#[derive(Clone, Debug, Default)]
pub struct WorkerStats {
    /// long polls sent, by operation, counted as they are sent
    pub long_polls: BTreeMap<&'static str, u64>,
    /// Workflow Task polls that returned a task, by workflow type
    pub wft_polls_succeeded: BTreeMap<usize, u64>,
    /// Workflow Task polls that returned no task
    pub wft_polls_empty: u64,
    /// Activity Task polls that returned no task
    pub act_polls_empty: u64,
    /// failed polls, by operation and gRPC status code
    pub poll_failures: BTreeMap<(&'static str, &'static str), u64>,
}

#[derive(Clone, Debug, Default)]
pub struct Metrics {
    /// SDK-observed latency per frontend API (end to end, including retries)
    pub client: Vec<OpStats>,
    /// per pod (PodId) per API/op
    pub fe: Vec<Vec<OpStats>>,
    pub hist: Vec<Vec<OpStats>>,
    pub matching: Vec<Vec<OpStats>>,
    pub persist: Vec<OpStats>,
    pub persist_by_pod: Vec<u64>,
    /// calls offered to each pod's persistence rate limiter (`<service>.persistenceMaxQPS`),
    /// rejected ones included. AppendHistoryNodes is not charged: it is part of the
    /// Create/UpdateWorkflowExecution call.
    pub persist_limited_by_pod: Vec<u64>,
    /// calls offered to each pod's namespace persistence limiter, per (pod, namespace)
    pub persist_ns_limited: BTreeMap<(PodId, usize), u64>,
    pub persist_conn_wait: [Histogram; 4],
    pub vis_persist: Vec<OpStats>,
    pub tasks: Vec<TaskStats>,
    pub tasks_by_pod: Vec<u64>,
    /// the enforced task scheduler limiter's holds per namespace, as [`TaskStats::held_back`]
    pub held_back_ns: Vec<Histogram>,
    /// the task scheduler limiter's refusals per namespace (would-be refusals in shadow mode)
    pub sched_throttled_ns: Vec<u64>,
    /// submissions the limiter is holding back now, counted by (task type, namespace, held
    /// since). Kept through the warm-up reset: they are still held.
    pub open_holds: BTreeMap<(usize, usize, Time), u32>,
    pub lock_wait: Histogram,
    pub lock_timeouts: u64,
    pub lock_hold: Histogram,
    pub shard_io_wait: Histogram,
    pub events_cache_hits: u64,
    pub events_cache_misses: u64,
    pub wf: Vec<WfStats>,
    /// per worker process (index into `Ctx::workers`), grown as processes report
    pub workers: Vec<WorkerStats>,
    /// (limiter, where) -> rejections
    pub rejections: BTreeMap<(String, String), u64>,
    pub schedule_actions: u64,
    pub schedule_rate_limited: u64,
    pub schedule_delay: Histogram,
    pub shard_moves: u64,
    pub shard_unavailable_waits: Histogram,
    pub history_long_polls: u64,
    /// frontend requests per (pod, namespace, API), for the `namespace` label Temporal puts on
    /// the frontend's `service_*` metrics
    pub fe_by_ns: BTreeMap<(PodId, usize, Api), OpStats>,
    /// frontend requests per (pod, namespace, visibility?) for namespace limiter headroom
    pub fe_ns_requests: BTreeMap<(PodId, usize, bool), u64>,
    pub samples: Vec<Sample>,
    pub notes: Vec<String>,
}

impl Metrics {
    pub fn new(n_types: usize) -> Self {
        Metrics {
            client: vec![OpStats::default(); Api::ALL.len()],
            persist: vec![OpStats::default(); PersistOp::ALL.len()],
            vis_persist: vec![OpStats::default(); PersistOp::ALL.len()],
            tasks: vec![TaskStats::default(); TaskType::ALL.len()],
            wf: vec![WfStats::default(); n_types],
            ..Default::default()
        }
    }

    fn pod_vec<T: Clone + Default>(v: &mut Vec<Vec<T>>, pod: PodId, n: usize) -> &mut Vec<T> {
        if v.len() <= pod {
            v.resize_with(pod + 1, Vec::new);
        }
        if v[pod].len() < n {
            v[pod].resize(n, T::default());
        }
        &mut v[pod]
    }

    pub fn fe_op(&mut self, pod: PodId, api: Api) -> &mut OpStats {
        &mut Self::pod_vec(&mut self.fe, pod, Api::ALL.len())[api.idx()]
    }

    pub fn hist_op(&mut self, pod: PodId, api: HistApi) -> &mut OpStats {
        &mut Self::pod_vec(&mut self.hist, pod, HistApi::ALL.len())[api.idx()]
    }

    pub fn match_op(&mut self, pod: PodId, api: MatchApi) -> &mut OpStats {
        &mut Self::pod_vec(&mut self.matching, pod, MatchApi::ALL.len())[api.idx()]
    }

    pub fn persist_pod(&mut self, pod: PodId) {
        if self.persist_by_pod.len() <= pod {
            self.persist_by_pod.resize(pod + 1, 0);
        }
        self.persist_by_pod[pod] += 1;
    }

    pub fn persist_limited_pod(&mut self, pod: PodId) {
        if self.persist_limited_by_pod.len() <= pod {
            self.persist_limited_by_pod.resize(pod + 1, 0);
        }
        self.persist_limited_by_pod[pod] += 1;
    }

    pub fn task_pod(&mut self, pod: PodId) {
        if self.tasks_by_pod.len() <= pod {
            self.tasks_by_pod.resize(pod + 1, 0);
        }
        self.tasks_by_pod[pod] += 1;
    }

    /// The task scheduler limiter refused a submission of task type `kind` in namespace `ns`.
    pub fn limiter_refused(&mut self, kind: usize, ns: usize) {
        self.tasks[kind].sched_throttled += 1;
        if self.sched_throttled_ns.len() <= ns {
            self.sched_throttled_ns.resize(ns + 1, 0);
        }
        self.sched_throttled_ns[ns] += 1;
    }

    /// The enforced limiter starts holding back a submission made at `since`.
    pub fn hold_opened(&mut self, kind: usize, ns: usize, since: Time) {
        *self.open_holds.entry((kind, ns, since)).or_default() += 1;
    }

    /// The enforced limiter let a submission made at `since` through `held` later, after
    /// holding it back if `opened`.
    pub fn hold_closed(&mut self, kind: usize, ns: usize, since: Time, opened: bool, held: Time) {
        if opened && let Some(n) = self.open_holds.get_mut(&(kind, ns, since)) {
            *n -= 1;
            if *n == 0 {
                self.open_holds.remove(&(kind, ns, since));
            }
        }
        self.tasks[kind].held_back.record(held);
        if self.held_back_ns.len() <= ns {
            self.held_back_ns.resize_with(ns + 1, Histogram::default);
        }
        self.held_back_ns[ns].record(held);
    }

    /// Worker process `wk`'s SDK counts.
    pub fn worker(&mut self, wk: usize) -> &mut WorkerStats {
        if self.workers.len() <= wk {
            self.workers.resize_with(wk + 1, WorkerStats::default);
        }
        &mut self.workers[wk]
    }

    pub fn reject(&mut self, limiter: &str, place: String) {
        *self
            .rejections
            .entry((limiter.to_string(), place))
            .or_default() += 1;
    }

    /// Clear everything accumulated so far (end of warm-up), keeping shapes.
    pub fn reset(&mut self) {
        let n = self.wf.len();
        let notes = std::mem::take(&mut self.notes);
        let samples = std::mem::take(&mut self.samples);
        let open_holds = std::mem::take(&mut self.open_holds);
        *self = Metrics::new(n);
        self.notes = notes;
        self.samples = samples;
        self.open_holds = open_holds;
    }

    pub fn fe_total(&self, api: Api) -> OpStats {
        let mut s = OpStats::default();
        for pod in &self.fe {
            if let Some(o) = pod.get(api.idx()) {
                s.merge(o);
            }
        }
        s
    }

    pub fn hist_total(&self, api: HistApi) -> OpStats {
        let mut s = OpStats::default();
        for pod in &self.hist {
            if let Some(o) = pod.get(api.idx()) {
                s.merge(o);
            }
        }
        s
    }

    pub fn match_total(&self, api: MatchApi) -> OpStats {
        let mut s = OpStats::default();
        for pod in &self.matching {
            if let Some(o) = pod.get(api.idx()) {
                s.merge(o);
            }
        }
        s
    }
}
