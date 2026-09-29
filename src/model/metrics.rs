//! Metric collection during a run. Organised by Temporal metric concepts so the report and the
//! Prometheus export can use Temporal's names (`service_latency`, `persistence_latency`,
//! `task_latency_schedule`, `poll_success_sync`, ...).

use std::collections::BTreeMap;

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
    pub noop: u64,
    pub load_latency: Histogram,
    pub schedule_latency: Histogram,
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
}

#[derive(Clone, Debug, Default)]
pub struct WfStats {
    pub started: u64,
    pub start_failed: u64,
    pub completed: u64,
    pub e2e: Histogram,
    pub wft_completed: u64,
    pub wft_timeouts: u64,
    pub wft_sched_to_start: Histogram,
    pub act_sched_to_start: Histogram,
    pub activities_completed: u64,
    pub activity_failures: u64,
    pub sticky_hits: u64,
    pub sticky_misses: u64,
    /// workflow tasks delivered through the normal queue (first task, after sticky timeouts)
    pub nonsticky_wfts: u64,
    pub sticky_unavailable: u64,
    pub history_pages_fetched: u64,
    pub signals_sent: u64,
    pub signals_failed: u64,
    pub eager_starts: u64,
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
    pub persist_conn_wait: [Histogram; 4],
    pub vis_persist: Vec<OpStats>,
    pub tasks: Vec<TaskStats>,
    pub tasks_by_pod: Vec<u64>,
    pub lock_wait: Histogram,
    pub lock_timeouts: u64,
    pub lock_hold: Histogram,
    pub shard_io_wait: Histogram,
    pub events_cache_hits: u64,
    pub events_cache_misses: u64,
    pub wf: Vec<WfStats>,
    /// (limiter, where) -> rejections
    pub rejections: BTreeMap<(String, String), u64>,
    pub schedule_actions: u64,
    pub schedule_rate_limited: u64,
    pub schedule_delay: Histogram,
    pub shard_moves: u64,
    pub shard_unavailable_waits: Histogram,
    pub history_long_polls: u64,
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
        *self = Metrics::new(n);
        self.notes = notes;
        self.samples = samples;
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
