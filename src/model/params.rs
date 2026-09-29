//! Resolved, immutable simulation parameters: scenario + Temporal dynamic config (+ overrides) +
//! calibration, turned into plain typed values so the hot simulation paths never touch strings.

use std::collections::BTreeMap;

use anyhow::{Context, bail};

use crate::config::dynamic::{Constraints, DcValue, DynamicConfig, TaskQueueType};
use crate::config::scenario::*;
use crate::sim::dist::Dist;
use crate::sim::executor::Time;
use crate::util::units::{fmt_bytes, fmt_us};

use super::ring::synthetic_addresses;
use super::types::*;

pub const US: f64 = 1.0;
pub const MS: f64 = 1_000.0;
pub const SEC: f64 = 1_000_000.0;
pub const MIB: f64 = 1024.0 * 1024.0;

/// Where a parameter value came from (for the report's "effective configuration" section).
#[derive(Clone, Debug, Default)]
pub struct Provenance {
    pub notes: Vec<String>,
    pub warnings: Vec<String>,
}

/// CPU cost table (microseconds of CPU per operation on the handling pod).
#[derive(Clone, Debug)]
pub struct Costs {
    pub frontend: [f64; Api::ALL.len()],
    pub frontend_per_command: f64,
    pub history: [f64; HistApi::ALL.len()],
    pub history_per_command: f64,
    pub history_per_event_read: f64,
    pub history_cache_miss: f64,
    pub task: [f64; TaskType::ALL.len()],
    pub task_noop: f64,
    pub queue_load_base: f64,
    pub queue_load_per_task: f64,
    pub matching: [f64; MatchApi::ALL.len()],
    pub matching_dispatch: f64,
    pub matching_backlog_per_task: f64,
    pub matching_forward: f64,
    pub persistence_client: f64,
    pub worker_scheduler_wft: f64,
    /// multiplicative calibration factors per service (applied at use)
    pub scale: [f64; 4],
}

impl Default for Costs {
    fn default() -> Self {
        let mut frontend = [120.0; Api::ALL.len()];
        for api in Api::ALL {
            frontend[api.idx()] = match api {
                Api::StartWorkflowExecution => 180.0,
                Api::SignalWorkflowExecution => 130.0,
                Api::PollWorkflowTaskQueue | Api::PollActivityTaskQueue => 110.0,
                Api::RespondWorkflowTaskCompleted => 160.0,
                Api::RespondActivityTaskCompleted | Api::RespondActivityTaskFailed => 110.0,
                Api::RecordActivityTaskHeartbeat => 90.0,
                Api::QueryWorkflow => 130.0,
                Api::DescribeWorkflowExecution => 110.0,
                Api::GetWorkflowExecutionHistory | Api::PollWorkflowExecutionHistory => 160.0,
                Api::ListWorkflowExecutions => 250.0,
                Api::CountWorkflowExecutions => 180.0,
            };
        }
        let mut history = [350.0; HistApi::ALL.len()];
        for api in HistApi::ALL {
            history[api.idx()] = match api {
                HistApi::StartWorkflowExecution => 650.0,
                HistApi::SignalWorkflowExecution => 420.0,
                HistApi::RecordWorkflowTaskStarted => 420.0,
                HistApi::RecordActivityTaskStarted => 320.0,
                HistApi::RespondWorkflowTaskCompleted => 520.0,
                HistApi::RespondActivityTaskCompleted => 420.0,
                HistApi::RespondActivityTaskFailed => 420.0,
                HistApi::RecordActivityTaskHeartbeat => 260.0,
                HistApi::RecordChildExecutionCompleted => 420.0,
                HistApi::DescribeWorkflowExecution => 260.0,
                HistApi::GetWorkflowExecutionHistory => 220.0,
                HistApi::QueryWorkflow => 300.0,
            };
        }
        let mut task = [250.0; TaskType::ALL.len()];
        for t in TaskType::ALL {
            task[t.idx()] = match t {
                TaskType::TransferWorkflowTask | TaskType::TransferActivityTask => 260.0,
                TaskType::TransferCloseExecution => 320.0,
                TaskType::TransferStartChildExecution => 550.0,
                TaskType::TimerUserTimer => 420.0,
                TaskType::TimerActivityRetryTimer => 350.0,
                TaskType::TimerWorkflowTaskTimeout | TaskType::TimerActivityTimeout => 380.0,
                TaskType::VisibilityStartExecution
                | TaskType::VisibilityUpsertExecution
                | TaskType::VisibilityCloseExecution => 220.0,
            };
        }
        let mut matching = [110.0; MatchApi::ALL.len()];
        for m in MatchApi::ALL {
            matching[m.idx()] = match m {
                MatchApi::AddWorkflowTask | MatchApi::AddActivityTask => 130.0,
                MatchApi::PollWorkflowTaskQueue | MatchApi::PollActivityTaskQueue => 110.0,
                MatchApi::QueryWorkflow => 120.0,
            };
        }
        Costs {
            frontend,
            frontend_per_command: 8.0,
            history,
            history_per_command: 90.0,
            history_per_event_read: 3.0,
            history_cache_miss: 260.0,
            task,
            task_noop: 130.0,
            queue_load_base: 60.0,
            queue_load_per_task: 12.0,
            matching,
            matching_dispatch: 60.0,
            matching_backlog_per_task: 12.0,
            matching_forward: 70.0,
            persistence_client: 45.0,
            worker_scheduler_wft: 450.0,
            scale: [1.0; 4],
        }
    }
}

impl Costs {
    fn apply_overrides(&mut self, ov: &CostOverrides, prov: &mut Provenance) -> anyhow::Result<()> {
        for (svc, ops) in &ov.0 {
            for (op, d) in ops {
                let v = d.0;
                let hit = match svc.as_str() {
                    "frontend" => {
                        if op == "per_command" {
                            self.frontend_per_command = v;
                            true
                        } else if let Some(a) = Api::ALL.iter().find(|a| a.as_str() == op) {
                            self.frontend[a.idx()] = v;
                            true
                        } else {
                            false
                        }
                    }
                    "history" => match op.as_str() {
                        "per_command" => {
                            self.history_per_command = v;
                            true
                        }
                        "per_event_read" => {
                            self.history_per_event_read = v;
                            true
                        }
                        "cache_miss" => {
                            self.history_cache_miss = v;
                            true
                        }
                        "task_noop" => {
                            self.task_noop = v;
                            true
                        }
                        _ => {
                            if let Some(a) = HistApi::ALL.iter().find(|a| a.as_str() == op) {
                                self.history[a.idx()] = v;
                                true
                            } else if let Some(t) = TaskType::ALL.iter().find(|t| t.as_str() == op)
                            {
                                self.task[t.idx()] = v;
                                true
                            } else {
                                false
                            }
                        }
                    },
                    "matching" => match op.as_str() {
                        "dispatch" => {
                            self.matching_dispatch = v;
                            true
                        }
                        "forward" => {
                            self.matching_forward = v;
                            true
                        }
                        "backlog_per_task" => {
                            self.matching_backlog_per_task = v;
                            true
                        }
                        _ => {
                            if let Some(m) = MatchApi::ALL.iter().find(|m| m.as_str() == op) {
                                self.matching[m.idx()] = v;
                                true
                            } else {
                                false
                            }
                        }
                    },
                    "worker" => {
                        if op == "scheduler_workflow_task" {
                            self.worker_scheduler_wft = v;
                            true
                        } else {
                            false
                        }
                    }
                    "persistence" => {
                        if op == "client" {
                            self.persistence_client = v;
                            true
                        } else {
                            false
                        }
                    }
                    _ => bail!("costs: unknown service {svc:?}"),
                };
                if !hit {
                    bail!("costs.{svc}: unknown operation {op:?}");
                }
                prov.notes
                    .push(format!("cpu cost {svc}.{op} = {}", fmt_us(v)));
            }
        }
        Ok(())
    }
}

/// Per task-queue-type matching settings (resolved with task queue precedence).
#[derive(Clone, Debug)]
pub struct TqTypeParams {
    pub read_partitions: u32,
    pub write_partitions: u32,
    pub fwd_max_outstanding_polls: u32,
    pub fwd_max_outstanding_tasks: u32,
    pub fwd_max_rate: f64,
    pub fwd_max_children: u32,
    pub long_poll_expiration: Time,
    pub backlog_negligible_age: Time,
    pub max_wait_for_poller_before_fwd: Time,
    pub get_tasks_batch: u32,
    pub get_tasks_reload_at: u32,
    pub max_task_batch: u32,
    pub outstanding_appends_threshold: u32,
    pub task_delete_interval: Time,
    pub max_task_delete_batch: u32,
    /// Per-partition dispatch limit when explicitly configured (see notes in `matching.rs`).
    pub dispatch_rate: Option<f64>,
}

#[derive(Clone, Debug)]
pub struct TqParams {
    pub ns: usize,
    pub name: String,
    pub wf: TqTypeParams,
    pub act: TqTypeParams,
    /// system per-namespace worker queue (scheduler)
    pub system: bool,
}

#[derive(Clone, Debug)]
pub struct NsParams {
    pub name: String,
    pub id: String,
    pub fe_ns_rps: f64,
    pub fe_global_ns_rps: f64,
    pub fe_ns_burst_ratio: f64,
    pub fe_ns_count: i64,
    pub fe_global_ns_count: i64,
    pub fe_vis_rps: f64,
    pub fe_global_vis_rps: f64,
    pub fe_vis_burst_ratio: f64,
    pub enable_eager_start: bool,
    pub enable_eager_activity: bool,
    /// `history.taskSchedulerNamespaceMaxQPS` (per pod) and
    /// `history.taskSchedulerGlobalNamespaceMaxQPS` (cluster-wide); 0 = fall back
    pub task_sched_ns_max_qps: f64,
    pub task_sched_global_ns_max_qps: f64,
    pub default_wft_timeout: Time,
    pub history_long_poll: Time,
    /// `limit.historySize.error` and `.warn`: a running workflow whose history is larger than
    /// the error limit is terminated at its next update; the warn limit only logs
    pub history_size_error: f64,
    pub history_size_warn: f64,
    pub per_ns_worker_count: u32,
    pub scheduler_start_rps: f64,
    pub scheduler_la_sleep_limit: Time,
    /// host task scheduler channel weights, `[category][priority]` with priorities high, low
    /// and preemptable (`history.*ProcessorSchedulerActiveRoundRobinWeights`)
    pub sched_weights: [[u32; 3]; 3],
    /// namespace persistence limits (0 = fall back to the pod's rate):
    /// `history.persistenceNamespaceMaxQPS`, `history.persistenceGlobalNamespaceMaxQPS` (split
    /// by shard ownership), `history.persistencePerShardNamespaceMaxQPS`, and matching's
    /// per-pod and cluster-wide (divided by pods) forms
    pub hist_persist_ns_qps: f64,
    pub hist_persist_global_ns_qps: f64,
    pub hist_persist_shard_ns_qps: f64,
    pub matching_persist_ns_qps: f64,
    pub matching_persist_global_ns_qps: f64,
}

/// Dynamic config knobs that are global (not namespace / task queue scoped).
#[derive(Clone, Debug)]
pub struct Knobs {
    pub operator_rps_ratio: f64,
    pub persistence_burst_ratio: f64,
    pub ringpop_replica_points: u32,
    // frontend
    pub fe_rps: f64,
    pub fe_global_rps: f64,
    pub fe_persistence_max_qps: f64,
    pub keepalive_max_conn_age: Time,
    pub fe_poll_wait_for_ns_token: bool,
    // history
    pub history_rps: f64,
    pub history_persistence_max_qps: f64,
    pub history_persistence_global_max_qps: f64,
    pub shard_io_concurrency: u32,
    pub cache_max_size: usize,
    pub cache_non_user_lock_timeout: Time,
    pub events_cache_max_bytes: f64,
    pub scheduler_workers: [u32; 3],
    /// history task scheduler rate limiter: `history.taskSchedulerEnableRateLimiter`, shadow
    /// mode, startup delay, and the per-pod and cluster-wide rates (0 = fall back)
    pub task_sched_enabled: bool,
    pub task_sched_shadow: bool,
    pub task_sched_startup_delay: Time,
    pub task_sched_max_qps: f64,
    pub task_sched_global_max_qps: f64,
    /// the execution queue scheduler for busy workflows
    /// (`history.taskSchedulerEnableExecutionQueueScheduler` and its settings)
    pub eqs_enabled: bool,
    pub eqs_max_queues: usize,
    pub eqs_queue_ttl: Time,
    pub eqs_queue_concurrency: u32,
    pub task_batch: [u32; 3],
    pub max_poll_rps: [f64; 3],
    pub max_poll_host_rps: [f64; 3],
    pub ack_interval: [Time; 3],
    pub queue_pending_max: u32,
    pub timer_max_time_shift: Time,
    pub shard_update_min_interval: Time,
    pub shard_update_min_tasks: u32,
    pub acquire_shard_concurrency: u32,
    /// ringpop gossip window before a membership change is acted on everywhere
    pub membership_propagation: Time,
    /// per-shard engine start after acquisition (queue state load, processors)
    pub shard_engine_start: Time,
    // matching
    pub matching_rps: f64,
    pub matching_persistence_max_qps: f64,
    pub matching_persistence_global_max_qps: f64,
    // worker
    pub worker_persistence_max_qps: f64,
    // visibility
    pub es_bulk_actions: u32,
    pub es_flush_interval: Time,
    pub es_workers: u32,
}

#[derive(Clone, Debug)]
pub struct FleetParams {
    pub name: String,
    pub ns: usize,
    pub tq: usize,
    pub processes: u32,
    pub wf_pollers: u32,
    pub act_pollers: u32,
    pub wf_slots: u32,
    pub act_slots: u32,
    pub sticky_cache: u32,
    pub sticky_timeout: Time,
    pub cpu: Option<f64>,
    pub activities_per_second: Option<f64>,
    pub poll_timeout: Time,
    /// deadline of the workers' other SDK calls, retries included
    pub rpc_timeout: Time,
    pub eager_activities: bool,
    /// internal fleet running on the worker service (per-namespace scheduler workers)
    pub system: bool,
}

/// An activity step's timeouts after the server fills them in (`validateAndNormalizeTimeouts`
/// in `chasm/lib/activity/validator.go`); 0 means none.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ActTimeouts {
    pub schedule_to_start: Time,
    pub start_to_close: Time,
    pub schedule_to_close: Time,
    pub heartbeat: Time,
}

impl ActTimeouts {
    /// Normalise as the server does, with no workflow run timeout: a schedule-to-close timeout
    /// bounds and fills in schedule-to-start and start-to-close; without it, start-to-close is
    /// required, and tempdes fills in `default_start_to_close` where an SDK would refuse the
    /// command. The heartbeat timeout never exceeds start-to-close.
    pub fn normalize(
        schedule_to_start: Option<Time>,
        start_to_close: Option<Time>,
        schedule_to_close: Option<Time>,
        heartbeat: Option<Time>,
        default_start_to_close: Time,
    ) -> ActTimeouts {
        let set = |t: Option<Time>| t.filter(|&v| v > 0);
        let (s2s, stc, s2c) = (
            set(schedule_to_start),
            set(start_to_close),
            set(schedule_to_close),
        );
        let (schedule_to_start, start_to_close, schedule_to_close) = match (s2c, stc) {
            (Some(c), _) => (s2s.map_or(c, |v| v.min(c)), stc.map_or(c, |v| v.min(c)), c),
            (None, Some(v)) => (s2s.unwrap_or(0), v, 0),
            (None, None) => (s2s.unwrap_or(0), default_start_to_close, 0),
        };
        ActTimeouts {
            schedule_to_start,
            start_to_close,
            schedule_to_close,
            heartbeat: set(heartbeat).map_or(0, |h| h.min(start_to_close)),
        }
    }
}

/// How one activity's attempts go, drawn when it is scheduled: the attempts before `attempts`
/// fail and are retried, and the last succeeds, or fails with a non-retryable error. An attempt
/// after the last (the last timed out, and was retried) ends the same way.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AttemptPlan {
    /// 0: no plan; each attempt fails at the step's failure rate
    pub attempts: u32,
    pub non_retryable: bool,
}

impl AttemptPlan {
    /// Whether attempt `attempt` fails, and whether its failure is non-retryable; `None` without
    /// a plan.
    pub fn outcome(self, attempt: u32) -> Option<(bool, bool)> {
        (self.attempts > 0).then(|| {
            let last = attempt >= self.attempts;
            (!last || self.non_retryable, last && self.non_retryable)
        })
    }
}

/// The attempt plans of a step's activities (`attempts` and `non_retryable`) and their
/// cumulative shares.
#[derive(Clone, Debug, PartialEq)]
pub struct AttemptsP {
    pub plans: Vec<AttemptPlan>,
    pub cumulative: Vec<f64>,
}

impl AttemptsP {
    /// `None` when neither field is set.
    pub fn new(
        attempts: Option<&AttemptsSpec>,
        non_retryable: Option<&BTreeMap<u32, f64>>,
    ) -> Option<AttemptsP> {
        let plan = |n: u32, non_retryable| AttemptPlan {
            attempts: n.max(1),
            non_retryable,
        };
        let failing: Vec<(AttemptPlan, f64)> = non_retryable
            .into_iter()
            .flatten()
            .map(|(n, s)| (plan(*n, true), *s))
            .collect();
        let rest = 1.0 - failing.iter().map(|p| p.1).sum::<f64>();
        let mut pairs: Vec<(AttemptPlan, f64)> = match (attempts, non_retryable) {
            (None, None) => return None,
            (Some(AttemptsSpec::Shares(m)), _) => {
                m.iter().map(|(n, s)| (plan(*n, false), *s)).collect()
            }
            (Some(AttemptsSpec::Count(n)), _) => vec![(plan(*n, false), rest)],
            (None, Some(_)) => vec![(plan(1, false), rest)],
        };
        pairs.extend(failing);
        pairs.retain(|p| p.1 > 0.0);
        let total: f64 = pairs.iter().map(|p| p.1).sum();
        let mut acc = 0.0;
        let (mut plans, mut cumulative) = (Vec::new(), Vec::new());
        for (p, s) in pairs {
            acc += s / total;
            plans.push(p);
            cumulative.push(acc);
        }
        Some(AttemptsP { plans, cumulative })
    }

    /// The plan at uniform draw `u` in [0, 1).
    pub fn sample(&self, u: f64) -> AttemptPlan {
        let i = self.cumulative.partition_point(|&c| c <= u);
        self.plans[i.min(self.plans.len() - 1)]
    }
}

/// An activity's retry policy with the namespace defaults filled in
/// (`retrypolicy.EnsureDefaults`).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RetryPolicyP {
    pub initial: Time,
    pub coefficient: f64,
    pub max_interval: Time,
    /// attempts in total, the first included; 0 = unlimited
    pub max_attempts: u32,
}

impl RetryPolicyP {
    /// `nextBackoffInterval` (`service/history/workflow/retry.go`): the wait before the attempt
    /// after `attempt`, or `None` when the policy stops: the attempts are used up, or the next
    /// attempt would start after `expiration` (the first schedule plus the schedule-to-close
    /// timeout).
    pub fn next_delay(&self, attempt: u32, now: Time, expiration: Option<Time>) -> Option<Time> {
        if self.max_attempts > 0 && attempt >= self.max_attempts {
            return None;
        }
        let interval = (self.initial as f64 * self.coefficient.powi(attempt as i32 - 1))
            .min(self.max_interval as f64) as Time;
        match expiration {
            Some(e) if now.saturating_add(interval) > e => None,
            _ => Some(interval),
        }
    }
}

#[derive(Clone, Debug)]
pub enum StepP {
    Activity {
        count: u32,
        parallel: bool,
        duration: Dist,
        /// heartbeat interval of the activity code
        heartbeat: Option<Time>,
        failure_rate: f64,
        /// attempt plans, one drawn per activity when scheduled (instead of `failure_rate`)
        attempts: Option<AttemptsP>,
        /// how long a failed attempt runs (default `duration`)
        failed_duration: Option<Box<Dist>>,
        retry: RetryPolicyP,
        timeouts: ActTimeouts,
        on_failure: OnFailure,
        tq: usize,
    },
    LocalActivity {
        count: u32,
        duration: Dist,
    },
    Timer(Dist),
    Child {
        wf_type: usize,
        count: u32,
    },
    WaitSignal {
        count: u32,
        timeout: Option<Time>,
    },
    /// `Activity` and `Child` members started by one workflow task; the step ends when all
    /// `activities` and `children` have
    Parallel {
        members: Vec<StepP>,
        activities: u32,
        children: u32,
    },
}

#[derive(Clone, Debug)]
pub struct WfTypeParams {
    pub name: String,
    pub ns: usize,
    pub tq: usize,
    pub start_rate: f64,
    pub arrival: Arrival,
    pub ramp: Option<(f64, Time)>,
    pub starters: u32,
    pub eager_start: bool,
    pub await_result: bool,
    /// deadline of each start call, retries included
    pub rpc_timeout: Time,
    pub wft_processing: Dist,
    pub replay_per_event: f64,
    pub steps: Vec<StepP>,
    pub payload_bytes: f64,
    /// scheduler (system) workflow type
    pub system_scheduler: bool,
}

impl WfTypeParams {
    /// The activity step an activity runs for: step `step`, or its member `member` when the
    /// step is parallel.
    pub fn activity(&self, step: usize, member: u8) -> Option<&StepP> {
        match self.steps.get(step)? {
            StepP::Parallel { members, .. } => members.get(usize::from(member)),
            s => Some(s),
        }
        .filter(|s| matches!(s, StepP::Activity { .. }))
    }

    /// Whether an activity of step `step` (member `member`) that fails for good fails the
    /// workflow, rather than counting as done (`on_failure: continue`).
    pub fn fails_workflow(&self, step: usize, member: u8) -> bool {
        !matches!(
            self.activity(step, member),
            Some(StepP::Activity {
                on_failure: OnFailure::Continue,
                ..
            })
        )
    }
}

#[derive(Clone, Debug)]
pub struct SignalParams {
    pub wf_type: usize,
    pub rate: f64,
    pub hot: bool,
    pub hot_workflows: u32,
    pub clients: u32,
    pub rpc_timeout: Time,
}

#[derive(Clone, Debug)]
pub struct QueryParams {
    pub wf_type: usize,
    pub rate: f64,
    pub describe: bool,
    pub rpc_timeout: Time,
}

#[derive(Clone, Debug)]
pub struct VisLoadParams {
    pub ns: usize,
    pub rate: f64,
    pub op: VisibilityOp,
    pub rpc_timeout: Time,
}

#[derive(Clone, Debug)]
pub struct ScheduleParams {
    pub ns: usize,
    pub count: u32,
    pub interval: Time,
    pub aligned: bool,
    pub wf_type: usize,
    pub scheduler_type: usize,
}

#[derive(Clone, Debug)]
pub struct EventParams {
    pub at: Time,
    pub label: String,
    pub replicas: Option<ReplicasPatch>,
    pub dc: Vec<(String, DcValue)>,
    pub start_rate: Option<(usize, f64)>,
}

#[derive(Clone, Debug)]
pub struct VisParams {
    pub kind: VisibilityKind,
    pub capacity: u32,
    pub write: Dist,
    pub read: Dist,
    pub bulk: Dist,
}

#[derive(Clone, Debug)]
pub struct Params {
    pub name: String,
    pub seed: u64,
    pub warmup: Time,
    pub duration: Time,
    pub num_shards: u32,
    pub replicas: Replicas,
    pub cpu: [f64; 4],
    pub addresses: [Vec<String>; 4],
    /// Spare addresses used when an event scales a service up.
    pub spare_addresses: [Vec<String>; 4],
    pub net_client: Time, // one way
    pub net_internal: Time,
    pub client_lb: ClientLb,
    pub proxy_latency: Time,
    pub proxy_discovery: Time,
    pub store: StoreKind,
    pub max_conns: [u32; 4],
    pub db_capacity: u32,
    pub db_latency: Vec<Dist>, // indexed by PersistOp
    /// operations whose service time already includes the history append that
    /// Create/UpdateWorkflowExecution carry: set by calibration, since production's
    /// `persistence_latency` measures the whole call (indexed by PersistOp)
    pub db_includes_append: Vec<bool>,
    /// time a statement adds per byte of history it writes, and per byte a
    /// `ReadHistoryBranch` reads (µs)
    pub db_write_us_per_byte: f64,
    pub db_read_us_per_byte: f64,
    pub vis: VisParams,
    pub costs: Costs,
    pub k: Knobs,
    pub namespaces: Vec<NsParams>,
    pub task_queues: Vec<TqParams>,
    pub fleets: Vec<FleetParams>,
    pub wf_types: Vec<WfTypeParams>,
    pub signals: Vec<SignalParams>,
    pub queries: Vec<QueryParams>,
    pub vis_loads: Vec<VisLoadParams>,
    pub schedules: Vec<ScheduleParams>,
    pub events: Vec<EventParams>,
    pub report: ReportSpec,
    pub dc: DynamicConfig,
    pub prov: Provenance,
    /// Effective values of the dynamic config keys this model uses (for reports).
    pub effective_dc: BTreeMap<String, String>,
}

/// Keys the simulator models, grouped for reporting.
pub const MODELED_KEYS: &[&str] = &[
    "frontend.rps",
    "frontend.globalRPS",
    "frontend.namespaceRPS",
    "frontend.globalNamespaceRPS",
    "frontend.namespaceBurstRatio",
    "frontend.namespaceCount",
    "frontend.globalNamespaceCount",
    "frontend.namespaceRPS.visibility",
    "frontend.globalNamespaceRPS.visibility",
    "frontend.namespaceBurstRatio.visibility",
    "frontend.persistenceMaxQPS",
    "frontend.keepAliveMaxConnectionAge",
    "frontend.pollWaitForNamespaceRateLimitToken",
    "system.operatorRPSRatio",
    "system.persistenceQPSBurstRatio",
    "system.ringpopReplicaPoints",
    "system.enableEagerWorkflowStart",
    "system.enableActivityEagerExecution",
    "history.rps",
    "history.persistenceMaxQPS",
    "history.persistenceGlobalMaxQPS",
    "history.persistenceNamespaceMaxQPS",
    "history.persistenceGlobalNamespaceMaxQPS",
    "history.persistencePerShardNamespaceMaxQPS",
    "history.shardIOConcurrency",
    "history.hostLevelCacheMaxSize",
    "history.cacheNonUserContextLockTimeout",
    "history.eventsCacheMaxSizeBytes",
    "limit.historySize.error",
    "limit.historySize.warn",
    "limit.blobSize.error",
    "limit.blobSize.warn",
    "history.transferProcessorSchedulerWorkerCount",
    "history.timerProcessorSchedulerWorkerCount",
    "history.visibilityProcessorSchedulerWorkerCount",
    "history.taskSchedulerEnableRateLimiter",
    "history.taskSchedulerEnableRateLimiterShadowMode",
    "history.taskSchedulerRateLimiterStartupDelay",
    "history.taskSchedulerMaxQPS",
    "history.taskSchedulerGlobalMaxQPS",
    "history.taskSchedulerNamespaceMaxQPS",
    "history.taskSchedulerGlobalNamespaceMaxQPS",
    "history.transferProcessorSchedulerActiveRoundRobinWeights",
    "history.timerProcessorSchedulerActiveRoundRobinWeights",
    "history.visibilityProcessorSchedulerActiveRoundRobinWeights",
    "history.taskSchedulerEnableExecutionQueueScheduler",
    "history.taskSchedulerExecutionQueueSchedulerMaxQueues",
    "history.taskSchedulerExecutionQueueSchedulerQueueTTL",
    "history.taskSchedulerExecutionQueueSchedulerQueueConcurrency",
    "history.transferTaskBatchSize",
    "history.timerTaskBatchSize",
    "history.visibilityTaskBatchSize",
    "history.transferProcessorMaxPollRPS",
    "history.timerProcessorMaxPollRPS",
    "history.visibilityProcessorMaxPollRPS",
    "history.transferProcessorMaxPollHostRPS",
    "history.timerProcessorMaxPollHostRPS",
    "history.visibilityProcessorMaxPollHostRPS",
    "history.transferProcessorUpdateAckInterval",
    "history.timerProcessorUpdateAckInterval",
    "history.visibilityProcessorUpdateAckInterval",
    "history.queuePendingTasksMaxCount",
    "history.timerProcessorMaxTimeShift",
    "history.shardUpdateMinInterval",
    "history.shardUpdateMinTasksCompleted",
    "history.acquireShardConcurrency",
    "system.ringpopApproximateMaxPropagationTime",
    "history.defaultWorkflowTaskTimeout",
    "history.defaultActivityRetryPolicy",
    "history.longPollExpirationInterval",
    "matching.rps",
    "matching.persistenceMaxQPS",
    "matching.persistenceGlobalMaxQPS",
    "matching.persistenceNamespaceMaxQPS",
    "matching.persistenceGlobalNamespaceMaxQPS",
    "matching.numTaskqueueReadPartitions",
    "matching.numTaskqueueWritePartitions",
    "matching.forwarderMaxOutstandingPolls",
    "matching.forwarderMaxOutstandingTasks",
    "matching.forwarderMaxRatePerSecond",
    "matching.forwarderMaxChildrenPerNode",
    "matching.longPollExpirationInterval",
    "matching.backlogNegligibleAge",
    "matching.maxWaitForPollerBeforeFwd",
    "matching.getTasksBatchSize",
    "matching.getTasksReloadAt",
    "matching.maxTaskBatchSize",
    "matching.outstandingTaskAppendsThreshold",
    "matching.taskDeleteInterval",
    "matching.maxTaskDeleteBatchSize",
    "admin.matchingNamespaceTaskqueueToPartitionDispatchRate",
    "admin.matchingNamespaceToPartitionDispatchRate",
    "worker.persistenceMaxQPS",
    "worker.perNamespaceWorkerCount",
    "worker.schedulerNamespaceStartWorkflowRPS",
    "worker.schedulerLocalActivitySleepLimit",
    "worker.ESProcessorBulkActions",
    "worker.ESProcessorFlushInterval",
    "worker.ESProcessorNumOfWorkers",
];

pub const PER_NS_WORKER_TQ: &str = "temporal-sys-per-ns-tq";
pub const SCHEDULER_WF_TYPE: &str = "temporal-sys-scheduler-workflow";

fn dur_t(us: f64) -> Time {
    us.max(0.0).round() as Time
}

/// `history.defaultActivityRetryPolicy`: the retry settings the server applies to fields an
/// activity's retry policy leaves unset.
struct RetryDefaults {
    initial: Time,
    max_interval_coefficient: f64,
    coefficient: f64,
    max_attempts: u32,
}

fn default_retry_policy(dc: &DynamicConfig, prec: &[Constraints]) -> RetryDefaults {
    let mut d = RetryDefaults {
        initial: 1_000_000,
        max_interval_coefficient: 100.0,
        coefficient: 2.0,
        max_attempts: 0,
    };
    let num = |v: &DcValue| match v {
        DcValue::Int(i) => Some(*i as f64),
        DcValue::Float(f) => Some(*f),
        _ => None,
    };
    if let Some(m) = dc.map("history.defaultActivityRetryPolicy", prec) {
        for (k, v) in &m {
            match k.as_str() {
                "InitialIntervalInSeconds" => {
                    let us = match v {
                        DcValue::Str(s) => crate::config::dynamic::parse_temporal_duration(s),
                        other => num(other).map(|secs| secs * SEC),
                    };
                    if let Some(us) = us.filter(|&us| us > 0.0) {
                        d.initial = us as Time;
                    }
                }
                "MaximumIntervalCoefficient" => {
                    if let Some(c) = num(v).filter(|&c| c > 0.0) {
                        d.max_interval_coefficient = c;
                    }
                }
                "BackoffCoefficient" => {
                    if let Some(c) = num(v).filter(|&c| c >= 1.0) {
                        d.coefficient = c;
                    }
                }
                "MaximumAttempts" => {
                    if let Some(n) = num(v).filter(|&n| n >= 0.0) {
                        d.max_attempts = n as u32;
                    }
                }
                _ => {}
            }
        }
    }
    d
}

/// Task scheduler channel weights (high, low, preemptable) for one queue category and
/// namespace. Temporal's default is 10 / 9 / 1 (`DefaultActiveTaskPriorityWeight`). A configured
/// map with an unknown priority name or a non-numeric weight falls back to that default
/// (`ConvertDynamicConfigValueToWeights`), and a priority left out, or given a weight of 0 or
/// less, gets 1 (`DefaultPriorityWeight`).
fn round_robin_weights(dc: &DynamicConfig, key: &str, prec: &[Constraints]) -> [u32; 3] {
    const DEFAULT: [u32; 3] = [10, 9, 1];
    let Some(m) = dc.map(key, prec) else {
        return DEFAULT;
    };
    let mut weights = [1u32; 3];
    for (name, v) in &m {
        let i = match name.as_str() {
            "high" => 0,
            "low" => 1,
            "preemptable" => 2,
            _ => return DEFAULT,
        };
        let w = match v {
            DcValue::Int(n) => *n,
            DcValue::Float(f) => *f as i64,
            _ => return DEFAULT,
        };
        weights[i] = if w > 0 { w as u32 } else { 1 };
    }
    weights
}

impl Params {
    /// Build parameters. `dc` must already contain files, inline values and overrides.
    pub fn build(sc: &Scenario, dc: DynamicConfig, replicas: Replicas) -> anyhow::Result<Params> {
        let mut prov = Provenance::default();
        prov.warnings.extend(dc.warnings.iter().cloned());
        prov.notes.extend(sc.import_notes.iter().cloned());
        let g = DynamicConfig::prec_global();

        // --- namespaces -------------------------------------------------------------------
        let mut ns_names: Vec<String> = sc.namespaces.iter().map(|n| n.name.clone()).collect();
        let mut add_ns = |n: &str| {
            if !ns_names.iter().any(|x| x == n) {
                ns_names.push(n.to_string());
            }
        };
        for w in &sc.workflows {
            add_ns(&w.namespace);
        }
        for f in &sc.workers {
            add_ns(&f.namespace);
        }
        for s in &sc.schedules {
            add_ns(&s.namespace);
        }
        for v in &sc.load.visibility {
            add_ns(&v.namespace);
        }
        if ns_names.is_empty() {
            ns_names.push("default".into());
        }
        let mut namespaces = Vec::new();
        for (i, name) in ns_names.iter().enumerate() {
            let id = sc
                .namespaces
                .iter()
                .find(|n| &n.name == name)
                .and_then(|n| n.id.clone())
                .unwrap_or_else(|| synthetic_uuid(sc.seed, i as u64));
            let p = DynamicConfig::prec_namespace(name);
            namespaces.push(NsParams {
                name: name.clone(),
                id,
                fe_ns_rps: dc.int("frontend.namespaceRPS", &p, 2400) as f64,
                fe_global_ns_rps: dc.int("frontend.globalNamespaceRPS", &p, 0) as f64,
                fe_ns_burst_ratio: dc.float("frontend.namespaceBurstRatio", &p, 2.0).max(1.0),
                fe_ns_count: dc.int("frontend.namespaceCount", &p, 1200),
                fe_global_ns_count: dc.int("frontend.globalNamespaceCount", &p, 0),
                fe_vis_rps: dc.int("frontend.namespaceRPS.visibility", &p, 10) as f64,
                fe_global_vis_rps: dc.int("frontend.globalNamespaceRPS.visibility", &p, 0) as f64,
                fe_vis_burst_ratio: dc
                    .float("frontend.namespaceBurstRatio.visibility", &p, 1.0)
                    .max(1.0),
                enable_eager_start: dc.boolean("system.enableEagerWorkflowStart", &p, true),
                enable_eager_activity: dc.boolean("system.enableActivityEagerExecution", &p, false),
                task_sched_ns_max_qps: dc.int("history.taskSchedulerNamespaceMaxQPS", &p, 0) as f64,
                task_sched_global_ns_max_qps: dc.int(
                    "history.taskSchedulerGlobalNamespaceMaxQPS",
                    &p,
                    0,
                ) as f64,
                default_wft_timeout: dur_t(dc.duration_us(
                    "history.defaultWorkflowTaskTimeout",
                    &p,
                    10.0 * SEC,
                )),
                history_long_poll: dur_t(dc.duration_us(
                    "history.longPollExpirationInterval",
                    &p,
                    20.0 * SEC,
                )),
                history_size_error: dc.int("limit.historySize.error", &p, 50 * 1024 * 1024) as f64,
                history_size_warn: dc.int("limit.historySize.warn", &p, 10 * 1024 * 1024) as f64,
                per_ns_worker_count: dc.int("worker.perNamespaceWorkerCount", &p, 1).max(1) as u32,
                scheduler_start_rps: dc.float(
                    "worker.schedulerNamespaceStartWorkflowRPS",
                    &p,
                    30.0,
                ),
                scheduler_la_sleep_limit: dur_t(dc.duration_us(
                    "worker.schedulerLocalActivitySleepLimit",
                    &p,
                    5.0 * SEC,
                )),
                sched_weights: [
                    "history.transferProcessorSchedulerActiveRoundRobinWeights",
                    "history.timerProcessorSchedulerActiveRoundRobinWeights",
                    "history.visibilityProcessorSchedulerActiveRoundRobinWeights",
                ]
                .map(|key| round_robin_weights(&dc, key, &p)),
                hist_persist_ns_qps: dc.int("history.persistenceNamespaceMaxQPS", &p, 0) as f64,
                hist_persist_global_ns_qps: dc.int(
                    "history.persistenceGlobalNamespaceMaxQPS",
                    &p,
                    0,
                ) as f64,
                hist_persist_shard_ns_qps: dc.int(
                    "history.persistencePerShardNamespaceMaxQPS",
                    &p,
                    0,
                ) as f64,
                matching_persist_ns_qps: dc.int("matching.persistenceNamespaceMaxQPS", &p, 0)
                    as f64,
                matching_persist_global_ns_qps: dc.int(
                    "matching.persistenceGlobalNamespaceMaxQPS",
                    &p,
                    0,
                ) as f64,
            });
        }
        let ns_idx = |n: &str| namespaces.iter().position(|x| x.name == n).unwrap();

        // --- global knobs -----------------------------------------------------------------
        let store = sc.cluster.persistence.store;
        let mut shard_io = dc.int("history.shardIOConcurrency", &g, 1).max(1) as u32;
        if store == StoreKind::Cassandra && shard_io != 1 {
            prov.warnings.push(format!(
                "history.shardIOConcurrency={shard_io} has no effect with Cassandra: Temporal 1.31.0 forces the shard IO semaphore to 1 for Cassandra (service/history/shard/context_impl.go)"
            ));
            shard_io = 1;
        }
        let hist_pqps = dc.int("history.persistenceMaxQPS", &g, 9000) as f64;
        let k = Knobs {
            operator_rps_ratio: dc.float("system.operatorRPSRatio", &g, 0.2),
            persistence_burst_ratio: dc
                .float("system.persistenceQPSBurstRatio", &g, 1.0)
                .max(0.1),
            ringpop_replica_points: dc.int("system.ringpopReplicaPoints", &g, 100).max(1) as u32,
            fe_rps: dc.int("frontend.rps", &g, 2400) as f64,
            fe_global_rps: dc.int("frontend.globalRPS", &g, 0) as f64,
            fe_persistence_max_qps: dc.int("frontend.persistenceMaxQPS", &g, 2000) as f64,
            keepalive_max_conn_age: dur_t(dc.duration_us(
                "frontend.keepAliveMaxConnectionAge",
                &g,
                300.0 * SEC,
            )),
            fe_poll_wait_for_ns_token: dc.boolean(
                "frontend.pollWaitForNamespaceRateLimitToken",
                &DynamicConfig::prec_namespace(&namespaces[0].name),
                false,
            ),
            history_rps: dc.int("history.rps", &g, 3000) as f64,
            history_persistence_max_qps: hist_pqps,
            history_persistence_global_max_qps: dc.int("history.persistenceGlobalMaxQPS", &g, 0)
                as f64,
            shard_io_concurrency: shard_io,
            cache_max_size: dc.int("history.hostLevelCacheMaxSize", &g, 128_000).max(1) as usize,
            cache_non_user_lock_timeout: dur_t(dc.duration_us(
                "history.cacheNonUserContextLockTimeout",
                &g,
                500.0 * MS,
            )),
            events_cache_max_bytes: dc.int("history.eventsCacheMaxSizeBytes", &g, 512 * 1024)
                as f64,
            scheduler_workers: [
                dc.int("history.transferProcessorSchedulerWorkerCount", &g, 512)
                    .max(1) as u32,
                dc.int("history.timerProcessorSchedulerWorkerCount", &g, 512)
                    .max(1) as u32,
                dc.int("history.visibilityProcessorSchedulerWorkerCount", &g, 512)
                    .max(1) as u32,
            ],
            task_sched_enabled: dc.boolean("history.taskSchedulerEnableRateLimiter", &g, false),
            task_sched_shadow: dc.boolean(
                "history.taskSchedulerEnableRateLimiterShadowMode",
                &g,
                true,
            ),
            task_sched_startup_delay: dur_t(dc.duration_us(
                "history.taskSchedulerRateLimiterStartupDelay",
                &g,
                5.0 * SEC,
            )),
            task_sched_max_qps: dc.int("history.taskSchedulerMaxQPS", &g, 0) as f64,
            task_sched_global_max_qps: dc.int("history.taskSchedulerGlobalMaxQPS", &g, 0) as f64,
            eqs_enabled: dc.boolean(
                "history.taskSchedulerEnableExecutionQueueScheduler",
                &g,
                false,
            ),
            eqs_max_queues: dc
                .int(
                    "history.taskSchedulerExecutionQueueSchedulerMaxQueues",
                    &g,
                    500,
                )
                .max(0) as usize,
            eqs_queue_ttl: dur_t(dc.duration_us(
                "history.taskSchedulerExecutionQueueSchedulerQueueTTL",
                &g,
                5.0 * SEC,
            )),
            // values <= 0 are capped to 1
            eqs_queue_concurrency: dc
                .int(
                    "history.taskSchedulerExecutionQueueSchedulerQueueConcurrency",
                    &g,
                    2,
                )
                .max(1) as u32,
            task_batch: [
                dc.int("history.transferTaskBatchSize", &g, 100).max(1) as u32,
                dc.int("history.timerTaskBatchSize", &g, 100).max(1) as u32,
                dc.int("history.visibilityTaskBatchSize", &g, 100).max(1) as u32,
            ],
            max_poll_rps: [
                dc.int("history.transferProcessorMaxPollRPS", &g, 20) as f64,
                dc.int("history.timerProcessorMaxPollRPS", &g, 20) as f64,
                dc.int("history.visibilityProcessorMaxPollRPS", &g, 20) as f64,
            ],
            max_poll_host_rps: {
                let t = dc.int("history.transferProcessorMaxPollHostRPS", &g, 0) as f64;
                let tm = dc.int("history.timerProcessorMaxPollHostRPS", &g, 0) as f64;
                let v = dc.int("history.visibilityProcessorMaxPollHostRPS", &g, 0) as f64;
                [
                    if t > 0.0 { t } else { hist_pqps * 0.3 },
                    if tm > 0.0 { tm } else { hist_pqps * 0.3 },
                    if v > 0.0 { v } else { hist_pqps * 0.15 },
                ]
            },
            ack_interval: [
                dur_t(dc.duration_us("history.transferProcessorUpdateAckInterval", &g, 30.0 * SEC)),
                dur_t(dc.duration_us("history.timerProcessorUpdateAckInterval", &g, 30.0 * SEC)),
                dur_t(dc.duration_us(
                    "history.visibilityProcessorUpdateAckInterval",
                    &g,
                    30.0 * SEC,
                )),
            ],
            queue_pending_max: dc
                .int("history.queuePendingTasksMaxCount", &g, 10_000)
                .max(1) as u32,
            timer_max_time_shift: dur_t(dc.duration_us(
                "history.timerProcessorMaxTimeShift",
                &g,
                1.0 * SEC,
            )),
            shard_update_min_interval: dur_t(dc.duration_us(
                "history.shardUpdateMinInterval",
                &g,
                300.0 * SEC,
            )),
            shard_update_min_tasks: dc
                .int("history.shardUpdateMinTasksCompleted", &g, 1000)
                .max(0) as u32,
            acquire_shard_concurrency: dc.int("history.acquireShardConcurrency", &g, 10).max(1)
                as u32,
            membership_propagation: dur_t(
                dc.duration_us("system.ringpopApproximateMaxPropagationTime", &g, 3.0 * SEC) / 2.0,
            ),
            shard_engine_start: 100_000,
            matching_rps: dc.int("matching.rps", &g, 1200) as f64,
            matching_persistence_max_qps: dc.int("matching.persistenceMaxQPS", &g, 3000) as f64,
            matching_persistence_global_max_qps: dc.int("matching.persistenceGlobalMaxQPS", &g, 0)
                as f64,
            worker_persistence_max_qps: dc.int("worker.persistenceMaxQPS", &g, 500) as f64,
            es_bulk_actions: dc.int("worker.ESProcessorBulkActions", &g, 500).max(1) as u32,
            es_flush_interval: dur_t(dc.duration_us(
                "worker.ESProcessorFlushInterval",
                &g,
                1.0 * SEC,
            )),
            es_workers: dc.int("worker.ESProcessorNumOfWorkers", &g, 2).max(1) as u32,
        };
        if dc.is_set("history.shardIOTimeout") {
            prov.notes.push(
                "history.shardIOTimeout only bounds shard acquisition I/O (UpdateShard/GetOrCreateShard); request writes wait on the caller's deadline".into(),
            );
        }

        // --- task queues ------------------------------------------------------------------
        let mut task_queues: Vec<TqParams> = Vec::new();
        let tq_type_params = |ns: &str,
                              tq: &str,
                              t: TaskQueueType,
                              dc: &DynamicConfig,
                              prov: &mut Provenance| {
            let p = DynamicConfig::prec_task_queue(ns, tq, t);
            let read = dc.int("matching.numTaskqueueReadPartitions", &p, 4).max(1) as u32;
            let write = dc.int("matching.numTaskqueueWritePartitions", &p, 4).max(1) as u32;
            if write > read {
                prov.warnings.push(format!(
                    "{ns}/{tq} {}: numTaskqueueWritePartitions ({write}) > numTaskqueueReadPartitions ({read}) — tasks written to partitions >= {read} are never read",
                    t.as_str()
                ));
            }
            // Dispatch limits are only enforced once the effective value changes from the
            // constructor default (see matching research notes); model them only when set.
            let admin_ns = dc.is_set("admin.matchingNamespaceToPartitionDispatchRate");
            let admin_tq = dc.is_set("admin.matchingNamespaceTaskqueueToPartitionDispatchRate");
            let dispatch = if admin_ns || admin_tq {
                let a = dc.float(
                    "admin.matchingNamespaceToPartitionDispatchRate",
                    &DynamicConfig::prec_namespace(ns),
                    10_000.0,
                );
                let b = dc.float(
                    "admin.matchingNamespaceTaskqueueToPartitionDispatchRate",
                    &p,
                    1000.0,
                );
                Some(a.min(b))
            } else {
                None
            };
            TqTypeParams {
                read_partitions: read,
                write_partitions: write.min(read),
                fwd_max_outstanding_polls: dc
                    .int("matching.forwarderMaxOutstandingPolls", &p, 1)
                    .max(0) as u32,
                fwd_max_outstanding_tasks: dc
                    .int("matching.forwarderMaxOutstandingTasks", &p, 1)
                    .max(0) as u32,
                fwd_max_rate: dc.float("matching.forwarderMaxRatePerSecond", &p, 10.0),
                fwd_max_children: dc
                    .int("matching.forwarderMaxChildrenPerNode", &p, 20)
                    .max(1) as u32,
                long_poll_expiration: dur_t(dc.duration_us(
                    "matching.longPollExpirationInterval",
                    &p,
                    60.0 * SEC,
                )),
                backlog_negligible_age: dur_t(dc.duration_us(
                    "matching.backlogNegligibleAge",
                    &p,
                    5.0 * SEC,
                )),
                max_wait_for_poller_before_fwd: dur_t(dc.duration_us(
                    "matching.maxWaitForPollerBeforeFwd",
                    &p,
                    200.0 * MS,
                )),
                get_tasks_batch: dc.int("matching.getTasksBatchSize", &p, 1000).max(1) as u32,
                get_tasks_reload_at: dc.int("matching.getTasksReloadAt", &p, 100).max(0) as u32,
                max_task_batch: dc.int("matching.maxTaskBatchSize", &p, 100).max(1) as u32,
                outstanding_appends_threshold: dc
                    .int("matching.outstandingTaskAppendsThreshold", &p, 250)
                    .max(1) as u32,
                task_delete_interval: dur_t(dc.duration_us(
                    "matching.taskDeleteInterval",
                    &p,
                    15.0 * SEC,
                )),
                max_task_delete_batch: dc.int("matching.maxTaskDeleteBatchSize", &p, 100).max(1)
                    as u32,
                dispatch_rate: dispatch,
            }
        };
        let tq_index = |ns: &str,
                        tq: &str,
                        system: bool,
                        task_queues: &mut Vec<TqParams>,
                        prov: &mut Provenance|
         -> usize {
            if let Some(i) = task_queues
                .iter()
                .position(|t| namespaces[t.ns].name == ns && t.name == tq)
            {
                return i;
            }
            let mut wf = tq_type_params(ns, tq, TaskQueueType::Workflow, &dc, prov);
            let mut act = tq_type_params(ns, tq, TaskQueueType::Activity, &dc, prov);
            if system && !dc.is_set("matching.numTaskqueueReadPartitions") {
                // per-namespace worker queue defaults to one partition
                for t in [&mut wf, &mut act] {
                    t.read_partitions = 1;
                    t.write_partitions = 1;
                }
            }
            task_queues.push(TqParams {
                ns: ns_idx(ns),
                name: tq.to_string(),
                wf,
                act,
                system,
            });
            task_queues.len() - 1
        };

        // --- worker fleets ----------------------------------------------------------------
        let mut fleets = Vec::new();
        for f in &sc.workers {
            let tq = tq_index(
                &f.namespace,
                &f.task_queue,
                false,
                &mut task_queues,
                &mut prov,
            );
            fleets.push(FleetParams {
                name: f.name.clone(),
                ns: ns_idx(&f.namespace),
                tq,
                processes: f.processes.max(1),
                wf_pollers: f.workflow_pollers.max(1),
                act_pollers: f.activity_pollers,
                wf_slots: f.workflow_slots.max(1),
                act_slots: f.activity_slots.max(1),
                sticky_cache: f.sticky_cache_size,
                sticky_timeout: f.sticky_schedule_to_start_timeout.us(),
                cpu: f.cpu,
                activities_per_second: f.task_queue_activities_per_second,
                poll_timeout: f.poll_timeout.us().max(2_000_000),
                rpc_timeout: f.rpc_timeout.us().max(1_000),
                eager_activities: f.eager_activities,
                system: false,
            });
        }

        // --- workflow types -----------------------------------------------------------------
        let type_names: Vec<String> = sc.workflows.iter().map(|w| w.type_name.clone()).collect();
        let mut wf_types = Vec::new();
        for w in &sc.workflows {
            let tq = tq_index(
                &w.namespace,
                &w.task_queue,
                false,
                &mut task_queues,
                &mut prov,
            );
            let mut steps = Vec::new();
            let retry_defaults =
                default_retry_policy(&dc, &DynamicConfig::prec_namespace(&w.namespace));
            let build = |s: &Step,
                         task_queues: &mut Vec<TqParams>,
                         prov: &mut Provenance|
             -> anyhow::Result<StepP> {
                Ok(match s {
                    Step::Activity(a) => {
                        let duration = a
                            .duration
                            .build()
                            .map_err(|e| anyhow::anyhow!("{}: {e}", w.type_name))?;
                        let failed_duration = a
                            .failed_duration
                            .as_ref()
                            .map(|d| d.build())
                            .transpose()
                            .map_err(|e| {
                                anyhow::anyhow!("{}: failed_duration: {e}", w.type_name)
                            })?;
                        let longest_p99 = failed_duration
                            .as_ref()
                            .map_or(0.0, |d| d.quantile(0.99))
                            .max(duration.quantile(0.99));
                        let heartbeat = a.heartbeat.map(|d| d.us().max(1_000));
                        let timeouts = ActTimeouts::normalize(
                            a.schedule_to_start_timeout.map(|d| d.us()),
                            a.start_to_close_timeout.map(|d| d.us()),
                            a.schedule_to_close_timeout.map(|d| d.us()),
                            a.heartbeat_timeout
                                .map(|d| d.us())
                                .or(heartbeat.map(|h| h * 2)),
                            // no timeouts given: 10 × the p99 of an attempt (successful or
                            // failed), 10s–1h
                            (longest_p99 * 10.0).clamp(10.0e6, 3_600.0e6) as Time,
                        );
                        let initial = a
                            .retry_initial
                            .map(|d| d.us().max(1))
                            .unwrap_or(retry_defaults.initial);
                        let retry = RetryPolicyP {
                            initial,
                            coefficient: a
                                .backoff_coefficient
                                .unwrap_or(retry_defaults.coefficient),
                            max_interval: a.max_interval.map(|d| d.us()).unwrap_or(
                                (initial as f64 * retry_defaults.max_interval_coefficient) as Time,
                            ),
                            max_attempts: a.max_attempts.unwrap_or(retry_defaults.max_attempts),
                        };
                        StepP::Activity {
                            count: a.count.max(1),
                            parallel: a.parallel,
                            duration,
                            heartbeat,
                            failure_rate: a.failure_rate,
                            attempts: AttemptsP::new(a.attempts.as_ref(), a.non_retryable.as_ref()),
                            failed_duration: failed_duration.map(Box::new),
                            retry,
                            timeouts,
                            on_failure: a.on_failure,
                            tq: tq_index(
                                &w.namespace,
                                a.task_queue.as_deref().unwrap_or(&w.task_queue),
                                false,
                                task_queues,
                                prov,
                            ),
                        }
                    }
                    Step::LocalActivity(l) => StepP::LocalActivity {
                        count: l.count.max(1),
                        duration: l
                            .duration
                            .build()
                            .map_err(|e| anyhow::anyhow!("{}: {e}", w.type_name))?,
                    },
                    Step::Timer(d) => StepP::Timer(
                        d.build()
                            .map_err(|e| anyhow::anyhow!("{}: timer: {e}", w.type_name))?,
                    ),
                    Step::ChildWorkflow(c) => StepP::Child {
                        wf_type: type_names
                            .iter()
                            .position(|t| t == &c.workflow_type)
                            .unwrap(),
                        count: c.count.max(1),
                    },
                    Step::WaitSignal(ws) => StepP::WaitSignal {
                        count: ws.count.max(1),
                        timeout: ws.timeout.map(|d| d.us()),
                    },
                    Step::Parallel(_) => {
                        anyhow::bail!("{}: parallel steps don't nest", w.type_name)
                    }
                })
            };
            for s in &w.steps {
                steps.push(match s {
                    Step::Parallel(members) => {
                        let members = members
                            .iter()
                            .map(|m| build(m, &mut task_queues, &mut prov))
                            .collect::<anyhow::Result<Vec<_>>>()?;
                        let (mut activities, mut children) = (0, 0);
                        for m in &members {
                            match m {
                                StepP::Activity { count, .. } => activities += count,
                                StepP::Child { count, .. } => children += count,
                                _ => {}
                            }
                        }
                        StepP::Parallel {
                            members,
                            activities,
                            children,
                        }
                    }
                    s => build(s, &mut task_queues, &mut prov)?,
                });
            }
            let ns = ns_idx(&w.namespace);
            // a payload over `limit.blobSize.error` is rejected (`blobSizeChecker`), one over
            // `limit.blobSize.warn` logged
            let prec = DynamicConfig::prec_namespace(&w.namespace);
            let blob_error = dc.int("limit.blobSize.error", &prec, 2 * 1024 * 1024) as f64;
            let blob_warn = dc.int("limit.blobSize.warn", &prec, 512 * 1024) as f64;
            if w.payload_bytes.0 > blob_error {
                bail!(
                    "{}: payload_bytes {} is over limit.blobSize.error ({}) for namespace {}: the server rejects each such payload",
                    w.type_name,
                    fmt_bytes(w.payload_bytes.0),
                    fmt_bytes(blob_error),
                    w.namespace
                );
            }
            if w.payload_bytes.0 > blob_warn {
                prov.warnings.push(format!(
                    "{}: payload_bytes {} is over limit.blobSize.warn ({}): the server logs a warning for each such payload",
                    w.type_name,
                    fmt_bytes(w.payload_bytes.0),
                    fmt_bytes(blob_warn)
                ));
            }
            if w.eager_start && !namespaces[ns].enable_eager_start {
                prov.warnings.push(format!(
                    "{}: eager_start requested but system.enableEagerWorkflowStart is false for namespace {} — starts go through matching",
                    w.type_name, w.namespace
                ));
            }
            wf_types.push(WfTypeParams {
                name: w.type_name.clone(),
                ns,
                tq,
                start_rate: w.start_rate.map(|r| r.0).unwrap_or(0.0),
                arrival: w.arrival,
                ramp: w
                    .ramp
                    .as_ref()
                    .map(|r| (r.from.clamp(0.0, 1.0), r.over.us())),
                starters: w.starters.max(1),
                eager_start: w.eager_start && namespaces[ns].enable_eager_start,
                await_result: w.await_result,
                rpc_timeout: w.rpc_timeout.us().max(1_000),
                wft_processing: w.wft_processing.build().map_err(|e| anyhow::anyhow!(e))?,
                replay_per_event: w.replay_per_event.0,
                steps,
                payload_bytes: w.payload_bytes.0.max(16.0),
                system_scheduler: false,
            });
        }
        for f in &fleets {
            if f.eager_activities && !namespaces[f.ns].enable_eager_activity {
                prov.warnings.push(format!(
                    "fleet {}: eager_activities requested but system.enableActivityEagerExecution is false for namespace {}",
                    f.name, namespaces[f.ns].name
                ));
            }
        }

        // --- schedules (per-namespace scheduler workflows on the worker service) --------------
        let mut schedules = Vec::new();
        for s in &sc.schedules {
            let ns = ns_idx(&s.namespace);
            let sys_tq = tq_index(
                &s.namespace,
                PER_NS_WORKER_TQ,
                true,
                &mut task_queues,
                &mut prov,
            );
            // one system fleet per namespace
            if !fleets.iter().any(|f| f.system && f.ns == ns) {
                fleets.push(FleetParams {
                    name: format!("per-ns-worker/{}", s.namespace),
                    ns,
                    tq: sys_tq,
                    processes: namespaces[ns].per_ns_worker_count,
                    wf_pollers: 4,
                    act_pollers: 4,
                    wf_slots: 1000,
                    act_slots: 1000,
                    sticky_cache: 10_000,
                    sticky_timeout: 5_000_000,
                    cpu: None,
                    activities_per_second: None,
                    poll_timeout: 70_000_000,
                    rpc_timeout: 10_000_000,
                    eager_activities: false,
                    system: true,
                });
            }
            let sched_type = match wf_types
                .iter()
                .position(|t: &WfTypeParams| t.system_scheduler && t.ns == ns)
            {
                Some(i) => i,
                None => {
                    wf_types.push(WfTypeParams {
                        name: SCHEDULER_WF_TYPE.into(),
                        ns,
                        tq: sys_tq,
                        start_rate: 0.0,
                        arrival: Arrival::Poisson,
                        ramp: None,
                        starters: 1,
                        eager_start: false,
                        await_result: false,
                        rpc_timeout: 10_000_000,
                        wft_processing: Dist::lognormal_p50_p99(1_500.0, 8_000.0),
                        replay_per_event: 20.0,
                        steps: Vec::new(),
                        payload_bytes: 2048.0,
                        system_scheduler: true,
                    });
                    wf_types.len() - 1
                }
            };
            schedules.push(ScheduleParams {
                ns,
                count: s.count,
                interval: s.interval.us().max(1_000_000),
                aligned: s.aligned,
                wf_type: type_names
                    .iter()
                    .position(|t| t == &s.workflow_type)
                    .unwrap(),
                scheduler_type: sched_type,
            });
        }

        // --- extra load -----------------------------------------------------------------------
        let find_type = |n: &str| -> anyhow::Result<usize> {
            wf_types
                .iter()
                .position(|t| t.name == n)
                .with_context(|| format!("unknown workflow type {n}"))
        };
        let mut signals = Vec::new();
        for s in &sc.load.signals {
            signals.push(SignalParams {
                wf_type: find_type(&s.workflow_type)?,
                rate: s.rate.0,
                hot: s.target == SignalTarget::Hot,
                hot_workflows: s.hot_workflows.max(1),
                clients: s.clients.max(1),
                rpc_timeout: s.rpc_timeout.us().max(1_000),
            });
        }
        let mut queries = Vec::new();
        for q in &sc.load.queries {
            queries.push(QueryParams {
                wf_type: find_type(&q.workflow_type)?,
                rate: q.rate.0,
                describe: false,
                rpc_timeout: q.rpc_timeout.us().max(1_000),
            });
        }
        for q in &sc.load.describes {
            queries.push(QueryParams {
                wf_type: find_type(&q.workflow_type)?,
                rate: q.rate.0,
                describe: true,
                rpc_timeout: q.rpc_timeout.us().max(1_000),
            });
        }
        let vis_loads = sc
            .load
            .visibility
            .iter()
            .map(|v| VisLoadParams {
                ns: ns_idx(&v.namespace),
                rate: v.rate.0,
                op: v.op,
                rpc_timeout: v.rpc_timeout.us().max(1_000),
            })
            .collect();

        // --- events ---------------------------------------------------------------------------
        let mut events = Vec::new();
        for e in &sc.events {
            let sr = match &e.start_rate {
                Some(p) => Some((find_type(&p.workflow_type)?, p.rate.0)),
                None => None,
            };
            for k in e.dynamic_config.keys() {
                if crate::config::dynamic::registry().get(k).is_none() {
                    prov.warnings
                        .push(format!("event at {}: unknown dynamic config key {k}", e.at));
                }
            }
            events.push(EventParams {
                at: e.at.us(),
                label: e.label.clone().unwrap_or_default(),
                replicas: e.replicas,
                dc: e
                    .dynamic_config
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
                start_rate: sr,
            });
        }
        events.sort_by_key(|e| e.at);

        // --- persistence ------------------------------------------------------------------------
        let cass = store == StoreKind::Cassandra;
        let mut db_latency: Vec<Dist> = PersistOp::ALL
            .iter()
            .map(|op| {
                let (p50, p99) = op.default_latency_ms(cass);
                Dist::lognormal_p50_p99(p50 * MS, p99 * MS)
            })
            .collect();
        if let Some(d) = sc.cluster.persistence.latency.get("default") {
            let d = d.build().map_err(|e| anyhow::anyhow!(e))?;
            for op in PersistOp::ALL {
                if !op.is_visibility() {
                    db_latency[op.idx()] = d.clone();
                }
            }
        }
        for (name, d) in &sc.cluster.persistence.latency {
            if name == "default" {
                continue;
            }
            let Some(op) = PersistOp::parse(name) else {
                bail!(
                    "cluster.persistence.latency: unknown operation {name:?} (expected a Temporal persistence operation such as UpdateWorkflowExecution)"
                );
            };
            db_latency[op.idx()] = d.build().map_err(|e| anyhow::anyhow!(e))?;
        }
        let db_capacity = sc.cluster.persistence.capacity.unwrap_or(match store {
            StoreKind::Cassandra => 256,
            StoreKind::Sqlite => 1,
            _ => 128,
        });
        let max_conns = match (sc.cluster.persistence.max_conns, store) {
            (Some(m), _) => [m.frontend, m.history, m.matching, m.worker],
            (None, StoreKind::Cassandra) => [100_000; 4],
            (None, _) => [20, 20, 20, 20],
        };
        if store != StoreKind::Cassandra && sc.cluster.persistence.max_conns.is_none() {
            prov.notes.push(
                "persistence.max_conns not set: assuming the Temporal SQL default maxConns=20 per pod".into(),
            );
        }

        let vs = &sc.cluster.visibility;
        let vis = VisParams {
            kind: vs.store,
            capacity: vs.capacity.unwrap_or(64),
            write: match &vs.write_latency {
                Some(d) => d.build().map_err(|e| anyhow::anyhow!(e))?,
                None => Dist::lognormal_p50_p99(4.0 * MS, 25.0 * MS),
            },
            read: match &vs.read_latency {
                Some(d) => d.build().map_err(|e| anyhow::anyhow!(e))?,
                None => Dist::lognormal_p50_p99(25.0 * MS, 250.0 * MS),
            },
            bulk: match &vs.bulk_latency {
                Some(d) => d.build().map_err(|e| anyhow::anyhow!(e))?,
                None => Dist::lognormal_p50_p99(30.0 * MS, 200.0 * MS),
            },
        };

        // --- addresses ---------------------------------------------------------------------------
        let mut addresses: [Vec<String>; 4] = Default::default();
        let mut spare: [Vec<String>; 4] = Default::default();
        let max_extra = sc
            .events
            .iter()
            .filter_map(|e| e.replicas)
            .map(|r| {
                [
                    r.frontend.unwrap_or(0),
                    r.history.unwrap_or(0),
                    r.matching.unwrap_or(0),
                    r.worker.unwrap_or(0),
                ]
            })
            .fold([0u32; 4], |a, b| {
                [
                    a[0].max(b[0]),
                    a[1].max(b[1]),
                    a[2].max(b[2]),
                    a[3].max(b[3]),
                ]
            });
        for svc in Service::ALL {
            let want = (match svc {
                Service::Frontend => replicas.frontend,
                Service::History => replicas.history,
                Service::Matching => replicas.matching,
                Service::Worker => replicas.worker.max(1),
            }) as usize;
            let total = want.max(max_extra[svc.idx()] as usize) + 1;
            let mut all = match sc.cluster.member_addresses.get(svc.as_str()) {
                Some(real) if !real.is_empty() => real.clone(),
                _ => Vec::new(),
            };
            if !all.is_empty() && all.len() < want {
                prov.warnings.push(format!(
                    "cluster.member_addresses.{svc}: {} addresses for {want} replicas; synthesising the rest",
                    all.len()
                ));
            }
            if all.len() < total {
                let synth =
                    synthetic_addresses(svc.as_str(), total + all.len(), svc.grpc_port(), sc.seed);
                for a in synth {
                    if all.len() >= total {
                        break;
                    }
                    if !all.contains(&a) {
                        all.push(a);
                    }
                }
            }
            spare[svc.idx()] = all.split_off(want);
            addresses[svc.idx()] = all;
        }

        let mut costs = Costs::default();
        costs.apply_overrides(&sc.costs, &mut prov)?;

        let mut effective_dc = BTreeMap::new();
        for key in MODELED_KEYS {
            effective_dc.insert((*key).to_string(), dc.describe(key));
        }
        // the registry shows these defaults as Go expressions
        for (key, default) in [
            (
                "history.defaultActivityRetryPolicy",
                "InitialIntervalInSeconds 1, MaximumIntervalCoefficient 100, BackoffCoefficient 2, MaximumAttempts 0 (default)",
            ),
            (
                "history.transferProcessorSchedulerActiveRoundRobinWeights",
                "high 10, low 9, preemptable 1 (default)",
            ),
            (
                "history.timerProcessorSchedulerActiveRoundRobinWeights",
                "high 10, low 9, preemptable 1 (default)",
            ),
            (
                "history.visibilityProcessorSchedulerActiveRoundRobinWeights",
                "high 10, low 9, preemptable 1 (default)",
            ),
        ] {
            if !dc.is_set(key) {
                effective_dc.insert(key.to_string(), default.to_string());
            }
        }
        for key in dc.configured_keys() {
            if !MODELED_KEYS.iter().any(|m| m.eq_ignore_ascii_case(&key)) {
                prov.notes.push(format!(
                    "dynamic config {key} = {} is valid for 1.31.0 but not modelled by the simulator",
                    dc.describe(&key)
                ));
            }
        }

        Ok(Params {
            name: sc.name.clone().unwrap_or_else(|| "scenario".into()),
            seed: sc.seed,
            warmup: sc.warmup().us(),
            duration: sc.duration.us(),
            num_shards: sc.cluster.num_history_shards,
            replicas,
            cpu: [
                sc.cluster.resources.frontend.cpu,
                sc.cluster.resources.history.cpu,
                sc.cluster.resources.matching.cpu,
                sc.cluster.resources.worker.cpu,
            ],
            addresses,
            spare_addresses: spare,
            net_client: sc.cluster.network.client_rtt.us() / 2,
            net_internal: sc.cluster.network.internal_rtt.us() / 2,
            client_lb: sc.cluster.network.client_lb,
            proxy_latency: sc.cluster.network.proxy_latency.us(),
            proxy_discovery: sc.cluster.network.proxy_discovery.us(),
            store,
            max_conns,
            db_capacity,
            db_latency,
            db_includes_append: vec![false; PersistOp::ALL.len()],
            db_write_us_per_byte: sc
                .cluster
                .persistence
                .write_per_mib
                .map_or(20.0 * MS, |d| d.us() as f64)
                / MIB,
            db_read_us_per_byte: sc
                .cluster
                .persistence
                .read_per_mib
                .map_or(5.0 * MS, |d| d.us() as f64)
                / MIB,
            vis,
            costs,
            k,
            namespaces,
            task_queues,
            fleets,
            wf_types,
            signals,
            queries,
            vis_loads,
            schedules,
            events,
            report: sc.report.clone(),
            dc,
            prov,
            effective_dc,
        })
    }

    pub fn fleet_for_tq(&self, tq: usize) -> Option<usize> {
        self.fleets.iter().position(|f| f.tq == tq)
    }
}

/// Deterministic namespace UUID-looking string.
fn synthetic_uuid(seed: u64, i: u64) -> String {
    let mut r = crate::sim::rng::Rng::new(seed ^ (0x5eed_0000 + i));
    let a = r.next_u64();
    let b = r.next_u64();
    format!(
        "{:08x}-{:04x}-4{:03x}-{:04x}-{:012x}",
        (a >> 32) as u32,
        (a >> 16) as u16,
        (a & 0xfff) as u16,
        ((b >> 48) as u16 & 0x3fff) | 0x8000,
        b & 0xffff_ffff_ffff
    )
}
