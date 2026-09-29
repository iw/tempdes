//! Runtime state of the simulated cluster and the shared simulation context.
//!
//! State is split into independently borrowed cells so that flows can mutate e.g. a workflow
//! while recording metrics. Rule: never hold a `RefCell` borrow across an `.await`.

use std::cell::{Cell, RefCell};
use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::rc::Rc;

use crate::sim::executor::{Sender, Time, now};
use crate::sim::rng::Rng;
use crate::sim::stats::{Histogram, TimeGauge};
use crate::sim::sync::{Semaphore, WeightedSemaphore};
use crate::util::lru::Lru;

use super::metrics::Metrics;
use super::params::Params;
use super::ratelimit::{PriorityLimiter, SchedulerLimiter, TokenBucket};
use super::ring::HashRing;
use super::servers::FcfsServers;
use super::types::*;

pub type Ctx = Rc<Sim>;

// --- pods ---------------------------------------------------------------------------------------

pub struct Pod {
    pub svc: Service,
    /// index within its service (ordinal of the StatefulSet/Deployment replica)
    pub ordinal: usize,
    pub addr: String,
    pub alive: bool,
    pub cpu: FcfsServers,
    pub db_pool: Semaphore,
    /// persistence priority limiter (7 levels, `common/persistence/client/quotas.go`)
    pub persist_limiter: PriorityLimiter,
    /// per-namespace persistence priority limiters (`<service>.persistenceNamespaceMaxQPS`,
    /// falling back to the pod's rate); empty when the pod's persistence is unlimited
    pub ns_persist_limiters: Vec<PriorityLimiter>,
    /// history: per (namespace, shard) limiters, created when
    /// `history.persistencePerShardNamespaceMaxQPS` is set for the namespace
    pub shard_ns_limiters: std::collections::BTreeMap<(usize, ShardId), PriorityLimiter>,
    /// history `history.rps` / matching `matching.rps` / frontend host `frontend.rps` limiter
    pub rps_limiter: PriorityLimiter,
    pub fe: Option<FrontendState>,
    pub hist: Option<HistoryHostState>,
}

pub struct FrontendState {
    /// host visibility bucket (same rate as frontend.rps, separate bucket)
    pub vis_limiter: PriorityLimiter,
    /// per namespace execution limiter
    pub ns_limiters: Vec<PriorityLimiter>,
    /// per namespace visibility limiter
    pub ns_vis_limiters: Vec<TokenBucket>,
    /// in-flight long-running requests per (namespace, api)
    pub concurrent: HashMap<(usize, Api), i64>,
    pub concurrent_max: HashMap<(usize, Api), i64>,
    /// client-side matching load balancer: outstanding polls per (tq, kind, partition)
    pub poll_lb: HashMap<(usize, TqKind), Vec<u32>>,
    /// SDK connections to this pod (`round_robin`: one subchannel per client process)
    pub connections: u32,
    /// `proxy`: when the proxy starts sending traffic to this pod (a pod added by scaling
    /// waits for registration and health checks)
    pub ready_at: Time,
}

pub struct HistoryHostState {
    pub cache: Lru,
    /// the host task schedulers (transfer, timer, visibility): IWRR over (namespace, priority)
    /// channels in front of `history.*ProcessorSchedulerWorkerCount` workers
    pub schedulers: [WeightedSemaphore; 3],
    /// each scheduler's execution queue scheduler
    /// (`history.taskSchedulerEnableExecutionQueueScheduler`)
    pub exec_queues: [ExecQueues; 3],
    pub load_limiters: [TokenBucket; 3],
    pub pending_in_scheduler: [TimeGauge; 3],
    pub owned_shards: u32,
    pub shard_acquire: Semaphore,
    /// the task scheduler's rate limiter (`history.taskSchedulerEnableRateLimiter`)
    pub sched_limiter: SchedulerLimiter,
    /// when the pod started (the limiter waits `history.taskSchedulerRateLimiterStartupDelay`)
    pub started_at: Time,
    /// tasks the limiter refused (`task_scheduler_throttled`), in shadow mode too
    pub sched_throttled: u64,
}

/// The execution queue scheduler of one host task scheduler
/// (`common/tasks/execution_queue_scheduler.go`): per-workflow FIFO queues, each with up to
/// `history.taskSchedulerExecutionQueueSchedulerQueueConcurrency` workers of its own, so tasks
/// of a contended workflow run one or two at a time instead of failing on its lock. A queue is
/// created when a task fails with BUSY_WORKFLOW, receives every later task of that workflow, and
/// is removed once idle for `...QueueTTL`. At `...MaxQueues` queues, new workflows fall back to
/// the regular scheduler.
#[derive(Default)]
pub struct ExecQueues {
    pub queues: std::collections::BTreeMap<(WfId, u32), ExecQueue>,
    /// tasks accepted into a queue
    pub submitted: u64,
    /// busy-workflow tasks refused because `MaxQueues` queues existed
    pub rejected: u64,
    pub max_queues_seen: usize,
}

pub struct ExecQueue {
    pub workers: Semaphore,
    /// tasks queued or running
    pub tasks: u32,
    pub idle_since: Time,
}

impl ExecQueues {
    /// Whether `key` has a live queue at `t` (removing it once idle for longer than `ttl`).
    pub fn has(&mut self, key: (WfId, u32), t: Time, ttl: Time) -> bool {
        match self.queues.get(&key) {
            Some(q) if q.tasks > 0 || t.saturating_sub(q.idle_since) <= ttl => true,
            Some(_) => {
                self.queues.remove(&key);
                false
            }
            None => false,
        }
    }

    /// Add a task to `key`'s queue, creating it when `create` and fewer than `max` queues exist.
    /// Returns the queue's workers.
    pub fn submit(
        &mut self,
        key: (WfId, u32),
        create: bool,
        t: Time,
        ttl: Time,
        max: usize,
        concurrency: u32,
    ) -> Option<Semaphore> {
        if !self.has(key, t, ttl) {
            if !create {
                return None;
            }
            // the sweeper removes queues idle for longer than the TTL
            self.queues
                .retain(|_, q| q.tasks > 0 || t.saturating_sub(q.idle_since) <= ttl);
            if self.queues.len() >= max {
                self.rejected += 1;
                return None;
            }
            self.queues.insert(
                key,
                ExecQueue {
                    workers: Semaphore::new(concurrency.max(1)),
                    tasks: 0,
                    idle_since: t,
                },
            );
            self.max_queues_seen = self.max_queues_seen.max(self.queues.len());
        }
        let q = self.queues.get_mut(&key).expect("queue exists");
        q.tasks += 1;
        self.submitted += 1;
        Some(q.workers.clone())
    }

    /// A task of `key`'s queue finished.
    pub fn done(&mut self, key: (WfId, u32), t: Time) {
        if let Some(q) = self.queues.get_mut(&key) {
            q.tasks = q.tasks.saturating_sub(1);
            if q.tasks == 0 {
                q.idle_since = t;
            }
        }
    }

    pub fn reset_stats(&mut self) {
        self.submitted = 0;
        self.rejected = 0;
        self.max_queues_seen = self.queues.len();
    }
}

// --- history shards -----------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
pub struct HistTask {
    pub kind: TaskType,
    pub wf: WfId,
    pub wf_gen: u32,
    pub created: Time,
    pub fire_at: Time,
    /// type-specific reference (activity seq, wft seq, timer seq, child index)
    pub r: u32,
    pub r2: u32,
}

pub struct ShardQueue {
    /// immediate queues: persisted tasks not yet loaded
    pub unloaded: VecDeque<HistTask>,
    /// timer queue: persisted timers not yet loaded, ordered by fire time
    pub timers: BinaryHeap<std::cmp::Reverse<(Time, u64, usize)>>,
    pub timer_store: Vec<Option<HistTask>>,
    pub timer_free: Vec<usize>,
    pub reader_active: bool,
    /// earliest time the timer reader is scheduled to wake
    pub timer_wake_at: Option<Time>,
    pub timer_wake_seq: u64,
    pub pending: u32,
    pub load_limiter: TokenBucket,
    pub completed_since_ack: u32,
    pub last_ack: Time,
}

impl ShardQueue {
    pub fn new(poll_rps: f64) -> Self {
        ShardQueue {
            unloaded: VecDeque::new(),
            timers: BinaryHeap::new(),
            timer_store: Vec::new(),
            timer_free: Vec::new(),
            reader_active: false,
            timer_wake_at: None,
            timer_wake_seq: 0,
            pending: 0,
            load_limiter: TokenBucket::new(poll_rps, poll_rps),
            completed_since_ack: 0,
            last_ack: now(),
        }
    }

    pub fn push_timer(&mut self, t: HistTask, seq: u64) {
        let slot = if let Some(s) = self.timer_free.pop() {
            self.timer_store[s] = Some(t);
            s
        } else {
            self.timer_store.push(Some(t));
            self.timer_store.len() - 1
        };
        self.timers.push(std::cmp::Reverse((t.fire_at, seq, slot)));
    }

    pub fn next_timer_at(&self) -> Option<Time> {
        self.timers.peek().map(|r| r.0.0)
    }

    pub fn pop_timer(&mut self) -> Option<HistTask> {
        let std::cmp::Reverse((_, _, slot)) = self.timers.pop()?;
        let t = self.timer_store[slot].take();
        self.timer_free.push(slot);
        t
    }

    pub fn backlog(&self) -> usize {
        self.unloaded.len() + self.timers.len()
    }
}

pub struct Shard {
    pub id: ShardId,
    pub owner: PodId,
    /// bumps on ownership change; part of the mutable state cache key (cold cache after move)
    pub epoch: u32,
    pub available_at: Time,
    pub io_sem: Semaphore,
    pub queues: [ShardQueue; 3],
    pub events_cache: Lru,
    pub tasks_completed_since_update: u32,
    pub last_shard_update: Time,
    pub writes: u64,
    pub api_requests: u64,
    pub persistence_ops: u64,
}

// --- workflows ----------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WftState {
    None,
    Scheduled {
        seq: u32,
        attempt: u32,
        sticky: bool,
        at: Time,
    },
    Started {
        seq: u32,
        attempt: u32,
        sticky: bool,
        at: Time,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ActState {
    Scheduled,
    Started,
    Backoff,
}

#[derive(Clone, Debug)]
pub struct ActInfo {
    pub seq: u32,
    pub attempt: u32,
    pub state: ActState,
    pub step: usize,
    pub tq: usize,
    /// when the current attempt was scheduled (a retry: when it became due)
    pub scheduled_at: Time,
    /// when the first attempt was scheduled (schedule-to-close counts from here)
    pub first_scheduled_at: Time,
    pub started_at: Time,
    /// the last heartbeat of the running attempt (0 = none yet)
    pub last_heartbeat: Time,
    /// activity timeout timer tasks created, one bit per timeout kind (`TimerTaskStatus`)
    pub timers: u8,
    /// fire time of the current heartbeat timer task
    pub hb_timer_at: Time,
    /// how its attempts go, drawn when it was scheduled (none: each attempt fails at the
    /// step's failure rate)
    pub plan: super::params::AttemptPlan,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WfStatus {
    Running,
    Closed,
}

pub struct Wf {
    pub wgen: u32,
    pub key: u64,
    pub wf_type: usize,
    pub ns: usize,
    pub shard: ShardId,
    pub lock: Semaphore,
    pub status: WfStatus,
    pub start_time: Time,
    pub parent: Option<(WfId, u32)>,
    pub history_events: u32,
    pub history_bytes: f64,
    pub wft: WftState,
    pub wft_seq: u32,
    pub sticky_worker: Option<usize>,
    pub last_started_event: u32,
    pub buffered_events: u32,
    // --- workflow program progress (driven by the SDK side) ---
    pub step: usize,
    pub step_started: bool,
    pub step_remaining: u32,
    pub completed_in_step: u32,
    /// activities of the current step that failed for good (timed out or out of retries)
    pub failed_in_step: u32,
    pub timer_seq: u32,
    pub timer_pending: Option<u32>,
    pub timer_fired: bool,
    pub signals_received: u32,
    pub signals_consumed: u32,
    pub activities: Vec<ActInfo>,
    pub next_act_seq: u32,
    pub children_pending: u32,
    pub children_done: u32,
    /// hot entity workflows loop on signals forever
    pub entity: bool,
    pub close_waiters: Vec<Sender<()>>,
    pub history_waiters: Vec<Sender<()>>,
    /// index in the per-type running list
    pub running_idx: usize,
    /// schedule bookkeeping (scheduler workflows)
    pub schedule: Option<ScheduleState>,
}

#[derive(Clone, Copy, Debug)]
pub struct ScheduleState {
    pub sched: usize,
    pub due_actions: u32,
    pub next_fire: Time,
    pub waiting_rate_limit: bool,
}

#[derive(Default)]
pub struct WfStore {
    pub slots: Vec<Option<Wf>>,
    pub free: Vec<u32>,
    pub next_key: u64,
    pub running_by_type: Vec<Vec<WfId>>,
    pub hot: Vec<Vec<WfId>>,
}

impl WfStore {
    pub fn insert(&mut self, mut wf: Wf) -> WfId {
        let t = wf.wf_type;
        if self.running_by_type.len() <= t {
            self.running_by_type.resize_with(t + 1, Vec::new);
        }
        let id = if let Some(i) = self.free.pop() {
            let wgen = self.slots[i as usize].as_ref().map(|w| w.wgen).unwrap_or(0);
            wf.wgen = wgen.wrapping_add(1);
            i
        } else {
            self.slots.push(None);
            (self.slots.len() - 1) as u32
        };
        wf.running_idx = self.running_by_type[t].len();
        self.running_by_type[t].push(id);
        self.slots[id as usize] = Some(wf);
        id
    }

    #[inline]
    pub fn get(&self, id: WfId, wgen: u32) -> Option<&Wf> {
        self.slots
            .get(id as usize)
            .and_then(|s| s.as_ref())
            .filter(|w| w.wgen == wgen)
    }

    #[inline]
    pub fn get_mut(&mut self, id: WfId, wgen: u32) -> Option<&mut Wf> {
        self.slots
            .get_mut(id as usize)
            .and_then(|s| s.as_mut())
            .filter(|w| w.wgen == wgen)
    }

    /// Remove from the running index (on close). The slot itself is kept until `release`.
    pub fn mark_closed(&mut self, id: WfId) {
        let (t, idx) = {
            let w = self.slots[id as usize].as_mut().unwrap();
            w.status = WfStatus::Closed;
            (w.wf_type, w.running_idx)
        };
        let list = &mut self.running_by_type[t];
        if idx < list.len() && list[idx] == id {
            list.swap_remove(idx);
            if idx < list.len() {
                let moved = list[idx];
                if let Some(w) = self.slots[moved as usize].as_mut() {
                    w.running_idx = idx;
                }
            }
        }
    }

    /// Free the slot (after the closed workflow's remaining tasks can no longer reference it).
    pub fn release(&mut self, id: WfId) {
        if let Some(s) = self.slots.get_mut(id as usize)
            && let Some(w) = s.as_mut()
        {
            // keep wgen so stale references fail the generation check
            w.status = WfStatus::Closed;
        }
        self.free.push(id);
    }

    pub fn running(&self) -> usize {
        self.running_by_type.iter().map(Vec::len).sum()
    }
}

// --- matching -----------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TqKind {
    Workflow,
    Activity,
}

impl TqKind {
    pub fn as_str(self) -> &'static str {
        match self {
            TqKind::Workflow => "Workflow",
            TqKind::Activity => "Activity",
        }
    }
    pub fn type_int(self) -> u32 {
        match self {
            TqKind::Workflow => 1,
            TqKind::Activity => 2,
        }
    }
}

/// A task offered to matching.
#[derive(Clone, Copy, Debug)]
pub struct MTask {
    pub wf: WfId,
    pub wf_gen: u32,
    pub kind: TqKind,
    /// wft seq or activity seq
    pub r: u32,
    /// activity attempt
    pub r2: u32,
    pub created: Time,
    pub from_backlog: bool,
    pub query: bool,
}

/// What a waiting poller receives.
pub struct Matched {
    pub task: MTask,
    /// partition where the match happened (RecordTaskStarted is issued from its host)
    pub at_partition: usize,
    /// sync match: completion reported back to the AddTask caller
    pub sync_done: Option<Sender<Res<()>>>,
    pub query_done: Option<Sender<Res<()>>>,
}

pub struct PollWaiter {
    pub tx: Sender<Matched>,
    pub since: Time,
    pub deadline: Time,
    pub forwarded: bool,
}

pub struct WriteReq {
    pub task: MTask,
    pub done: Sender<Res<()>>,
}

pub struct Partition {
    pub id: usize,
    pub tq: usize,
    pub kind: TqKind,
    pub part: u32,
    pub sticky_of: Option<usize>,
    pub routing_key: String,
    pub host: PodId,
    pub parent: Option<usize>,
    pub pollers: VecDeque<PollWaiter>,
    /// backlog tasks loaded in memory, ready to dispatch
    pub backlog_mem: VecDeque<MTask>,
    /// backlog tasks persisted but not yet read back (GetTasks)
    pub backlog_db: VecDeque<MTask>,
    pub write_queue: VecDeque<WriteReq>,
    pub writer_active: bool,
    pub reader_active: bool,
    pub fwd_tasks_inflight: u32,
    pub fwd_polls_inflight: u32,
    pub fwd_limiter: TokenBucket,
    pub last_poll: Time,
    pub dispatch_limiter: Option<TokenBucket>,
    pub acked_since_delete: u32,
    pub last_delete: Time,
    pub range_left: u32,
    pub last_ack_update: Time,
    pub loaded: bool,
    // stats
    pub sync_matches: u64,
    pub async_matches: u64,
    pub forwarded_tasks: u64,
    pub forwarded_polls: u64,
    pub remote_matches: u64,
    pub adds: u64,
    pub polls: u64,
    pub poll_timeouts: u64,
    pub writes: u64,
    pub write_rejects: u64,
    pub backlog_gauge: TimeGauge,
    pub pollers_gauge: TimeGauge,
    pub poll_wait: Histogram,
    pub task_wait: Histogram,
}

impl Partition {
    pub fn backlog_len(&self) -> u64 {
        (self.backlog_mem.len() + self.backlog_db.len()) as u64
    }

    /// Age of the oldest backlog task.
    pub fn backlog_head_age(&self) -> Time {
        let t = now();
        let mem = self.backlog_mem.front().map(|x| x.created);
        let db = self.backlog_db.front().map(|x| x.created);
        match (mem, db) {
            (Some(a), Some(b)) => t.saturating_sub(a.min(b)),
            (Some(a), None) | (None, Some(a)) => t.saturating_sub(a),
            (None, None) => 0,
        }
    }

    pub fn update_gauges(&mut self) {
        let b = self.backlog_len() as f64;
        self.backlog_gauge.set(b);
        let p = self.pollers.len() as f64;
        self.pollers_gauge.set(p);
    }
}

#[derive(Default)]
pub struct MatchingState {
    pub parts: Vec<Partition>,
    /// (tq, kind) -> partition ids ordered by partition number
    pub by_tq: HashMap<(usize, TqKind), Vec<usize>>,
    /// worker process -> sticky partition id
    pub sticky: HashMap<usize, usize>,
}

// --- SDK workers & clients ---------------------------------------------------------------------

pub struct WorkerProc {
    pub fleet: usize,
    pub ordinal: u32,
    pub conn: PodId,
    pub conn_expires: Time,
    pub rr: RoundRobin,
    pub wft_slots: Semaphore,
    pub act_slots: Semaphore,
    pub cpu: Option<FcfsServers>,
    pub sticky_cache: Lru,
    pub sticky_partition: usize,
    pub last_sticky_poll: Time,
    pub poll_toggle: u64,
    /// Go SDK poller balancing: outstanding sticky / regular workflow polls and the last
    /// sticky backlog hint.
    pub pending_sticky: u32,
    pub pending_regular: u32,
    pub sticky_backlog: u64,
    /// the worker service pod hosting this process (system per-namespace workers only)
    pub host_pod: Option<PodId>,
}

pub struct Client {
    pub conn: PodId,
    pub conn_expires: Time,
    pub rr: RoundRobin,
}

/// gRPC client-side round robin state of one process (`network.client_lb: round_robin`).
#[derive(Clone, Debug, Default)]
pub struct RoundRobin {
    /// frontend pods from the last DNS resolution, one subchannel (connection) each
    pub subchannels: Vec<PodId>,
    /// when each subchannel's connection receives GOAWAY (max connection age ± 10%)
    pub expires: Vec<Time>,
    /// picker position
    pub next: usize,
    /// a subchannel closed since the last resolution, so the channel wants to re-resolve
    pub resolve_pending: bool,
    /// time of the last DNS resolution (None before the first request)
    pub resolved_at: Option<Time>,
}

// --- database -----------------------------------------------------------------------------------

pub struct Db {
    pub servers: FcfsServers,
    pub vis_servers: FcfsServers,
}

pub struct EsBulk {
    pub buffer: Vec<Sender<()>>,
    pub flush_scheduled: bool,
    pub inflight: u32,
}

// --- the simulation context -------------------------------------------------------------------

pub struct Rings {
    pub history: HashRing,
    pub matching: HashRing,
    pub worker: HashRing,
    /// ring member index -> pod id
    pub history_pods: Vec<PodId>,
    pub matching_pods: Vec<PodId>,
    pub worker_pods: Vec<PodId>,
}

pub struct Sim {
    pub p: Params,
    pub rng: RefCell<Rng>,
    pub pods: RefCell<Vec<Pod>>,
    pub svc_pods: RefCell<[Vec<PodId>; 4]>,
    pub rings: RefCell<Rings>,
    pub shards: RefCell<Vec<Shard>>,
    pub wfs: RefCell<WfStore>,
    pub matching: RefCell<MatchingState>,
    pub workers: RefCell<Vec<WorkerProc>>,
    pub clients: RefCell<Vec<Client>>,
    pub db: RefCell<Db>,
    pub es: RefCell<Vec<EsBulk>>,
    pub m: RefCell<Metrics>,
    pub timer_seq: RefCell<u64>,
    /// `proxy`: round robin position over the frontend pods
    pub proxy_next: Cell<usize>,
    pub schedule_buckets: RefCell<HashMap<(usize, PodId), TokenBucket>>,
    pub measuring: RefCell<bool>,
}

impl Sim {
    #[inline]
    pub fn rand(&self) -> f64 {
        self.rng.borrow_mut().f64()
    }

    #[inline]
    pub fn rand_index(&self, n: usize) -> usize {
        self.rng.borrow_mut().index(n.max(1))
    }

    pub fn next_timer_seq(&self) -> u64 {
        let mut s = self.timer_seq.borrow_mut();
        *s += 1;
        *s
    }

    pub fn live_pods(&self, svc: Service) -> Vec<PodId> {
        self.svc_pods.borrow()[svc.idx()].clone()
    }

    pub fn n_live(&self, svc: Service) -> usize {
        self.svc_pods.borrow()[svc.idx()].len()
    }

    pub fn shard_owner(&self, shard: ShardId) -> PodId {
        self.shards.borrow()[(shard - 1) as usize].owner
    }

    pub fn wf_shard(&self, wf: WfId, wgen: u32) -> Option<ShardId> {
        self.wfs.borrow().get(wf, wgen).map(|w| w.shard)
    }

    /// The namespace of a workflow (0 once its slot is gone).
    pub fn wf_ns(&self, wf: WfId, wgen: u32) -> usize {
        self.wfs.borrow().get(wf, wgen).map_or(0, |w| w.ns)
    }
}
