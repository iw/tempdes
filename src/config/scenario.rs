//! Scenario file schema (YAML).
//!
//! A scenario describes the EKS deployment (replica counts, pod CPU, persistence), Temporal
//! dynamic config (inline and/or files in Temporal's own format), the SDK worker fleets and the
//! workload (workflow shapes, start rates, signals, visibility queries, schedules). See
//! `examples/scenarios/*.yaml` for annotated examples.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::Context;
use serde::Deserialize;

use crate::config::dynamic::{DcValue, InlineCv};
use crate::sim::dist::DurDist;
use crate::util::units::{Bytes, Dur, Rate};

fn default_seed() -> u64 {
    1
}
fn default_ns() -> String {
    "default".into()
}
fn one() -> u32 {
    1
}
fn yes() -> bool {
    true
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scenario {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default = "default_seed")]
    pub seed: u64,
    /// Measured simulated time (after warm-up).
    pub duration: Dur,
    /// Simulated time discarded before measuring (default 30s).
    #[serde(default)]
    pub warmup: Option<Dur>,
    pub cluster: ClusterSpec,
    /// Temporal dynamic config files (same format as the server's), relative to the scenario.
    #[serde(default)]
    pub dynamic_config_files: Vec<String>,
    /// Inline dynamic config (Temporal format), applied after the files.
    #[serde(default)]
    pub dynamic_config: BTreeMap<String, Vec<InlineCv>>,
    #[serde(default)]
    pub namespaces: Vec<NamespaceSpec>,
    #[serde(default)]
    pub workers: Vec<WorkerFleetSpec>,
    #[serde(default)]
    pub workflows: Vec<WorkflowSpec>,
    #[serde(default)]
    pub load: LoadSpec,
    #[serde(default)]
    pub schedules: Vec<ScheduleSpec>,
    #[serde(default)]
    pub events: Vec<EventSpec>,
    #[serde(default)]
    pub calibration: Option<CalibrationSpec>,
    /// CPU cost overrides per service and operation (microseconds of CPU).
    #[serde(default)]
    pub costs: CostOverrides,
    #[serde(default)]
    pub report: ReportSpec,

    /// Directory the scenario was loaded from (for resolving relative paths).
    #[serde(skip)]
    pub base_dir: PathBuf,
    /// What was imported from Helm values (reported as notes).
    #[serde(skip)]
    pub import_notes: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterSpec {
    /// A temporalio/helm-charts values file; replicas, CPU limits, numHistoryShards, SQL
    /// maxConns and server.dynamicConfig are taken from it (explicit scenario values for
    /// dynamic config win; CLI overrides win over everything).
    #[serde(default)]
    pub helm_values: Option<String>,
    /// `persistence.numHistoryShards` (static config, fixed at cluster creation).
    #[serde(default)]
    pub num_history_shards: u32,
    #[serde(default)]
    pub replicas: Replicas,
    #[serde(default)]
    pub resources: PodResources,
    /// Optional real ringpop member addresses (`ip:port`) per service, to reproduce the exact
    /// shard / partition placement of a live cluster.
    #[serde(default)]
    pub member_addresses: BTreeMap<String, Vec<String>>,
    #[serde(default)]
    pub network: NetworkSpec,
    pub persistence: PersistenceSpec,
    #[serde(default)]
    pub visibility: VisibilitySpec,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Replicas {
    #[serde(default)]
    pub frontend: u32,
    #[serde(default)]
    pub history: u32,
    #[serde(default)]
    pub matching: u32,
    #[serde(default = "one")]
    pub worker: u32,
}

impl Default for Replicas {
    fn default() -> Self {
        Replicas {
            frontend: 0,
            history: 0,
            matching: 0,
            worker: 1,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PodResources {
    #[serde(default = "PodSpec::fe")]
    pub frontend: PodSpec,
    #[serde(default = "PodSpec::hist")]
    pub history: PodSpec,
    #[serde(default = "PodSpec::mat")]
    pub matching: PodSpec,
    #[serde(default = "PodSpec::wrk")]
    pub worker: PodSpec,
}

impl Default for PodResources {
    fn default() -> Self {
        PodResources {
            frontend: PodSpec::fe(),
            history: PodSpec::hist(),
            matching: PodSpec::mat(),
            worker: PodSpec::wrk(),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PodSpec {
    /// CPU cores available to the Go runtime (container CPU limit → GOMAXPROCS).
    pub cpu: f64,
}

impl PodSpec {
    fn fe() -> Self {
        PodSpec { cpu: 2.0 }
    }
    fn hist() -> Self {
        PodSpec { cpu: 4.0 }
    }
    fn mat() -> Self {
        PodSpec { cpu: 2.0 }
    }
    fn wrk() -> Self {
        PodSpec { cpu: 1.0 }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NetworkSpec {
    /// Round trip between SDK clients/workers and the frontend (through the NLB).
    #[serde(default = "NetworkSpec::client")]
    pub client_rtt: Dur,
    /// Round trip between Temporal pods.
    #[serde(default = "NetworkSpec::internal")]
    pub internal_rtt: Dur,
    /// How SDK clients and workers spread their requests over the frontend pods.
    #[serde(default)]
    pub client_lb: ClientLb,
    /// `proxy` only: latency the proxy adds to every request.
    #[serde(default = "NetworkSpec::proxy_latency")]
    pub proxy_latency: Dur,
    /// `proxy` only: how long a new frontend pod waits for traffic (target registration and
    /// health checks).
    #[serde(default = "NetworkSpec::proxy_discovery")]
    pub proxy_discovery: Dur,
}

impl NetworkSpec {
    fn client() -> Dur {
        Dur::from_ms(2.0)
    }
    fn internal() -> Dur {
        Dur::from_ms(0.5)
    }
    fn proxy_latency() -> Dur {
        Dur::from_ms(1.0)
    }
    fn proxy_discovery() -> Dur {
        Dur::from_secs(15.0)
    }
}

impl Default for NetworkSpec {
    fn default() -> Self {
        NetworkSpec {
            client_rtt: Self::client(),
            internal_rtt: Self::internal(),
            client_lb: ClientLb::default(),
            proxy_latency: Self::proxy_latency(),
            proxy_discovery: Self::proxy_discovery(),
        }
    }
}

/// How SDK clients and workers reach the frontend pods.
#[derive(Clone, Copy, Debug, Deserialize, serde::Serialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ClientLb {
    /// One gRPC connection per process, placed by an L4 load balancer (a ClusterIP Service or
    /// an NLB). Every request of the process goes to that pod until the connection receives
    /// GOAWAY at `frontend.keepAliveMaxConnectionAge`.
    #[default]
    Pinned,
    /// gRPC client-side load balancing: a `dns:///` target on a headless Service with the
    /// `round_robin` policy. The process connects to every frontend pod DNS returns and rotates
    /// requests over them.
    RoundRobin,
    /// Per-request balancing by an L7 proxy (an AWS ALB with a gRPC target group, Envoy, a
    /// service mesh).
    Proxy,
}

impl ClientLb {
    pub fn as_str(self) -> &'static str {
        match self {
            ClientLb::Pinned => "pinned",
            ClientLb::RoundRobin => "round_robin",
            ClientLb::Proxy => "proxy",
        }
    }
}

impl std::fmt::Display for ClientLb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for ClientLb {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        match s.trim().to_ascii_lowercase().replace('-', "_").as_str() {
            "pinned" => Ok(ClientLb::Pinned),
            "round_robin" => Ok(ClientLb::RoundRobin),
            "proxy" => Ok(ClientLb::Proxy),
            other => Err(format!(
                "unknown client load balancing {other:?} (expected pinned, round_robin or proxy)"
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum StoreKind {
    Cassandra,
    Mysql,
    Postgresql,
    Sqlite,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PersistenceSpec {
    pub store: StoreKind,
    /// `maxConns` per pod (static persistence config) for SQL stores. Cassandra multiplexes
    /// streams, so the default there is effectively unlimited.
    #[serde(default)]
    pub max_conns: Option<MaxConns>,
    /// Concurrent operations the database serves before requests queue (e.g. Aurora vCPUs x
    /// ~4, or Cassandra nodes x concurrent_writes/4). Can be inferred by calibration.
    #[serde(default)]
    pub capacity: Option<u32>,
    /// Per-operation service time at low load. Keys are Temporal persistence operation names
    /// (`UpdateWorkflowExecution`, ...) or `default`.
    #[serde(default)]
    pub latency: BTreeMap<String, DurDist>,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MaxConns {
    #[serde(default = "MaxConns::d")]
    pub frontend: u32,
    #[serde(default = "MaxConns::d")]
    pub history: u32,
    #[serde(default = "MaxConns::d")]
    pub matching: u32,
    #[serde(default = "MaxConns::d")]
    pub worker: u32,
}

impl MaxConns {
    fn d() -> u32 {
        20
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum VisibilityKind {
    #[default]
    Elasticsearch,
    Opensearch,
    Postgresql,
    Mysql,
}

#[derive(Clone, Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct VisibilitySpec {
    #[serde(default)]
    pub store: VisibilityKind,
    #[serde(default)]
    pub capacity: Option<u32>,
    #[serde(default)]
    pub write_latency: Option<DurDist>,
    #[serde(default)]
    pub read_latency: Option<DurDist>,
    /// Elasticsearch bulk request latency.
    #[serde(default)]
    pub bulk_latency: Option<DurDist>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NamespaceSpec {
    pub name: String,
    /// Namespace UUID; only matters for exact shard hashing of known workflow IDs.
    #[serde(default)]
    pub id: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerFleetSpec {
    pub name: String,
    #[serde(default = "default_ns")]
    pub namespace: String,
    pub task_queue: String,
    /// Worker processes (pods).
    #[serde(default = "one")]
    pub processes: u32,
    /// MaxConcurrentWorkflowTaskPollers per process.
    #[serde(default = "WorkerFleetSpec::wf_pollers")]
    pub workflow_pollers: u32,
    /// MaxConcurrentActivityTaskPollers per process.
    #[serde(default = "WorkerFleetSpec::act_pollers")]
    pub activity_pollers: u32,
    /// MaxConcurrentWorkflowTaskExecutionSize per process.
    #[serde(default = "WorkerFleetSpec::slots")]
    pub workflow_slots: u32,
    /// MaxConcurrentActivityExecutionSize per process.
    #[serde(default = "WorkerFleetSpec::slots")]
    pub activity_slots: u32,
    /// Sticky workflow cache size per process (WorkerCacheSize / maxCachedWorkflows).
    #[serde(default = "WorkerFleetSpec::sticky")]
    pub sticky_cache_size: u32,
    #[serde(default = "WorkerFleetSpec::sticky_timeout")]
    pub sticky_schedule_to_start_timeout: Dur,
    /// CPU cores per worker process available for workflow task processing (None = unbounded).
    #[serde(default)]
    pub cpu: Option<f64>,
    /// TaskQueueActivitiesPerSecond (server-side dispatch limit for the task queue).
    #[serde(default)]
    pub task_queue_activities_per_second: Option<f64>,
    /// SDK long-poll deadline.
    #[serde(default = "WorkerFleetSpec::poll_timeout")]
    pub poll_timeout: Dur,
    /// Request eager activity execution (needs `system.enableActivityEagerExecution`).
    #[serde(default)]
    pub eager_activities: bool,
}

impl WorkerFleetSpec {
    fn wf_pollers() -> u32 {
        2
    }
    fn act_pollers() -> u32 {
        2
    }
    fn slots() -> u32 {
        1000
    }
    fn sticky() -> u32 {
        10_000
    }
    fn sticky_timeout() -> Dur {
        Dur::from_secs(5.0)
    }
    fn poll_timeout() -> Dur {
        Dur::from_secs(70.0)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Arrival {
    #[default]
    Poisson,
    Uniform,
}

/// How workflow IDs are chosen for starts.
#[derive(Clone, Debug, Deserialize, Default, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum IdPattern {
    /// Every start uses a fresh workflow ID (the common case).
    #[default]
    Unique,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowSpec {
    #[serde(rename = "type")]
    pub type_name: String,
    #[serde(default = "default_ns")]
    pub namespace: String,
    pub task_queue: String,
    /// Client start rate. Omit for workflow types only started as children or by schedules.
    #[serde(default)]
    pub start_rate: Option<Rate>,
    #[serde(default)]
    pub arrival: Arrival,
    /// Linear ramp of the start rate from `ramp.from` (fraction) over `ramp.over`.
    #[serde(default)]
    pub ramp: Option<RampSpec>,
    /// Client processes issuing starts (each holds one gRPC connection to a frontend pod).
    #[serde(default = "WorkflowSpec::starters")]
    pub starters: u32,
    /// Request eager workflow start (first workflow task returned inline).
    #[serde(default)]
    pub eager_start: bool,
    /// Client waits for the result (long-polls GetWorkflowExecutionHistory).
    #[serde(default)]
    pub await_result: bool,
    /// Worker-side time to process a workflow task when the workflow is in the sticky cache.
    #[serde(default = "WorkflowSpec::wft")]
    pub wft_processing: DurDist,
    /// Extra worker-side replay time per history event on a sticky cache miss.
    #[serde(default = "WorkflowSpec::replay")]
    pub replay_per_event: Dur,
    #[serde(default)]
    pub steps: Vec<Step>,
    /// Typical payload size (inputs/results); affects history size.
    #[serde(default = "WorkflowSpec::payload")]
    pub payload_bytes: Bytes,
    #[serde(default)]
    pub id_pattern: IdPattern,
}

impl WorkflowSpec {
    fn starters() -> u32 {
        4
    }
    fn wft() -> DurDist {
        DurDist::p50_p99(2.0, 12.0)
    }
    fn replay() -> Dur {
        Dur(20.0)
    }
    fn payload() -> Bytes {
        Bytes(1024.0)
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RampSpec {
    /// Starting fraction of `start_rate` (e.g. 0.2).
    #[serde(default)]
    pub from: f64,
    /// Ramp duration from simulation start.
    pub over: Dur,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Step {
    Activity(ActivityStep),
    LocalActivity(LocalActivityStep),
    Timer(DurDist),
    ChildWorkflow(ChildStep),
    WaitSignal(WaitSignalStep),
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActivityStep {
    #[serde(default = "one")]
    pub count: u32,
    /// Run `count` activities in parallel (default) or one after another.
    #[serde(default = "yes")]
    pub parallel: bool,
    pub duration: DurDist,
    /// Heartbeat interval (activity calls RecordActivityTaskHeartbeat this often).
    #[serde(default)]
    pub heartbeat: Option<Dur>,
    /// Probability an attempt fails (retried by the server with backoff).
    #[serde(default)]
    pub failure_rate: f64,
    /// Initial retry interval (Temporal default 1s, coefficient 2).
    #[serde(default)]
    pub retry_initial: Option<Dur>,
    /// Dispatch to a different task queue than the workflow's.
    #[serde(default)]
    pub task_queue: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalActivityStep {
    #[serde(default = "one")]
    pub count: u32,
    pub duration: DurDist,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChildStep {
    pub workflow_type: String,
    #[serde(default = "one")]
    pub count: u32,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WaitSignalStep {
    #[serde(default = "one")]
    pub count: u32,
    /// Give up waiting after this long (a timer).
    #[serde(default)]
    pub timeout: Option<Dur>,
}

#[derive(Clone, Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct LoadSpec {
    #[serde(default)]
    pub signals: Vec<SignalLoad>,
    #[serde(default)]
    pub queries: Vec<QueryLoad>,
    #[serde(default)]
    pub describes: Vec<QueryLoad>,
    #[serde(default)]
    pub visibility: Vec<VisibilityLoad>,
}

#[derive(Clone, Copy, Debug, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SignalTarget {
    /// Uniformly random running workflow of the type.
    #[default]
    Running,
    /// A fixed small set of "entity" workflows (hot keys).
    Hot,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignalLoad {
    pub workflow_type: String,
    pub rate: Rate,
    #[serde(default)]
    pub target: SignalTarget,
    /// Number of hot workflows when `target: hot` (they are started at time 0 and run forever).
    #[serde(default = "one")]
    pub hot_workflows: u32,
    #[serde(default = "WorkflowSpec::starters")]
    pub clients: u32,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryLoad {
    pub workflow_type: String,
    pub rate: Rate,
}

#[derive(Clone, Copy, Debug, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum VisibilityOp {
    #[default]
    List,
    Count,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VisibilityLoad {
    #[serde(default = "default_ns")]
    pub namespace: String,
    pub rate: Rate,
    #[serde(default)]
    pub op: VisibilityOp,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScheduleSpec {
    #[serde(default = "default_ns")]
    pub namespace: String,
    /// Number of schedules.
    pub count: u32,
    pub interval: Dur,
    /// All schedules fire at the same instant (cron-style `0 * * * *`) instead of spread out.
    #[serde(default = "yes")]
    pub aligned: bool,
    /// Workflow type started by each action (must exist in `workflows`).
    pub workflow_type: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventSpec {
    pub at: Dur,
    #[serde(default)]
    pub replicas: Option<ReplicasPatch>,
    #[serde(default)]
    pub dynamic_config: BTreeMap<String, DcValue>,
    #[serde(default)]
    pub start_rate: Option<StartRatePatch>,
    #[serde(default)]
    pub label: Option<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ReplicasPatch {
    #[serde(default)]
    pub frontend: Option<u32>,
    #[serde(default)]
    pub history: Option<u32>,
    #[serde(default)]
    pub matching: Option<u32>,
    #[serde(default)]
    pub worker: Option<u32>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StartRatePatch {
    pub workflow_type: String,
    pub rate: Rate,
}

#[derive(Clone, Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct CalibrationSpec {
    /// Observations file(s): YAML observations or Prometheus exposition text.
    #[serde(default)]
    pub observations: Vec<String>,
    /// Use observed `persistence_latency` histograms as DB service times (default true).
    #[serde(default = "yes")]
    pub persistence_latency: bool,
    /// Scale per-service CPU costs so simulated CPU matches observed CPU (default true when
    /// CPU observations are present).
    #[serde(default = "yes")]
    pub cpu: bool,
    /// Scale the scenario's start / signal rates to the observed frontend `service_requests`
    /// rates for StartWorkflowExecution / SignalWorkflowExecution (default true).
    #[serde(default = "yes")]
    pub workload: bool,
}

/// CPU cost overrides: `{ history: { RespondWorkflowTaskCompleted: 900us }, frontend: {..} }`.
#[derive(Clone, Debug, Deserialize, Default)]
#[serde(transparent)]
pub struct CostOverrides(pub BTreeMap<String, BTreeMap<String, Dur>>);

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReportSpec {
    /// Utilisation at which a resource is flagged as a warning / critical hotspot.
    #[serde(default = "ReportSpec::warn")]
    pub warn_utilization: f64,
    #[serde(default = "ReportSpec::crit")]
    pub critical_utilization: f64,
    /// Latency objective for client-facing API p99.
    #[serde(default = "ReportSpec::slo")]
    pub api_p99_slo: Dur,
    /// How many items to list per hotspot category.
    #[serde(default = "ReportSpec::top")]
    pub top: usize,
}

impl ReportSpec {
    fn warn() -> f64 {
        0.70
    }
    fn crit() -> f64 {
        0.90
    }
    fn slo() -> Dur {
        Dur::from_ms(500.0)
    }
    fn top() -> usize {
        5
    }
}

impl Default for ReportSpec {
    fn default() -> Self {
        ReportSpec {
            warn_utilization: Self::warn(),
            critical_utilization: Self::crit(),
            api_p99_slo: Self::slo(),
            top: Self::top(),
        }
    }
}

impl Scenario {
    pub fn load(path: &Path) -> anyhow::Result<Scenario> {
        let base = path.parent().unwrap_or_else(|| Path::new("."));
        Self::load_with_base(path, base)
    }

    /// Load `path`, resolving the scenario's own relative paths (Helm values, dynamic config
    /// files, calibration observations) against `base_dir` rather than the file's folder. A
    /// saved profile keeps a copy of the scenario away from its original folder.
    pub fn load_with_base(path: &Path, base_dir: &Path) -> anyhow::Result<Scenario> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading scenario {}", path.display()))?;
        let mut sc: Scenario = serde_saphyr::from_str(&text)
            .map_err(|e| anyhow::anyhow!("{}: {e}", path.display()))?;
        sc.base_dir = base_dir.to_path_buf();
        if let Some(h) = sc.cluster.helm_values.clone() {
            let hp = sc.resolve_path(&h);
            sc.import_notes = crate::config::helm::apply(&mut sc, &hp)?;
        }
        sc.validate()?;
        Ok(sc)
    }

    pub fn parse_str(text: &str) -> anyhow::Result<Scenario> {
        let mut sc: Scenario = serde_saphyr::from_str(text).map_err(|e| anyhow::anyhow!("{e}"))?;
        sc.base_dir = PathBuf::from(".");
        sc.validate()?;
        Ok(sc)
    }

    /// Files the scenario itself refers to (Helm values, dynamic config files, calibration
    /// observations), as written in the scenario.
    pub fn referenced_files(&self) -> Vec<String> {
        let mut files: Vec<String> = self.cluster.helm_values.iter().cloned().collect();
        files.extend(self.dynamic_config_files.iter().cloned());
        if let Some(c) = &self.calibration {
            files.extend(c.observations.iter().cloned());
        }
        files
    }

    pub fn resolve_path(&self, p: &str) -> PathBuf {
        let pb = PathBuf::from(p);
        if pb.is_absolute() {
            pb
        } else {
            self.base_dir.join(pb)
        }
    }

    pub fn warmup(&self) -> Dur {
        self.warmup.unwrap_or(Dur::from_secs(30.0))
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        let c = &self.cluster;
        anyhow::ensure!(
            c.num_history_shards >= 1,
            "cluster.num_history_shards must be set (>= 1), directly or through cluster.helm_values"
        );
        anyhow::ensure!(
            c.replicas.frontend >= 1 && c.replicas.history >= 1 && c.replicas.matching >= 1,
            "cluster.replicas: frontend, history and matching need at least 1 replica (directly or through cluster.helm_values)"
        );
        anyhow::ensure!(self.duration.0 > 0.0, "duration must be > 0");
        let wf_types: Vec<&str> = self
            .workflows
            .iter()
            .map(|w| w.type_name.as_str())
            .collect();
        for w in &self.workflows {
            anyhow::ensure!(
                self.workers
                    .iter()
                    .any(|f| f.task_queue == w.task_queue && f.namespace == w.namespace),
                "workflow {} uses task queue {}/{} but no worker fleet polls it",
                w.type_name,
                w.namespace,
                w.task_queue
            );
            for s in &w.steps {
                match s {
                    Step::ChildWorkflow(c) => anyhow::ensure!(
                        wf_types.contains(&c.workflow_type.as_str()),
                        "workflow {}: child workflow type {} is not defined",
                        w.type_name,
                        c.workflow_type
                    ),
                    Step::Activity(a) => {
                        let tq = a.task_queue.as_deref().unwrap_or(&w.task_queue);
                        anyhow::ensure!(
                            self.workers
                                .iter()
                                .any(|f| f.task_queue == tq && f.namespace == w.namespace),
                            "workflow {}: activity task queue {tq} has no worker fleet",
                            w.type_name
                        );
                        anyhow::ensure!(
                            (0.0..1.0).contains(&a.failure_rate),
                            "workflow {}: failure_rate must be in [0,1)",
                            w.type_name
                        );
                    }
                    _ => {}
                }
            }
            w.wft_processing
                .build()
                .map_err(|e| anyhow::anyhow!("workflow {}: wft_processing: {e}", w.type_name))?;
        }
        for s in &self.load.signals {
            anyhow::ensure!(
                wf_types.contains(&s.workflow_type.as_str()),
                "load.signals: workflow type {} is not defined",
                s.workflow_type
            );
        }
        for s in &self.schedules {
            anyhow::ensure!(
                wf_types.contains(&s.workflow_type.as_str()),
                "schedules: workflow type {} is not defined",
                s.workflow_type
            );
        }
        for (op, d) in &self.cluster.persistence.latency {
            d.build()
                .map_err(|e| anyhow::anyhow!("cluster.persistence.latency.{op}: {e}"))?;
        }
        Ok(())
    }
}
