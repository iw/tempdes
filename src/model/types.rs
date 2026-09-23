//! Identifiers and enumerations shared across the Temporal model, named after the operation /
//! error names Temporal uses in its metrics so reports and emitted metrics line up with
//! production dashboards.

use std::fmt;

/// Temporal server roles deployed as separate EKS workloads.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Service {
    Frontend = 0,
    History = 1,
    Matching = 2,
    Worker = 3,
}

impl Service {
    pub const ALL: [Service; 4] = [
        Service::Frontend,
        Service::History,
        Service::Matching,
        Service::Worker,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Service::Frontend => "frontend",
            Service::History => "history",
            Service::Matching => "matching",
            Service::Worker => "worker",
        }
    }

    /// gRPC port (ringpop members are identified by `podIP:grpcPort`).
    pub fn grpc_port(self) -> u16 {
        match self {
            Service::Frontend => 7233,
            Service::History => 7234,
            Service::Matching => 7235,
            Service::Worker => 7239,
        }
    }

    pub fn idx(self) -> usize {
        self as usize
    }

    pub fn parse(s: &str) -> Option<Service> {
        match s.to_ascii_lowercase().as_str() {
            "frontend" => Some(Service::Frontend),
            "history" => Some(Service::History),
            "matching" => Some(Service::Matching),
            "worker" => Some(Service::Worker),
            _ => None,
        }
    }
}

impl fmt::Display for Service {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Index of a pod across all services.
pub type PodId = usize;
/// History shard (1-based like Temporal; index = shard - 1).
pub type ShardId = u32;
pub type WfId = u32;

/// Frontend (and SDK-visible) API operations.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Api {
    StartWorkflowExecution,
    SignalWorkflowExecution,
    PollWorkflowTaskQueue,
    PollActivityTaskQueue,
    RespondWorkflowTaskCompleted,
    RespondActivityTaskCompleted,
    RespondActivityTaskFailed,
    RecordActivityTaskHeartbeat,
    QueryWorkflow,
    DescribeWorkflowExecution,
    GetWorkflowExecutionHistory,
    ListWorkflowExecutions,
    CountWorkflowExecutions,
}

impl Api {
    pub const ALL: [Api; 13] = [
        Api::StartWorkflowExecution,
        Api::SignalWorkflowExecution,
        Api::PollWorkflowTaskQueue,
        Api::PollActivityTaskQueue,
        Api::RespondWorkflowTaskCompleted,
        Api::RespondActivityTaskCompleted,
        Api::RespondActivityTaskFailed,
        Api::RecordActivityTaskHeartbeat,
        Api::QueryWorkflow,
        Api::DescribeWorkflowExecution,
        Api::GetWorkflowExecutionHistory,
        Api::ListWorkflowExecutions,
        Api::CountWorkflowExecutions,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Api::StartWorkflowExecution => "StartWorkflowExecution",
            Api::SignalWorkflowExecution => "SignalWorkflowExecution",
            Api::PollWorkflowTaskQueue => "PollWorkflowTaskQueue",
            Api::PollActivityTaskQueue => "PollActivityTaskQueue",
            Api::RespondWorkflowTaskCompleted => "RespondWorkflowTaskCompleted",
            Api::RespondActivityTaskCompleted => "RespondActivityTaskCompleted",
            Api::RespondActivityTaskFailed => "RespondActivityTaskFailed",
            Api::RecordActivityTaskHeartbeat => "RecordActivityTaskHeartbeat",
            Api::QueryWorkflow => "QueryWorkflow",
            Api::DescribeWorkflowExecution => "DescribeWorkflowExecution",
            Api::GetWorkflowExecutionHistory => "GetWorkflowExecutionHistory",
            Api::ListWorkflowExecutions => "ListWorkflowExecutions",
            Api::CountWorkflowExecutions => "CountWorkflowExecutions",
        }
    }

    pub fn idx(self) -> usize {
        self as usize
    }

    /// Priority in the frontend priority rate limiter (`service/frontend/configs/quotas.go`).
    pub fn frontend_priority(self) -> usize {
        match self {
            Api::StartWorkflowExecution
            | Api::SignalWorkflowExecution
            | Api::RespondWorkflowTaskCompleted
            | Api::RespondActivityTaskCompleted
            | Api::RecordActivityTaskHeartbeat => 1,
            Api::GetWorkflowExecutionHistory => 2,
            Api::DescribeWorkflowExecution
            | Api::QueryWorkflow
            | Api::RespondActivityTaskFailed => 3,
            Api::PollWorkflowTaskQueue | Api::PollActivityTaskQueue => 4,
            // visibility APIs use their own bucket at P1
            Api::ListWorkflowExecutions | Api::CountWorkflowExecutions => 1,
        }
    }

    pub fn is_visibility(self) -> bool {
        matches!(
            self,
            Api::ListWorkflowExecutions | Api::CountWorkflowExecutions
        )
    }

    /// Counted by the per-namespace concurrent long-running request limiter
    /// (`frontend.namespaceCount`).
    pub fn is_long_running(self) -> bool {
        matches!(
            self,
            Api::PollWorkflowTaskQueue | Api::PollActivityTaskQueue | Api::QueryWorkflow
        )
    }

    /// Persistence priority for history calls made on behalf of this API
    /// (`common/persistence/client/quotas.go`): Start/Signal get 1, other API calls 2.
    pub fn persistence_priority(self) -> usize {
        match self {
            Api::StartWorkflowExecution
            | Api::SignalWorkflowExecution
            | Api::GetWorkflowExecutionHistory => 1,
            _ => 2,
        }
    }
}

impl fmt::Display for Api {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// History service internal API operations (as seen in history `service_requests`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum HistApi {
    StartWorkflowExecution,
    SignalWorkflowExecution,
    RecordWorkflowTaskStarted,
    RecordActivityTaskStarted,
    RespondWorkflowTaskCompleted,
    RespondActivityTaskCompleted,
    RespondActivityTaskFailed,
    RecordActivityTaskHeartbeat,
    RecordChildExecutionCompleted,
    DescribeWorkflowExecution,
    GetWorkflowExecutionHistory,
    QueryWorkflow,
}

impl HistApi {
    pub const ALL: [HistApi; 12] = [
        HistApi::StartWorkflowExecution,
        HistApi::SignalWorkflowExecution,
        HistApi::RecordWorkflowTaskStarted,
        HistApi::RecordActivityTaskStarted,
        HistApi::RespondWorkflowTaskCompleted,
        HistApi::RespondActivityTaskCompleted,
        HistApi::RespondActivityTaskFailed,
        HistApi::RecordActivityTaskHeartbeat,
        HistApi::RecordChildExecutionCompleted,
        HistApi::DescribeWorkflowExecution,
        HistApi::GetWorkflowExecutionHistory,
        HistApi::QueryWorkflow,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            HistApi::StartWorkflowExecution => "StartWorkflowExecution",
            HistApi::SignalWorkflowExecution => "SignalWorkflowExecution",
            HistApi::RecordWorkflowTaskStarted => "RecordWorkflowTaskStarted",
            HistApi::RecordActivityTaskStarted => "RecordActivityTaskStarted",
            HistApi::RespondWorkflowTaskCompleted => "RespondWorkflowTaskCompleted",
            HistApi::RespondActivityTaskCompleted => "RespondActivityTaskCompleted",
            HistApi::RespondActivityTaskFailed => "RespondActivityTaskFailed",
            HistApi::RecordActivityTaskHeartbeat => "RecordActivityTaskHeartbeat",
            HistApi::RecordChildExecutionCompleted => "RecordChildExecutionCompleted",
            HistApi::DescribeWorkflowExecution => "DescribeWorkflowExecution",
            HistApi::GetWorkflowExecutionHistory => "GetWorkflowExecutionHistory",
            HistApi::QueryWorkflow => "QueryWorkflow",
        }
    }

    pub fn idx(self) -> usize {
        self as usize
    }
}

/// Matching service operations.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum MatchApi {
    AddWorkflowTask,
    AddActivityTask,
    PollWorkflowTaskQueue,
    PollActivityTaskQueue,
    QueryWorkflow,
}

impl MatchApi {
    pub const ALL: [MatchApi; 5] = [
        MatchApi::AddWorkflowTask,
        MatchApi::AddActivityTask,
        MatchApi::PollWorkflowTaskQueue,
        MatchApi::PollActivityTaskQueue,
        MatchApi::QueryWorkflow,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            MatchApi::AddWorkflowTask => "AddWorkflowTask",
            MatchApi::AddActivityTask => "AddActivityTask",
            MatchApi::PollWorkflowTaskQueue => "PollWorkflowTaskQueue",
            MatchApi::PollActivityTaskQueue => "PollActivityTaskQueue",
            MatchApi::QueryWorkflow => "QueryWorkflow",
        }
    }

    pub fn idx(self) -> usize {
        self as usize
    }
}

/// Persistence operations (the `operation` tag of `persistence_*` metrics).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum PersistOp {
    CreateWorkflowExecution,
    UpdateWorkflowExecution,
    GetWorkflowExecution,
    GetCurrentExecution,
    AppendHistoryNodes,
    ReadHistoryBranch,
    GetTransferTasks,
    GetTimerTasks,
    GetVisibilityTasks,
    RangeCompleteTransferTasks,
    RangeCompleteTimerTasks,
    RangeCompleteVisibilityTasks,
    UpdateShard,
    GetOrCreateShard,
    CreateTasks,
    GetTasks,
    CompleteTasksLessThan,
    UpdateTaskQueue,
    GetTaskQueue,
    // visibility store
    RecordWorkflowExecutionStarted,
    UpsertWorkflowExecution,
    RecordWorkflowExecutionClosed,
    ListWorkflowExecutions,
    CountWorkflowExecutions,
}

impl PersistOp {
    pub const ALL: [PersistOp; 24] = [
        PersistOp::CreateWorkflowExecution,
        PersistOp::UpdateWorkflowExecution,
        PersistOp::GetWorkflowExecution,
        PersistOp::GetCurrentExecution,
        PersistOp::AppendHistoryNodes,
        PersistOp::ReadHistoryBranch,
        PersistOp::GetTransferTasks,
        PersistOp::GetTimerTasks,
        PersistOp::GetVisibilityTasks,
        PersistOp::RangeCompleteTransferTasks,
        PersistOp::RangeCompleteTimerTasks,
        PersistOp::RangeCompleteVisibilityTasks,
        PersistOp::UpdateShard,
        PersistOp::GetOrCreateShard,
        PersistOp::CreateTasks,
        PersistOp::GetTasks,
        PersistOp::CompleteTasksLessThan,
        PersistOp::UpdateTaskQueue,
        PersistOp::GetTaskQueue,
        PersistOp::RecordWorkflowExecutionStarted,
        PersistOp::UpsertWorkflowExecution,
        PersistOp::RecordWorkflowExecutionClosed,
        PersistOp::ListWorkflowExecutions,
        PersistOp::CountWorkflowExecutions,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            PersistOp::CreateWorkflowExecution => "CreateWorkflowExecution",
            PersistOp::UpdateWorkflowExecution => "UpdateWorkflowExecution",
            PersistOp::GetWorkflowExecution => "GetWorkflowExecution",
            PersistOp::GetCurrentExecution => "GetCurrentExecution",
            PersistOp::AppendHistoryNodes => "AppendHistoryNodes",
            PersistOp::ReadHistoryBranch => "ReadHistoryBranch",
            PersistOp::GetTransferTasks => "GetTransferTasks",
            PersistOp::GetTimerTasks => "GetTimerTasks",
            PersistOp::GetVisibilityTasks => "GetVisibilityTasks",
            PersistOp::RangeCompleteTransferTasks => "RangeCompleteTransferTasks",
            PersistOp::RangeCompleteTimerTasks => "RangeCompleteTimerTasks",
            PersistOp::RangeCompleteVisibilityTasks => "RangeCompleteVisibilityTasks",
            PersistOp::UpdateShard => "UpdateShard",
            PersistOp::GetOrCreateShard => "GetOrCreateShard",
            PersistOp::CreateTasks => "CreateTasks",
            PersistOp::GetTasks => "GetTasks",
            PersistOp::CompleteTasksLessThan => "CompleteTasksLessThan",
            PersistOp::UpdateTaskQueue => "UpdateTaskQueue",
            PersistOp::GetTaskQueue => "GetTaskQueue",
            PersistOp::RecordWorkflowExecutionStarted => "RecordWorkflowExecutionStarted",
            PersistOp::UpsertWorkflowExecution => "UpsertWorkflowExecution",
            PersistOp::RecordWorkflowExecutionClosed => "RecordWorkflowExecutionClosed",
            PersistOp::ListWorkflowExecutions => "ListWorkflowExecutions",
            PersistOp::CountWorkflowExecutions => "CountWorkflowExecutions",
        }
    }

    pub fn idx(self) -> usize {
        self as usize
    }

    pub fn parse(s: &str) -> Option<PersistOp> {
        PersistOp::ALL
            .into_iter()
            .find(|o| o.as_str().eq_ignore_ascii_case(s))
    }

    pub fn is_visibility(self) -> bool {
        matches!(
            self,
            PersistOp::RecordWorkflowExecutionStarted
                | PersistOp::UpsertWorkflowExecution
                | PersistOp::RecordWorkflowExecutionClosed
                | PersistOp::ListWorkflowExecutions
                | PersistOp::CountWorkflowExecutions
        )
    }

    /// Operations counted against the persistence rate limiter (AppendHistoryNodes is part of
    /// the Create/Update call and not charged separately).
    pub fn rate_limited(self) -> bool {
        !matches!(self, PersistOp::AppendHistoryNodes)
    }

    /// Default service time (p50, p99) in milliseconds at low load for SQL / Cassandra.
    pub fn default_latency_ms(self, cassandra: bool) -> (f64, f64) {
        use PersistOp::*;
        let (p50, p99) = match self {
            CreateWorkflowExecution => (4.0, 20.0),
            UpdateWorkflowExecution => (3.0, 15.0),
            GetWorkflowExecution => (1.5, 8.0),
            GetCurrentExecution => (1.0, 5.0),
            AppendHistoryNodes => (1.5, 8.0),
            ReadHistoryBranch => (2.0, 10.0),
            GetTransferTasks | GetTimerTasks | GetVisibilityTasks => (1.5, 8.0),
            RangeCompleteTransferTasks | RangeCompleteTimerTasks | RangeCompleteVisibilityTasks => {
                (2.0, 10.0)
            }
            UpdateShard | GetOrCreateShard => (2.0, 10.0),
            CreateTasks => (3.0, 15.0),
            GetTasks => (2.0, 10.0),
            CompleteTasksLessThan => (2.0, 10.0),
            UpdateTaskQueue | GetTaskQueue => (2.0, 10.0),
            RecordWorkflowExecutionStarted
            | UpsertWorkflowExecution
            | RecordWorkflowExecutionClosed => (3.0, 20.0),
            ListWorkflowExecutions | CountWorkflowExecutions => (25.0, 250.0),
        };
        if cassandra && !self.is_visibility() {
            // lightweight transactions (Paxos) make conditional writes slower
            match self {
                CreateWorkflowExecution
                | UpdateWorkflowExecution
                | CreateTasks
                | UpdateTaskQueue
                | UpdateShard => (p50 * 1.6, p99 * 1.6),
                _ => (p50, p99),
            }
        } else {
            (p50, p99)
        }
    }
}

impl fmt::Display for PersistOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// History task categories.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Category {
    Transfer = 0,
    Timer = 1,
    Visibility = 2,
}

impl Category {
    pub const ALL: [Category; 3] = [Category::Transfer, Category::Timer, Category::Visibility];

    pub fn as_str(self) -> &'static str {
        match self {
            Category::Transfer => "transfer",
            Category::Timer => "timer",
            Category::Visibility => "visibility",
        }
    }

    pub fn idx(self) -> usize {
        self as usize
    }

    pub fn load_op(self) -> PersistOp {
        match self {
            Category::Transfer => PersistOp::GetTransferTasks,
            Category::Timer => PersistOp::GetTimerTasks,
            Category::Visibility => PersistOp::GetVisibilityTasks,
        }
    }

    pub fn range_complete_op(self) -> PersistOp {
        match self {
            Category::Transfer => PersistOp::RangeCompleteTransferTasks,
            Category::Timer => PersistOp::RangeCompleteTimerTasks,
            Category::Visibility => PersistOp::RangeCompleteVisibilityTasks,
        }
    }
}

/// History task types (the `task_type` tag of `task_*` metrics).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TaskType {
    TransferWorkflowTask,
    TransferActivityTask,
    TransferCloseExecution,
    TransferStartChildExecution,
    TimerWorkflowTaskTimeout,
    TimerActivityTimeout,
    TimerUserTimer,
    TimerActivityRetryTimer,
    VisibilityStartExecution,
    VisibilityUpsertExecution,
    VisibilityCloseExecution,
}

impl TaskType {
    pub const ALL: [TaskType; 11] = [
        TaskType::TransferWorkflowTask,
        TaskType::TransferActivityTask,
        TaskType::TransferCloseExecution,
        TaskType::TransferStartChildExecution,
        TaskType::TimerWorkflowTaskTimeout,
        TaskType::TimerActivityTimeout,
        TaskType::TimerUserTimer,
        TaskType::TimerActivityRetryTimer,
        TaskType::VisibilityStartExecution,
        TaskType::VisibilityUpsertExecution,
        TaskType::VisibilityCloseExecution,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            TaskType::TransferWorkflowTask => "TransferActiveTaskWorkflowTask",
            TaskType::TransferActivityTask => "TransferActiveTaskActivityTask",
            TaskType::TransferCloseExecution => "TransferActiveTaskCloseExecution",
            TaskType::TransferStartChildExecution => "TransferActiveTaskStartChildExecution",
            TaskType::TimerWorkflowTaskTimeout => "TimerActiveTaskWorkflowTaskTimeout",
            TaskType::TimerActivityTimeout => "TimerActiveTaskActivityTimeout",
            TaskType::TimerUserTimer => "TimerActiveTaskUserTimer",
            TaskType::TimerActivityRetryTimer => "TimerActiveTaskActivityRetryTimer",
            TaskType::VisibilityStartExecution => "VisibilityTaskStartExecution",
            TaskType::VisibilityUpsertExecution => "VisibilityTaskUpsertExecution",
            TaskType::VisibilityCloseExecution => "VisibilityTaskCloseExecution",
        }
    }

    pub fn idx(self) -> usize {
        self as usize
    }

    pub fn category(self) -> Category {
        match self {
            TaskType::TransferWorkflowTask
            | TaskType::TransferActivityTask
            | TaskType::TransferCloseExecution
            | TaskType::TransferStartChildExecution => Category::Transfer,
            TaskType::TimerWorkflowTaskTimeout
            | TaskType::TimerActivityTimeout
            | TaskType::TimerUserTimer
            | TaskType::TimerActivityRetryTimer => Category::Timer,
            TaskType::VisibilityStartExecution
            | TaskType::VisibilityUpsertExecution
            | TaskType::VisibilityCloseExecution => Category::Visibility,
        }
    }

    /// Timeout tasks run at Low priority in the host scheduler (and BackgroundLow persistence).
    pub fn low_priority(self) -> bool {
        matches!(
            self,
            TaskType::TimerWorkflowTaskTimeout | TaskType::TimerActivityTimeout
        )
    }
}

/// Errors surfaced by simulated RPCs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Err {
    /// ResourceExhausted with a cause.
    ResourceExhausted(ReCause, Scope),
    DeadlineExceeded,
    ShardOwnershipLost,
    Unavailable,
    NotFound,
    StickyWorkerUnavailable,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Scope {
    System,
    Namespace,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ReCause {
    RpsLimit,
    ConcurrentLimit,
    PersistenceLimit,
    BusyWorkflow,
    SystemOverloaded,
}

impl ReCause {
    pub fn as_str(self) -> &'static str {
        match self {
            ReCause::RpsLimit => "RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT",
            ReCause::ConcurrentLimit => "RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT",
            ReCause::PersistenceLimit => "RESOURCE_EXHAUSTED_CAUSE_PERSISTENCE_LIMIT",
            ReCause::BusyWorkflow => "RESOURCE_EXHAUSTED_CAUSE_BUSY_WORKFLOW",
            ReCause::SystemOverloaded => "RESOURCE_EXHAUSTED_CAUSE_SYSTEM_OVERLOADED",
        }
    }
}

impl Scope {
    pub fn as_str(self) -> &'static str {
        match self {
            Scope::System => "RESOURCE_EXHAUSTED_SCOPE_SYSTEM",
            Scope::Namespace => "RESOURCE_EXHAUSTED_SCOPE_NAMESPACE",
        }
    }
}

impl Err {
    pub fn label(self) -> String {
        match self {
            Err::ResourceExhausted(c, s) => {
                format!("ResourceExhausted({}, {})", short_cause(c), short_scope(s))
            }
            Err::DeadlineExceeded => "DeadlineExceeded".into(),
            Err::ShardOwnershipLost => "ShardOwnershipLost".into(),
            Err::Unavailable => "Unavailable".into(),
            Err::NotFound => "NotFound".into(),
            Err::StickyWorkerUnavailable => "StickyWorkerUnavailable".into(),
        }
    }

    pub fn is_resource_exhausted(self) -> bool {
        matches!(self, Err::ResourceExhausted(..))
    }
}

fn short_cause(c: ReCause) -> &'static str {
    match c {
        ReCause::RpsLimit => "RpsLimit",
        ReCause::ConcurrentLimit => "ConcurrentLimit",
        ReCause::PersistenceLimit => "PersistenceLimit",
        ReCause::BusyWorkflow => "BusyWorkflow",
        ReCause::SystemOverloaded => "SystemOverloaded",
    }
}

fn short_scope(s: Scope) -> &'static str {
    match s {
        Scope::System => "system",
        Scope::Namespace => "namespace",
    }
}

pub type Res<T> = Result<T, Err>;
