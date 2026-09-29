# How tempdes models Temporal 1.31.0

This page describes what the simulator does at each step of a request, and which Temporal
1.31.0 code it follows. Source paths refer to the `temporalio/temporal` repository at tag
`v1.31.0`. The last section lists what is not modelled.

## Simulation kernel (`src/sim`)

* **Executor.** A single-threaded, deterministic discrete-event executor built on Rust futures.
  * Every RPC handler, queue reader, SDK poller loop and workload generator is an `async` task
    that waits only on simulated time or on other tasks.
  * Ties are broken by insertion order.
  * The random number generator is xoshiro256++, forked per component, so a seed reproduces a
    run exactly. Sweep cells use the same seed.
* **Contention points** are FIFO semaphores that keep time-weighted utilisation and wait
  statistics. They model:
  * the shard IO semaphore;
  * workflow locks;
  * SQL connection pools;
  * scheduler worker pools;
  * SDK slots.

  Waiters that time out are skipped when their turn comes. Priority semaphores serve High
  waiters before Low, like Temporal's `PrioritySemaphore`.
* **CPU and the database** are first-come-first-served multi-server stations.
  * Each pod has `GOMAXPROCS` servers (the container CPU limit).
  * The database has `persistence.capacity` servers.
  * Service demand is known when a request arrives, so each burst costs one timer event.
* **Distributions** are stored as piecewise-linear functions of the standard normal quantile in
  log space.
  * A `{p50, p99}` pair is exactly a lognormal.
  * Quantiles read from a `persistence_latency` histogram keep the observed shape, tail included.
* **Histograms** are log-linear with about 6% relative error. The Prometheus export re-buckets
  them into Temporal's default millisecond buckets.

## Routing and membership

| Mechanism | Temporal 1.31.0 | tempdes |
|---|---|---|
| Workflow → shard | `common/util.go` `WorkflowIDToHistoryShard`: `farm.Fingerprint32(nsID + "_" + wfID) % numShards + 1` | Same hash (`src/util/farmhash.rs`), verified against go-farm test vectors |
| Shard → history pod | ringpop hash ring, `common/membership/ringpop/service_resolver.go`, key `strconv.Itoa(shardID)` | Same ring (`src/model/ring.rs`): `system.ringpopReplicaPoints` points per member, hashed `Fingerprint32(address + index)` |
| Task-queue partition → matching pod | ring lookup of `"<nsID>:<name>:<type>"` | Same. Partition names follow `/_sys/<tq>/<n>` for n > 0 |
| Client → frontend pod | gRPC connection through an NLB / kube Service; the server sends GOAWAY at `frontend.keepAliveMaxConnectionAge` (`service/frontend/fx.go`) | `cluster.network.client_lb`. `pinned` (default): one connection per process on a random live frontend, reconnected at max age ±10%. `round_robin`: grpc-go / grpc-java client-side balancing on a headless Service, with a subchannel per resolved pod, one call per subchannel in turn, and DNS re-resolved on GOAWAY or a lost pod at most every 30 s. `proxy`: per-call round robin over healthy pods, plus `proxy_latency`, with new pods joining after `proxy_discovery`. See [EKS.md](EKS.md) |

Pod addresses are synthesised as `10.x.y.z:<grpc port>`, or taken from
`cluster.member_addresses`. Different pod IPs change placement, so an uneven spread of shards
at small replica counts is realistic.

**Scaling events** (`events[].replicas`) change the ring:

* **Removed pods** stop serving immediately. Their shards become unowned until acquired.
* **Moved shards** are unavailable from the event on. After
  `system.ringpopApproximateMaxPropagationTime / 2` (gossip), each new owner acquires them with
  `history.acquireShardConcurrency` in parallel (`service/history/shard/controller_impl.go`
  `acquireShards`). Each acquisition costs:
  * `GetOrCreateShard` + `UpdateShard` (range ID bump);
  * CPU time;
  * a fixed engine start (queue state load, processors).
* **Requests for a moving shard** retry while the shard is unavailable, as `ShardOwnershipLost`
  redirects would. The wait is recorded as `shard_unavailable`.
* **The new owner's mutable-state cache is cold**, because the cache key includes the shard's
  range epoch.
* **Matching partitions** move at the event itself, with no gossip delay. Waiting polls
  return empty and the pollers poll again. In-memory backlog tasks go back to the database
  backlog, and the new owner reads them again.

## Frontend (`src/model/frontend.rs`)

Admission follows the interceptor order in `service/frontend/fx.go`:

1. **Concurrent request limit** (`common/rpc/interceptor/concurrent_request_limit.go`).
   Long-running requests (polls, `QueryWorkflow`, history long polls) per namespace, per API, per
   instance are capped at `frontend.namespaceCount`, or `frontend.globalNamespaceCount` divided
   by the number of frontends. Excess requests are rejected with `ResourceExhausted` (`CONCURRENT_LIMIT`).
2. **Namespace rate limit** (`common/rpc/interceptor/namespace_rate_limit.go`).
   * The per-instance rate is `frontend.namespaceRPS`, or `frontend.globalNamespaceRPS` divided
     by the number of frontends. Burst is `rate × frontend.namespaceBurstRatio`.
   * Visibility APIs use the separate `frontend.namespaceRPS.visibility` buckets (10/s default).
   * Polls fail fast unless `frontend.pollWaitForNamespaceRateLimitToken` is set.
3. **Host rate limit** (`common/rpc/interceptor/rate_limit.go`): `frontend.rps` or
   `frontend.globalRPS` divided by the number of frontends.

Limits 2 and 3 are **priority rate limiters** (`common/quotas/priority_rate_limiter_impl.go`).
They keep one token bucket per priority. An admitted request at priority *p* also reserves a
token from every lower-priority bucket, which can drive those buckets negative.

* Priorities come from `service/frontend/configs/quotas.go`: Start, Signal, Respond and
  heartbeat calls are P1, `GetWorkflowExecutionHistory` P2, Describe, Query and
  `RespondActivityTaskFailed` P3, and polls P4. Operator traffic gets `system.operatorRPSRatio`
  of the rate at P0.
* The namespace limiter renames a history long poll (`GetWorkflowExecutionHistory` with
  `WaitNewEvent`, as a client waiting for a result sends) to `PollWorkflowExecutionHistory`
  and admits it at P5, the lowest priority (`namespace_rate_limit.go`). The host limiter
  classifies by gRPC method, so there it stays at P2. The concurrent request limit counts it,
  but not a plain `GetWorkflowExecutionHistory`.
* Under pressure, high-priority calls consume the budget of lower priorities, so **history long
  polls and worker polls starve first**. Workers then look idle while tasks back up in
  matching.

After admission, the handler spends CPU (per-API cost, plus a per-command cost for
`RespondWorkflowTaskCompleted`) and calls history or matching over the internal network. Frontend
persistence is limited by `frontend.persistenceMaxQPS`.

## History (`src/model/history.rs`, `infra.rs`)

Each API follows the real handler sequence (`service/history/api/*`):

1. `history.rps` admission. This is a priority limiter with the priorities from
   `service/history/configs/quotas.go`: API 1, background-high 2, background-low 3,
   preemptable 4.
2. CPU.
3. Shard ownership check. If the shard is moving, the request waits for acquisition.
4. **Workflow lock** (`service/history/workflow/cache/cache.go`):
   * API callers wait until their deadline minus 500 ms.
   * Queue tasks and other non-API callers wait at most
     `history.cacheNonUserContextLockTimeout` (500 ms).
   * A timeout returns `BUSY_WORKFLOW` (`service/history/consts/const.go`).
5. **Mutable state** through the host-level LRU (`history.hostLevelCacheMaxSize`, keyed with the
   shard epoch). A miss costs `GetWorkflowExecution`. `StartWorkflowExecution` doesn't populate
   the cache.
6. **Persistence under the shard IO semaphore** (`service/history/shard/context_impl.go`,
   `history.shardIOConcurrency`; forced to 1 on Cassandra, with a warning):
   `UpdateWorkflowExecution` / `CreateWorkflowExecution`. The store appends the new history
   events first, inside the same call (`UpdateWorkflowExecution` in
   `common/persistence/sql/execution.go` and `cassandra/execution_store.go`). The append is one
   more database statement but not a separate persistence call: it passes the rate limiters
   once with the write, and its time is part of the write's `persistence_latency`.
7. Task generation (transfer, timer and visibility tasks) is written in the same transaction.
8. Lock release, then post-lock reads. For example, `RecordWorkflowTaskStarted` reads history
   through the events cache (`service/history/events/cache.go`, `history.eventsCacheMaxSizeBytes`)
   or `ReadHistoryBranch`.

State changes are computed first and applied only if the write succeeds. A throttled or
timed-out write evicts the workflow from the cache, like Temporal's `clearMutableState`.

These workflow behaviours are simulated:

* The workflow task lifecycle: schedule, sticky or normal queue, start, and complete or time out.
  The workflow task timeout is `history.defaultWorkflowTaskTimeout`, and a sticky
  schedule-to-start timeout falls back to the normal queue.
* Activities: retries with the activity's retry policy, heartbeats, and the four activity
  timeouts (see [Activity timeouts](#activity-timeouts)).
* User timers.
* Child workflows: start through a transfer task, and completion recorded on the parent.
* Signals. A signal that arrives while a workflow task is running is buffered and flushed into
  the next workflow task.
* Queries and `DescribeWorkflowExecution`.
* `GetWorkflowExecutionHistory` long polls, which expire at
  `history.longPollExpirationInterval`.
* Eager workflow start, where the first workflow task is returned inline. It needs
  `system.enableEagerWorkflowStart`.
* Eager activity dispatch, which needs `system.enableActivityEagerExecution`.

**Persistence calls** (`infra::persist`) go through three stages in order:

1. **The persistence priority limiters** (`common/persistence/client/quotas.go`), in the order
   of `allow` in `persistence_rate_limited_clients.go`. A call made for a namespace meets the
   namespace's per-shard limiter, then the namespace limiter, then the pod's limiter; system
   calls (queue loads, shard management) meet only the pod's.
   * The pod limit is `<service>.persistenceMaxQPS`, or the pod's share of
     `persistenceGlobalMaxQPS`, with burst `system.persistenceQPSBurstRatio`. History splits
     the cluster-wide number by shard ownership, `global × owned shards ÷ numHistoryShards`
     (`service/history/shard/ownership_based_quota_calculator.go`), and re-derives it whenever
     ownership changes. Frontend and matching divide it by their pod count.
   * The namespace limit is `<service>.persistenceNamespaceMaxQPS`, or the pod's share of
     `persistenceGlobalNamespaceMaxQPS` (history by shard ownership, matching by pod count).
     Unset, it is the pod's own rate (`newPriorityNamespaceRateLimiter`), so one busy namespace
     can use a whole pod's budget but no more.
   * `history.persistencePerShardNamespaceMaxQPS`, when set, limits each namespace on each
     shard.
   * Callers have priorities: API calls 1–2, shard management 1, queue loads 3, background
     4–5, preemptable 6. History task executors and matching's task queue managers call on
     behalf of the task's namespace.
   * A rejected call fails immediately with `ResourceExhausted` (`PERSISTENCE_LIMIT`), of
     namespace scope from the first two limiters and system scope from the pod's, as in
     Temporal. There is no waiting for a token.
2. **The pod's SQL connection pool** (`maxConns`). Waiting here is reported as
   `connection-pool`.
3. **The database station.** Each operation draws a service time from its distribution.
   Cassandra lightweight-transaction operations get a 1.6× default.

## History task queues (`src/model/queues.rs`)

Transfer, timer and visibility queues follow `service/history/queues/`:

* **Reading** (`queue_immediate.go`, `queue_scheduled.go`, `reader.go`):
  * A per-shard reader re-reads tasks from the database with `GetTransferTasks`,
    `GetTimerTasks` or `GetVisibilityTasks`.
  * Batches are `history.*TaskBatchSize` tasks.
  * Reads are rate-limited per shard by `history.*ProcessorMaxPollRPS` (20/s). They are also
    limited per host by `history.*ProcessorMaxPollHostRPS`, whose defaults are 0.3 / 0.3 / 0.15
    × `history.persistenceMaxQPS` (`timer_queue_factory.go` and similar).
  * A reader pauses while its shard has `history.queuePendingTasksMaxCount` tasks loaded.
  * The timer queue looks ahead by `history.timerProcessorMaxTimeShift` and wakes at the
    earliest pending timer (`lookAheadTask`).
* **Scheduling** (`scheduler.go`): each queue type has `history.*ProcessorSchedulerWorkerCount`
  workers per host (512), fed by an interleaved weighted round robin
  (`common/tasks/interleaved_weighted_round_robin.go`).
  * A task waits in the channel of its namespace and priority. A freed worker goes to the next
    waiting channel in the flattened round-robin order, where each channel appears as often as
    its weight: `history.*ProcessorSchedulerActiveRoundRobinWeights`, by default high 10, low
    9 and preemptable 1. A saturated pool therefore serves ten high-priority tasks for every
    nine low ones, and busy namespaces take turns instead of one namespace's backlog going
    first. With nothing waiting, a task takes a free worker at once.
  * **The execution queue scheduler** (`execution_aware_scheduler.go`,
    `execution_queue_scheduler.go`) is off unless
    `history.taskSchedulerEnableExecutionQueueScheduler` is set. When a task fails with
    `BUSY_WORKFLOW`, it moves to a queue of its own workflow instead of being resubmitted to the
    shared pool, and while that queue exists every task of the workflow goes there once the
    round robin dispatches it. Each queue runs
    `history.taskSchedulerExecutionQueueSchedulerQueueConcurrency` tasks at a time (2) and
    closes after `...QueueTTL` (5 s) idle. At `...MaxQueues` (500) open queues, busy tasks fall
    back to the shared pool.
* **The scheduler's rate limiter** (`scheduler_quotas.go`, `common/tasks/rate_limited_scheduler.go`)
  is off unless `history.taskSchedulerEnableRateLimiter` is set, and then starts
  `history.taskSchedulerRateLimiterStartupDelay` after the pod.
  * Each task priority has a namespace bucket and a pod bucket, bursting to twice their rate. A
    task is admitted only when both have a token, and it also reserves a token at each lower
    priority.
  * The pod rate is `history.taskSchedulerGlobalMaxQPS` split by shard ownership, else
    `history.taskSchedulerMaxQPS`, else the pod's persistence rate. The namespace rate works the
    same way from the `Namespace` settings, else it is the pod rate.
  * A refused task counts as `task_scheduler_throttled`. In shadow mode
    (`history.taskSchedulerEnableRateLimiterShadowMode`, on by default) it runs anyway.
    Otherwise it goes to the rescheduler (`reader.go`, `rescheduler.go`): it waits the task
    backoff (1 s × 1.1ⁿ, up to 20% less), then retries every 2 s ± 50% until admitted.
* **Execution** (`executable.go`):
  * The task takes the workflow lock as a non-API caller, loads mutable state and does its work.
    Examples: `AddWorkflowTask`/`AddActivityTask` to matching, starting a child, firing a
    timer, or a visibility upsert.
  * Stale tasks, such as a timeout for an already-completed workflow task, cost a small no-op.
  * An activity retry timer pushes the next attempt straight to matching
    (`executeActivityRetryTimerTask` in `timer_queue_active_task_executor.go`). The failed
    attempt's write already recorded the retry, so the timer writes no mutable state and creates
    no transfer task.
* **Retries** (`executable.go`, `rescheduler.go`):
  * Each failure increments the task's attempt. A throttling failure (persistence limit, matching
    `ResourceExhausted`) also counts a throttle; a `BUSY_WORKFLOW` failure leaves that count
    alone, and any other error resets it.
  * A failed task is resubmitted immediately while its attempt is at most 10
    (`shouldResubmitOnNack`), except that throttling allows only one immediate resubmit.
  * Otherwise it backs off 1 s × 1.1ⁿ⁻¹ at attempt n, or when throttled the larger of that and
    3 s × 1.5ᵐ⁻¹ for the m-th throttle in a row, with the rescheduler's jitter (up to 20% less).
  * Each retry records its cause, so hotspots can name the limiter responsible.
* **Checkpoints**: every `history.*ProcessorUpdateAckInterval`, a shard runs
  `RangeComplete*Tasks`. It runs `UpdateShard` at most every `history.shardUpdateMinInterval`
  or `history.shardUpdateMinTasksCompleted` tasks.

## Activity timeouts (`src/model/activity.rs`)

History enforces activity timeouts with timer tasks, as Temporal does
(`service/history/workflow/timer_sequence.go`, and `executeActivityTimeoutTask` in
`timer_queue_active_task_executor.go`).

* **Filling in.** A step's timeouts are normalised as `validateAndNormalizeTimeouts` does.
  Schedule-to-close bounds schedule-to-start and start-to-close, and stands in for them when
  they're not set; the heartbeat timeout never exceeds start-to-close. An activity with neither
  schedule-to-close nor start-to-close gets a start-to-close of ten times its duration's p99,
  between 10 s and 1 h (an SDK would refuse to schedule it). An activity that heartbeats
  without a `heartbeat_timeout` gets twice its heartbeat interval. Unset retry-policy fields
  come from `history.defaultActivityRetryPolicy` for the namespace.
* **One timer per workflow.** A workflow keeps a single activity timer task, for its earliest
  pending timeout. Every transaction that changes its activities (scheduling, start,
  completion, failure, heartbeat) creates the next one if it is missing
  (`CreateNextActivityTimer`). Schedule-to-close runs from the first schedule,
  schedule-to-start from the attempt's scheduled time, and start-to-close and heartbeat from
  the attempt's start (heartbeat from the latest heartbeat).
* **Firing.** The timer task takes the workflow lock, loads mutable state and processes every
  expired timeout in order.
  * A heartbeat timeout that finds a newer heartbeat re-arms itself, which costs a
    mutable-state write.
  * Schedule-to-start and schedule-to-close timeouts fail the activity.
  * Start-to-close and heartbeat timeouts retry the attempt after the policy's next interval
    (`nextBackoffInterval`: initial × coefficientⁿ⁻¹, capped at the maximum interval). The
    activity fails instead when the attempts are used up or the retry would start after the
    schedule-to-close deadline. A retry clears the attempt's timers
    (`UpdateActivityInfoForRetries`).
  * The write records the outcome. A failed activity adds history events and a workflow task.
* **Stale attempts.** A worker's heartbeat, completion or failure for an attempt that has timed
  out is rejected with `NotFound`, as a stale task token is.
* **The workflow's reaction.** An activity that fails for good fails its workflow, as a Go
  workflow returning the error does, unless the step says `on_failure: continue`.
* Timeouts are counted by kind; the server metrics for activities that fail on them are
  `schedule_to_start_timeout`, `start_to_close_timeout`, `schedule_to_close_timeout` and
  `heartbeat_timeout`.

## Matching (`src/model/matching.rs`)

The model follows the 1.31 priority matcher (`service/matching/pri_matcher.go`,
`pri_task_writer.go`, `pri_task_reader.go`, `pri_forwarder.go`, `pri_backlog_manager.go`).

* **Partitions.** Each task queue has `matching.numTaskqueueWritePartitions` /
  `ReadPartitions` partitions per task type. History adds each task to a random write
  partition. Each frontend sends polls to the partition with the fewest of its own outstanding
  polls.
* **Sync match.** `AddTask` matches a waiting poller immediately. The exception is a partition
  whose backlog head is older than `matching.backlogNegligibleAge`: new tasks then queue behind
  the backlog.
  * A child partition with no poller forwards the task to its parent. Forwarding is limited by
    `matching.forwarderMaxOutstandingTasks` in flight and `matching.forwarderMaxRatePerSecond`,
    and waits `matching.maxWaitForPollerBeforeFwd` when there is a backlog.
  * Child partitions forward waiting polls to the root, `matching.forwarderMaxOutstandingPolls`
    at a time.
  * A sync-matched `AddTask` returns only after the poller's `RecordTaskStarted` to history
    completes.
* **Backlog.**
  * Unmatched tasks go to the writer. Its buffer holds `matching.outstandingTaskAppendsThreshold`
    appends (250); beyond that it rejects with `SystemOverloaded`, and history retries.
  * The writer issues one `CreateTasks` of up to `matching.maxTaskBatchSize` tasks at a time.
  * The reader keeps up to 1,000 tasks in memory (the fast path). It otherwise reloads with
    `GetTasks` of `matching.getTasksBatchSize` when the buffer falls to
    `matching.getTasksReloadAt`.
  * Acknowledged ranges are deleted every `matching.taskDeleteInterval`.
* **Limits.**
  * `matching.rps` is a per-host priority limiter (`service/matching/configs/quotas.go`).
  * `admin.matching*DispatchRate` and the task queue's `TaskQueueActivitiesPerSecond` cap
    dispatch.
  * `matching.persistenceMaxQPS` limits matching persistence.
  * Polls expire at `matching.longPollExpirationInterval`.
* **Sticky queues.** Each worker process has a sticky queue on the matching pod that owns it.
  * A sticky task not started within the sticky schedule-to-start timeout moves to the normal
    queue.
  * If the sticky worker has no poller, `AddWorkflowTask` fails with `StickyWorkerUnavailable`
    and history reschedules the task on the normal queue.

## Workers and clients (`src/model/sdk.rs`)

Workers follow the Go SDK.

* **Pollers** split between sticky and normal queues with the Go SDK's balancer
  (`internal_task_pollers.go`). A poller polls the sticky queue when the last sticky response
  reported a backlog, or when no more sticky polls than normal polls are outstanding.
* **Capacity.** Workflow and activity slots cap concurrent work. The sticky cache (LRU of
  `sticky_cache_size`) decides whether a workflow task is a cheap sticky hit or a full replay:
  `GetWorkflowExecutionHistory` plus `replay_per_event` × history length.
* **Workflow programs** are interpreted from `steps`. They produce commands such as
  `ScheduleActivityTask`, `StartTimer`, `StartChildWorkflowExecution` and
  `CompleteWorkflowExecution`.
  * Local activities run inside the workflow task and add their duration to its processing
    time. The workflow-task heartbeat that the SDK sends for very long local activities is not
    modelled.
  * Eager activities are requested on `RespondWorkflowTaskCompleted`.
* **Retries** follow the SDK's gRPC retry interceptor (`internal/common/retry/interceptor.go`).
  * Each call gets one context deadline and every retry happens inside it. The deadline is
    `rpc_timeout` (10 s by default, `defaultRPCTimeout`), set per worker fleet, workflow starter
    and load generator; a history long poll gets 65 s (`defaultGetHistoryTimeout`).
  * `ResourceExhausted` and `Unavailable` are retried after 200 ms × 2ⁿ with ±20% jitter, at
    most 6 s apart. A call still failing at its deadline returns `DeadlineExceeded`.
  * The deadline travels with the request, so an API caller waiting for a workflow lock gives
    up 500 ms before it with `BUSY_WORKFLOW`, and the SDK retries until the deadline.
  * Worker polls are single attempts with `poll_timeout`: the SDK builds their context without
    retry options (`internal_task_pollers.go`), and the poller loop polls again.
  * Client latency includes the retries.
* **Activities** run for a duration drawn from the step, heartbeating every `heartbeat`. The
  SDK gives the activity a context that ends at the earlier of start-to-close from its start
  and schedule-to-close from its first schedule (`calculateActivityDeadline`). The activity is
  assumed to honour it: it stops there, and the SDK drops the result without responding
  ("Activity complete after timeout" in `internal_task_handlers.go`). A heartbeat rejected
  with `NotFound` means the attempt already timed out; the SDK cancels the activity and sends
  nothing.
* **Schedule-to-start** is measured as the SDK measures it (`internal_task_pollers.go`): from the
  poll response's `ScheduledTime` for a workflow task, or `CurrentAttemptScheduledTime` for an
  activity, to `StartedTime`.
  * It includes history's hand-off to matching as well as the wait in matching. A throttled
    transfer task, for example, backs off 3 s or more before it reaches matching.
  * A retry attempt counts from when it was due: Temporal sets the activity's scheduled time to
    the retry time (`updateActivityInfoForRetries` in `mutable_state_impl.go`).
  * The matching partition table's dispatch column covers only the wait in matching.

## Worker service

* **Scheduler workflows** (`service/worker/scheduler/workflow.go`, `fx.go`) run on the
  per-namespace worker task queue, hosted on `worker.perNamespaceWorkerCount` worker pods.
  * Starts share one token bucket per namespace of `worker.schedulerNamespaceStartWorkflowRPS`,
    divided across those workers.
  * If the wait is at most `worker.schedulerLocalActivitySleepLimit`, it is spent inside the
    local activity. Otherwise the workflow returns `RateLimited` and sleeps on a timer.
  * Aligned schedules (the same cron) therefore produce long action delays.
* **Visibility (Elasticsearch).** Writes from the history visibility queue go through a per-host
  bulk processor: `worker.ESProcessorBulkActions`, `worker.ESProcessorFlushInterval` and
  `worker.ESProcessorNumOfWorkers` concurrent bulks. SQL visibility writes go straight to the
  visibility database.

## CPU costs

Each operation has a CPU cost in microseconds per service (`Costs::default` in
`src/model/params.rs`). Examples: frontend `StartWorkflowExecution` 180 µs, history
`RespondWorkflowTaskCompleted` 520 µs plus 90 µs per command, and a mutable-state cache miss
260 µs. These defaults are rough estimates for current-generation x86 cores. Two ways to adjust
them:

* **Calibrate** with `container_cpu_usage_seconds_total`. Pilot runs of the observed
  configuration scale each service's costs by observed ÷ simulated cores, clamped to 0.05–20×.
* **Override** individual entries under `costs:`. Keys are API names, task types, or
  `per_command`, `per_event_read`, `cache_miss`, `task_noop`, `dispatch`, `forward`,
  `backlog_per_task` and `scheduler_workflow_task`.

## Calibrating persistence latency (`src/calibrate.rs`, `src/run.rs`)

Production's `persistence_latency` measures the whole call: the wait for a connection, queueing
in the database, and for Create/UpdateWorkflowExecution the history append inside the call.
Used directly as the service time, it would count the queueing twice, and the append once more.

* An observed Create/UpdateWorkflowExecution latency replaces the write together with its
  append, so calibrated writes run no separate append statement.
* Each operation's service time is the observed distribution times a factor. Up to three pilot
  runs of the observed configuration, at the observed load, adjust the factor by observed ÷
  simulated mean latency until they agree within 3%. The factor stays between 0.05 and 1,
  since latency can't be shorter than service time, and an operation with fewer than 50 calls
  in the pilot keeps the observed distribution.
* The CPU fit runs in the same pilots. Sweep cells and `--load` runs reuse the results.

## Hotspot rules (`src/report/rules.rs`)

Rules read the collected statistics and emit hotspots with evidence, Temporal metric names and
knobs.

* **Utilisation thresholds.** Resources at 70% utilisation are a warning and at 90% are
  critical. Both are configurable under `report:`.
* **Rate limiters.** A limiter that rejects requests is a `rate-limit` hotspot. One that runs at
  70% or more of its limit on any pod is a `headroom` warning, raised before rejections start.
* **Throughput.** Two tests, each flagged only beyond a 3σ Poisson band:
  * *start shortfall*: accepted starts fall short of the offered rate, or more than 1% of
    starts fail;
  * *not keeping up*: workflows close (complete or fail) more slowly than a healthy cluster
    would close them. The expected rate comes from the workflow's own run time, sampled 2,000
    times from its steps, and from the warm-up.
    * Each sample includes activity durations, failed attempts and the retry policy's
      intervals, timers, children, signal timeouts and a workflow task per step.
    * With starts at a steady rate from time zero, a healthy cluster closes, at each moment of
      the window, the workflows started at least their run time earlier.
    * So a long retry tail counts: with intervals that double up to 100 s, a few runs take
      many minutes, and a workflow that runs longer than the warm-up closes during only part
      of the window, or not at all.

    It needs a 10% shortfall at least, and shows how fast the number of running workflows grew.
    Workflows with no known run time (waiting for signals without a timeout) get only the start
    test.
* **Activity timeouts.** Timeouts that fired, by kind, are an `activity-timeouts` hotspot:
  critical when activities failed for good or more than 1% of attempts timed out.
* **Causal ranking.** After detection, a causal pass re-ranks results:
  * when polls are rejected, the frontend or matching limiter that rejects them outranks the
    worker schedule-to-start latency and the matching backlog they cause;
  * a `history.rps`, `matching.rps` or persistence limit that rejects calls outranks the
    schedule-to-start latency, backlog, timeouts, throttled queue tasks and API symptoms that
    its retries cause. Limiters raised together keep their own order, so the one rejecting
    more calls comes first;
  * database saturation outranks the shard, lock, connection-pool and dispatch symptoms it
    causes;
  * the shard rule attributes a hot shard to the one workflow whose lock drives it.
* **Headline.** It names the top hotspot and the busiest resource: CPU, database, shard IO or
  any rate limiter. With no hotspots, it also estimates headroom.

## What is not modelled

* Multi-cluster replication.
* Nexus.
* Workflow Update.
* Worker Versioning and deployments.
* CHASM-based components.
* Archival.
* Batch operations.
* The classic and fairness matchers.
* DNS caching, TLS handshake cost and cross-AZ effects of the client load-balancing modes.
* GC pauses and memory pressure.
* Database-internal contention (row locks, vacuum, compaction).
* The minute a new pod runs on its per-pod persistence setting before its limiter first
  re-reads the cluster-wide share.
* Kubernetes scheduling and pod restarts other than scaling events.

Dynamic config keys that aren't simulated are still validated, and they appear in reports as
*(not simulated)* when relevant.
