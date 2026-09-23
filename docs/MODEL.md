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
| Client → frontend pod | gRPC connection through an NLB / kube Service; the server sends GOAWAY at `frontend.keepAliveMaxConnectionAge` (`service/frontend/fx.go`) | One connection per client and worker process, placed on a random live frontend and reconnected at max age ±10% jitter |

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

* Priorities come from `service/frontend/configs/quotas.go`: Start, Signal and Respond calls are
  P1, polls are P4, and operator traffic gets `system.operatorRPSRatio` of the rate at P0.
* Under pressure, high-priority calls consume the budget of lower priorities, so **polls starve
  first**. Workers then look idle while tasks back up in matching.

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
   `AppendHistoryNodes`, then `UpdateWorkflowExecution` / `CreateWorkflowExecution`.
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
* Activities: retries with backoff, heartbeats and their timeouts, and start-to-close timeouts.
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

1. **The pod's persistence priority limiter** (`common/persistence/client/quotas.go`).
   * The limit is `<service>.persistenceMaxQPS`, or `persistenceGlobalMaxQPS` divided by the
     number of pods, with burst `system.persistenceQPSBurstRatio`.
   * Callers have priorities: API calls 1–2, shard management 1, queue loads 3, background
     4–5, preemptable 6.
   * A rejected call fails immediately with `ResourceExhausted` (`PERSISTENCE_LIMIT`), as in
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
  workers per host (512). High-priority tasks run before low.
* **Execution** (`executable.go`):
  * The task takes the workflow lock as a non-API caller, loads mutable state and does its work.
    Examples: `AddWorkflowTask`/`AddActivityTask` to matching, starting a child, firing a
    timer, or a visibility upsert.
  * Stale tasks, such as a timeout for an already-completed workflow task, cost a small no-op.
* **Retries** (`rescheduler.go`):
  * A `BUSY_WORKFLOW` failure is resubmitted immediately, up to 10 attempts. After that it backs
    off 1 s × 1.1ⁿ.
  * A throttling failure (persistence limit, matching `ResourceExhausted`) backs off
    max(1 s × 1.1ⁿ, 3 s × 1.5ⁿ⁻¹).
  * Each retry records its cause, so hotspots can name the limiter responsible.
* **Checkpoints**: every `history.*ProcessorUpdateAckInterval`, a shard runs
  `RangeComplete*Tasks`. It runs `UpdateShard` at most every `history.shardUpdateMinInterval`
  or `history.shardUpdateMinTasksCompleted` tasks.

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
* **Retries.** Transient errors back off from 100 ms and `ResourceExhausted` errors from 1 s,
  both doubling up to 10 s. Each call has a deadline, and client latency includes retries.

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

* **Calibrate** with `container_cpu_usage_seconds_total`. A pilot run of the observed
  configuration scales each service's costs by observed ÷ simulated cores, clamped to 0.05–20×.
* **Override** individual entries under `costs:`. Keys are API names, task types, or
  `per_command`, `per_event_read`, `cache_miss`, `task_noop`, `dispatch`, `forward`,
  `backlog_per_task` and `scheduler_workflow_task`.

## Hotspot rules (`src/report/rules.rs`)

Rules read the collected statistics and emit hotspots with evidence, Temporal metric names and
knobs.

* **Utilisation thresholds.** Resources at 70% utilisation are a warning and at 90% are
  critical. Both are configurable under `report:`.
* **Rate limiters.** A limiter that rejects requests is a `rate-limit` hotspot. One that runs at
  70% or more of its limit on any pod is a `headroom` warning, raised before rejections start.
* **Throughput.** A shortfall is flagged only beyond a 3σ Poisson band.
* **Causal ranking.** After detection, a causal pass re-ranks results:
  * when polls are rejected, the frontend or matching limiter that rejects them outranks the
    worker schedule-to-start latency and the matching backlog they cause;
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
* Frontend DNS or NLB effects beyond connection pinning.
* GC pauses and memory pressure.
* Database-internal contention (row locks, vacuum, compaction).
* Kubernetes scheduling and pod restarts other than scaling events.

Dynamic config keys that aren't simulated are still validated, and they appear in reports as
*(not simulated)* when relevant.
