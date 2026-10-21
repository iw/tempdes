# Changelog

All notable changes to tempdes are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **`profile init`.** `tempdes profile init NAME` starts a profile from a prototype: a
  `profile.yaml` and a `scenario.yaml` showing every setting, each commented, the optional ones
  commented out with their defaults or an example. It runs as written. `--description` sets the
  line `profile list` shows. A test switches every commented-out setting on and runs the
  result, so the prototype stays valid as the settings change.

- **Fetching into a profile.** With `--profile NAME`, `metrics fetch` writes into the profile's
  `metrics` folder in the store (`observed.yaml`, and `baseline.yaml`), and `workload fetch`
  into its `workload` folder (`histories/`). `workload import --profile NAME` reads
  `workload/histories` and writes `workload/workflows.yaml` rather than printing it. Relative
  paths are taken inside the folder, and absolute ones as given. `metrics fetch` says when the
  profile doesn't read the file yet. `workload import -o` now writes its file readable only by
  you, as it names your workflow and activity types and task queues. `profile save` now copies observed metrics into
  `metrics/` too, rather than `observed/`; profiles saved before keep working as they are.

- **`workload fetch` through the Temporal CLI.** `--cli` lists and fetches with the Temporal CLI
  (`temporal workflow list`, `workflow show` and `operator namespace describe`), run with its
  own configuration, for a frontend that only its gRPC port reaches. `--namespace` and
  `--address` are passed on, and `--cli-arg` passes anything else, such as
  `--cli-arg=--env=prod`. Payloads are dropped whether the CLI prints them encoded or decoded,
  and workflow IDs containing `/`, which the HTTP API can't take, are fetched.

- **`workload fetch` slice by slice.** The window is listed one slice at a time (`--slice`, a
  minute by default; a window of more than 1,440 slices gets longer ones). `fetch.yaml` counts
  each type's starts and closes in every slice, how its executions stood when listed, and the
  p50, p90, p99 and max durations of the closed ones. The summary gives each type a line: when
  its starts ran and peaked, its statuses and its durations. A `fetch.yaml` written before
  reads as it did.

- **`metrics fetch` records the dispatch path.** It adds six metrics that tempdes doesn't read
  yet, to compare with a simulation by eye:
  - **Waits:** `cache_latency` for mutable state, which is mostly the wait for a workflow's
    lock, and `syncmatch_latency`, which includes the poller's start record in history.
  - **Backlog:** `approximate_backlog_age_seconds`, recorded as the largest in the window
    (`max_over_time`) rather than summed. `task_write_throttle_count` counts appends matching
    refused with SystemOverloaded.
  - **Shards and pollers:** `shard_closed_count`, and the SDKs' `temporal_num_pollers` by poller
    type.

  A zero throttle rate and a backlog age of zero are kept, because each says the limit never
  came into play. `metrics queries` prints the same queries.

- **The registry generator's Go lexing is public.** `config::registry_gen` exports the helpers
  it reads Temporal's source with (`collect_go_files`, `relative`, `one_line`, `is_func_decl`,
  `is_space`, `is_ident`, `skip_opaque`, `closing`, `split_args`, `string_const`), so other
  readers of the source can share them rather than copy them.

- **A fleet that is down.** A worker fleet's `processes` can be 0: no Worker polls its task
  queue, so its tasks wait in Matching and the Workers' metrics have no series for it.

- **Timed signal rates.** An event's `signal_rate: { workflow_type, rate }` sets the rate of
  the signal loads that send to that type, as `start_rate` does for starts. A signal load can
  start at `0/s`, so signals can begin mid-run. An event for a type no signal load sends to is
  refused.

- **Every long poll counted.** The Workers' metrics add `temporal_long_request`, each
  worker process's polls by `operation`, counted as they are sent, as the SDKs count them.
  Beside it, a `temporal_long_request_failure` with no series means no poll failed.

- **The frontend's metrics carry `namespace`.** `service_requests`, `service_latency`,
  `service_errors_resource_exhausted` and `service_error_with_type` from the frontend are
  written per namespace, as Temporal writes them, so a query for one namespace's polls or
  signals finds them.

- **The Workers' metrics in the Prometheus output.** `--prom` adds the Go SDK's metrics under
  its tally reporter's names, one series per worker process (`instance`):
  - `temporal_worker_task_slots_available` and `temporal_worker_task_slots_used`, by
    `worker_type`;
  - `temporal_num_pollers`, the outstanding polls by `poller_type` (`workflow_task`,
    `workflow_sticky_task`, `activity_task`);
  - `temporal_workflow_task_queue_poll_succeed` by `workflow_type`,
    `temporal_workflow_task_queue_poll_empty` and `temporal_activity_poll_no_task`;
  - `temporal_long_request_failure` for failed polls, by `operation` and `status_code`;
  - `temporal_workflow_task_schedule_to_start_latency` and
    `temporal_activity_schedule_to_start_latency`, by `workflow_type`, recorded where Matching
    hands a task to a poll.

  Matching adds `approximate_backlog_age_seconds`, the age of each partition's oldest backlog
  task. The server's own system workers are left out. In `baseline.yaml` with two Workflow Task
  slots per worker process, no slot is free and tasks wait about a second in Matching; with the
  scenario's 200, about 1,100 are free and tasks wait about 13 ms.

- **Scraping a run in progress.** `report::prom::render_as(ctx, Reading::Scrape)` reads a run
  as Prometheus would scrape it at the current simulated time: counters and histograms since the
  warm-up ended, and gauges at their current value (`approximate_backlog_count` and
  `persistence_sql_in_use` are otherwise means over the measurement window). A run advanced in
  slices with `model::build::start` and `Executor::run_until` can be scraped every few simulated
  seconds, as a real cluster is. `render`, and `--prom`, still read the measurement window.

- **`tempdes workload fetch`.** Fetches histories for `workload import` from a Temporal
  frontend's HTTP API. It lists the executions started in a window, counts them by type, and
  fetches up to `--limit` of each type (300 by default), spread over the window, page by page.
  - **Connecting:** it uses the Temporal CLI's client profiles. It reads a profile from
    `temporal.toml`, with the `TEMPORAL_*` environment variables over it, resolved as sdk-core
    resolves them. The profile gives the address, namespace, API key, TLS settings (client
    certificate, CA, server name and host verification) and `grpc_meta` headers. Requests go
    to the frontend's `httpPort` (7243) on the profile's host, or to `--http-url`.
  - **Privacy:** payloads, and failures' messages and stack traces, are dropped before
    anything is written. The directory and its files are readable only by you.
  - **Start rates:** `fetch.yaml` records each type's count, and `workload import` takes each
    type's start rate from it rather than estimating it from a sample's start times.
  - **Reruns:** a rerun into the same directory fetches only the histories still missing.

  The `fetch` feature adds the `toml` crate, already in the default build through Topcoat.

- **`history.cacheTTL`.** A cached workflow expires an hour after it was cached (the setting's
  default), however often it is read, as in Temporal's cache. The access that finds it expired
  is a cache miss and loads the mutable state again with `GetWorkflowExecution`. Runs shorter
  than the TTL are unchanged. With `-d history.cacheTTL=10s`, `GetWorkflowExecution` calls
  roughly double in `baseline.yaml` (152/s to 297/s) and in `child-workflows.yaml`, and
  `hot-entity.yaml`'s entities reload every 10 s.

- **`tempdes metrics fetch` and `scan`.** Both read from a Prometheus HTTP API.
  - **`fetch`:** runs the queries of `metrics queries` as instant queries at the end of a window
    and writes an observations file. With `--baseline-end` it also writes a second file for the
    window before the run.
  - **`scan`:** prints the rate of workflow-starting calls, minute by minute, and suggests the
    steadiest windows. Each rate spans four scrape intervals, measured from the data, or the
    window `--rate-window` gives, so sparse scrapes still give rates. When the range has no
    such calls, it lists the `service_name` and `operation` values there are.
  - **Metric names:** whichever names the exporter uses (tally or OpenTelemetry, with or without
    a `temporal_` prefix) are recognised. Histogram bounds in seconds or milliseconds are both
    written as milliseconds.
  - **Renaming:** `--rename OLD=NEW` keeps private label values out of the files.
  - **Access:** a bearer token, basic auth or extra headers, and `--ca-file` or `--insecure`
    for TLS.

  The `fetch` feature, on by default, adds `ureq` over rustls with the system's certificates,
  so no OpenSSL is needed. `deny.toml` now allows the ISC license (ring, rustls-webpki,
  untrusted), and has an exception for `webpki-root-certs`, used only on wasm32.
  `metrics queries` now prints the same queries `fetch` runs:
  - start rates per namespace;
  - `cache_requests` and `cache_miss` separately, and likewise `poll_success` and
    `poll_success_sync`;
  - the backlog by namespace;
  - completions and mutable-state clears, for notes.

- **Signal-with-start and update-with-start.** A workflow type's `start_with: signal` or
  `start_with: update` starts it with SignalWithStartWorkflowExecution or update-with-start
  (`ExecuteMultiOperation`), and `existing` sends a share of the calls to the type's running
  workflows, which are signaled or updated instead. Both read the workflow ID's current run
  first. Update-with-start keeps the update in memory until the first workflow task carries it,
  sends an update to a running workflow on a speculative workflow task that is written only when
  it completes, writes the update's accepted and completed events with that completion, holds
  the call for up to `history.longPollExpirationInterval`, and re-sends it as the Go SDK does.
  The call counts against `frontend.namespaceCount`, and `history.maxInFlightUpdates` is
  simulated. Reports summarise the calls under the workflow table, calibration scales each start
  call's types by its own observed rate, and `with-start.yaml` is a new example.

- **Run profiles.** `tempdes profile save NAME SCENARIO [options]` saves a scenario with a run's
  options under a name. The options are replica counts, dynamic config, observed metrics, load,
  client load balancing, duration, warm-up and seed. `--profile NAME` repeats the run with
  `run`, `sweep` or `ui`, and command-line options apply on top. `profile list`, `show`,
  `remove` and `dir` manage profiles. A scenario file placed in the store as `<name>.yaml` is a
  profile too. Profiles are kept private:
  - they live outside any repository, in `~/.config/tempdes/profiles` (`%APPDATA%\tempdes\profiles`
    on Windows);
  - they hold their own copies of the scenario and metrics files;
  - their directories and files are readable only by you;
  - saving warns about source files that git could commit by accident.
- **`tempdes ui`.** A live view of a running simulation, served by the Topcoat web framework.
  The page shows the cluster's request paths with CPU per pod, limiter headroom and the flows
  between services; the request and task paths stage by stage, each with its saturation and
  the symptoms it causes downstream; time series; and the report's hotspots re-ranked every
  five simulated seconds. The load multiplier, replica counts and runtime dynamic config keys
  can be changed while it runs. The simulator gained the hooks this needs: stepping a built
  cluster in slices, a live load multiplier, interval histograms and a windowed analysis.
  The `ui` feature is on by default; `--no-default-features` builds the lean CLI.
- **Simulation kernel.** A deterministic discrete-event model of Temporal Server 1.31.0 on EKS
  covering:
  - the frontend, history, matching and worker services;
  - SDK workers and clients;
  - persistence, visibility and cluster membership.
- **Commands.**
  - `tempdes run` simulates one configuration and ranks its hotspots. Each hotspot names the
    Temporal metrics to watch and the knobs that change it.
  - `tempdes sweep` compares hotspots across a grid of replica counts and dynamic config values.
  - `tempdes dc` inspects dynamic config against the embedded registry of all 613 keys in
    Temporal 1.31.0, and validates dynamic config files against it.
  - `tempdes metrics` helps collect observed metrics (PromQL queries, a template, a parser for
    Prometheus scrapes).
- **Calibration.** Observed metrics set database service times, database capacity,
  per-service CPU costs and workload rates. A validation table compares predictions with the
  observed values.
- **Helm import.** Replica counts, CPU limits, shard count, SQL connections and dynamic config
  can be read from `temporalio/helm-charts` values files.
- **Timeline events.** Replica counts, dynamic config and load can change during a run.
- **Reports.** Text, JSON, Prometheus text format (with Temporal metric names), and
  self-contained HTML with time-series charts, a shard map and sweep heatmaps.
- **`gen-dc-registry`.** Regenerates the dynamic config registry from a Temporal source checkout
  and checks it for drift.
- **Examples.** Ten example scenarios, each reproducing a hotspot: baseline, hot entities,
  frontend throttling, database-bound, aligned schedules, scale-out, large Cassandra cluster,
  Helm import, frontend load balancing and frontend scale-out.
- **Client load balancing.** `cluster.network.client_lb` sets how SDK clients and workers
  reach the frontend pods:
  - `pinned`, the default: one connection per process;
  - `round_robin`: gRPC client-side load balancing on a headless Service;
  - `proxy`: per-request L7 balancing, such as an ALB or a service mesh.

  It is also available as `--client-lb` and as a `client_lb=` sweep column. Frontend rate-limit
  hotspots now show frontend load skew and suggest `client_lb` when connections are pinned.
- **EKS guide.** [docs/EKS.md](docs/EKS.md) covers each option: a headless Service with SDK
  settings, an ALB for external clients, and frontend connection-age and shutdown settings.

- **History task scheduler rate limiter.** `history.taskSchedulerEnableRateLimiter` and its
  settings are simulated: namespace and pod buckets per task priority, the cluster-wide rates
  split by shard ownership, and the fallback to the persistence rate. In shadow mode it counts
  `task_scheduler_throttled` and holds nothing back, and the report says what it would hold back.
  Otherwise refused tasks wait in the rescheduler, and a long wait is reported as a rate-limit
  hotspot.

- **Markdown reports.** `run --md FILE` writes the report as Markdown, with GitHub-flavoured
  tables, for pull requests, issues and docs; `--verbose` applies to it as to the text report.
  `sweep --md FILE` writes one table per metric and each cell's top hotspot.

- **Activity timeouts and retry policies.** Schedule-to-start, start-to-close,
  schedule-to-close and heartbeat timeouts are enforced by history's timer queue, as in
  Temporal, with one activity timer task per workflow for its earliest timeout. Start-to-close
  and heartbeat timeouts retry the attempt while the retry policy allows. The other two fail
  the activity, as do used-up retries. New activity fields set the timeouts
  (`schedule_to_start_timeout`, `start_to_close_timeout`, `schedule_to_close_timeout`,
  `heartbeat_timeout`) and the retry policy (`backoff_coefficient`, `max_interval`,
  `max_attempts`, alongside `retry_initial`). `history.defaultActivityRetryPolicy` fills in
  what an activity leaves unset. `on_failure: fail | continue` sets whether a failed activity
  fails its workflow. Workers stop at their attempt's deadline and drop the result, as the Go
  SDK does. The report adds an `activity-timeouts` hotspot and a `failed` column for
  workflows.
- **Per-call SDK deadlines.** `rpc_timeout` (10 s by default) sets the deadline of each SDK
  call, retries included. It can be set for worker fleets, workflow starters, and signal,
  query, describe and visibility load.
- **"Not keeping up" throughput rule.** Workflows that close more slowly than they start are
  reported with the rate at which running workflows pile up. The expected closing rate allows
  for the warm-up and for the workflow's own run time, sampled from its steps with their
  failed attempts and retry intervals. A long retry tail (intervals doubling up to 100 s) is
  therefore not mistaken for a cluster falling behind. Workflows with no known run time, such
  as entities waiting for signals, keep only the start-shortfall test.
- **Multi-tenant scheduling and limits.**
  - The history task scheduler interleaves (namespace, priority) channels by weight, as
    Temporal's interleaved weighted round robin does
    (`history.*ProcessorSchedulerActiveRoundRobinWeights`, high 10, low 9, preemptable 1).
  - The execution queue scheduler (`history.taskSchedulerEnableExecutionQueueScheduler` and
    its `MaxQueues`, `QueueTTL` and `QueueConcurrency` settings) moves a busy workflow's tasks
    into a queue of its own.
  - Per-namespace persistence limits are checked before the pod's limit:
    `persistenceNamespaceMaxQPS` and `persistenceGlobalNamespaceMaxQPS` for history and
    matching, and `history.persistencePerShardNamespaceMaxQPS`. Their rejections name the
    namespace.

  95 dynamic config keys are now simulated, 13 of them new.

- **Workloads from workflow histories.** `tempdes workload import` reads exported histories
  (`temporal workflow show --output json`, or the Web UI's download) and writes the
  `workflows:` entries of a scenario:
  - each step's durations, from its own time with waits in the cluster left out;
  - attempt counts, retry policies and timeouts, as recorded;
  - parallel groups, local activities, timers, children and waits for signals;
  - workflow task processing times.

  Executions that took different paths become workflow types of their own, with their share of
  the start rate. Payloads are never read. The summary reports the waits in the cluster that
  were left out and the event kinds that aren't modelled.
- **Planned activity attempts.** `attempts: 5` or `attempts: { 1: 0.8, 5: 0.2 }` on an activity
  step sets how many attempts each activity makes, instead of failing every attempt at
  `failure_rate`. A per-attempt failure rate gives a geometric number of attempts with a long
  tail; a status poll that always takes about five attempts doesn't have one.
- **Non-retryable failures and failed-attempt durations.**
  - `non_retryable: { 1: 0.02 }` on an activity step fails that share of activities on the given
    attempt with a non-retryable error. The server doesn't retry it: `RetryActivity` returns
    `RETRY_STATE_NON_RETRYABLE_FAILURE`. The report counts these failures apart
    (`activities_non_retryable`) and doesn't raise them as hotspots, since they are the
    workload's outcome.
  - `failed_duration` sets how long a failed attempt runs. Quick rejections, and attempts that
    hang until their start-to-close timeout, can then differ from successful attempts.
- **`workload import` reads why retries stopped.**
  - An activity's recorded retry state decides its plan:
    - non-retryable failures become `non_retryable`;
    - activities whose retries ran out get an attempt count they can't reach;
    - schedule-to-start timeouts, the recorded cluster's queueing, are left out.
  - The run time of the failed attempts before the last is estimated and written as
    `failed_duration`: the time before the last attempt, less the retry intervals and queue
    waits.
  - Attempt shares are written to three significant digits, so rare counts aren't rounded to 0.
- **Parallel steps.** A `parallel` step starts its members, `activity` and `child_workflow`
  steps, in one workflow task, and ends when they all have. Each member keeps its own settings,
  `on_failure` included. `workload import` writes one when a workflow task starts activities
  of several types, activities and children, or children of several types:
  - each type is a member with its own durations, attempts and retry policy;
  - before, parallel activities of different types were pooled into one distribution;
  - before, activities and children started together ran one after the other in the
    simulation;
  - before, children of several types were imported as the most common type.
- **Payload sizes cost time and room.** A workflow type's `payload_bytes` (1 KiB by default) is
  carried by the events that hold inputs and results: the workflow's input and result,
  activities' and children's inputs and results, and local activities' markers. Every event
  weighs 128 bytes besides, as measured in histories that record their size, and a signal's
  input 256.
  - Writes take `cluster.persistence.write_per_mib` (20 ms by default) for each MiB of history
    they append, and `ReadHistoryBranch` takes `read_per_mib` (5 ms) for each MiB it reads.
    Calibration's pilot runs fit the rest of each operation's latency at the scenario's sizes.
  - Each shard's events cache holds `history.eventsCacheMaxSizeBytes` (512 KiB) of events: the
    start event, activities' scheduled events, children's initiated events and the close event.
    Activity starts, child starts and children reporting to their parents read their event from
    it; a miss reads the whole batch the event was written in. Large payloads leave room for
    fewer events, and the report raises an events cache hotspot.
  - A history over `limit.historySize.error` (50 MiB) is terminated at its next workflow task
    completion, as `enforceHistorySizeCheck` does; one over `limit.historySize.warn` (10 MiB)
    is counted. The report adds a `history-size` hotspot, and `terminated`,
    `histories_over_warn` and `max_history_bytes` for each workflow type.
  - A `payload_bytes` over `limit.blobSize.error` (2 MiB) for its namespace is refused when the
    scenario loads; one over `limit.blobSize.warn` (512 KiB) runs with a warning.

  99 dynamic config keys are now simulated.
- **`workload import` estimates payload sizes.** Servers record on each workflow task the size
  of the history before it (`historySizeBytes`). Less 128 bytes an event and 256 a signal, over
  the events that carry payloads, it gives each type's `payload_bytes`, still without reading
  payloads. The summary gives the range for events of 95–180 bytes, and notes when events that
  aren't modelled (markers, search attribute upserts, updates) may inflate the estimate.
- **`workload import` sketches worker fleets.** Histories record which worker process started
  each task. The output now opens with a commented `workers:` block: a fleet per task queue
  with the number of processes seen, to uncomment once its pollers and slots, which histories
  don't record, are set. The summary lists the SDKs the workers report. Identities, which name
  hosts, are counted but never written.

### Fixed

- **The backlog of a queue nothing polls.** Matching's metrics left out a partition that had
  been neither polled nor matched, so a task queue with no workers showed no
  `approximate_backlog_count` or `approximate_backlog_age_seconds` at all, however large its
  backlog. A partition that tasks were added to is now reported, polled or not.

- **A partition dispatch limit never recovered from a refusal.** When the limiter refused a
  dispatch, Matching took a token to time its retry (`reserve_delay`), and the retry took
  another with `allow()`. Every refused attempt deepened the bucket's debt, so a partition with
  `admin.matchingNamespaceTaskqueueToPartitionDispatchRate` set stopped dispatching for good:
  with two partitions at 50/s, one dispatched nothing from the start and the other stopped
  after 13 minutes. The retry now waits for a token without taking one (`TokenBucket::delay`),
  and a partition keeps one retry pending at a time, as Matching's task reader waits on its
  limiter. Both partitions now dispatch 50/s for as long as their backlog lasts.

- **Update-with-start caches its new run.** As in Temporal, the run it creates goes into the
  host cache with its mutable state, so its first workflow task makes no `GetWorkflowExecution`
  call. In `with-start.yaml`, `GetWorkflowExecution` calls fall from 76.5/s to 52.5/s, one fewer
  for each of the ~24 carts created per second.

- **Child workflows start the way Temporal starts them.** Behaviour follows
  `processStartChildExecution`:
  - The start-child transfer task holds the parent's lock through the child's start and the
    write recording it, so children started together start one after another.
  - ChildWorkflowExecutionStarted wakes the parent with a workflow task unless one is pending.
  - A child starts without a workflow task. Once the start is recorded, the parent's task
    schedules the child's first one through history's new `ScheduleWorkflowTask` call.
  - A task whose write fails after the child has started is retried, starting the same child
    again under its request ID. Before, the parent's ChildWorkflowExecutionStarted was dropped.

  In the new `child-workflows.yaml` example (four children per order), the change raises parent
  workflow tasks by 18% and `UpdateWorkflowExecution` calls by 11%. The parents' lock wait p99
  goes from 8 ms to 36 ms.

- **Failed calls clear cached mutable state as in Temporal.** A load that failed (a throttled or
  timed-out `GetWorkflowExecution`) left the workflow counted as cached, so its next access
  skipped the load. A call or task that fails while holding a workflow now clears the workflow's
  mutable state for the errors Temporal's workflow cache is released with:
  - failed writes, loads and event reads;
  - stale tasks in `RecordWorkflowTaskStarted`, `RecordActivityTaskStarted`,
    `RespondActivityTask*` and `RecordActivityTaskHeartbeat`;
  - a closed parent in `RecordChildExecutionCompleted`;
  - an update over `history.maxInFlightUpdates`.

  The workflow stays in the cache, as in Temporal. Its next access counts as a hit in
  `cache_requests` / `cache_miss` but loads the mutable state again with `GetWorkflowExecution`.
  Under persistence throttling this multiplies the loads: in `baseline.yaml` with
  `history.persistenceMaxQPS=600`, refused `GetWorkflowExecution` calls rise from 438/s to
  3,537/s and successful updates halve. Runs without failures are unchanged. A parent's
  ChildWorkflowExecutionStarted write now loads the parent's mutable state first and applies
  only when it succeeds.

- **Observed metric labels match whole name parts.** A label filter that matched any value
  ending with it counted an observed `SignalWithStartWorkflowExecution` as
  `StartWorkflowExecution`. A value now has to end with the filter after a separator, as in
  `temporal-history` or a gRPC method path.

- **The events cache was never filled.** Activity starts looked their scheduled event up in the
  shard's events cache, but nothing put events in, so every start read history from the
  database. The cache is now filled as Temporal fills it, weighed in bytes, and emptied when its
  shard changes owner. In the example scenarios, `ReadHistoryBranch` calls fall by 17–48%.

- **Schedule-to-start is measured as the SDK measures it.** Workflow task and activity
  schedule-to-start now run from the task's scheduled time, as the SDK's
  `temporal_*_schedule_to_start_latency` metrics do, instead of from when history handed the task
  to matching. They now include delays in history, such as a throttled transfer task backing off
  for 3 s or more; before, a heavily throttled run could report tens of milliseconds. A retry
  attempt counts from when it was due. Worker schedule-to-start hotspots now name every internal
  limiter rejecting calls, not only the one throttling polls.
- **Activity retries go straight to matching.** A retry timer now pushes the next attempt to
  matching, as Temporal does. It used to write mutable state and create a transfer task as well,
  which overstated persistence load, history task load and retry latency for workloads with many
  retries.
- **History persistence limits follow shard ownership.** `history.persistenceGlobalMaxQPS` is
  split by the shards each pod owns, as in Temporal, instead of evenly across pods. Pods that
  own more shards get more of the budget, so an even split overstated persistence throttling
  when ownership was uneven. The JSON report lists each pod's `persistence_qps_limit`.
- **SDK retries share one deadline per call.** As in the Go SDK, a call's retries happen inside
  its one context deadline (backoff 200 ms × 2ⁿ ±20%, at most 6 s apart), and a call still
  failing at the deadline returns `DeadlineExceeded`. Before, each attempt had its own
  timeout, so a signal queued behind a hot workflow's lock could take 20 s at p99; it now
  stops at 10 s.
- **History long polls run at the lowest priority.** A client waiting for a result
  (`await_result`) long-polls `GetWorkflowExecutionHistory`. The frontend's namespace limiter
  admits that as `PollWorkflowExecutionHistory` at priority 5, below worker polls, and
  `frontend.namespaceCount` counts it. Before, it ran at priority 2 and wasn't counted. Metrics
  still report it under `GetWorkflowExecutionHistory`.
- **History task retries follow `executable.go`.** A failed task is resubmitted at once until its
  tenth attempt, but throttling allows only one immediate resubmit. After that it backs off
  1 s × 1.1ⁿ⁻¹, or when throttled the larger of that and 3 s × 1.5ᵐ⁻¹ for the m-th throttle in
  a row. Before, a throttled task always backed off first, scaled by its attempts rather than
  its throttles in a row, and other errors were resubmitted at once only after their first
  failure. That overstated throttling delays; the immediate resubmit can raise rejection counts.
- **Calibration no longer counts queueing and history appends twice.** Observed
  `persistence_latency` includes queueing, and for Create/UpdateWorkflowExecution the history
  append inside the call. Calibrated writes no longer add a separate `AppendHistoryNodes`, and
  pilot runs fit each operation's service time so that the simulated mean latency, queueing
  included, matches the observed mean. Uncalibrated writes run the append inside the write, with
  one rate-limiter charge and one latency, as Temporal's SQL and Cassandra stores do.
- **Rejecting limiters keep their order.** When several internal limiters reject calls, the
  report ranks each above the symptoms it causes. It used to give them all the same score, so
  the order came from their names, and a limiter rejecting a few calls a second could head the
  report over one rejecting a thousand times as many. They now keep their order by rejection
  rate.
- **Throttled activity retries point at `matching.rps`.** An activity retry timer pushes the
  next attempt to matching, so its `RPS_LIMIT` throttling comes from `matching.rps`. The
  history-queue hotspot suggested `history.rps`. Each throttled task type now names the limit
  of the service it calls.
- **Documentation.** The README and model docs described activity timeouts before they were
  simulated. They also described only one of the throughput tests, and gave stale counts of
  simulated keys. All three are corrected.

### Security

- **rustls-webpki 0.103.16 and webpki-root-certs 1.0.10.** The `fetch` feature's certificate
  verification moves from rustls-webpki 0.103.15 and webpki-root-certs 1.0.9, which
  RUSTSEC-2026-0382 and RUSTSEC-2026-0383 affect: both could trust distrusted emSign
  intermediates.

- **rustls 0.23.46.** The `fetch` feature's TLS client, used by `metrics fetch`, `metrics scan`
  and `workload fetch`, moves from rustls 0.23.45, which RUSTSEC-2026-0346, -0347, -0348 and
  -0349 affect.

[Unreleased]: https://github.com/iw/tempdes/commits/main
