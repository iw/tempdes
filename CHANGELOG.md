# Changelog

All notable changes to tempdes are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

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

### Fixed

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

[Unreleased]: https://github.com/iw/tempdes/commits/main
