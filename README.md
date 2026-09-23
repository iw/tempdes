# tempdes

[![CI](https://github.com/iw/tempdes/actions/workflows/ci.yml/badge.svg)](https://github.com/iw/tempdes/actions/workflows/ci.yml)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)
[![Rust 1.98.1](https://img.shields.io/badge/rust-1.98.1-orange.svg?logo=rust)](rust-toolchain.toml)

A discrete-event simulator that finds hotspots in **Temporal Server 1.31.0** clusters running on
EKS, before they show up in production.

You describe a deployment and a workload. Two dimensions are adjustable:

* **replica counts** for the frontend, history, matching and worker services;
* **dynamic config**, in Temporal's own file format, for the settings that matter most for
  throughput. 75 settings are simulated, and all 613 keys in 1.31.0 are validated.

tempdes simulates the cluster request by request and reports what saturates first: CPU, database
connections, a shard's IO semaphore, a single workflow's lock, a rate limiter, a history task
queue, a task-queue partition, or shard movement during a rollout. Each finding names the
Temporal metrics to watch and the dynamic config keys (or replica counts) that change it.

You can also give it **metrics your cluster already emits** (`persistence_latency`,
`service_requests`, CPU usage and others). tempdes uses them to calibrate database service times,
per-service CPU costs and workload rates. It then prints a table comparing its predictions with
your observed values.

```text
$ tempdes run examples/scenarios/hot-entity.yaml

  3 critical / 2 warning hotspots — top: workflow lock contention (lock wait p99 9.50s, 4961 BUSY_WORKFLOW
  timeouts). Busiest resource: shard 151 IO at 100%.

  CRITICAL #1  workflow-lock  workflow lock contention (lock wait p99 9.50s, 4961 BUSY_WORKFLOW timeouts)
             · CartWorkflow-0 (#0) (shard 363): lock 100% busy, wait p99 9.50s, 1953 busy-workflow timeouts
             watch: history_workflow_execution_cache_latency, acquire_lock_failed, task_errors_workflow_busy, …
             knob: history.cacheNonUserContextLockTimeout = 500ms (default)  — longer waits reduce retries …
  CRITICAL #3  shard  hot history shard 151 (100% IO busy, wait p99 2.11ms)
             Shard 151 is busy because workflow CartWorkflow-1 (#1) writes to it continuously … raising
             history.shardIOConcurrency or adding history pods will not help — the fix is fewer writes per
             workflow (batch signals, split the entity) or a faster database write.
```

---

## Contents

- [Install](#install)
- [Quick tour](#quick-tour)
- [Dimension 1: replica counts](#dimension-1-replica-counts)
- [Dimension 2: dynamic config](#dimension-2-dynamic-config)
- [Sweeps: replicas × dynamic config](#sweeps-replicas--dynamic-config)
- [Feeding in Temporal metrics](#feeding-in-temporal-metrics)
- [Reading the report](#reading-the-report)
- [Scenario reference](#scenario-reference)
- [Example scenarios](#example-scenarios)
- [What is modelled](#what-is-modelled)
- [Limitations](#limitations)
- [Development](#development)
- [Contributing](#contributing)
- [License](#license)

## Install

tempdes needs **Rust 1.98.1** or newer (edition 2024). To install the `tempdes` command from
this repository:

```bash
cargo install --locked --git https://github.com/iw/tempdes tempdes
```

To build from a clone instead, run the following. `rust-toolchain.toml` pins 1.98.1, and rustup
installs it on first use.

```bash
git clone https://github.com/iw/tempdes && cd tempdes
cargo build --release
```

The binary is `target/release/tempdes`. It depends only on `serde`, `serde-saphyr` (YAML),
`serde_json`, `clap` and `anyhow`, and the simulation runs on a single thread per run.
A 60-second simulation at 150 workflows/s takes one to two seconds. Sweeps run their
cells in parallel.

## Quick tour

```bash
# one configuration → hotspot report (exit code 1 if anything is critical)
tempdes run examples/scenarios/baseline.yaml

# change the two dimensions from the command line
tempdes run examples/scenarios/baseline.yaml -r history=6 -d history.shardIOConcurrency=4

# grid: replica counts down, dynamic config across
tempdes sweep examples/scenarios/baseline.yaml --load 1.3 \
    --rows matching=3,4,6 --cols matching.rps=1200,2400 --html out/sweep.html

# calibrate against production metrics, then compare predictions with observations
tempdes run examples/scenarios/baseline.yaml -o examples/metrics/observed.yaml

# dynamic config tooling
tempdes dc modeled                       # the 75 simulated keys, with 1.31.0 defaults
tempdes dc explain history.shardIOConcurrency
tempdes dc validate examples/dynamicconfig/with-mistakes.yaml

# metrics tooling
tempdes metrics queries --window 15m     # PromQL to collect calibration inputs
tempdes metrics template > observed.yaml
tempdes metrics show examples/metrics/observed.yaml
```

`run` also writes `--json` (the full result), `--prom` (simulated metrics in Prometheus text
format with Temporal metric names) and `--html` (a self-contained report with time-series charts
and a shard map). `-v` prints per-pod, per-shard and per-partition tables.

## Dimension 1: replica counts

Replica counts come from `cluster.replicas` in the scenario:

```yaml
cluster:
  replicas: { frontend: 3, history: 6, matching: 3, worker: 1 }
  resources:                 # per-pod CPU limit → GOMAXPROCS
    history: { cpu: 4 }
```

You can also set them in these ways:

| How | Example |
|---|---|
| CLI override | `-r history=6 -r matching=4` |
| sweep rows | `--rows history=3,6,9` (repeat `--rows` for a cartesian product) |
| linked sweep rows | `--rows frontend+history=2+3,3+6,4+9` |
| Helm values | `cluster.helm_values: values-prod.yaml` reads `server.<svc>.replicaCount` |
| mid-run scaling | `events: [{ at: 50s, replicas: { history: 5 } }]` |

Replica counts change more than capacity. Each pod joins the ringpop hash ring under its
`ip:port`, so the replica count decides:

* **which history pod owns which shards.** Placement uses 100 virtual points per member, as in
  Temporal. With few pods, shard counts per pod are uneven.
* **where task-queue partitions live.** Partitions are placed on matching pods by the same ring,
  which also decides where the root partition sits.
* **how rate limits are split.** Settings such as `frontend.globalNamespaceRPS` are divided
  across frontend pods.
* **how SDK traffic spreads.** Each client and worker keeps one gRPC connection pinned to one
  frontend pod until `frontend.keepAliveMaxConnectionAge` expires, so load is uneven across
  frontends.

To reproduce the exact placement of a live cluster, list the pods' real addresses in
`cluster.member_addresses`.

Scaling while the simulation runs moves shards. The old owner closes them and the new owner
acquires them with `GetOrCreateShard` + `UpdateShard`, after a ringpop propagation delay.
Requests wait during the move, and the new owner starts with a cold mutable-state cache. The
`membership` hotspot and the `shard_unavailable` latency show the cost.

## Dimension 2: dynamic config

Dynamic config uses the server's own format, including constraints:

```yaml
dynamic_config_files: [ ../dynamicconfig/production.yaml ]   # applied first
dynamic_config:                                                # then these
  history.shardIOConcurrency:
    - value: 4
  frontend.namespaceRPS:
    - value: 5000
      constraints: { namespace: payments }
    - value: 2400
```

These keys are resolved the way Temporal 1.31.0 resolves them:

* **Scope and precedence.** Each key has a scope (Global, Namespace, TaskQueue, ShardID and so
  on). The most specific constraint match wins.
* **Case.** Key matching is case-insensitive.
* **Type conversion.** An int key given a float (`2500.0`) fails conversion and falls back to
  the default, as it does in the server.
* **Durations.** `"30s"`, `"1h"` and bare seconds are accepted.

CLI overrides use `-d key=value` and `-d 'key[namespace=orders,taskQueueName=q]=value'`.
Sweep columns use `--cols key=v1,v2,v3`, and a pseudo-key `load=0.5,1,2` scales all start and
signal rates.

`tempdes dc validate FILE` checks a production dynamic config file against the 1.31.0 registry.
It exits 1 when it finds problems. It catches:

* typos, with did-you-mean suggestions;
* type mismatches;
* constraints that can never match the key's scope;
* write partitions set higher than read partitions.

Keys that are valid but not simulated are still validated and reported. When one of them is a
useful fix for a hotspot, the report marks it *(not simulated)*.

### Simulated settings (highlights)

Run `tempdes dc modeled` for the full list of 75 keys with their defaults and descriptions.

| Area | Keys |
|---|---|
| Frontend limits | `frontend.rps`, `frontend.globalRPS`, `frontend.namespaceRPS`, `frontend.globalNamespaceRPS`, `frontend.namespaceBurstRatio`, `frontend.namespaceCount` / `globalNamespaceCount`, `frontend.namespaceRPS.visibility` (+ global/burst), `frontend.pollWaitForNamespaceRateLimitToken`, `frontend.keepAliveMaxConnectionAge`, `system.operatorRPSRatio` |
| Persistence | `{frontend,history,matching,worker}.persistenceMaxQPS`, `{history,matching}.persistenceGlobalMaxQPS`, `system.persistenceQPSBurstRatio` |
| History | `history.rps`, `history.shardIOConcurrency`, `history.hostLevelCacheMaxSize`, `history.cacheNonUserContextLockTimeout`, `history.eventsCacheMaxSizeBytes`, `history.acquireShardConcurrency`, `history.defaultWorkflowTaskTimeout`, `history.longPollExpirationInterval` |
| History task queues | `*ProcessorSchedulerWorkerCount`, `*TaskBatchSize`, `*ProcessorMaxPollRPS`, `*ProcessorMaxPollHostRPS`, `*ProcessorUpdateAckInterval`, `history.queuePendingTasksMaxCount`, `history.timerProcessorMaxTimeShift`, `history.shardUpdateMin{Interval,TasksCompleted}` |
| Matching | `matching.rps`, `matching.numTaskqueue{Read,Write}Partitions`, `matching.forwarderMax{OutstandingPolls,OutstandingTasks,RatePerSecond,ChildrenPerNode}`, `matching.outstandingTaskAppendsThreshold`, `matching.maxTaskBatchSize`, `matching.getTasksBatchSize`, `matching.getTasksReloadAt`, `matching.maxWaitForPollerBeforeFwd`, `matching.backlogNegligibleAge`, `matching.longPollExpirationInterval`, `admin.matching*DispatchRate` |
| Worker service | `worker.perNamespaceWorkerCount`, `worker.schedulerNamespaceStartWorkflowRPS`, `worker.schedulerLocalActivitySleepLimit`, `worker.ESProcessor{BulkActions,FlushInterval,NumOfWorkers}` |
| Membership / features | `system.ringpopReplicaPoints`, `system.ringpopApproximateMaxPropagationTime`, `system.enableEagerWorkflowStart`, `system.enableActivityEagerExecution` |

Static config that behaves like a dimension is part of `cluster`: `numHistoryShards`, SQL
`maxConns` per pod, and the store type. With Cassandra, Temporal forces
`history.shardIOConcurrency` to 1, and tempdes does the same with a warning.

## Sweeps: replicas × dynamic config

```bash
tempdes sweep examples/scenarios/baseline.yaml --load 1.3 \
    --rows matching=3,4,6 --cols matching.rps=1200,2400
```

```text
STATUS (critical/warning hotspot count · top hotspot category)
  replicas \ dynamic config      matching.rps=1200    matching.rps=2400
  matching=3                 CRIT 1c/7w rate-limit  WARN 0c/2w headroom
  matching=4                 CRIT 1c/6w rate-limit  WARN 0c/2w headroom
  matching=6                   WARN 0c/3w headroom  WARN 0c/2w headroom

WORKFLOW END-TO-END p99
  matching=3                            12.85s              2.82s
  matching=4                            11.80s              2.82s
  matching=6                             2.82s              2.82s

RATE-LIMIT REJECTIONS /s
  matching=3                             243/s                  0
  matching=4                             201/s                  0
  matching=6                                 0                  0
```

In this sweep, going from 3 to 4 matching pods barely helps. Partitions are placed by the hash
ring, so one pod still carries too many of them. Six pods, or doubling `matching.rps`, fixes the
problem.

Each cell is a full simulation with the same seed. The output includes these grids:

* hotspot status;
* throughput;
* end-to-end p99;
* start latency;
* maximum CPU per service;
* database and hottest-shard utilisation;
* rate-limit rejections;
* the top hotspot per cell.

`--csv` and `--json` write every cell. `--html` writes a heatmap you can switch between metrics,
with per-cell detail.

## Feeding in Temporal metrics

Observed metrics serve two purposes: **calibration** changes the model's parameters, and
**validation** compares the model's predictions with production.

```bash
tempdes metrics queries --window 15m   # the PromQL to run
tempdes metrics template               # an annotated observations file
tempdes run scenario.yaml -o observed.yaml
```

An observations file lists Temporal metric names with labels. Each entry gives a `rate`,
`increase` (with `window`), `value`, quantiles (`p50`/`p90`/`p99` or a `quantiles` map) or raw
histogram `buckets`:

```yaml
window: 15m
metrics:
  - name: persistence_latency
    labels: { operation: UpdateWorkflowExecution }
    p50: 3.4ms
    p99: 24ms
  - name: persistence_latency
    labels: { operation: GetTransferTasks }
    buckets: { "0.001": 5210, "0.002": 18020, "0.005": 26310, "0.01": 27950, "+Inf": 28570 }
  - name: container_cpu_usage_seconds_total
    labels: { container: temporal-history }
    rate: 3.9
  - name: service_requests
    labels: { service_name: frontend, operation: StartWorkflowExecution }
    rate: 180/s
```

You can also give raw Prometheus scrapes of the Temporal pods, either with `-o scrape.prom` or
in the observations file:

```yaml
prometheus: { before: scrape-0900.prom, after: scrape-0915.prom, interval: 15m }
```

Counters then become rates, and histograms become quantiles. Both tally names
(`persistence_latency_bucket`) and OpenTelemetry names (`temporal_persistence_latency_milliseconds_bucket`)
are recognised.

### What each metric informs

| Observed metric | Calibrates |
|---|---|
| `persistence_latency{operation}` | database service-time distribution per persistence operation. Quantiles are fitted piecewise in log space, so heavy tails are kept. |
| `visibility_persistence_latency{operation}` | visibility store read/write latency |
| `persistence_requests{operation}` + `db_utilization` (or `rds_cpu_utilization`) | database capacity (concurrent operations before queueing) |
| `container_cpu_usage_seconds_total{container=temporal-<svc>}` (or `cpu_cores{service_name}`) | per-service CPU cost scale, from a pilot simulation of the observed configuration. Sweep cells reuse the same scale. |
| `service_requests{service_name=frontend, operation=StartWorkflowExecution / SignalWorkflowExecution}` | workload start and signal rates |

Validation rows cover these values:

* request rates and `service_latency` p99 per API;
* persistence rates and latency;
* mutable-state cache miss ratio (`cache_requests` / `cache_miss`);
* sync-match ratio (`poll_success_sync` / `poll_success`);
* CPU cores per service;
* `approximate_backlog_count`.

In the calibrated example, history CPU is 4.01 simulated vs 3.90 observed, sync match 0.72 vs
0.78, and persistence p99s are within ±9%.

Turn individual calibrations off in the scenario with
`calibration: { persistence_latency: false, cpu: false, workload: false }`.

## Reading the report

The report opens with a headline, followed by ranked hotspots. Each hotspot has:

* a severity (`CRITICAL` / `WARNING` / `INFO`);
* a category;
* the evidence (the pods, shards, workflows or partitions involved);
* **watch**: the Temporal metrics that show the problem in production;
* **knob**: the dynamic config keys or replica counts that change it, with their current
  effective values.

A causal pass moves root causes above their symptoms. For example, a saturated database ranks
above the backlog and schedule-to-start latency it causes. A hot shard is attributed to the
single hot workflow writing to it.

| Category | Meaning |
|---|---|
| `throughput` | completions fall short of offered starts (a Poisson 3σ test, so noise isn't flagged) |
| `cpu`, `imbalance` | pod CPU saturation; uneven load across pods of one service |
| `database`, `connection-pool` | database busy; per-pod SQL `maxConns` pools saturated (bursty pools are labelled as such) |
| `shard` | a shard's IO semaphore is busy, or bursty write contention |
| `workflow-lock` | per-workflow mutable-state lock saturation and `BUSY_WORKFLOW` timeouts |
| `history-queue` | transfer/timer/visibility task retries, throttling and scheduler backlog, with the throttle cause |
| `visibility` | Elasticsearch bulk processor or visibility persistence saturation |
| `matching-backlog`, `matching-placement` | task backlogs and dispatch latency; uneven partition placement |
| `rate-limit` | limiters rejecting requests, e.g. `frontend.namespaceRPS`, `history.rps`, `matching.rps`, persistence QPS, `namespaceCount` |
| `headroom` | a limiter running at ≥70% of its limit on some pod, before rejections start |
| `cache`, `sticky-cache` | mutable-state / events cache misses; sticky-queue misses and non-sticky workflow tasks |
| `workers`, `workflow-tasks` | worker slots or pollers limiting throughput; schedule-to-start latency; workflow task timeouts |
| `schedules` | schedule actions delayed or rate-limited on the per-namespace worker |
| `api-latency`, `api-errors` | client-observed p99 over `report.api_p99_slo`; error rates |
| `membership` | shards unavailable while they move between history pods |

The warning and critical utilisation thresholds (0.7 / 0.9) and the API SLO are set under
`report:` in the scenario.

## Scenario reference

```yaml
name: orders-baseline
seed: 7
warmup: 20s                   # discarded before measuring
duration: 60s                 # measured simulated time

cluster:
  helm_values: ../helm/values-prod.yaml    # optional: replicas, CPU, shards, SQL maxConns, dynamicConfig
  num_history_shards: 512
  replicas: { frontend: 3, history: 3, matching: 3, worker: 1 }
  resources: { frontend: { cpu: 2 }, history: { cpu: 4 }, matching: { cpu: 2 }, worker: { cpu: 1 } }
  member_addresses: { history: ["10.0.1.12:7234", "10.0.2.40:7234"] }   # optional, exact ring placement
  network: { client_rtt: 2ms, internal_rtt: 0.5ms }
  persistence:
    store: postgresql          # postgresql | mysql | cassandra | sqlite
    max_conns: { frontend: 20, history: 50, matching: 30, worker: 10 }
    capacity: 160              # concurrent DB operations before queueing (calibratable)
    latency:                   # per Temporal persistence operation, or `default`
      UpdateWorkflowExecution: { p50: 3ms, p99: 14ms }
  visibility: { store: elasticsearch }

dynamic_config_files: []
dynamic_config: { history.shardIOConcurrency: [ { value: 1 } ] }

namespaces: [ { name: orders } ]

workers:                       # SDK worker fleets (Go SDK behaviour)
  - name: order-workers
    namespace: orders
    task_queue: orders
    processes: 6
    workflow_pollers: 10       # MaxConcurrentWorkflowTaskPollers
    activity_pollers: 16
    workflow_slots: 200
    activity_slots: 200
    sticky_cache_size: 5000
    sticky_schedule_to_start_timeout: 5s
    # cpu: 2, task_queue_activities_per_second: 500, poll_timeout: 70s, eager_activities: false

workflows:
  - type: OrderWorkflow
    namespace: orders
    task_queue: orders
    start_rate: 150/s          # omit for child-only / schedule-only types
    arrival: poisson           # poisson | uniform
    # ramp: { from: 0.2, over: 30s }
    starters: 4                # client processes (each pins one frontend connection)
    # eager_start: false, await_result: false
    wft_processing: { p50: 2ms, p99: 12ms }
    # replay_per_event: 50us, payload_bytes: 2KiB
    steps:
      - activity: { count: 1, duration: { p50: 30ms, p99: 250ms } }
      - activity: { count: 2, parallel: true, duration: { p50: 60ms, p99: 400ms },
                    failure_rate: 0.01, heartbeat: 10s }
      - local_activity: { count: 1, duration: 5ms }
      - timer: 2s
      - child_workflow: { workflow_type: ShipmentWorkflow, count: 1 }
      - wait_signal: { count: 1, timeout: 1h }

load:                          # traffic that is not workflow starts
  signals:   [ { workflow_type: CartWorkflow, rate: 400/s, target: hot, hot_workflows: 3 } ]
  queries:   [ { workflow_type: OrderWorkflow, rate: 20/s } ]
  describes: [ { workflow_type: OrderWorkflow, rate: 10/s } ]
  visibility: [ { namespace: orders, rate: 5/s, op: list } ]

schedules:
  - { namespace: orders, count: 2000, interval: 1m, aligned: true, workflow_type: ReportWorkflow }

events:                        # changes during the run
  - { at: 50s, replicas: { history: 5 }, label: scale history }
  - { at: 80s, dynamic_config: { matching.rps: 2400 } }
  - { at: 90s, start_rate: { workflow_type: OrderWorkflow, rate: 300/s } }

calibration:
  observations: [ ../metrics/observed.yaml ]

costs:                         # CPU µs per operation, overriding the defaults
  history: { RespondWorkflowTaskCompleted: 900us, per_command: 120us }

report: { warn_utilization: 0.7, critical_utilization: 0.9, api_p99_slo: 500ms, top: 5 }
```

Durations and latencies accept a constant (`5ms`), `{ p50, p99 }` for a lognormal fit, a
`quantiles` map, `{ dist: exp, mean }` and `{ dist: uniform, min, max }`. Rates accept `150/s`,
`9000/m` or a bare number.

## Example scenarios

| Scenario | What it shows |
|---|---|
| `baseline.yaml` | A healthy order workload at 150 wf/s. The first limit it hits is `matching.rps` on the busiest matching pod (the `headroom` warning). |
| `hot-entity.yaml` | Entity workflows with signal fan-in. A few "celebrity" workflows saturate their lock and shard, and more history pods don't help. |
| `frontend-throttling.yaml` | A cluster-wide `frontend.globalNamespaceRPS` split over four frontends plus pinned connections. Polls (priority 4) starve first. |
| `db-bound.yaml` | A small Aurora instance. The database and the SQL connection pools are the root cause, with backlog and timeouts as symptoms. |
| `schedules.yaml` | 2,000 schedules aligned to the minute. They are rate-limited by `worker.schedulerNamespaceStartWorkflowRPS` on the per-namespace worker. |
| `scale-out.yaml` | History scaled from 3 to 5 pods mid-run: shard movement, the unavailability window and cold caches. |
| `cassandra-large.yaml` | 4,096 shards on Cassandra at 1,000 wf/s, with shard IO forced to 1. Useful for sweeps. |
| `from-helm.yaml` | Deployment read from a `temporalio/helm-charts` values file. |

## What is modelled

[docs/MODEL.md](docs/MODEL.md) describes the mechanics with references to the Temporal 1.31.0
source. In summary:

* **Routing.**
  * Shards: `farm.Fingerprint32(namespaceID + "_" + workflowID) % numHistoryShards + 1`,
    checked against go-farm test vectors.
  * History pods and matching partitions: ringpop hash ring placement.
  * Clients: SDK connections pinned to frontend pods.
* **Frontend.**
  * `frontend.rps` and namespace priority rate limiters: higher priorities reserve tokens from
    lower ones, so polls starve first.
  * The long-running-request concurrency limit.
  * The visibility limiter.
* **History.**
  * Per-workflow lock, with API deadline versus the non-user lock timeout.
  * Host-level mutable-state LRU cache and events cache.
  * Shard IO semaphore.
  * Persistence priority rate limiter, which rejects immediately.
  * `history.rps`.
  * Transfer, timer and visibility queues: reader rate limits and batching, pending-task limits,
    the 512-worker scheduler, and busy/throttled retry backoff.
  * Timer lookahead and checkpoints.
* **Matching.**
  * The 1.31 matcher: sync match or backlog, and forwarding from child to root partitions.
  * Writer buffer overflow (`SystemOverloaded`), task batching and backlog reloads.
  * Sticky queues and `StickyWorkerUnavailable`.
* **Workers (Go SDK).**
  * Poller balancing between sticky and normal queues.
  * Slots and the sticky cache, with replay on a miss.
  * Eager workflow start and eager activities.
  * Heartbeats, retries and timeouts.
  * Child workflows, signals with buffered events, and local activities.
* **Worker service.**
  * Scheduler workflows on the per-namespace worker.
  * Elasticsearch bulk processor.
* **Infrastructure.**
  * CPU as `GOMAXPROCS` cores per pod.
  * Per-pod SQL connection pools.
  * The database as a multi-server queue with per-operation service times. Cassandra LWT
    operations are slower.
  * Network round trips.
  * SDK retries with backoff.

## Limitations

tempdes is a model. Use its findings to decide what to load-test and which metric to watch.
Don't treat them as guarantees.

* **CPU costs.** The default per-operation costs are estimates for mid-size pods. Calibrate them
  with `container_cpu_usage_seconds_total`, or override them under `costs:`. Absolute CPU
  numbers are not trustworthy until you do. Ratios and trends are more robust.
* **Database.** The database is a queue with independent service times. Lock contention inside
  the database, vacuum/compaction, and Cassandra LWT contention on one partition are not
  modelled beyond latency. Hot rows show up through the per-workflow lock and shard semaphore
  instead.
* **Features not modelled.**
  * Multi-cluster replication.
  * Nexus.
  * Workflow Update.
  * Worker Versioning.
  * CHASM-based schedulers.
  * Matchers other than the 1.31 default priority matcher. `matching.enableFairness` and
    `matching.useNewMatcher: false` are validated but not simulated.
  * Archival.
  * Batch operations.
  * GC pauses.
  * Pod restarts other than scaling.
* **SDK behaviour.** Workers follow the Go SDK's poller and sticky-cache behaviour. Other SDKs
  differ in detail.
* **Settings without an effect.** Dynamic config keys outside `tempdes dc modeled` are
  validated and shown, but they don't change the simulation.

## Development

These are the checks CI runs:

```bash
cargo fmt --all --check
cargo clippy --all-targets --locked
cargo test --locked         # unit tests + end-to-end scenario checks (tests/scenarios.rs)
cargo doc --no-deps --locked
cargo deny check            # licenses, advisories and sources (cargo install cargo-deny)
```

The end-to-end tests check that each example surfaces the hotspot it was built to show. They
also check that runs are deterministic for a given seed, and that replica and dynamic config
changes move results in the expected direction.

The dynamic config registry (`data/dynamicconfig-1.31.0.json`, embedded in the binary) is
generated from a Temporal source checkout by the `gen-dc-registry` binary. It finds every
`New<Scope><Type>Setting(...)` registration with a small Go lexer and evaluates constant
defaults such as `5*time.Minute`. The Temporal version is read from the source.

```bash
git clone --depth 1 --branch v1.31.0 https://github.com/temporalio/temporal ../temporal
cargo run --release --bin gen-dc-registry -- ../temporal -o data/dynamicconfig-1.31.0.json
```

With `--check data/dynamicconfig-1.31.0.json` instead of `-o`, it compares rather than writes.
It lists added, removed and changed settings, and exits 1 if the file is out of date.

Source layout:

* `src/sim/`: the deterministic async executor, semaphores, statistics and distributions.
* `src/model/`: the Temporal services.
* `src/config/`: the scenario, dynamic config, Helm import and the registry generator
  (`registry_gen.rs`, run by `src/bin/gen-dc-registry.rs`).
* `src/metrics/`: observations and the Prometheus parser.
* `src/calibrate.rs` and `src/run.rs`: calibration and single runs.
* `src/report/`: hotspot rules and the text, JSON, Prometheus and HTML outputs.
* `src/sweep.rs`: sweeps.

## Contributing

Contributions are welcome: bug reports, scenarios that reproduce production behaviour, model
fixes backed by Temporal source references, and new hotspot rules. See
[CONTRIBUTING.md](CONTRIBUTING.md) for how to build, test and propose changes. Everyone taking
part is expected to follow the [code of conduct](CODE_OF_CONDUCT.md). Please report security
issues privately, as described in [SECURITY.md](SECURITY.md).

## License

Licensed under either of

* Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or
  <https://www.apache.org/licenses/LICENSE-2.0>)
* MIT license ([LICENSE-MIT](LICENSE-MIT) or <https://opensource.org/licenses/MIT>)

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in
the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any
additional terms or conditions.

Parts of tempdes are derived from go-farm and ringpop-go, and the dynamic config registry is
extracted from Temporal. [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md) has their notices.
tempdes is an independent project and is not affiliated with or endorsed by Temporal Technologies.
