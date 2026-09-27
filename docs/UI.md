# The live view (`tempdes ui`)

`tempdes ui` simulates one configuration and serves a page that shows the cluster while it
runs. Where `tempdes run` gives you the ranked hotspots at the end, the live view shows how
they come about: which service saturates first, and how that saturation turns into waiting,
retries and rejections upstream and downstream of it.

```bash
tempdes ui examples/scenarios/baseline.yaml --open
tempdes ui examples/scenarios/db-bound.yaml --speed 5 -r history=6
tempdes ui examples/scenarios/hot-entity.yaml --speed 0   # as fast as the machine allows
```

The command takes the same scenario, overrides (`-r`, `-d`, `--load`, `--seed`,
`--client-lb`, `--duration`, `--warmup`) and observed metrics (`-o`) as `run`. It listens on
`127.0.0.1:3000` by default (`--host`, `--port`; port 0 picks a free one) and `--open` opens the
page in a browser. The simulation is open-ended: it keeps running past the scenario's duration
until you stop the command, so the hotspots you see are the report's rules applied to the
measurement window so far.

## What you see

**Masthead.** The simulated clock, the phase (warming up or measuring), the achieved speed,
the live load multiplier and the run number, plus the current hotspot headline and counts.

**Controls.** Pause and resume, the speed (simulated seconds per wall second; warm-up always
runs as fast as possible), a restart with the same seed (an identical replay) or a new one, and
three kinds of live change:

| Control | What it does in the simulation |
|---|---|
| Load | Multiplies every workflow start and signal rate from that moment on. |
| Replicas | Scales a service the way a scenario event does: new pods join the ringpop ring, shards or partitions move, a cold cache follows. |
| Dynamic config | Applies one of the keys the simulator re-reads at runtime to the live pods: `history.rps`, `matching.rps`, `frontend.rps`, `frontend.namespaceRPS`, `*.persistenceMaxQPS`, `history.shardIOConcurrency` and the `*ProcessorSchedulerWorkerCount` keys. |

Every change is stamped on the timeline and drawn as a dashed mark on the charts, so cause
and effect line up in time.

**Cluster.** The figure follows the request paths of Temporal 1.31.0: SDK clients and workers
call the frontend, the frontend calls history and matching, history hands tasks to matching and
matching reports started tasks back, and every service talks to persistence. Each service box
shows its request rate, one CPU bar per pod, and the rate limiter closest to its limit on the
busiest pod (`frontend.rps`, `history.rps`, `matching.rps`, the persistence QPS limits, or a
namespace limit). Flows get thicker with traffic. When a limiter rejects, the edge it protects
says so in red, and the box shows the rejection rate.

**Where the load lands.** Two rows of stages, one per path:

* the *request path*: clients → frontend admission → history → persistence;
* the *task path*: history task queues → matching → workers → completion.

Each stage shows its own saturation (a utilisation or a rate) and the symptoms other stages
cause in it: retries, timeouts, rejections and waiting. Read left to right to see a stage's
saturation land on the ones that depend on it; read right to left to find the stage that
explains a symptom.

**Over time.** Throughput, the busiest pod's CPU per service, database utilisation,
rate-limit rejections, the matching backlog, workflow task schedule-to-start latency,
end-to-end latency and `StartWorkflowExecution` latency over the last three minutes of
simulated time.

**Hotspots.** The report's rules, run every five simulated seconds over the measurement
window so far, with the same causal ranking, evidence, metrics to watch and knobs as the
report.

**Detail.** The history shard map (each square is a shard, coloured by its IO utilisation in
the last interval, grouped by owner) and the report's tables: pods, workflows and APIs,
matching partitions and persistence operations, cumulative since warm-up.

**Timeline.** Scenario events and live changes with what the simulator did in response
(shards moved, limits changed).

## How the numbers are computed

Every half second of simulated time the simulation thread differences the simulator's
counters and latency histograms against the previous snapshot. Rates, utilisations and
quantiles on the page therefore describe the last interval, not the average since warm-up.
The tables and the hotspots use the cumulative statistics, as the report does.

Warm-up runs at full speed. Once the warm-up reset has happened the thread paces itself to
the requested speed; if the simulation cannot keep up (a heavy scenario at a high speed), it
simply runs as fast as it can and the masthead shows the achieved speed.

A given seed reproduces a run exactly, live changes included, as long as you make them at the
same simulated times. Restart replays the same seed from the start; the load multiplier
carries over, replica and dynamic config changes do not.

## How it is built

The page is served by [Topcoat](https://github.com/tokio-rs/topcoat), a server-rendered Rust
web framework:

* `src/ui/engine.rs` runs the simulator on a dedicated thread (the simulator is
  single-threaded) in slices of simulated time, applies commands between slices, publishes
  frames on a `tokio::sync::watch` channel and runs the hotspot analysis;
* `src/ui/frame.rs` is the frame: the interval's rates, utilisations and latencies per
  service, pod and flow;
* `src/ui/views.rs` renders the page and its fragments with Topcoat components; every live
  number is rendered from the current frame and tagged with its path in the frame JSON;
* `src/ui/app.rs` is the Topcoat router: the page, the fragments, a server-sent event stream
  of frames (`/events`), JSON routes (`/api/frame`, `/api/history`) and the control route
  (`POST /api/control`);
* `src/ui/app.js` applies each frame to the tagged elements, draws the charts and the shard
  map, and posts control changes. Structure that changes (pods after scaling, a new hotspot
  analysis) is re-rendered by the server as a fragment and swapped in.

Fonts are IBM Plex Sans and Mono, declared with Topcoat's Fontsource support and loaded from
jsDelivr; the page falls back to system fonts offline.

Topcoat's optional browser runtime (signals, shards, `live!` regions) is not used. Its browser
script is served from an asset bundle that only the separate `topcoat` CLI produces after a
build, and a `cargo install`ed tool cannot depend on that step. Server-sent events and a
small script give the same result with everything compiled into the binary.

`tempdes` is built with the `ui` feature by default. `cargo build --no-default-features`
gives the lean command-line tool without Topcoat and tokio.
