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
  `remove` and `dir` manage profiles. Profiles are kept private:
  - they live outside any repository, in `~/.config/tempdes/profiles`;
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

[Unreleased]: https://github.com/iw/tempdes/commits/main
