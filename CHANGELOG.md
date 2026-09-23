# Changelog

All notable changes to tempdes are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

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
- **Examples.** Eight example scenarios, each reproducing a hotspot: baseline, hot entities,
  frontend throttling, database-bound, aligned schedules, scale-out, large Cassandra cluster and
  Helm import.

[Unreleased]: https://github.com/iw/tempdes/commits/main
