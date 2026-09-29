//! End-to-end checks: each example scenario must surface the hotspot it was built to show, and
//! the simulator must be deterministic and respond to replica / dynamic config changes in the
//! expected direction.

use std::path::Path;

use tempdes::config::dynamic::{Constraints, DcValue};
use tempdes::config::scenario::{ClientLb, Scenario};
use tempdes::model::types::{Api, PersistOp};
use tempdes::report::{self, RunResult, Severity};
use tempdes::run::{self, Overrides};
use tempdes::util::units::Dur;

fn scenario(file: &str) -> Scenario {
    Scenario::load(
        &Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("examples/scenarios")
            .join(file),
    )
    .expect("scenario loads")
}

fn simulate(file: &str, ov: Overrides) -> RunResult {
    simulate_scenario(&scenario(file), ov)
}

fn simulate_scenario(sc: &Scenario, ov: Overrides) -> RunResult {
    simulate_with_ctx(sc, ov).0
}

/// The result together with the simulated cluster, for checks on raw metrics.
fn simulate_with_ctx(sc: &Scenario, ov: Overrides) -> (RunResult, run::RunOutput) {
    let p = run::prepare(sc, &ov, None).expect("parameters resolve");
    let out = run::run_params(p);
    let r = report::analyze(&out.ctx, &out.info, None);
    (r, out)
}

fn with_dc(mut ov: Overrides, key: &str, value: DcValue) -> Overrides {
    ov.dc.push((key.into(), value, Constraints::default()));
    ov
}

fn short() -> Overrides {
    Overrides {
        warmup_s: Some(8.0),
        duration_s: Some(20.0),
        ..Default::default()
    }
}

fn has(r: &RunResult, category: &str, sev: Severity) -> bool {
    r.hotspots
        .iter()
        .any(|h| h.category == category && h.severity <= sev)
}

fn categories(r: &RunResult) -> Vec<String> {
    r.hotspots
        .iter()
        .map(|h| format!("{:?} {} {}", h.severity, h.category, h.title))
        .collect()
}

#[test]
fn baseline_is_healthy_and_keeps_up() {
    let r = simulate("baseline.yaml", short());
    assert!(
        !r.hotspots.iter().any(|h| h.severity == Severity::Critical),
        "unexpected critical hotspots: {:#?}",
        categories(&r)
    );
    let w = &r.workflows[0];
    assert!(
        (w.started_per_s - w.offered_start_rate).abs() / w.offered_start_rate < 0.1,
        "started {}",
        w.started_per_s
    );
    assert!(
        w.completed_per_s > 0.8 * w.offered_start_rate,
        "completed {}",
        w.completed_per_s
    );
    // e2e ≈ activities + 2s timer
    assert!(
        w.e2e.p50_ms > 2_000.0 && w.e2e.p50_ms < 3_500.0,
        "e2e p50 {}",
        w.e2e.p50_ms
    );
    assert_eq!(w.wft_timeouts, 0);
}

#[test]
fn simulation_is_deterministic() {
    let a = simulate("baseline.yaml", short());
    let b = simulate("baseline.yaml", short());
    assert_eq!(a.sim_steps, b.sim_steps);
    assert_eq!(
        a.workflows[0].completed_per_s,
        b.workflows[0].completed_per_s
    );
    assert_eq!(a.apis[0].latency.p99_ms, b.apis[0].latency.p99_ms);
    let c = simulate(
        "baseline.yaml",
        Overrides {
            seed: Some(99),
            ..short()
        },
    );
    assert_ne!(
        a.sim_steps, c.sim_steps,
        "a different seed should change the trajectory"
    );
}

#[test]
fn hot_entities_saturate_their_workflow_lock() {
    let r = simulate("hot-entity.yaml", short());
    assert!(
        has(&r, "workflow-lock", Severity::Critical),
        "{:#?}",
        categories(&r)
    );
    assert_eq!(
        r.hotspots[0].category,
        "workflow-lock",
        "{:#?}",
        categories(&r)
    );
    assert!(r.history.lock_timeouts > 0);
    let hot = &r.history.hot_workflows[0];
    assert!(hot.util > 0.9, "hot workflow lock {}", hot.util);
}

#[test]
fn global_namespace_budget_throttles_polls_first() {
    let r = simulate("frontend-throttling.yaml", short());
    assert!(
        r.limits
            .iter()
            .any(|l| l.limiter == "frontend.namespaceRPS" && l.rejected > 0),
        "{:#?}",
        r.limits
    );
    // P1 calls (Start/Respond) are protected; the rejections land on polls (P4)
    let start = r
        .apis
        .iter()
        .find(|a| a.api == "StartWorkflowExecution")
        .unwrap();
    assert!(start.errors.is_empty(), "{:?}", start.errors);
    let polls_rejected: u64 = r
        .apis
        .iter()
        .filter(|a| a.api.starts_with("Poll"))
        .flat_map(|a| a.errors.values())
        .sum();
    assert!(polls_rejected > 0);
    assert_eq!(
        r.hotspots[0].category,
        "rate-limit",
        "{:#?}",
        categories(&r)
    );
}

#[test]
fn small_database_is_the_root_cause() {
    let r = simulate("db-bound.yaml", short());
    assert!(
        r.persistence.utilization > 0.9,
        "db {}",
        r.persistence.utilization
    );
    assert_eq!(r.hotspots[0].category, "database", "{:#?}", categories(&r));
    assert!(has(&r, "connection-pool", Severity::Critical));
}

#[test]
fn aligned_schedules_are_rate_limited() {
    let r = simulate(
        "schedules.yaml",
        Overrides {
            warmup_s: Some(5.0),
            duration_s: Some(130.0),
            ..Default::default()
        },
    );
    let s = r.schedules.as_ref().expect("schedule results");
    assert!(s.rate_limited > 0);
    assert!(
        s.action_delay.p99_ms > 30_000.0,
        "delay p99 {}",
        s.action_delay.p99_ms
    );
    assert!(
        has(&r, "schedules", Severity::Warning),
        "{:#?}",
        categories(&r)
    );
}

#[test]
fn scale_out_moves_shards_and_blocks_briefly() {
    let r = simulate(
        "scale-out.yaml",
        Overrides {
            warmup_s: Some(20.0),
            duration_s: Some(45.0),
            ..Default::default()
        },
    );
    assert!(
        r.history.shard_moves > 100,
        "moves {}",
        r.history.shard_moves
    );
    assert!(r.history.shard_unavailable.p99_ms > 500.0);
    assert_eq!(r.config.replicas["history"], 5);
}

#[test]
fn more_history_replicas_lower_history_cpu() {
    let mut ov = short();
    ov.start_rate_scale = Some(2.0);
    ov.replicas = vec![("history".into(), 2)];
    let two = simulate("baseline.yaml", ov.clone());
    ov.replicas = vec![("history".into(), 6)];
    let six = simulate("baseline.yaml", ov);
    let cpu = |r: &RunResult| {
        r.services
            .iter()
            .find(|s| s.service == "history")
            .unwrap()
            .cpu_max
    };
    assert!(
        cpu(&six) < cpu(&two) * 0.6,
        "2 pods {} vs 6 pods {}",
        cpu(&two),
        cpu(&six)
    );
}

#[test]
fn dynamic_config_override_takes_effect() {
    let mut ov = short();
    ov.dc.push((
        "frontend.namespaceRPS".into(),
        DcValue::Int(100),
        Constraints {
            namespace: Some("orders".into()),
            ..Default::default()
        },
    ));
    let r = simulate("baseline.yaml", ov);
    assert!(
        r.limits
            .iter()
            .any(|l| l.limiter == "frontend.namespaceRPS"),
        "{:#?}",
        r.limits
    );
    assert!(
        r.config.effective_dynamic_config["frontend.namespaceRPS"].contains("100"),
        "{}",
        r.config.effective_dynamic_config["frontend.namespaceRPS"]
    );
}

#[test]
fn cassandra_ignores_shard_io_concurrency() {
    let mut ov = short();
    ov.dc.push((
        "history.shardIOConcurrency".into(),
        DcValue::Int(4),
        Constraints::default(),
    ));
    ov.start_rate_scale = Some(0.1);
    let r = simulate("cassandra-large.yaml", ov);
    assert_eq!(r.history.shard_io_concurrency, 1);
    assert!(
        r.warnings.iter().any(|w| w.contains("Cassandra")),
        "{:#?}",
        r.warnings
    );
}

#[test]
fn limiter_headroom_is_reported_before_rejections() {
    let r = simulate("baseline.yaml", short());
    let h = r
        .hotspots
        .iter()
        .find(|h| h.category == "headroom")
        .unwrap_or_else(|| panic!("no headroom hotspot: {:#?}", categories(&r)));
    assert!(h.resource.starts_with("matching.rps"), "{}", h.resource);
    assert!(
        !r.limits
            .iter()
            .any(|l| l.limiter == "matching.rps" && l.rejected > 0),
        "matching.rps should not reject yet: {:#?}",
        r.limits
    );
    // pods expose the utilisation that drove the warning
    let matching = r.services.iter().find(|s| s.service == "matching").unwrap();
    let worst = matching
        .pods
        .iter()
        .flat_map(|p| p.limit_util.iter())
        .filter(|(n, _)| n == "matching.rps")
        .map(|(_, u)| *u)
        .fold(0.0, f64::max);
    assert!(
        (0.7..1.0).contains(&worst),
        "matching.rps utilisation {worst}"
    );
}

/// Requests/s of the busiest live frontend over the mean.
fn frontend_skew(r: &RunResult) -> f64 {
    let fe = r.services.iter().find(|s| s.service == "frontend").unwrap();
    let rps: Vec<f64> = fe
        .pods
        .iter()
        .filter(|p| p.alive)
        .map(|p| p.requests_per_s)
        .collect();
    rps.iter().copied().fold(0.0, f64::max) / (rps.iter().sum::<f64>() / rps.len() as f64)
}

fn frontend_rejections(r: &RunResult) -> u64 {
    r.limits
        .iter()
        .filter(|l| l.limiter.starts_with("frontend."))
        .map(|l| l.rejected)
        .sum()
}

#[test]
fn client_side_load_balancing_spreads_frontend_load() {
    let pinned = simulate("frontend-lb.yaml", short());
    assert!(
        frontend_skew(&pinned) > 1.15,
        "pinned skew {}",
        frontend_skew(&pinned)
    );
    assert!(frontend_rejections(&pinned) > 0, "{:#?}", pinned.limits);
    assert!(
        pinned
            .hotspots
            .iter()
            .any(|h| h.knobs.iter().any(|k| k.key == "cluster.network.client_lb")),
        "the report should suggest client load balancing: {:#?}",
        categories(&pinned)
    );
    for lb in [ClientLb::RoundRobin, ClientLb::Proxy] {
        let r = simulate(
            "frontend-lb.yaml",
            Overrides {
                client_lb: Some(lb),
                ..short()
            },
        );
        assert!(frontend_skew(&r) < 1.05, "{lb}: skew {}", frontend_skew(&r));
        assert_eq!(frontend_rejections(&r), 0, "{lb}: {:#?}", r.limits);
        assert_eq!(r.config.client_lb, lb.as_str());
    }
}

#[test]
fn new_frontends_get_traffic_only_once_clients_find_them() {
    // share of frontend requests served by the pods added at 30s (ordinals 3..6)
    let new_share = |lb: ClientLb, max_age: &str| {
        let r = simulate(
            "frontend-scale-out.yaml",
            Overrides {
                client_lb: Some(lb),
                start_rate_scale: Some(0.25),
                duration_s: Some(100.0),
                dc: vec![(
                    "frontend.keepAliveMaxConnectionAge".into(),
                    DcValue::Str(max_age.into()),
                    Constraints::default(),
                )],
                ..Default::default()
            },
        );
        let fe = r.services.iter().find(|s| s.service == "frontend").unwrap();
        let (mut new, mut all) = (0.0, 0.0);
        for p in fe.pods.iter().filter(|p| p.alive) {
            all += p.requests_per_s;
            if p.name.ends_with("-3") || p.name.ends_with("-4") || p.name.ends_with("-5") {
                new += p.requests_per_s;
            }
        }
        new / all
    };
    // with the default 5m max connection age, clients keep their connections and resolved
    // addresses through the whole window
    assert!(new_share(ClientLb::Pinned, "5m") < 0.02);
    assert!(new_share(ClientLb::RoundRobin, "5m") < 0.02);
    // a proxy adds new pods after registration and health checks (15s)
    assert!(new_share(ClientLb::Proxy, "5m") > 0.3);
    // round robin clients re-resolve DNS when a connection reaches the max age
    let rr = new_share(ClientLb::RoundRobin, "1m");
    assert!(rr > 0.2, "round robin share with a 1m max age: {rr}");
}

/// Calibrate `file` against `examples/metrics/observed.yaml` (StartWorkflowExecution 180/s).
fn calibrate(file: &str, ov: &Overrides) -> (Scenario, run::Calibration) {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let sc = Scenario::load(&root.join("examples/scenarios").join(file)).expect("scenario loads");
    let observed = root.join("examples/metrics/observed.yaml");
    let obs = run::load_observations(&sc, &[observed.display().to_string()])
        .expect("observations load")
        .expect("observations given");
    let cal = run::calibrate(&sc, ov, obs).expect("calibration");
    (sc, cal)
}

#[test]
fn load_multiplier_scales_the_calibrated_workload() {
    let base = short();
    let heavy = Overrides {
        start_rate_scale: Some(1.5),
        ..short()
    };
    let (sc, cal_base) = calibrate("baseline.yaml", &base);
    let (_, cal_heavy) = calibrate("baseline.yaml", &heavy);
    // the CPU pilot always simulates the observed load
    assert_eq!(cal_base.cpu_scale, cal_heavy.cpu_scale);
    assert!(cal_base.cpu_scale.iter().any(Option::is_some));

    let offered = |ov: &Overrides, cal: &run::Calibration| {
        let p = run::prepare(&sc, ov, Some(cal)).expect("parameters resolve");
        let rate: f64 = p.wf_types.iter().map(|t| t.start_rate).sum();
        (rate, p.prov.notes.clone())
    };
    let (observed, _) = offered(&base, &cal_base);
    assert!(
        (observed - 180.0).abs() < 0.5,
        "calibrated start rate {observed}"
    );
    let (scaled, notes) = offered(&heavy, &cal_heavy);
    assert!(
        (scaled - 270.0).abs() < 0.5,
        "--load 1.5 on top of calibration: {scaled}"
    );
    assert!(notes.iter().any(|n| n.contains("load ×1.5")), "{notes:#?}");
    assert!(run::validation_obs(&heavy, Some(&cal_heavy.obs)).is_none());
    assert!(run::validation_obs(&base, Some(&cal_base.obs)).is_some());
}

#[test]
fn history_appends_ride_inside_their_writes() {
    // Create/UpdateWorkflowExecution persist their history events inside the call (the SQL
    // and Cassandra stores append, then write the mutable state), so there is no separate
    // AppendHistoryNodes call, and every persistence call is charged to the limiter. At
    // 150 wf/s each history pod makes about 3,000 calls/s: a 3,600/s limit has room.
    let mut ov = short();
    ov.dc.push((
        "history.persistenceMaxQPS".into(),
        DcValue::Int(3600),
        Constraints::default(),
    ));
    let r = simulate("baseline.yaml", ov);
    assert!(
        !r.limits
            .iter()
            .any(|l| l.limiter.starts_with("history.persistence")),
        "{:#?}",
        r.limits
    );
    assert!(
        !r.persistence
            .ops
            .iter()
            .any(|o| o.op == "AppendHistoryNodes"),
        "{:#?}",
        r.persistence.ops
    );
    let history = r.services.iter().find(|s| s.service == "history").unwrap();
    for p in &history.pods {
        let util = p
            .limit_util
            .iter()
            .find(|(n, _)| n == "history.persistenceMaxQPS")
            .map(|(_, u)| *u)
            .unwrap();
        let calls = p.persistence_per_s / 3600.0;
        assert!(
            util < 1.0 && util > 0.5,
            "{}: persistence limit use {util}",
            p.name
        );
        assert!(
            (util / calls - 1.0).abs() < 0.02,
            "{}: {util} vs {calls}",
            p.name
        );
    }
}

#[test]
fn schedule_to_start_counts_from_the_scheduled_time() {
    // The SDK measures schedule-to-start from the task's scheduled time, so a task that reaches
    // matching late is late even when matching dispatches it at once. Reading each shard's
    // transfer queue once a second holds tasks in history for up to a second.
    let healthy = simulate("baseline.yaml", short());
    let mut ov = short();
    ov.dc.push((
        "history.transferProcessorMaxPollRPS".into(),
        DcValue::Int(1),
        Constraints::default(),
    ));
    let slow = simulate("baseline.yaml", ov);
    let (h, s) = (&healthy.workflows[0], &slow.workflows[0]);
    for (task, healthy_p99, slow_p99) in [
        (
            "workflow task",
            h.wft_schedule_to_start.p99_ms,
            s.wft_schedule_to_start.p99_ms,
        ),
        (
            "activity",
            h.activity_schedule_to_start.p99_ms,
            s.activity_schedule_to_start.p99_ms,
        ),
    ] {
        assert!(
            healthy_p99 < 200.0,
            "{task} p99 {healthy_p99} ms when healthy"
        );
        assert!(
            slow_p99 > 500.0,
            "{task} p99 {slow_p99} ms with a slow hand-off"
        );
    }
    let matching_wait = slow
        .matching
        .partitions
        .iter()
        .map(|p| p.task_wait.p99_ms)
        .fold(0.0, f64::max);
    assert!(
        matching_wait < 200.0,
        "matching wait p99 {matching_wait} ms"
    );
}

/// Activities that fail half their attempts, retried after 100 ms.
const RETRYING: &str = r#"
name: retrying
duration: 20s
cluster:
  num_history_shards: 512
  replicas: { frontend: 3, history: 3, matching: 3, worker: 1 }
  persistence: { store: postgresql }
namespaces:
  - name: orders
workers:
  - name: order-workers
    namespace: orders
    task_queue: orders
    processes: 6
    workflow_pollers: 10
    activity_pollers: 16
workflows:
  - type: OrderWorkflow
    namespace: orders
    task_queue: orders
    start_rate: 100/s
    steps:
      - activity: { count: 2, failure_rate: 0.5, retry_initial: 100ms, duration: { p50: 30ms, p99: 250ms } }
      - activity: { count: 1, failure_rate: 0.5, retry_initial: 100ms, duration: { p50: 20ms, p99: 150ms } }
"#;

#[test]
fn activity_retries_go_straight_to_matching() {
    // Temporal's retry timer task pushes the next attempt to matching itself, with no transfer
    // task and no mutable-state write (executeActivityRetryTimerTask). So each attempt is
    // dispatched by exactly one history task: a transfer task for the first attempt, and a retry
    // timer for each retry.
    let sc = Scenario::parse_str(RETRYING).expect("scenario parses");
    let r = simulate_scenario(&sc, short());
    let tasks = |t: &str| {
        r.history
            .tasks
            .iter()
            .find(|x| x.task_type == t)
            .map_or(0.0, |x| x.per_s)
    };
    let api = |a: &str| r.apis.iter().find(|x| x.api == a).map_or(0.0, |x| x.per_s);
    let transfers = tasks("TransferActiveTaskActivityTask");
    let retries = tasks("TimerActiveTaskActivityRetryTimer");
    let attempts = api("RespondActivityTaskCompleted") + api("RespondActivityTaskFailed");
    assert!(
        retries > 0.3 * attempts,
        "{retries} retries/s of {attempts} attempts/s"
    );
    assert!(
        ((transfers + retries) / attempts - 1.0).abs() < 0.1,
        "{transfers} transfer and {retries} retry tasks/s for {attempts} attempts/s"
    );
}

#[test]
fn history_persistence_limit_follows_shard_ownership() {
    // History splits a cluster-wide persistence limit by shard ownership: each pod enforces
    // global × owned shards ÷ numHistoryShards, not global ÷ pods.
    let mut ov = short();
    ov.dc.push((
        "history.persistenceGlobalMaxQPS".into(),
        DcValue::Int(9000),
        Constraints::default(),
    ));
    let r = simulate("baseline.yaml", ov);
    let history = r.services.iter().find(|s| s.service == "history").unwrap();
    let shards = f64::from(r.history.num_shards);
    let mut limits = Vec::new();
    for p in history.pods.iter().filter(|p| p.alive) {
        let expected = 9000.0 * p.owned as f64 / shards;
        assert!(
            (p.persistence_qps_limit - expected).abs() < 1e-6,
            "{}: {} vs {expected}",
            p.name,
            p.persistence_qps_limit
        );
        limits.push(p.persistence_qps_limit);
    }
    // uneven ownership gives uneven limits, which still add up to the cluster-wide number
    assert!(
        (limits.iter().sum::<f64>() - 9000.0).abs() < 1e-6,
        "{limits:?}"
    );
    let max = limits.iter().copied().fold(0.0, f64::max);
    let min = limits.iter().copied().fold(f64::INFINITY, f64::min);
    assert!(max > min, "{limits:?}");
}

/// The history task scheduler's rate limiter at 900 tasks/s for the cluster, well below the
/// baseline's history task rate.
fn scheduler_limiter(shadow: bool) -> Overrides {
    let mut ov = short();
    for (key, value) in [
        (
            "history.taskSchedulerEnableRateLimiter",
            DcValue::Bool(true),
        ),
        (
            "history.taskSchedulerEnableRateLimiterShadowMode",
            DcValue::Bool(shadow),
        ),
        ("history.taskSchedulerGlobalMaxQPS", DcValue::Int(900)),
    ] {
        ov.dc.push((key.into(), value, Constraints::default()));
    }
    ov
}

#[test]
fn task_scheduler_limiter_in_shadow_mode_only_counts() {
    let off = simulate("baseline.yaml", short());
    let shadow = simulate("baseline.yaml", scheduler_limiter(true));
    let ts = &shadow.history.task_scheduler;
    assert_eq!(ts.mode, "shadow");
    assert!(
        ts.throttled_per_s > 100.0,
        "{} refusals/s",
        ts.throttled_per_s
    );
    assert!(
        shadow
            .hotspots
            .iter()
            .any(|h| h.title.contains("shadow mode")),
        "{:#?}",
        categories(&shadow)
    );
    assert_eq!(off.history.task_scheduler.throttled_per_s, 0.0);
    // nothing is held back: the run is the same as with the limiter off
    let (a, b) = (&off.workflows[0], &shadow.workflows[0]);
    assert_eq!(a.completed_per_s, b.completed_per_s);
    assert_eq!(a.e2e.p99_ms, b.e2e.p99_ms);
}

#[test]
fn task_scheduler_limiter_holds_tasks_back_outside_shadow_mode() {
    let r = simulate("baseline.yaml", scheduler_limiter(false));
    let ts = &r.history.task_scheduler;
    assert_eq!(ts.mode, "on");
    assert!(ts.throttled_per_s > 0.0);
    let wait = r
        .history
        .tasks
        .iter()
        .map(|t| t.schedule.p99_ms)
        .fold(0.0, f64::max);
    assert!(wait > 1000.0, "scheduling wait p99 {wait} ms");
    // a limiter inside the cluster ranks above the delays it causes
    let top = &r.hotspots[0];
    assert_eq!(
        (top.category.as_str(), top.resource.as_str()),
        ("rate-limit", "history.taskSchedulerGlobalMaxQPS"),
        "{:#?}",
        categories(&r)
    );
}

#[test]
fn markdown_report_covers_the_run() {
    let r = simulate("db-bound.yaml", short());
    let md = tempdes::report::markdown::render_run(&r, false);
    assert!(md.starts_with("# tempdes: "), "{md}");
    for section in [
        "## Hotspots",
        "## Workflows",
        "## API latency",
        "## Pods",
        "## Database",
        "## History",
        "## Matching",
    ] {
        assert!(md.contains(&format!("\n{section}")), "missing {section}");
    }
    let top = &r.hotspots[0];
    assert!(md.contains(&format!(
        "### 1. Critical · {} · {}",
        top.category, top.title
    )));
    assert!(md.contains("| OrderWorkflow |"));
    assert!(!md.contains('\x1b'), "no terminal colour codes");
    // every table row has as many cells as its header
    let mut header = None;
    for line in md.lines() {
        if !line.starts_with('|') {
            header = None;
            continue;
        }
        let cells = line.replace("\\|", "").matches('|').count();
        match header {
            None => header = Some(cells),
            Some(n) => assert_eq!(cells, n, "ragged row: {line}"),
        }
    }
}

#[test]
fn sdk_retries_share_the_call_deadline() {
    // The Go SDK gives each call one context deadline (10s unless the client sets another) and
    // retries inside it, so a signal queued behind a hot workflow's lock fails at the deadline
    // instead of retrying past it.
    let signal = |r: &RunResult| {
        r.apis
            .iter()
            .find(|a| a.api == "SignalWorkflowExecution")
            .cloned()
            .expect("signals")
    };
    // within 20s the celebrity carts' lock queues stay under the default 10s
    let s = signal(&simulate("hot-entity.yaml", short()));
    assert!(
        s.latency.p99_ms > 3_000.0 && s.latency.max_ms < 10_000.0,
        "{:?}",
        s.latency
    );
    assert!(!s.errors.contains_key("DeadlineExceeded"), "{:?}", s.errors);
    // the deadline is set per client, and no retry outlives it
    let mut sc = scenario("hot-entity.yaml");
    for l in &mut sc.load.signals {
        l.rpc_timeout = Dur::from_secs(2.0);
    }
    let s = signal(&simulate_scenario(&sc, short()));
    assert!(s.latency.max_ms <= 2_000.0, "{:?}", s.latency);
    assert!(s.errors["DeadlineExceeded"] > 100, "{:?}", s.errors);
}

/// Clients that wait for each result with a history long poll; `LIMIT` is replaced by a
/// frontend limit.
const RESULT_WAITERS: &str = r#"
name: result-waiters
duration: 20s
cluster:
  num_history_shards: 128
  replicas: { frontend: 1, history: 2, matching: 2, worker: 1 }
  persistence: { store: postgresql }
dynamic_config:
  LIMIT
namespaces:
  - name: orders
workers:
  - name: order-workers
    namespace: orders
    task_queue: orders
    processes: 2
    workflow_pollers: 4
    activity_pollers: 4
workflows:
  - type: OrderWorkflow
    namespace: orders
    task_queue: orders
    start_rate: 50/s
    await_result: true
    steps:
      - timer: 5s
"#;

#[test]
fn history_long_polls_are_counted_and_shed_first() {
    // GetWorkflowExecutionHistory with WaitNewEvent is a long-running request for
    // frontend.namespaceCount ...
    let sc = Scenario::parse_str(
        &RESULT_WAITERS.replace("LIMIT", "frontend.namespaceCount: [{ value: 100 }]"),
    )
    .expect("scenario parses");
    let r = simulate_scenario(&sc, short());
    assert!(
        r.limits
            .iter()
            .any(|l| l.limiter == "frontend.namespaceCount" && l.rejected > 0),
        "{:#?}",
        r.limits
    );
    // ... and the namespace rate limiter runs it at P5, below worker polls at P4
    let sc = Scenario::parse_str(
        &RESULT_WAITERS.replace("LIMIT", "frontend.namespaceRPS: [{ value: 150 }]"),
    )
    .expect("scenario parses");
    let (_, out) = simulate_with_ctx(&sc, short());
    let m = out.ctx.m.borrow();
    let rejected = |api: Api| {
        let o = m.fe_total(api);
        o.error_count() as f64 / o.count.max(1) as f64
    };
    let (long_polls, polls) = (
        rejected(Api::PollWorkflowExecutionHistory),
        rejected(Api::PollWorkflowTaskQueue),
    );
    assert!(
        long_polls > 0.5 && long_polls > 3.0 * polls,
        "long polls {long_polls}, worker polls {polls}"
    );
}

/// Long activities that heartbeat, on workers with fewer slots than the load needs; `EXTRA` is
/// replaced by more activity options.
const TIMEOUTS: &str = r#"
name: activity-timeouts
duration: 20s
cluster:
  num_history_shards: 256
  replicas: { frontend: 2, history: 2, matching: 2, worker: 1 }
  persistence: { store: postgresql }
namespaces:
  - name: orders
workers:
  - name: order-workers
    namespace: orders
    task_queue: orders
    processes: 2
    workflow_pollers: 8
    activity_pollers: 8
    activity_slots: 50
workflows:
  - type: OrderWorkflow
    namespace: orders
    task_queue: orders
    start_rate: 40/s
    steps:
      - activity: { count: 1, duration: { p50: 3s, p99: 5s }, heartbeat: 1s, schedule_to_start_timeout: 5s EXTRA }
      - activity: { count: 1, duration: { p50: 20ms, p99: 150ms } }
"#;

#[test]
fn schedule_to_start_timeouts_fail_activities() {
    // 100 slots for about 130 concurrent activities: tasks queue, and those waiting longer than
    // the schedule-to-start timeout fail without a retry, failing their workflows
    let sc = Scenario::parse_str(&TIMEOUTS.replace("EXTRA", "")).expect("scenario parses");
    let r = simulate_scenario(&sc, short());
    let w = &r.workflows[0];
    assert!(
        w.activity_timeouts
            .get("ScheduleToStart")
            .copied()
            .unwrap_or(0)
            > 10,
        "{:?}",
        w.activity_timeouts
    );
    assert!(w.activities_failed > 10 && w.failed_per_s > 0.5, "{w:#?}");
    // heartbeats keep arriving, so no heartbeat timeout; each heartbeat timer re-arms itself
    assert!(!w.activity_timeouts.contains_key("Heartbeat"));
    let timers = r
        .history
        .tasks
        .iter()
        .find(|t| t.task_type == "TimerActiveTaskActivityTimeout")
        .expect("activity timer tasks");
    assert!(timers.noop_fraction < 0.9, "{timers:#?}");
    assert!(
        has(&r, "activity-timeouts", Severity::Critical),
        "{:#?}",
        categories(&r)
    );
    // a workflow that handles the failure goes on to its next step
    let sc = Scenario::parse_str(&TIMEOUTS.replace("EXTRA", ", on_failure: continue"))
        .expect("scenario parses");
    let r = simulate_scenario(&sc, short());
    let w = &r.workflows[0];
    assert!(w.activities_failed > 10, "{w:#?}");
    assert_eq!(w.failed_per_s, 0.0);
    assert!(w.completed_per_s > 0.5 * w.started_per_s, "{w:#?}");
}

#[test]
fn start_to_close_timeouts_retry_until_attempts_run_out() {
    // every attempt runs 3s against a 1s start-to-close timeout: three attempts, then the
    // activity and its workflow fail
    let sc = Scenario::parse_str(
        &TIMEOUTS
            .replace("start_rate: 40/s", "start_rate: 5/s")
            .replace("activity_slots: 50", "activity_slots: 200")
            .replace(
                "heartbeat: 1s, schedule_to_start_timeout: 5s EXTRA",
                "start_to_close_timeout: 1s, max_attempts: 3, retry_initial: 100ms",
            ),
    )
    .expect("scenario parses");
    let r = simulate_scenario(&sc, short());
    let w = &r.workflows[0];
    let timeouts = w
        .activity_timeouts
        .get("StartToClose")
        .copied()
        .unwrap_or(0);
    assert!(w.activities_failed > 10, "{w:#?}");
    let per_activity = timeouts as f64 / w.activities_failed as f64;
    assert!(
        (2.5..3.5).contains(&per_activity),
        "{timeouts} timeouts for {} activities",
        w.activities_failed
    );
    assert!(w.failed_per_s > 0.0);
    let retries = r
        .history
        .tasks
        .iter()
        .find(|t| t.task_type == "TimerActiveTaskActivityRetryTimer")
        .map_or(0.0, |t| t.per_s);
    assert!(retries > 0.0);
}

#[test]
fn falling_behind_is_reported_with_its_cause_first() {
    // at a 2,000/s persistence limit per history pod the cluster completes a fraction of the
    // workflows it starts
    let r = simulate(
        "baseline.yaml",
        with_dc(short(), "history.persistenceMaxQPS", DcValue::Int(2000)),
    );
    let pos = |f: &dyn Fn(&report::Hotspot) -> bool| r.hotspots.iter().position(f);
    let behind = pos(&|h| h.category == "throughput" && h.title.contains("not keeping up"))
        .unwrap_or_else(|| panic!("{:#?}", categories(&r)));
    let limiter = pos(&|h| h.resource == "history.persistenceMaxQPS").expect("limiter hotspot");
    assert!(limiter < behind, "{:#?}", categories(&r));
}

#[test]
fn long_workflows_are_not_mistaken_for_falling_behind() {
    // workflows that run a minute keep piling up during a 20s window in a healthy cluster: the
    // expected completions allow for their duration
    let sc = Scenario::parse_str(
        &RESULT_WAITERS
            .replace("LIMIT", "frontend.namespaceRPS: [{ value: 2400 }]")
            .replace("await_result: true", "await_result: false")
            .replace("timer: 5s", "timer: 60s"),
    )
    .expect("scenario parses");
    let r = simulate_scenario(&sc, short());
    assert!(
        !r.hotspots
            .iter()
            .any(|h| h.title.contains("not keeping up")),
        "{:#?}",
        categories(&r)
    );
    // while short workflows keep up in the baseline
    let r = simulate("baseline.yaml", short());
    assert!(
        !r.hotspots
            .iter()
            .any(|h| h.title.contains("not keeping up"))
    );
}

/// A healthy cluster running a status poll that fails four attempts in five, retried with the
/// default policy (1s, doubling, at most 100s apart, unlimited attempts).
const RETRY_TAIL: &str = r#"
name: retry-tail
warmup: 30s
duration: 30s
cluster:
  num_history_shards: 64
  replicas: { frontend: 1, history: 1, matching: 1, worker: 1 }
  persistence: { store: postgresql }
namespaces:
  - name: default
workers:
  - name: pollers
    namespace: default
    task_queue: q
    processes: 2
    workflow_pollers: 8
    activity_pollers: 8
    workflow_slots: 200
    activity_slots: 400
workflows:
  - type: StatusWorkflow
    namespace: default
    task_queue: q
    start_rate: 20/s
    steps:
      - activity: { count: 1, failure_rate: 0.8, duration: 100ms }
"#;

#[test]
fn retry_tails_are_not_mistaken_for_falling_behind() {
    // Five attempts on average, but the intervals double: a third of the runs need six or more
    // attempts and take over half a minute, so fewer workflows close than start during the
    // window even though nothing waits in the cluster.
    let sc = Scenario::parse_str(RETRY_TAIL).expect("scenario parses");
    let r = simulate_scenario(&sc, Overrides::default());
    let w = &r.workflows[0];
    assert!(
        w.completed_per_s < 0.85 * w.started_per_s,
        "the retry tail should hold back completions: {w:#?}"
    );
    assert!(
        !r.hotspots.iter().any(|h| h.category == "throughput"),
        "{:#?}",
        r.hotspots.iter().map(|h| &h.title).collect::<Vec<_>>()
    );
}

#[test]
fn attempts_plan_the_retries_of_each_activity() {
    // a status poll that always takes three attempts: two fail, the third succeeds
    let with = |attempts: &str| {
        Scenario::parse_str(&RETRY_TAIL.replace(
            "failure_rate: 0.8, duration: 100ms",
            &format!("{attempts}, duration: 100ms"),
        ))
    };
    let ratio = |r: &RunResult| {
        let per_s = |api: &str| {
            r.apis
                .iter()
                .find(|a| a.api == api)
                .map_or(0.0, |a| a.per_s)
        };
        per_s("RespondActivityTaskFailed") / per_s("RespondActivityTaskCompleted")
    };
    let r = simulate_scenario(
        &with("attempts: 3").expect("scenario parses"),
        Overrides::default(),
    );
    assert!((ratio(&r) - 2.0).abs() < 0.1, "{}", ratio(&r));
    // no retry tail: a run takes about 3.3s, so the workflows close as fast as they start
    let w = &r.workflows[0];
    assert!(w.completed_per_s > 0.95 * w.started_per_s, "{w:#?}");
    assert!(!r.hotspots.iter().any(|h| h.category == "throughput"));
    // half the activities succeed at once, half need three attempts: one failure on average
    let r = simulate_scenario(
        &with("attempts: { 1: 0.5, 3: 0.5 }").expect("shares parse"),
        Overrides::default(),
    );
    assert!((ratio(&r) - 1.0).abs() < 0.15, "{}", ratio(&r));
    // attempts replaces failure_rate
    assert!(with("failure_rate: 0.5, attempts: 3").is_err());
    assert!(with("attempts: 0").is_err());
}

#[test]
fn non_retryable_errors_end_activities_without_retries() {
    let with = |plan: &str| {
        Scenario::parse_str(&RETRY_TAIL.replace(
            "failure_rate: 0.8, duration: 100ms",
            &format!("{plan}, duration: 100ms"),
        ))
    };
    let per_s = |r: &RunResult, api: &str| {
        r.apis
            .iter()
            .find(|a| a.api == api)
            .map_or(0.0, |a| a.per_s)
    };
    // 30% are rejected on their first attempt, and fail their workflow; the rest succeed at once
    let r = simulate_scenario(
        &with("non_retryable: { 1: 0.3 }").expect("scenario parses"),
        Overrides::default(),
    );
    let w = &r.workflows[0];
    assert!(
        (w.failed_per_s / w.started_per_s - 0.3).abs() < 0.05,
        "{w:#?}"
    );
    // every failure is final: no retries
    assert_eq!(w.activity_failures, w.activities_failed, "{w:#?}");
    // failures the scenario plans are its outcome, not a hotspot
    assert_eq!(w.activities_non_retryable, w.activities_failed, "{w:#?}");
    assert!(
        !r.hotspots.iter().any(|h| h.category == "activity-timeouts"),
        "{:?}",
        r.hotspots.iter().map(|h| &h.title).collect::<Vec<_>>()
    );
    // with retried attempts before them: half succeed at once, 20% on their third attempt, and
    // 30% fail for good on their second, so a failed attempt per activity on average, and 0.7
    // successes
    let r = simulate_scenario(
        &with("attempts: { 1: 0.5, 3: 0.2 }, non_retryable: { 2: 0.3 }, on_failure: continue")
            .expect("scenario parses"),
        Overrides::default(),
    );
    let ratio = per_s(&r, "RespondActivityTaskFailed") / per_s(&r, "RespondActivityTaskCompleted");
    assert!((ratio - 1.0 / 0.7).abs() < 0.15, "{ratio}");
    let w = &r.workflows[0];
    assert_eq!(w.failed_per_s, 0.0, "on_failure: continue");
    assert!(w.completed_per_s > 0.95 * w.started_per_s, "{w:#?}");
    // the plan replaces failure_rate, and with attempt shares it covers every activity
    assert!(with("failure_rate: 0.5, non_retryable: { 1: 0.1 }").is_err());
    assert!(with("attempts: { 1: 0.5 }, non_retryable: { 1: 0.1 }").is_err());
    assert!(with("non_retryable: { 0: 0.1 }").is_err());
    assert!(with("attempts: 3, non_retryable: { 1: 0.1 }").is_ok());
}

#[test]
fn failed_attempts_run_for_their_own_duration() {
    // three attempts of a 100ms activity, 1s and 2s apart: 3.3s when failures are as quick
    // as successes, 5.1s when a failed attempt takes 1s
    let with = |extra: &str| {
        Scenario::parse_str(&RETRY_TAIL.replace(
            "failure_rate: 0.8, duration: 100ms",
            &format!("attempts: 3, duration: 100ms{extra}"),
        ))
        .expect("scenario parses")
    };
    let p50 = |sc: &Scenario| {
        simulate_scenario(sc, Overrides::default()).workflows[0]
            .e2e
            .p50_ms
    };
    let (quick, slow) = (p50(&with("")), p50(&with(", failed_duration: 1s")));
    assert!(
        (slow - quick - 1_800.0).abs() < 300.0,
        "{quick}ms, then {slow}ms"
    );
    // failed attempts that hang time out at start-to-close instead of failing
    let r = simulate_scenario(
        &with(", failed_duration: 30s, start_to_close_timeout: 2s"),
        Overrides::default(),
    );
    let w = &r.workflows[0];
    assert!(
        w.activity_timeouts
            .get("StartToClose")
            .copied()
            .unwrap_or(0)
            > 0,
        "{w:#?}"
    );
    assert!(
        !r.apis
            .iter()
            .any(|a| a.api == "RespondActivityTaskFailed" && a.per_s > 0.0),
        "the worker doesn't respond to an attempt that timed out"
    );
    assert!(w.completed_per_s > 0.9 * w.started_per_s, "{w:#?}");
}

/// Two tenants on one cluster; `LIMIT` is replaced by dynamic config.
const TENANTS: &str = r#"
name: two-tenants
duration: 20s
cluster:
  num_history_shards: 256
  replicas: { frontend: 2, history: 2, matching: 2, worker: 1 }
  persistence: { store: postgresql }
dynamic_config:
  LIMIT
namespaces:
  - name: orders
  - name: batch
workers:
  - { name: order-workers, namespace: orders, task_queue: orders, processes: 2, workflow_pollers: 8, activity_pollers: 8 }
  - { name: batch-workers, namespace: batch, task_queue: batch, processes: 2, workflow_pollers: 8, activity_pollers: 8 }
workflows:
  - type: OrderWorkflow
    namespace: orders
    task_queue: orders
    start_rate: 50/s
    steps:
      - activity: { count: 2, duration: { p50: 20ms, p99: 100ms } }
  - type: BatchWorkflow
    namespace: batch
    task_queue: batch
    start_rate: 100/s
    steps:
      - activity: { count: 4, duration: { p50: 20ms, p99: 100ms } }
"#;

#[test]
fn namespace_persistence_limits_isolate_tenants() {
    // a per-namespace persistence limit throttles only its own namespace
    let sc = Scenario::parse_str(&TENANTS.replace(
        "LIMIT",
        "history.persistenceNamespaceMaxQPS: [{ value: 300, constraints: { namespace: batch } }]",
    ))
    .expect("scenario parses");
    let r = simulate_scenario(&sc, short());
    let ns_limit: Vec<_> = r
        .limits
        .iter()
        .filter(|l| l.limiter == "history.persistenceNamespaceMaxQPS")
        .collect();
    assert!(!ns_limit.is_empty(), "{:#?}", r.limits);
    assert!(
        ns_limit.iter().all(|l| l.place.ends_with("ns=batch")),
        "{ns_limit:#?}"
    );
    assert!(
        !r.limits
            .iter()
            .any(|l| l.limiter == "history.persistenceMaxQPS"),
        "{:#?}",
        r.limits
    );
    let orders = r
        .workflows
        .iter()
        .find(|w| w.workflow_type == "OrderWorkflow")
        .unwrap();
    assert!(
        orders.completed_per_s > 0.9 * orders.started_per_s,
        "{orders:#?}"
    );
}

#[test]
fn execution_queues_take_busy_workflows_off_the_shared_pool() {
    // with the execution queue scheduler, a task that fails on a busy workflow moves to that
    // workflow's own queue instead of being resubmitted to the shared scheduler
    let off = simulate("hot-entity.yaml", short());
    let on = simulate(
        "hot-entity.yaml",
        with_dc(
            short(),
            "history.taskSchedulerEnableExecutionQueueScheduler",
            DcValue::Bool(true),
        ),
    );
    assert!(off.history.exec_queues.is_none());
    let q = on.history.exec_queues.as_ref().expect("execution queues");
    assert!(q.submitted_per_s > 0.0 && q.max_queues > 0, "{q:?}");
    let runs: f64 = on
        .history
        .tasks
        .iter()
        .map(|t| t.exec_queue_runs_per_s)
        .sum();
    assert!(runs > 0.0);
}

#[test]
fn calibration_fits_service_times_to_observed_latency() {
    // production measures persistence latency with queueing and, for writes, the history append
    // included; the fitted service times make the calibrated run reproduce it
    let (sc, cal) = calibrate("baseline.yaml", &short());
    let update = PersistOp::UpdateWorkflowExecution.idx();
    let fit = cal.persistence_fit[update].expect("fitted");
    assert!(fit > 0.5 && fit <= 1.0, "fit {fit}");
    let p = run::prepare(&sc, &short(), Some(&cal)).expect("parameters resolve");
    assert!(p.db_includes_append[update]);
    let out = run::run_params(p);
    let r = report::analyze(&out.ctx, &out.info, Some(&cal.obs));
    let row = r
        .validation
        .iter()
        .find(|v| v.metric == "persistence_latency p99{UpdateWorkflowExecution}")
        .expect("validation row");
    assert!((row.ratio - 1.0).abs() < 0.2, "{row:?}");
}
