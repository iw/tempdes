//! End-to-end checks of `tempdes ui`: frames describe the simulated cluster, live changes take
//! effect on the running simulation, and the Topcoat routes respond.

#![cfg(feature = "ui")]

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tempdes::config::dynamic::DcValue;
use tempdes::config::scenario::Scenario;
use tempdes::model::build;
use tempdes::model::params::Params;
use tempdes::model::types::Service;
use tempdes::run::{self, Overrides};
use tempdes::ui::app::{self, ControlRequest, parse_control};
use tempdes::ui::engine::{Command, Engine, Options};
use tempdes::ui::frame::{AnalysisSummary, FLOWS, Frame, Meta, Phase, Sampler};
use topcoat::router::request::Request;
use topcoat::router::{Body, Method, StatusCode, header, to_bytes};

fn params(file: &str, warmup_s: f64) -> Params {
    let sc = Scenario::load(
        &Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("examples/scenarios")
            .join(file),
    )
    .expect("scenario loads");
    let ov = Overrides {
        warmup_s: Some(warmup_s),
        duration_s: Some(30.0),
        ..Default::default()
    };
    run::prepare(&sc, &ov, None).expect("parameters resolve")
}

fn meta(seq: u64, offered: f64) -> Meta {
    Meta {
        seq,
        run: 1,
        paused: false,
        speed: 0.0,
        actual_speed: 0.0,
        load_scale: 1.0,
        offered_per_s: offered,
        analysis: AnalysisSummary::default(),
    }
}

/// Frames are computed from the cluster state directly, without the engine thread.
#[test]
fn frames_describe_the_interval_just_simulated() {
    let p = params("baseline.yaml", 5.0);
    let offered: f64 = p.wf_types.iter().map(|w| w.start_rate).sum();
    let num_shards = p.num_shards as usize;
    let (ctx, mut ex) = build::build(p);
    let _rates = build::start(&ctx, &mut ex);
    let mut sampler = Sampler::new(&ctx);
    ex.run_until(4_000_000);
    let a = sampler.frame(&ctx, &meta(1, offered));
    assert_eq!(a.phase, Phase::Warmup);
    assert!((a.t - 4.0).abs() < 0.01, "t={}", a.t);
    assert!((a.dt - 4.0).abs() < 0.01, "dt={}", a.dt);
    assert_eq!(a.services.len(), 4);
    assert_eq!(a.services[1].service, "history");
    assert_eq!(a.services[1].replicas, 3);
    assert_eq!(a.pods.iter().filter(|p| p.alive).count(), 10);
    assert_eq!(a.flows.len(), FLOWS.len());
    assert_eq!(a.flows[0].id, "clients-frontend");
    assert!(
        a.workload.started_per_s > 100.0,
        "started {}",
        a.workload.started_per_s
    );
    assert!(
        a.flows[0].per_s > 100.0,
        "client traffic {}",
        a.flows[0].per_s
    );
    assert!(
        a.flows[2].per_s > a.flows[0].per_s,
        "history sees more calls than clients make"
    );
    assert!(a.flows[6].per_s > 0.0, "history writes to persistence");
    assert!(a.services[1].cpu_max > 0.0 && a.services[1].cpu_max <= 1.0);
    assert!(a.persistence.util > 0.0 && a.persistence.util <= 1.0);
    assert_eq!(a.shard_heat.len(), num_shards);
    let owners = a
        .shard_owners
        .as_ref()
        .expect("first frame carries the shard owners");
    assert_eq!(owners.owner.len(), num_shards);
    assert_eq!(owners.pods.len(), 3);
    assert!(a.latency.start.n > 0, "start latencies were observed");
    assert_eq!(a.events.len(), 0, "no events yet: {:?}", a.events);

    // the warm-up reset zeroes the counters; the engine resynchronises the sampler there so
    // the next frame covers only the interval since then
    ex.run_until(5_000_000);
    sampler.resync(&ctx);
    ex.run_until(7_000_000);
    let b = sampler.frame(&ctx, &meta(2, offered));
    assert_eq!(b.phase, Phase::Measuring);
    assert!((b.dt - 2.0).abs() < 0.01, "dt={}", b.dt);
    assert!(
        (b.measured_s - 2.0).abs() < 0.01,
        "measured {}",
        b.measured_s
    );
    assert!(
        b.workload.started_per_s > 100.0,
        "started {}",
        b.workload.started_per_s
    );
    assert!(
        b.shard_owners.is_none(),
        "owners are sent only when they change"
    );
    assert!(
        b.latency.start.n < a.latency.start.n,
        "interval quantiles use fewer samples"
    );
    let json = serde_json::to_value(&b).unwrap();
    assert_eq!(json["flows"][2]["id"], "frontend-history");
    assert!(json["services"][1]["limit"]["util"].as_f64().unwrap() > 0.0);
}

fn wait_for(engine: &Engine, what: &str, pred: impl Fn(&Frame) -> bool) -> Arc<Frame> {
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let f = engine.latest();
        if pred(&f) {
            return f;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {what} (t={})",
            f.t
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// The engine thread advances the simulation, publishes frames and applies commands.
#[test]
fn engine_applies_live_changes() {
    let engine = Engine::spawn(
        params("baseline.yaml", 3.0),
        Options {
            speed: 0.0,
            load: 1.0,
        },
    )
    .expect("engine starts");
    let f = wait_for(&engine, "measurement to start", |f| {
        f.phase == Phase::Measuring && f.t > 6.0
    });
    assert_eq!(f.run, 1);
    assert!((f.workload.offered_per_s - 150.0).abs() < 1e-9);
    assert!(
        f.workload.completed_per_s > 50.0,
        "completed {}",
        f.workload.completed_per_s
    );

    // load
    assert!(engine.send(Command::Load(2.0)));
    let f = wait_for(&engine, "load ×2", |f| {
        f.load_scale == 2.0 && f.workload.offered_per_s > 299.0
    });
    let t_load = f.t;
    let f = wait_for(&engine, "starts to follow the load", |f| f.t > t_load + 4.0);
    assert!(
        f.workload.started_per_s > 220.0,
        "started {}",
        f.workload.started_per_s
    );
    assert!(
        engine
            .shared()
            .events
            .iter()
            .any(|e| e.text.contains("load ×2.00")),
        "the change is on the timeline"
    );

    // replicas: shards move to the new history pod
    assert!(engine.send(Command::Replicas(Service::History, 4)));
    let f = wait_for(&engine, "a fourth history pod", |f| {
        f.services[1].replicas == 4
    });
    assert_eq!(
        f.pods
            .iter()
            .filter(|p| p.alive && p.service == "history")
            .count(),
        4
    );
    wait_for(&engine, "shard ownership to change", |f| {
        f.history.shard_moves > 0
    });
    assert!(
        engine
            .shared()
            .events
            .iter()
            .any(|e| e.text.contains("history replicas 3 -> 4"))
    );

    // dynamic config
    assert!(engine.send(Command::DynamicConfig(
        "matching.rps".into(),
        DcValue::Int(600)
    )));
    let t_dc = engine.latest().t;
    let f = wait_for(&engine, "matching.rps rejections", |f| {
        f.t > t_dc + 6.0 && f.limits.iter().any(|l| l.limiter == "matching.rps")
    });
    assert!(
        f.services[2].rejected_per_s > 0.0,
        "matching rejects at 600 rps"
    );
    assert!(
        engine
            .shared()
            .events
            .iter()
            .any(|e| e.text.contains("matching.rps -> 600"))
    );

    // hotspot analysis runs while the simulation is in progress
    let f = wait_for(&engine, "a hotspot analysis", |f| f.analysis.seq > 0);
    assert!(!f.analysis.headline.is_empty());
    assert!(engine.shared().analysis.is_some());
    assert!(engine.shared().series.len() > 10);

    // pause stops simulated time
    assert!(engine.send(Command::Pause));
    let f = wait_for(&engine, "pause", |f| f.paused);
    let t_pause = f.t;
    std::thread::sleep(Duration::from_millis(600));
    assert_eq!(
        engine.latest().t,
        t_pause,
        "time does not advance while paused"
    );
    assert!(engine.send(Command::Resume));
    wait_for(&engine, "resume", |f| !f.paused && f.t > t_pause);

    // restart with another seed starts a new run from t = 0
    assert!(engine.send(Command::Restart(Some(11))));
    let f = wait_for(&engine, "run 2", |f| f.run == 2);
    assert!(f.t < 5.0, "run 2 started over (t={})", f.t);
    assert_eq!(f.load_scale, 2.0, "the load multiplier carries over");
    assert_eq!(
        f.services[1].replicas, 3,
        "replica changes belong to the previous run"
    );
}

#[test]
fn control_requests_are_validated() {
    let p = params("baseline.yaml", 3.0);
    let req = |action: &str, value: Option<f64>, service: Option<&str>, key: Option<&str>| {
        ControlRequest {
            action: action.into(),
            value,
            service: service.map(str::to_string),
            key: key.map(str::to_string),
            seed: None,
        }
    };
    assert_eq!(
        parse_control(&req("pause", None, None, None), &p),
        Ok(Command::Pause)
    );
    assert_eq!(
        parse_control(&req("speed", Some(0.0), None, None), &p),
        Ok(Command::Speed(0.0))
    );
    assert!(parse_control(&req("speed", Some(1000.0), None, None), &p).is_err());
    assert_eq!(
        parse_control(&req("load", Some(1.5), None, None), &p),
        Ok(Command::Load(1.5))
    );
    assert!(parse_control(&req("load", Some(-1.0), None, None), &p).is_err());
    assert!(parse_control(&req("load", None, None, None), &p).is_err());
    assert_eq!(
        parse_control(&req("replicas", Some(5.0), Some("history"), None), &p),
        Ok(Command::Replicas(Service::History, 5))
    );
    assert!(parse_control(&req("replicas", Some(0.0), Some("history"), None), &p).is_err());
    assert!(parse_control(&req("replicas", Some(2.5), Some("history"), None), &p).is_err());
    assert!(parse_control(&req("replicas", Some(2.0), Some("database"), None), &p).is_err());
    assert_eq!(
        parse_control(&req("dc", Some(600.0), None, Some("Matching.RPS")), &p),
        Ok(Command::DynamicConfig(
            "matching.rps".into(),
            DcValue::Int(600)
        ))
    );
    assert!(
        parse_control(
            &req("dc", Some(1.0), None, Some("history.hostLevelCacheMaxSize")),
            &p
        )
        .is_err()
    );
    assert!(parse_control(&req("dc", Some(f64::NAN), None, Some("history.rps")), &p).is_err());
    assert!(parse_control(&req("explode", None, None, None), &p).is_err());
    let keys = app::runtime_keys(&p);
    assert!(keys.iter().any(|(k, v)| k == "history.rps" && *v > 0.0));
}

fn request(method: Method, path: &str, body: Option<&str>) -> Request {
    let mut b = Request::<()>::builder().method(method).uri(path);
    if body.is_some() {
        b = b.header(header::CONTENT_TYPE, "application/json");
    }
    b.body(body.map(|s| Body::from(s.to_string())).unwrap_or_default())
        .unwrap()
}

/// The page, its fragments, the JSON routes and the control route respond through the router.
#[test]
fn routes_respond() {
    let engine = Engine::spawn(
        params("baseline.yaml", 3.0),
        Options {
            speed: 0.0,
            load: 1.0,
        },
    )
    .expect("engine starts");
    wait_for(&engine, "a few frames", |f| f.seq > 4);
    let router = app::router(engine.clone());
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let send = |method: Method, path: &str, body: Option<&str>| -> (StatusCode, String, String) {
        rt.block_on(async {
            let response = router.handle(request(method, path, body)).await;
            let (parts, body) = response.into_parts();
            let bytes = to_bytes(body, usize::MAX).await.unwrap();
            let ct = parts
                .headers
                .get(header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_string();
            (
                parts.status,
                ct,
                String::from_utf8_lossy(&bytes).into_owned(),
            )
        })
    };

    let (status, ct, html) = send(Method::GET, "/", None);
    assert_eq!(status, StatusCode::OK);
    assert!(ct.starts_with("text/html"), "{ct}");
    assert!(html.starts_with("<!DOCTYPE html>"));
    assert!(html.contains("orders-baseline"));
    assert!(html.contains("data-bind=\"services.1.cpu_max|pct\""));
    assert!(html.contains("data-vbar=\"pods."));
    assert!(html.contains("id=\"topology\""));
    assert!(html.contains("<script src=\"/app.js\""));
    assert!(
        html.contains("IBM-Plex-Sans"),
        "the font stylesheet is linked"
    );
    assert!(!html.contains("&lt;svg"), "markup is not double-escaped");

    for path in [
        "/fragment/topology",
        "/fragment/hotspots",
        "/fragment/detail",
    ] {
        let (status, ct, body) = send(Method::GET, path, None);
        assert_eq!(status, StatusCode::OK, "{path}");
        assert!(ct.starts_with("text/html"), "{path}: {ct}");
        assert!(!body.contains("<html"), "{path} is a fragment");
    }

    let (status, ct, body) = send(Method::GET, "/api/frame", None);
    assert_eq!(status, StatusCode::OK);
    assert!(ct.starts_with("application/json"), "{ct}");
    let frame: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(frame["services"][0]["service"], "frontend");
    let (status, _, body) = send(Method::GET, "/api/history", None);
    assert_eq!(status, StatusCode::OK);
    assert!(
        serde_json::from_str::<serde_json::Value>(&body)
            .unwrap()
            .is_array()
    );

    let (status, ct, _) = send(Method::GET, "/app.css", None);
    assert_eq!(status, StatusCode::OK);
    assert!(ct.starts_with("text/css"), "{ct}");
    let (status, ct, _) = send(Method::GET, "/app.js", None);
    assert_eq!(status, StatusCode::OK);
    assert!(ct.starts_with("text/javascript"), "{ct}");

    let (status, _, body) = send(
        Method::POST,
        "/api/control",
        Some(r#"{"action":"load","value":1.5}"#),
    );
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.contains("\"ok\":true"));
    wait_for(&engine, "the load change", |f| f.load_scale == 1.5);
    let (status, _, body) = send(
        Method::POST,
        "/api/control",
        Some(r#"{"action":"load","value":99}"#),
    );
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    let (status, _, _) = send(Method::GET, "/nope", None);
    assert_eq!(status, StatusCode::NOT_FOUND);
}
