//! `tempdes metrics fetch` against a stand-in Prometheus HTTP API: the file it writes must read
//! back as the observations tempdes uses, whichever names the cluster's exporter gives metrics.

#![cfg(feature = "fetch")]

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;

use tempdes::calibrate::cpu_cores;
use tempdes::metrics::cmd::CPU_SELECTOR;
use tempdes::metrics::fetch::{Conn, FetchArgs, ScanArgs, fetch, scan};
use tempdes::metrics::observed::Observations;
use tempdes::model::types::Service;

/// How the stand-in names Temporal's metrics.
#[derive(Clone, Copy, Debug)]
enum Exporter {
    /// tally, Temporal's default: plain names, histogram bounds in seconds
    Tally,
    /// OpenTelemetry with a `temporal_` prefix: `_total` counters, bounds in milliseconds
    Otel,
}

impl Exporter {
    fn name(self, base: &str, hist: bool, counter: bool) -> String {
        match (self, hist, counter) {
            (Exporter::Tally, true, _) => format!("{base}_bucket"),
            (Exporter::Tally, false, _) => base.to_string(),
            (Exporter::Otel, true, _) => format!("temporal_{base}_milliseconds_bucket"),
            (Exporter::Otel, false, true) => format!("temporal_{base}_total"),
            (Exporter::Otel, false, false) => format!("temporal_{base}"),
        }
    }

    /// A latency in milliseconds, in the histogram's unit.
    fn latency(self, ms: f64) -> f64 {
        match self {
            Exporter::Tally => ms / 1000.0,
            Exporter::Otel => ms,
        }
    }
}

fn decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'%' if i + 2 < b.len() => {
                out.push(u8::from_str_radix(&s[i + 1..i + 3], 16).unwrap());
                i += 3;
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8(out).unwrap()
}

type Series = Vec<(Vec<(&'static str, String)>, f64)>;

/// The stand-in's answer to an instant query.
fn answer(e: Exporter, q: &str) -> Series {
    let one = |v: f64| vec![(vec![], v)];
    let counters = [
        "service_requests",
        "persistence_requests",
        "task_requests",
        "cache_requests",
        "cache_miss",
        "poll_success",
        "poll_success_sync",
        "service_errors_resource_exhausted",
        "workflow_success",
        "workflow_context_cleared",
    ];
    let gauges = ["approximate_backlog_count", "numshards_gauge"];
    let hists = ["service_latency", "persistence_latency"];
    if let Some(name) = q
        .strip_prefix("count by (le) (")
        .and_then(|r| r.strip_suffix(')'))
    {
        assert!(
            matches!(e, Exporter::Tally),
            "{name}: OpenTelemetry names carry the unit"
        );
        return ["0.001", "0.005", "0.01", "0.05", "0.1", "0.5", "1", "+Inf"]
            .iter()
            .map(|le| (vec![("le", le.to_string())], 1.0))
            .collect();
    }
    if let Some(name) = q.strip_prefix("count(").and_then(|r| r.strip_suffix(')')) {
        let known = counters.iter().any(|b| e.name(b, false, true) == name)
            || gauges.iter().any(|b| e.name(b, false, false) == name)
            || hists.iter().any(|b| e.name(b, true, false) == name)
            || name == "container_cpu_usage_seconds_total";
        return if known { one(3.0) } else { vec![] };
    }
    let labels = |pairs: &[(&'static str, &str)]| -> Vec<(&'static str, String)> {
        pairs.iter().map(|(k, v)| (*k, v.to_string())).collect()
    };
    if q.starts_with("histogram_quantile(") {
        let p = |q50: f64, q90: f64, q99: f64| {
            if q.starts_with("histogram_quantile(0.5,") {
                e.latency(q50)
            } else if q.starts_with("histogram_quantile(0.9,") {
                e.latency(q90)
            } else {
                e.latency(q99)
            }
        };
        if q.contains(&e.name("persistence_latency", true, false)) {
            return vec![
                (
                    labels(&[("operation", "UpdateWorkflowExecution")]),
                    p(35.0, 60.0, 85.0),
                ),
                // no samples in the window
                (labels(&[("operation", "GetWorkflowExecution")]), f64::NAN),
            ];
        }
        if q.contains(&e.name("service_latency", true, false)) {
            return vec![(
                labels(&[
                    ("service_name", "frontend"),
                    ("operation", "StartWorkflowExecution"),
                ]),
                p(20.0, 40.0, 54.0),
            )];
        }
        return vec![];
    }
    let name = |b: &str, counter: bool| e.name(b, false, counter);
    if q.contains(&name("service_requests", true)) && q.contains("frontend") {
        return vec![
            (
                labels(&[
                    ("namespace", "orders-prod"),
                    ("operation", "StartWorkflowExecution"),
                ]),
                125.0,
            ),
            (
                labels(&[
                    ("namespace", "orders-prod"),
                    ("operation", "PollWorkflowTaskQueue"),
                ]),
                900.0,
            ),
            (
                labels(&[
                    ("namespace", "temporal-system"),
                    ("operation", "StartWorkflowExecution"),
                ]),
                0.0,
            ),
        ];
    }
    if q.contains(&name("service_requests", true)) {
        return vec![(
            labels(&[
                ("service_name", "history"),
                ("operation", "RecordWorkflowTaskStarted"),
            ]),
            870.0,
        )];
    }
    if q.contains(&name("persistence_requests", true)) {
        return vec![(labels(&[("operation", "UpdateWorkflowExecution")]), 3750.0)];
    }
    if q.contains(&name("cache_miss", true)) {
        return one(100.0);
    }
    if q.contains(&name("cache_requests", true)) {
        return one(5000.0);
    }
    if q.contains(&name("poll_success_sync", true)) {
        return one(800.0);
    }
    if q.contains(&name("poll_success", true)) {
        return one(1000.0);
    }
    if q.contains(&name("approximate_backlog_count", false)) {
        return vec![(
            labels(&[("namespace", "orders-prod"), ("task_type", "Activity")]),
            1200.0,
        )];
    }
    if q.contains("container_cpu_usage_seconds_total") {
        assert!(q.contains(CPU_SELECTOR), "{q}");
        return vec![(labels(&[("container", "temporal-history")]), 3.9)];
    }
    if q == "aurora_load" {
        return one(35.0); // a percentage
    }
    vec![]
}

/// The minute-by-minute start rate of a load test from `--from`: a ramp, ten noisy minutes,
/// twenty steady ones at 125/s, then the ramp down. Split 80/20 over two namespaces.
fn range(params: &BTreeMap<String, String>) -> serde_json::Value {
    assert!(
        params["query"].contains("operation=~"),
        "{}",
        params["query"]
    );
    let start: f64 = params["start"].parse().unwrap();
    let end: f64 = params["end"].parse().unwrap();
    let step: f64 = params["step"].parse().unwrap();
    let rate = |minute: usize| match minute {
        0..10 => 12.5 * minute as f64,
        10..20 => 125.0 + if minute.is_multiple_of(2) { 5.0 } else { -5.0 },
        20..40 => 125.0,
        _ => 30.0,
    };
    let n = ((end - start) / step).round() as usize;
    let result: Vec<serde_json::Value> = [
        ("orders-prod", "StartWorkflowExecution", 0.8),
        ("payments", "ExecuteMultiOperation", 0.2),
    ]
    .iter()
    .map(|(ns, op, share)| {
        let values: Vec<serde_json::Value> = (0..=n)
            .map(|k| serde_json::json!([start + k as f64 * step, (rate(k) * share).to_string()]))
            .collect();
        serde_json::json!({ "metric": { "namespace": ns, "operation": op }, "values": values })
    })
    .collect();
    serde_json::json!({ "resultType": "matrix", "result": result })
}

/// Serve the stand-in on a free local port; returns its URL.
fn prometheus(e: Exporter) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut request = String::new();
            reader.read_line(&mut request).unwrap();
            loop {
                let mut header = String::new();
                if reader.read_line(&mut header).unwrap() <= 2 {
                    break;
                }
            }
            let target = request.split_whitespace().nth(1).unwrap_or_default();
            let (path, query) = target.split_once('?').unwrap_or((target, ""));
            let params: BTreeMap<String, String> = query
                .split('&')
                .filter_map(|kv| kv.split_once('='))
                .map(|(k, v)| (decode(k), decode(v)))
                .collect();
            let data = if path == "/api/v1/query_range" {
                range(&params)
            } else {
                assert_eq!(path, "/api/v1/query");
                assert!(params["time"].parse::<f64>().is_ok());
                let result: Vec<serde_json::Value> = answer(e, &params["query"])
                    .into_iter()
                    .map(|(labels, v)| {
                        let metric: serde_json::Map<String, serde_json::Value> = labels
                            .into_iter()
                            .map(|(k, v)| (k.to_string(), serde_json::Value::String(v)))
                            .collect();
                        serde_json::json!({ "metric": metric, "value": [1790893020.0, v.to_string()] })
                    })
                    .collect();
                serde_json::json!({ "resultType": "vector", "result": result })
            };
            let body = serde_json::json!({ "status": "success", "data": data }).to_string();
            let _ = write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\
                 Connection: close\r\n\r\n{body}",
                body.len()
            );
        }
    });
    url
}

#[test]
fn fetched_observations_read_back_whatever_the_exporter() {
    for e in [Exporter::Tally, Exporter::Otel] {
        let dir = std::env::temp_dir().join(format!("tempdes-fetch-{}-{e:?}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let out = dir.join("observed.yaml");
        fetch(&FetchArgs {
            conn: Conn {
                url: prometheus(e),
                headers: vec!["X-Scope-OrgID: test".into()],
                user: None,
                ca_file: None,
                insecure: false,
                timeout_s: 10.0,
            },
            end: "2026-10-01T22:17:00Z".into(),
            window: "15m".into(),
            output: out.clone(),
            baseline_end: Some("2026-10-01 21:55".into()),
            baseline_out: None,
            description: None,
            rename: vec!["orders-prod=orders".into()],
            cpu_selector: CPU_SELECTOR.into(),
            db_utilization: None,
            db_utilization_query: Some("aurora_load".into()),
        })
        .unwrap();
        let text = std::fs::read_to_string(&out).unwrap();
        // renamed, and zero rates of grouped series left out
        assert!(
            !text.contains("orders-prod") && !text.contains("temporal-system"),
            "{text}"
        );
        assert!(text.contains("window: 15m"), "{text}");
        let o = Observations::load(&out).unwrap();
        let start = [
            ("service_name", "frontend"),
            ("operation", "StartWorkflowExecution"),
        ];
        assert_eq!(o.rate("service_requests", &start), Some(125.0), "{e:?}");
        assert_eq!(
            o.rate("service_requests", &[("namespace", "orders")]),
            Some(1025.0),
            "{e:?}"
        );
        // milliseconds, whether the bounds were seconds or milliseconds
        let l = o
            .latency(
                "persistence_latency",
                &[("operation", "UpdateWorkflowExecution")],
            )
            .unwrap();
        for (q, ms) in [(0.5, 35.0), (0.9, 60.0), (0.99, 85.0)] {
            let us = l.quantile_us(q).unwrap();
            assert!((us - ms * 1000.0).abs() < 1.0, "{e:?} p{q}: {us}us");
        }
        assert!(
            o.latency(
                "persistence_latency",
                &[("operation", "GetWorkflowExecution")]
            )
            .is_none(),
            "{text}"
        );
        let api = o.latency("service_latency", &start).unwrap();
        assert!((api.quantile_us(0.99).unwrap() - 54_000.0).abs() < 1.0);
        // read as tempdes reads it (the loader drops `_total`)
        assert_eq!(cpu_cores(&o, Service::History), Some(3.9));
        assert_eq!(o.value_sum("db_utilization", &[]), Some(0.35));
        assert_eq!(o.value_sum("approximate_backlog_count", &[]), Some(1200.0));
        assert_eq!(
            o.rate("cache_miss", &[("cache_type", "mutablestate")]),
            Some(100.0)
        );
        assert_eq!(o.rate("poll_success_sync", &[]), Some(800.0));
        // the window before the run, beside it
        let baseline = Observations::load(&dir.join("baseline.yaml")).unwrap();
        assert_eq!(baseline.rate("service_requests", &start), Some(125.0));
        std::fs::remove_dir_all(&dir).unwrap();
    }
}

#[test]
fn scan_suggests_the_steadiest_window_near_the_peak() {
    let report = scan(&ScanArgs {
        conn: Conn {
            url: prometheus(Exporter::Tally),
            headers: vec![],
            user: None,
            ca_file: None,
            insecure: false,
            timeout_s: 10.0,
        },
        from: "2026-10-01T21:40:00Z".into(),
        to: "2026-10-01T22:40:00Z".into(),
        window: "15m".into(),
        step: "1m".into(),
        rename: vec!["orders-prod=orders".into()],
    })
    .unwrap();
    assert!(
        report.contains("orders/Start") && !report.contains("orders-prod"),
        "{report}"
    );
    // the first 15 minutes wholly inside the steady stretch (22:00 to 22:19) end at 22:14
    let best = report
        .lines()
        .find(|l| l.trim_start().starts_with("--end"))
        .unwrap_or_else(|| panic!("{report}"));
    assert!(
        best.contains("--end 2026-10-01T22:14:00Z") && best.contains("mean 125.0/s"),
        "{report}"
    );
}
