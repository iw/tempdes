//! `tempdes workload fetch` against a stand-in Temporal frontend HTTP API: it lists the window's
//! executions page by page, fetches histories page by page with the profile's credentials, drops
//! their payloads, and leaves a directory that `workload import` reads, start rates and all.

#![cfg(feature = "fetch")]

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use serde_json::{Value, json};
use tempdes::envconfig::{Choice, Loaded, load_with};
use tempdes::histories::fetch::{FetchArgs, fetch};
use tempdes::histories::{Options, import, read_manifest};

/// A payload as the frontend sends it with `noPayloadShorthand`: `{"card":"4111"}`.
const SECRET: &str = "eyJjYXJkIjoiNDExMSJ9";

/// A request the stand-in served.
#[derive(Clone, Debug)]
struct Request {
    path: String,
    params: BTreeMap<String, String>,
    /// lower-case names
    headers: BTreeMap<String, String>,
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

fn payloads() -> Value {
    json!({"payloads": [{"metadata": {"encoding": "anNvbi9wbGFpbg=="}, "data": SECRET}]})
}

/// `2026-10-07T01:mm:ss.mmmZ` for `ms` milliseconds after 01:00.
fn at(ms: u64) -> String {
    format!(
        "2026-10-07T01:{:02}:{:02}.{:03}Z",
        ms / 60_000,
        ms / 1000 % 60,
        ms % 1000
    )
}

/// One workflow's events: a workflow task, an activity, a workflow task, completion.
fn events(wtype: &str, activity: &str, start_ms: u64, parent: Option<(&str, &str)>) -> Vec<Value> {
    let mut started = json!({
        "workflowType": {"name": wtype},
        "taskQueue": {"name": "orders"},
        "input": payloads(),
        "header": {"fields": {"traceparent": payloads()["payloads"][0].clone()}},
    });
    if let Some((wid, rid)) = parent {
        started["parentWorkflowExecution"] = json!({"workflowId": wid, "runId": rid});
    }
    let kinds: [(&str, Value); 11] = [
        ("WORKFLOW_EXECUTION_STARTED", started),
        ("WORKFLOW_TASK_SCHEDULED", json!({})),
        (
            "WORKFLOW_TASK_STARTED",
            json!({"scheduledEventId": "2", "identity": "1@worker-a", "historySizeBytes": "700"}),
        ),
        (
            "WORKFLOW_TASK_COMPLETED",
            json!({"scheduledEventId": "2", "startedEventId": "3", "identity": "1@worker-a"}),
        ),
        (
            "ACTIVITY_TASK_SCHEDULED",
            json!({
                "activityType": {"name": activity},
                "taskQueue": {"name": "orders"},
                "input": payloads(),
                "startToCloseTimeout": "10s",
                "workflowTaskCompletedEventId": "4",
                "retryPolicy": {"initialInterval": "1s", "backoffCoefficient": 2, "maximumInterval": "100s"},
            }),
        ),
        (
            "ACTIVITY_TASK_STARTED",
            json!({
                "scheduledEventId": "5",
                "attempt": 2,
                "identity": "1@worker-a",
                "lastFailure": {
                    "message": "card 4111 declined",
                    "stackTrace": "at charge()",
                    "applicationFailureInfo": {"type": "Declined", "details": payloads()},
                },
            }),
        ),
        (
            "ACTIVITY_TASK_COMPLETED",
            json!({"scheduledEventId": "5", "startedEventId": "6", "result": payloads()}),
        ),
        ("WORKFLOW_TASK_SCHEDULED", json!({})),
        (
            "WORKFLOW_TASK_STARTED",
            json!({"scheduledEventId": "8", "identity": "1@worker-a", "historySizeBytes": "1500"}),
        ),
        (
            "WORKFLOW_TASK_COMPLETED",
            json!({"scheduledEventId": "8", "startedEventId": "9"}),
        ),
        (
            "WORKFLOW_EXECUTION_COMPLETED",
            json!({"result": payloads(), "workflowTaskCompletedEventId": "10"}),
        ),
    ];
    let offsets = [0, 5, 10, 20, 20, 1030, 1530, 1535, 1540, 1550, 1550];
    kinds
        .into_iter()
        .zip(offsets)
        .enumerate()
        .map(|(i, ((kind, attrs), off))| {
            let mut key: String = kind
                .split('_')
                .map(|w| w[..1].to_string() + &w[1..].to_lowercase())
                .collect();
            key = key[..1].to_lowercase() + &key[1..] + "EventAttributes";
            json!({
                "eventId": (i + 1).to_string(),
                "eventTime": at(start_ms + off),
                "eventType": format!("EVENT_TYPE_{kind}"),
                key: attrs,
            })
        })
        .collect()
}

/// The window's executions: (type, workflow ID, run ID, start in ms after 01:00, parent).
type Execution = (
    &'static str,
    &'static str,
    &'static str,
    u64,
    Option<(&'static str, &'static str)>,
);

const EXECUTIONS: [Execution; 6] = [
    ("Order", "o-1", "r-o1", 360_000, None),
    ("Order", "o-2", "r-o2", 480_000, None),
    ("Order", "o-3", "r-o3", 600_000, None),
    // the HTTP API can't take an ID with a '/'
    ("Order", "o/4", "r-o4", 700_000, None),
    ("Shipment", "s-1", "r-s1", 361_000, Some(("o-1", "r-o1"))),
    ("Shipment", "s-2", "r-s2", 481_000, Some(("o-2", "r-o2"))),
];

/// The stand-in's answer: status, content type and body.
fn answer(path: &str, params: &BTreeMap<String, String>) -> (u16, &'static str, Value) {
    let json = "application/json";
    if path == "/api/v1/namespaces/orders" {
        return (
            200,
            json,
            json!({
                "namespaceInfo": {"name": "orders", "state": "NAMESPACE_STATE_REGISTERED"},
                "config": {"workflowExecutionRetentionTtl": "2592000s"},
            }),
        );
    }
    if path == "/api/v1/namespaces/orders/workflows" {
        let query = &params["query"];
        if !query.contains("2026-10-07T01:20:00Z") {
            return (200, json, json!({}));
        }
        let info = |e: &Execution| {
            let mut v = json!({
                "execution": {"workflowId": e.1, "runId": e.2},
                "type": {"name": e.0},
                "startTime": at(e.3),
                "status": "WORKFLOW_EXECUTION_STATUS_COMPLETED",
                "historyLength": "11",
                "memo": {"fields": {"customer": payloads()["payloads"][0].clone()}},
            });
            if let Some((wid, rid)) = e.4 {
                v["parentExecution"] = json!({"workflowId": wid, "runId": rid});
            }
            v
        };
        let page: Vec<Value> = match params.get("nextPageToken").map(String::as_str) {
            None => EXECUTIONS[..3].iter().map(info).collect(),
            Some("cGFnZTI=") => EXECUTIONS[3..].iter().map(info).collect(),
            Some(t) => panic!("unknown list token {t}"),
        };
        let mut body = json!({"executions": page});
        if !params.contains_key("nextPageToken") {
            body["nextPageToken"] = json!("cGFnZTI=");
        }
        return (200, json, body);
    }
    let wid = path
        .strip_prefix("/api/v1/namespaces/orders/workflows/")
        .and_then(|r| r.strip_suffix("/history"))
        .unwrap_or_else(|| panic!("unexpected path {path}"));
    let e = EXECUTIONS
        .iter()
        .find(|e| e.1 == decode(wid) && e.2 == params["execution.runId"])
        .unwrap_or_else(|| panic!("no execution {wid}"));
    if e.2 == "r-o3" {
        return (
            404,
            json,
            json!({"code": 5, "message": "workflow execution not found"}),
        );
    }
    let content_type = if params.contains_key("noPayloadShorthand") {
        "application/json+no-payload-shorthand"
    } else {
        json
    };
    let all = events(
        e.0,
        if e.0 == "Order" { "Charge" } else { "Ship" },
        e.3,
        e.4,
    );
    // o-1's history comes in two pages
    let body = match (e.2, params.get("nextPageToken").map(String::as_str)) {
        ("r-o1", None) => json!({"history": {"events": all[..4]}, "nextPageToken": "bW9yZQ=="}),
        ("r-o1", Some("bW9yZQ==")) => json!({"history": {"events": all[4..]}}),
        (_, None) => json!({"history": {"events": all}, "archived": false}),
        (_, Some(t)) => panic!("unknown history token {t}"),
    };
    (200, content_type, body)
}

/// A stand-in frontend; its base URL and the requests it served.
fn frontend() -> (String, Arc<Mutex<Vec<Request>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let served = Arc::new(Mutex::new(Vec::new()));
    let log = served.clone();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            let mut headers = BTreeMap::new();
            loop {
                let mut h = String::new();
                if reader.read_line(&mut h).unwrap() <= 2 {
                    break;
                }
                if let Some((k, v)) = h.trim_end().split_once(':') {
                    headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_string());
                }
            }
            let target = line.split_whitespace().nth(1).unwrap_or_default();
            let (path, query) = target.split_once('?').unwrap_or((target, ""));
            let params: BTreeMap<String, String> = query
                .split('&')
                .filter(|kv| !kv.is_empty())
                .map(|kv| {
                    let (k, v) = kv.split_once('=').unwrap_or((kv, ""));
                    (decode(k), decode(v))
                })
                .collect();
            let (status, content_type, body) = answer(path, &params);
            log.lock().unwrap().push(Request {
                path: path.to_string(),
                params,
                headers,
            });
            let body = body.to_string();
            let _ = write!(
                stream,
                "HTTP/1.1 {status} X\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\n\
                 Connection: close\r\n\r\n{body}",
                body.len()
            );
        }
    });
    (url, served)
}

fn fresh_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("tempdes-wfetch-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

/// The `test` profile from a `temporal.toml`: an API key and a header. The environment is left
/// out, so the developer's own `TEMPORAL_*` variables don't reach the test.
fn profile(dir: &Path) -> Loaded {
    std::fs::create_dir_all(dir).unwrap();
    let file = dir.join("temporal.toml");
    std::fs::write(
        &file,
        "[profile.test]\naddress = \"127.0.0.1:7233\"\nnamespace = \"orders\"\napi_key = \"k123\"\n\n\
         [profile.test.grpc_meta]\ntenant_id = \"t1\"\n",
    )
    .unwrap();
    load_with(
        &Choice {
            file: Some(file),
            profile: Some("test".into()),
        },
        &BTreeMap::new(),
    )
    .unwrap()
}

fn args(url: &str, profile: Loaded, output: PathBuf) -> FetchArgs {
    FetchArgs {
        profile,
        // the profile's API key turns TLS on; the stand-in speaks plain HTTP
        http_url: Some(url.to_string()),
        end: "2026-10-07T01:20:00Z".into(),
        window: "15m".into(),
        types: Vec::new(),
        query: None,
        limit: 300,
        output,
        timeout_s: 10.0,
    }
}

#[test]
fn fetched_histories_import_without_their_payloads() {
    let (url, served) = frontend();
    let base = fresh_dir("import");
    let out = base.join("histories");
    let summary = fetch(&args(&url, profile(&base), out.clone())).unwrap();

    // every request carries the profile's API key and its header as gRPC metadata
    let reqs = served.lock().unwrap().clone();
    for r in &reqs {
        assert_eq!(r.headers["authorization"], "Bearer k123", "{}", r.path);
        assert_eq!(r.headers["grpc-metadata-tenant-id"], "t1", "{}", r.path);
    }
    assert_eq!(reqs[0].path, "/api/v1/namespaces/orders");
    // the window's executions, page by page
    let lists: Vec<&Request> = reqs
        .iter()
        .filter(|r| r.path == "/api/v1/namespaces/orders/workflows")
        .collect();
    assert_eq!(lists.len(), 2);
    assert_eq!(
        lists[0].params["query"],
        "StartTime >= '2026-10-07T01:05:00Z' AND StartTime < '2026-10-07T01:20:00Z'"
    );
    assert_eq!(lists[0].params["pageSize"], "1000");
    assert_eq!(lists[1].params["nextPageToken"], "cGFnZTI=");
    // histories page by page, with payloads in a shape that can be dropped
    let histories: Vec<&Request> = reqs
        .iter()
        .filter(|r| r.path.ends_with("/history"))
        .collect();
    assert!(
        histories
            .iter()
            .all(|r| r.params.contains_key("noPayloadShorthand"))
    );
    let o1: Vec<&&Request> = histories
        .iter()
        .filter(|r| r.params["execution.runId"] == "r-o1")
        .collect();
    assert_eq!(o1.len(), 2);
    assert_eq!(o1[1].params["nextPageToken"], "bW9yZQ==");
    // the ID with a '/' is never asked for
    assert!(
        !histories
            .iter()
            .any(|r| r.params["execution.runId"] == "r-o4")
    );

    // one file per history, with every page, and nothing of the payloads or failure messages
    let mut files: Vec<String> = std::fs::read_dir(&out)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    files.sort();
    assert_eq!(
        files,
        [
            "fetch.yaml",
            "r-o1.json",
            "r-o2.json",
            "r-s1.json",
            "r-s2.json"
        ]
    );
    for f in &files {
        let text = std::fs::read_to_string(out.join(f)).unwrap();
        for gone in [SECRET, "4111", "at charge()"] {
            assert!(!text.contains(gone), "{gone} in {f}");
        }
    }
    let o1: Value =
        serde_json::from_str(&std::fs::read_to_string(out.join("r-o1.json")).unwrap()).unwrap();
    assert_eq!(o1["events"].as_array().unwrap().len(), 11);
    assert_eq!(o1["workflowId"], "o-1");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = |p: &Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode(&out), 0o700);
        assert_eq!(mode(&out.join("r-o1.json")), 0o600);
        assert_eq!(mode(&out.join("fetch.yaml")), 0o600);
    }

    // the counts include what couldn't be fetched
    let m = read_manifest(&out.join("fetch.yaml")).unwrap();
    assert_eq!(m.namespace, "orders");
    assert_eq!(m.window_s, 900.0);
    let counts: Vec<(&str, u64, u64, u64)> = m
        .types
        .iter()
        .map(|t| (t.name.as_str(), t.started, t.children, t.fetched))
        .collect();
    assert_eq!(counts, [("Order", 4, 0, 2), ("Shipment", 2, 2, 2)]);
    assert!(summary.contains("1 skipped"), "{summary}");
    assert!(
        summary.contains("1 was deleted between listing and fetching"),
        "{summary}"
    );

    // import takes the start rate from the counts, not from the few start times it sees
    let program = import(std::slice::from_ref(&out), &Options::default()).unwrap();
    assert!(
        program
            .summary
            .contains("from the 4 starts `workload fetch` counted over 15.0m"),
        "{}",
        program.summary
    );

    // run again, it fetches only what is missing; with another query, it won't mix them
    let before = served.lock().unwrap().len();
    let again = fetch(&args(&url, profile(&base), out.clone())).unwrap();
    assert!(again.contains("(4 already there)"), "{again}");
    let fetched_again: Vec<String> = served.lock().unwrap()[before..]
        .iter()
        .filter(|r| r.path.ends_with("/history"))
        .map(|r| r.params["execution.runId"].clone())
        .collect();
    assert_eq!(fetched_again, ["r-o3"]);
    let mut other = args(&url, profile(&base), out.clone());
    other.types = vec!["Order".into()];
    let err = fetch(&other).unwrap_err().to_string();
    assert!(err.contains("holds histories fetched from orders"), "{err}");
}

#[test]
fn an_empty_window_says_how_long_the_namespace_keeps_executions() {
    let (url, _) = frontend();
    let base = fresh_dir("empty");
    let mut a = args(&url, profile(&base), base.join("histories"));
    a.end = "2026-10-07T03:00:00Z".into();
    let err = fetch(&a).unwrap_err().to_string();
    assert!(err.contains("no executions in orders matched"), "{err}");
    assert!(err.contains("keeps closed executions for 30 days"), "{err}");
    // nothing was written
    assert!(!base.join("histories").exists());
}
