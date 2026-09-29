//! Server-rendered views of the live page: the document, the controls, the cluster topology,
//! the pressure chain, the charts, the live hotspots and the detail tables.
//!
//! Every number that changes between frames is rendered from the current frame *and* tagged
//! with a `data-bind` (or `data-bar`, `data-heat`, ...) attribute naming its path in the frame
//! JSON, so the client script updates it in place from the event stream without a second
//! template. Values are resolved through the frame's JSON form on both sides, so the server
//! and the browser agree on what a path means. Structural changes (pods added by scaling, a
//! new hotspot analysis) re-render the affected fragment here.

use std::sync::Arc;

use serde_json::Value;
use topcoat::{
    Result,
    view::{View, component, view},
};

use crate::model::params::Params;
use crate::report::{Hotspot, RunResult, Severity};
use crate::util::units::{fmt_pct, fmt_rate, fmt_us};

use super::frame::{EventNote, Frame, Phase};

// --- formatting shared with the client script --------------------------------------------------

/// Format a frame value the way the client script does (`F` in `app.js`).
pub fn format_value(v: Option<&Value>, f: &str) -> String {
    if f == "text" || f == "key" {
        return v
            .and_then(Value::as_str)
            .map(|s| {
                // `key`: a dynamic config key without its service prefix and with `namespace`
                // shortened to `ns`, for the narrow labels of the cluster figure
                if f == "key" {
                    s.split_once('.')
                        .map_or(s, |(_, rest)| rest)
                        .replace("namespace", "ns")
                } else {
                    s.to_string()
                }
            })
            .unwrap_or_else(|| "–".into());
    }
    let Some(x) = v.and_then(Value::as_f64) else {
        return "–".into();
    };
    match f {
        "pct" => fmt_pct(x),
        "rate" => fmt_rate(x),
        "ms" => fmt_us(x * 1e3),
        "int" => format!("{x:.0}"),
        "secs" => format!("{x:.1} s"),
        "mult" => format!("×{x:.2}"),
        "num" => format!("{x:.1}"),
        "key" => format!("{x}"),
        "speed" => {
            if x <= 0.0 {
                "max".into()
            } else {
                format!("{x:.1}×")
            }
        }
        _ => format!("{x}"),
    }
}

/// Resolve a dotted path (`services.1.limit.util`) in the frame's JSON.
pub fn lookup<'a>(v: &'a Value, path: &str) -> Option<&'a Value> {
    let mut cur = v;
    for seg in path.split('.') {
        cur = match cur {
            Value::Object(m) => m.get(seg)?,
            Value::Array(a) => a.get(seg.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }
    if cur.is_null() { None } else { Some(cur) }
}

pub fn fmt_at(v: &Value, path: &str, f: &str) -> String {
    format_value(lookup(v, path), f)
}

fn num_at(v: &Value, path: &str) -> f64 {
    lookup(v, path).and_then(Value::as_f64).unwrap_or(0.0)
}

/// Utilisation → heat level 0..=5 (the same thresholds as the report and the client script).
pub fn heat(u: f64) -> u8 {
    match u {
        x if x < 0.05 => 0,
        x if x < 0.30 => 1,
        x if x < 0.55 => 2,
        x if x < 0.75 => 3,
        x if x < 0.90 => 4,
        _ => 5,
    }
}

fn n(v: f64) -> String {
    if (v - v.round()).abs() < 1e-6 {
        format!("{v:.0}")
    } else {
        format!("{v:.1}")
    }
}

fn service_index(svc: &str) -> usize {
    match svc {
        "frontend" => 0,
        "history" => 1,
        "matching" => 2,
        _ => 3,
    }
}

// --- small bound elements ------------------------------------------------------------------------

/// A value bound to a frame path: rendered now, updated by the client script.
#[component]
pub async fn bound(
    v: &Value,
    path: &str,
    f: &str,
    #[default] class: Option<&str>,
) -> Result<impl View> {
    Ok(view! {
        <span class=(class) data-bind=(format!("{path}|{f}"))>(fmt_at(v, path, f))</span>
    })
}

/// A horizontal utilisation bar bound to a frame path (0..1).
#[component]
pub async fn bar(v: &Value, path: &str) -> Result<impl View> {
    let u = num_at(v, path).clamp(0.0, 1.0);
    Ok(view! {
        <span class="track">
            <span
                class="fill"
                data-bar=(path)
                data-heat=(path)
                data-level=(heat(u).to_string())
                style=(format!("width:{:.1}%", u * 100.0))
            ></span>
        </span>
    })
}

// --- document ----------------------------------------------------------------------------------------

/// Everything the page needs, owned so the rendered view outlives the request handler.
pub struct PageData {
    pub frame: Arc<Frame>,
    /// the frame as JSON: what `data-bind` paths resolve against
    pub json: Value,
    pub params: Arc<Params>,
    pub analysis: Option<Arc<RunResult>>,
    pub events: Vec<EventNote>,
    pub temporal_version: String,
    /// dynamic config keys the simulator applies at runtime, with their current values
    pub dc_keys: Vec<(String, f64)>,
}

#[component]
pub async fn document(d: &PageData) -> Result<impl View> {
    let analysis = d.analysis.as_deref();
    Ok(view! {
        <!DOCTYPE html>
        <html lang="en">
            <head>
                <meta charset="utf-8">
                <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
                <title>(format!("{} · tempdes live", d.params.name))</title>
                <link rel="icon" href="data:,">
                topcoat::font::link(font: super::app::SANS)
                topcoat::font::link(font: super::app::MONO)
                <link rel="stylesheet" href="/app.css">
                <script src="/app.js" defer=""></script>
            </head>
            <body>
                <div class="page">
                    masthead(d: d)
                    controls(d: d)
                    <section class="topo-section" aria-label="cluster topology">
                        <div class="section-head">
                            <h2>"Cluster"</h2>
                            <span class="muted">"pod CPU as bars · limiter headroom · flows per second, thicker with traffic"</span>
                        </div>
                        <div id="topology" data-sig=(topology_signature(&d.frame))>
                            topology(frame: &d.frame, v: &d.json)
                        </div>
                    </section>
                    chain(v: &d.json)
                    charts(v: &d.json)
                    <section aria-label="hotspots">
                        <div class="section-head">
                            <h2>"Hotspots"</h2>
                            <span class="muted" id="analysis-note">
                                "ranked over the measurement window so far · refreshed every 5 s of simulated time"
                            </span>
                        </div>
                        <div id="hotspots">
                            hotspots(analysis: analysis, frame: &d.frame)
                        </div>
                    </section>
                    <section aria-label="detail">
                        <div class="section-head">
                            <h2>"Detail"</h2>
                            <span class="muted">"history shards by owner, then tables since warm-up"</span>
                        </div>
                        <div class="panel shards">
                            <div class="legend">
                                <span>"shard IO now: idle"</span>
                                <span class="ramp">
                                    <i data-level="0"></i><i data-level="1"></i><i data-level="2"></i>
                                    <i data-level="3"></i><i data-level="4"></i><i data-level="5"></i>
                                </span>
                                <span>"saturated · hottest shard "</span>
                                bound(v: &d.json, path: "history.hottest_shard", f: "int", class: Some("mono"))
                                <span>" on "</span>
                                bound(v: &d.json, path: "history.hottest_shard_owner", f: "text", class: Some("mono"))
                            </div>
                            <canvas id="shards" width="1000" height="60" aria-label="history shards coloured by IO utilisation"></canvas>
                        </div>
                        <div id="detail">
                            detail(analysis: analysis)
                        </div>
                    </section>
                    <section aria-label="events">
                        <div class="section-head">
                            <h2>"Timeline"</h2>
                            <span class="muted">"scenario events and live changes, newest first"</span>
                        </div>
                        <ul class="events" id="events">
                            for e in d.events.iter().rev().take(40) {
                                <li><span class="mono muted">(format!("{:.1}s", e.t))</span>" "(e.text.clone())</li>
                            }
                        </ul>
                    </section>
                    <footer>
                        "tempdes · Temporal "(d.temporal_version.clone())" model · "
                        (format!("{} history shards · {} · client LB {} · seed {}", d.params.num_shards, format!("{:?}", d.params.store).to_ascii_lowercase(), d.params.client_lb.as_str(), d.params.seed))
                        " · rendered by Topcoat; live values over server-sent events"
                    </footer>
                </div>
            </body>
        </html>
    })
}

fn topology_signature(f: &Frame) -> String {
    f.pods
        .iter()
        .filter(|p| p.alive)
        .map(|p| p.name.as_str())
        .collect::<Vec<_>>()
        .join(",")
}

// --- masthead & controls --------------------------------------------------------------------------------

#[component]
async fn masthead(d: &PageData) -> Result<impl View> {
    let f = &*d.frame;
    let phase = match f.phase {
        Phase::Warmup => "warming up",
        Phase::Measuring => "measuring",
    };
    Ok(view! {
        <header class="mast">
            <div class="mast-title">
                <span class="eyebrow">"Temporal "(d.temporal_version.clone())" · live simulation"</span>
                <h1>(d.params.name.clone())</h1>
                <p class="muted" id="headline" data-bind="analysis.headline|text" data-empty="The first hotspot analysis runs a few seconds after warm-up.">
                    (if f.analysis.headline.is_empty() { "The first hotspot analysis runs a few seconds after warm-up.".to_string() } else { f.analysis.headline.clone() })
                </p>
            </div>
            <div class="mast-status" role="status">
                <div class="clock">
                    <span class="mono big" data-bind="t|secs">(format!("{:.1} s", f.t))</span>
                    <span class="phase" id="phase" data-phase=(phase)>(phase)</span>
                </div>
                <div class="status-line">
                    <span class="muted">"speed "</span>
                    <span class="mono" data-bind="actual_speed|speed">(format_value(Some(&Value::from(f.actual_speed)), "speed"))</span>
                    <span class="muted">" · load "</span>
                    <span class="mono" data-bind="load_scale|mult">(format!("×{:.2}", f.load_scale))</span>
                    <span class="muted">" · run "</span>
                    <span class="mono" data-bind="run|int">(f.run.to_string())</span>
                </div>
                <div class="tallies">
                    <span class="tally c" data-show="analysis.critical" hidden=(f.analysis.critical == 0)>
                        <b data-bind="analysis.critical|int">(f.analysis.critical.to_string())</b>" critical"
                    </span>
                    <span class="tally w" data-show="analysis.warning" hidden=(f.analysis.warning == 0)>
                        <b data-bind="analysis.warning|int">(f.analysis.warning.to_string())</b>" warning"
                    </span>
                </div>
            </div>
        </header>
    })
}

#[component]
async fn controls(d: &PageData) -> Result<impl View> {
    let f = &*d.frame;
    // the presets, plus the speed given on the command line when it is not one of them
    let mut speeds: Vec<(f64, String)> = [0.5, 1.0, 2.0, 5.0, 10.0]
        .into_iter()
        .map(|x| (x, format!("{}×", n(x))))
        .collect();
    if f.speed > 0.0 && !speeds.iter().any(|(x, _)| (*x - f.speed).abs() < 1e-9) {
        speeds.push((f.speed, format!("{}×", n(f.speed))));
        speeds.sort_by(|a, b| a.0.total_cmp(&b.0));
    }
    speeds.push((0.0, "max".to_string()));
    Ok(view! {
        <section class="controls" aria-label="controls">
            <div class="ctl">
                <span class="ctl-label">"Run"</span>
                <div class="ctl-row">
                    <button type="button" class="btn" id="btn-pause" data-paused=(if f.paused { "true" } else { "false" })>
                        (if f.paused { "Resume" } else { "Pause" })
                    </button>
                    <label class="sel">
                        <span class="muted">"speed"</span>
                        <select id="speed">
                            for (s, label) in speeds {
                                <option value=(n(s)) selected=((s - f.speed).abs() < 1e-9)>(label)</option>
                            }
                        </select>
                    </label>
                    <button type="button" class="btn quiet" id="btn-restart" title="Replay from the start with the same seed">"Restart"</button>
                    <button type="button" class="btn quiet" id="btn-reseed" title="Start over with a new random seed">"New seed"</button>
                </div>
            </div>
            <div class="ctl">
                <span class="ctl-label">"Load"</span>
                <div class="ctl-row">
                    <input type="range" id="load" min="0" max="4" step="0.05" value=(format!("{:.2}", f.load_scale)) aria-label="load multiplier">
                    <output class="mono" id="load-out" for="load">(format!("×{:.2}", f.load_scale))</output>
                    <span class="muted small">"× the scenario's start and signal rates"</span>
                </div>
            </div>
            <div class="ctl">
                <span class="ctl-label">"Replicas"</span>
                <div class="ctl-row">
                    for (i, svc) in ["frontend", "history", "matching", "worker"].into_iter().enumerate() {
                        <span class="stepper">
                            <span class="muted">(svc)</span>
                            <button type="button" class="btn tiny" data-scale=(svc) data-delta="-1" aria-label=(format!("one fewer {svc} pod"))>"−"</button>
                            <span class="mono" data-bind=(format!("services.{i}.replicas|int"))>(f.services[i].replicas.to_string())</span>
                            <button type="button" class="btn tiny" data-scale=(svc) data-delta="1" aria-label=(format!("one more {svc} pod"))>"+"</button>
                        </span>
                    }
                </div>
            </div>
            <div class="ctl">
                <span class="ctl-label">"Dynamic config"</span>
                <div class="ctl-row">
                    <select id="dc-key" aria-label="dynamic config key">
                        for (key, current) in &d.dc_keys {
                            <option value=(key.clone()) data-current=(n(*current))>(key.clone())</option>
                        }
                    </select>
                    <input type="number" id="dc-value" class="mono" min="0" step="any" value=(d.dc_keys.first().map(|k| n(k.1)).unwrap_or_default()) aria-label="value">
                    <button type="button" class="btn" id="dc-apply">"Apply"</button>
                    <span class="muted small">"keys Temporal reads at runtime; the change applies to the live pods"</span>
                </div>
            </div>
        </section>
    })
}

// --- topology --------------------------------------------------------------------------------------------

#[derive(Clone, Copy)]
struct Rect {
    x: f64,
    y: f64,
    w: f64,
    h: f64,
}

const CLIENTS: Rect = Rect {
    x: 20.0,
    y: 40.0,
    w: 170.0,
    h: 118.0,
};
const WORKERS: Rect = Rect {
    x: 20.0,
    y: 186.0,
    w: 170.0,
    h: 182.0,
};
const WORKERSVC: Rect = Rect {
    x: 20.0,
    y: 396.0,
    w: 170.0,
    h: 140.0,
};
const FRONTEND: Rect = Rect {
    x: 300.0,
    y: 150.0,
    w: 190.0,
    h: 200.0,
};
const HISTORY: Rect = Rect {
    x: 580.0,
    y: 40.0,
    w: 190.0,
    h: 236.0,
};
const MATCHING: Rect = Rect {
    x: 580.0,
    y: 316.0,
    w: 190.0,
    h: 220.0,
};
const DB: Rect = Rect {
    x: 850.0,
    y: 150.0,
    w: 150.0,
    h: 230.0,
};

/// The cluster diagram: SDK processes, the four Temporal services and persistence, with the
/// flows between them.
#[component]
pub async fn topology(frame: &Frame, v: &Value) -> Result<impl View> {
    Ok(view! {
        <svg class="topo" viewBox="0 0 1010 550" role="img" aria-label="cluster topology">
            <defs>
                // a fixed-size arrowhead: markers must not grow with the flow's stroke width
                <marker id="arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="8" markerHeight="8" markerUnits="userSpaceOnUse" orient="auto-start-reverse">
                    <path d="M 0 0 L 8 4 L 0 8 z" class="arrowhead"></path>
                </marker>
            </defs>
            // flows first so nodes sit on top
            edge(i: 0, from: (CLIENTS.x + CLIENTS.w, CLIENTS.y + 70.0), to: (FRONTEND.x, FRONTEND.y + 40.0), v: v, side: -1.0)
            edge(i: 1, from: (WORKERS.x + WORKERS.w, WORKERS.y + 90.0), to: (FRONTEND.x, FRONTEND.y + 105.0), v: v, side: 1.0)
            edge(i: 9, from: (WORKERSVC.x + WORKERSVC.w, WORKERSVC.y + 60.0), to: (FRONTEND.x, FRONTEND.y + 170.0), v: v, side: 1.0)
            edge(i: 2, from: (FRONTEND.x + FRONTEND.w, FRONTEND.y + 50.0), to: (HISTORY.x, HISTORY.y + 120.0), v: v, side: -1.0)
            edge(i: 3, from: (FRONTEND.x + FRONTEND.w, FRONTEND.y + 150.0), to: (MATCHING.x, MATCHING.y + 90.0), v: v, side: 1.0)
            edge(i: 4, from: (HISTORY.x + 70.0, HISTORY.y + HISTORY.h), to: (MATCHING.x + 70.0, MATCHING.y), v: v, side: -1.0)
            edge(i: 5, from: (MATCHING.x + 130.0, MATCHING.y), to: (HISTORY.x + 130.0, HISTORY.y + HISTORY.h), v: v, side: 1.0)
            edge(i: 6, from: (HISTORY.x + HISTORY.w, HISTORY.y + 150.0), to: (DB.x, DB.y + 70.0), v: v, side: -1.0)
            edge(i: 7, from: (MATCHING.x + MATCHING.w, MATCHING.y + 110.0), to: (DB.x, DB.y + 175.0), v: v, side: 1.0)

            clients_node(v: v)
            workers_node(v: v)
            service_node(r: WORKERSVC, svc: "worker", title: "Worker service", frame: frame, v: v)
            service_node(r: FRONTEND, svc: "frontend", title: "Frontend", frame: frame, v: v)
            service_node(r: HISTORY, svc: "history", title: "History", frame: frame, v: v)
            service_node(r: MATCHING, svc: "matching", title: "Matching", frame: frame, v: v)
            db_node(v: v, frame: frame)
        </svg>
    })
}

/// A flow between two nodes, labelled with its rate and, when the limiter on it rejects, the
/// rejection rate.
#[component]
async fn edge(
    i: usize,
    from: (f64, f64),
    to: (f64, f64),
    v: &Value,
    side: f64,
) -> Result<impl View> {
    let (x1, y1) = from;
    let (x2, y2) = to;
    let vertical = (x2 - x1).abs() < 1.0;
    let d = if vertical {
        format!("M {} {} L {} {}", n(x1), n(y1), n(x2), n(y2))
    } else {
        let dx = (x2 - x1) / 2.0;
        format!(
            "M {} {} C {} {}, {} {}, {} {}",
            n(x1),
            n(y1),
            n(x1 + dx),
            n(y1),
            n(x2 - dx),
            n(y2),
            n(x2),
            n(y2)
        )
    };
    let (mx, my) = ((x1 + x2) / 2.0, (y1 + y2) / 2.0);
    // keep a label of up to 16 mono characters inside the gap between the two nodes
    let half = 16.0 * 6.6 / 2.0;
    let (lo, hi) = (x1.min(x2) + half + 4.0, x1.max(x2) - half - 4.0);
    let cx = if lo <= hi { mx.clamp(lo, hi) } else { mx };
    let (lx, ly) = if vertical {
        (mx + 8.0 * side, my + 4.0)
    } else if side > 0.0 {
        (cx, my + 17.0)
    } else {
        (cx, my - 9.0)
    };
    let per_s = format!("flows.{i}.per_s");
    let rej = format!("flows.{i}.rejected_per_s");
    let rejected = num_at(v, &rej);
    Ok(view! {
        <g class="flow">
            <path d=(d) class="flowline" marker-end="url(#arrow)" data-width=(per_s.clone()) stroke-width="1.2"></path>
            <text
                x=(n(lx))
                y=(n(ly))
                class="flow-label"
                text-anchor=(if vertical { if side > 0.0 { "start" } else { "end" } } else { "middle" })
                data-bind=(format!("{per_s}|rate"))
            >
                (fmt_at(v, &per_s, "rate"))
            </text>
            <text
                x=(n(lx))
                y=(n(ly + 12.0))
                class="flow-reject"
                text-anchor=(if vertical { if side > 0.0 { "start" } else { "end" } } else { "middle" })
                data-show=(rej.clone())
                visibility=(if rejected > 0.0 { "visible" } else { "hidden" })
            >
                "rejected "<tspan data-bind=(format!("{rej}|rate"))>(fmt_at(v, &rej, "rate"))</tspan>
            </text>
        </g>
    })
}

/// One line of "label value" inside a node.
#[component]
async fn nline(
    x: f64,
    y: f64,
    w: f64,
    label: &str,
    path: &str,
    f: &str,
    v: &Value,
    #[default] alarm: bool,
) -> Result<impl View> {
    let val = num_at(v, path);
    Ok(view! {
        <text x=(n(x)) y=(n(y)) class="nlabel">(label.to_string())</text>
        <text
            x=(n(x + w))
            y=(n(y))
            text-anchor="end"
            class=(if alarm { "nvalue alarm" } else { "nvalue" })
            data-bind=(format!("{path}|{f}"))
            data-alarm=(if alarm { Some(path.to_string()) } else { None })
            data-on=(if alarm && val > 0.0 { "true" } else { "false" })
        >
            (fmt_at(v, path, f))
        </text>
    })
}

/// A thin horizontal bar inside a node.
#[component]
async fn nbar(x: f64, y: f64, w: f64, path: &str, v: &Value) -> Result<impl View> {
    let u = num_at(v, path).clamp(0.0, 1.0);
    Ok(view! {
        <rect x=(n(x)) y=(n(y)) width=(n(w)) height="4" class="ntrack" rx="1"></rect>
        <rect
            x=(n(x))
            y=(n(y))
            width=(n(w * u))
            height="4"
            rx="1"
            class="nfill"
            data-svgw=(path)
            data-w=(n(w))
            data-heat=(path)
            data-level=(heat(u).to_string())
        ></rect>
    })
}

/// Lines shown under a service's request rate, before its pod bars.
fn service_lines(svc: &str) -> Vec<(&'static str, &'static str, &'static str)> {
    match svc {
        "history" => vec![
            ("shard IO max", "history.shard_io_max", "pct"),
            ("lock wait p99", "history.lock_wait.p99_ms", "ms"),
        ],
        "matching" => vec![
            ("backlog", "matching.backlog", "int"),
            ("sync match", "matching.sync_match_ratio", "pct"),
        ],
        "frontend" => vec![("persistence", "services.0.persistence_per_s", "rate")],
        _ => Vec::new(),
    }
}

#[component]
async fn service_node(
    r: Rect,
    svc: &'static str,
    title: &str,
    frame: &Frame,
    v: &Value,
) -> Result<impl View> {
    let si = service_index(svc);
    let pods: Vec<(usize, f64)> = frame
        .pods
        .iter()
        .enumerate()
        .filter(|(_, p)| p.alive && p.service == svc)
        .map(|(i, p)| (i, p.cpu))
        .collect();
    let inner = r.w - 24.0;
    let lines = service_lines(svc);
    // rows: title, requests, extra lines, then the bars end 62 px above the bottom, leaving
    // room for the CPU label, the limiter line and the rejection note
    let top = r.y + 52.0 + 18.0 * lines.len() as f64;
    let base = r.y + r.h - 62.0;
    let bar_h = (base - top).clamp(16.0, 56.0);
    let bw = ((inner + 3.0) / pods.len().max(1) as f64 - 3.0).clamp(2.0, 12.0);
    let limit_name = frame.services[si]
        .limit
        .as_ref()
        .map(|l| l.name.clone())
        .unwrap_or_default();
    let rej_path = format!("services.{si}.rejected_per_s");
    let req_path = format!("services.{si}.req_per_s");
    let limit_path = format!("services.{si}.limit.util");
    let rejected = frame.services[si].rejected_per_s;
    Ok(view! {
        <g class="node">
            <rect x=(n(r.x)) y=(n(r.y)) width=(n(r.w)) height=(n(r.h)) rx="4" class="nbox"></rect>
            <text x=(n(r.x + 12.0)) y=(n(r.y + 22.0)) class="ntitle">(title.to_string())</text>
            <text x=(n(r.x + r.w - 12.0)) y=(n(r.y + 22.0)) text-anchor="end" class="nsub">
                <tspan data-bind=(format!("services.{si}.replicas|int"))>(frame.services[si].replicas.to_string())</tspan>
                (if frame.services[si].replicas == 1 { " pod" } else { " pods" })
            </text>
            nline(x: r.x + 12.0, y: r.y + 40.0, w: inner, label: "requests", path: &req_path, f: "rate", v: v)
            for (k, (label, path, f)) in lines.iter().enumerate() {
                nline(x: r.x + 12.0, y: r.y + 58.0 + 18.0 * k as f64, w: inner, label: label, path: path, f: f, v: v)
            }
            // one bar per pod: CPU utilisation over the last interval
            <text x=(n(r.x + 12.0)) y=(n(base + 12.0)) class="nlabel">"CPU per pod"</text>
            <text x=(n(r.x + r.w - 12.0)) y=(n(base + 12.0)) text-anchor="end" class="nvalue" data-bind=(format!("services.{si}.cpu_max|pct"))>
                (fmt_pct(frame.services[si].cpu_max))
            </text>
            <line x1=(n(r.x + 12.0)) x2=(n(r.x + 12.0 + inner)) y1=(n(base + 0.5)) y2=(n(base + 0.5)) class="nbase"></line>
            for (k, (i, cpu)) in pods.iter().enumerate() {
                <rect
                    x=(n(r.x + 12.0 + k as f64 * (bw + 3.0)))
                    y=(n(base - bar_h * cpu))
                    width=(n(bw))
                    height=(n(bar_h * cpu))
                    class="pod"
                    data-vbar=(format!("pods.{i}.cpu"))
                    data-h=(n(bar_h))
                    data-base=(n(base))
                    data-heat=(format!("pods.{i}.cpu"))
                    data-level=(heat(*cpu).to_string())
                >
                    <title>(frame.pods[*i].name.clone())</title>
                </rect>
            }
            // the limiter closest to its limit on the busiest pod
            <text x=(n(r.x + 12.0)) y=(n(r.y + r.h - 34.0)) class="nlabel mono" data-bind=(format!("services.{si}.limit.name|key")) data-empty="no limiter">
                (if limit_name.is_empty() { "no limiter".to_string() } else { format_value(Some(&Value::from(limit_name.as_str())), "key") })
            </text>
            <text x=(n(r.x + r.w - 12.0)) y=(n(r.y + r.h - 34.0)) text-anchor="end" class="nvalue" data-bind=(format!("{limit_path}|pct"))>
                (fmt_at(v, &limit_path, "pct"))
            </text>
            nbar(x: r.x + 12.0, y: r.y + r.h - 28.0, w: inner, path: &limit_path, v: v)
            <text
                x=(n(r.x + 12.0))
                y=(n(r.y + r.h - 10.0))
                class="nreject"
                data-show=(rej_path.clone())
                visibility=(if rejected > 0.0 { "visible" } else { "hidden" })
            >
                "rejecting "<tspan data-bind=(format!("{rej_path}|rate"))>(fmt_rate(rejected))</tspan>
            </text>
        </g>
    })
}

#[component]
async fn clients_node(v: &Value) -> Result<impl View> {
    let r = CLIENTS;
    let inner = r.w - 24.0;
    Ok(view! {
        <g class="node">
            <rect x=(n(r.x)) y=(n(r.y)) width=(n(r.w)) height=(n(r.h)) rx="4" class="nbox"></rect>
            <text x=(n(r.x + 12.0)) y=(n(r.y + 22.0)) class="ntitle">"SDK clients"</text>
            nline(x: r.x + 12.0, y: r.y + 44.0, w: inner, label: "offered", path: "workload.offered_per_s", f: "rate", v: v)
            nline(x: r.x + 12.0, y: r.y + 62.0, w: inner, label: "started", path: "workload.started_per_s", f: "rate", v: v)
            nline(x: r.x + 12.0, y: r.y + 80.0, w: inner, label: "start p99", path: "latency.start.p99_ms", f: "ms", v: v)
            nline(x: r.x + 12.0, y: r.y + 98.0, w: inner, label: "errors", path: "latency.api_errors_per_s", f: "rate", v: v, alarm: true)
        </g>
    })
}

#[component]
async fn workers_node(v: &Value) -> Result<impl View> {
    let r = WORKERS;
    let inner = r.w - 24.0;
    Ok(view! {
        <g class="node">
            <rect x=(n(r.x)) y=(n(r.y)) width=(n(r.w)) height=(n(r.h)) rx="4" class="nbox"></rect>
            <text x=(n(r.x + 12.0)) y=(n(r.y + 22.0)) class="ntitle">"SDK workers"</text>
            nline(x: r.x + 12.0, y: r.y + 40.0, w: inner, label: "processes", path: "workers.processes", f: "int", v: v)
            nline(x: r.x + 12.0, y: r.y + 60.0, w: inner, label: "workflow slots", path: "workers.wft_slot_util", f: "pct", v: v)
            nbar(x: r.x + 12.0, y: r.y + 66.0, w: inner, path: "workers.wft_slot_util", v: v)
            nline(x: r.x + 12.0, y: r.y + 88.0, w: inner, label: "activity slots", path: "workers.act_slot_util", f: "pct", v: v)
            nbar(x: r.x + 12.0, y: r.y + 94.0, w: inner, path: "workers.act_slot_util", v: v)
            nline(x: r.x + 12.0, y: r.y + 116.0, w: inner, label: "polls waiting", path: "workers.outstanding_polls", f: "int", v: v)
            nline(x: r.x + 12.0, y: r.y + 134.0, w: inner, label: "WFT s2s p99", path: "latency.wft_schedule_to_start.p99_ms", f: "ms", v: v)
            nline(x: r.x + 12.0, y: r.y + 152.0, w: inner, label: "sticky hits", path: "workers.sticky_hit_ratio", f: "pct", v: v)
            nline(x: r.x + 12.0, y: r.y + 170.0, w: inner, label: "WFT timeouts", path: "workers.wft_timeouts_per_s", f: "rate", v: v, alarm: true)
        </g>
    })
}

#[component]
async fn db_node(v: &Value, frame: &Frame) -> Result<impl View> {
    let r = DB;
    let inner = r.w - 24.0;
    let rejected = frame.persistence.rejected_per_s;
    Ok(view! {
        <g class="node">
            <rect x=(n(r.x)) y=(n(r.y)) width=(n(r.w)) height=(n(r.h)) rx="4" class="nbox"></rect>
            <text x=(n(r.x + 12.0)) y=(n(r.y + 22.0)) class="ntitle">"Persistence"</text>
            nline(x: r.x + 12.0, y: r.y + 44.0, w: inner, label: "database busy", path: "persistence.util", f: "pct", v: v)
            nbar(x: r.x + 12.0, y: r.y + 50.0, w: inner, path: "persistence.util", v: v)
            nline(x: r.x + 12.0, y: r.y + 74.0, w: inner, label: "operations", path: "persistence.ops_per_s", f: "rate", v: v)
            nline(x: r.x + 12.0, y: r.y + 92.0, w: inner, label: "queue wait p99", path: "persistence.queue_wait.p99_ms", f: "ms", v: v)
            nline(x: r.x + 12.0, y: r.y + 116.0, w: inner, label: "history pool", path: "persistence.pool_util.history", f: "pct", v: v)
            nbar(x: r.x + 12.0, y: r.y + 122.0, w: inner, path: "persistence.pool_util.history", v: v)
            nline(x: r.x + 12.0, y: r.y + 144.0, w: inner, label: "matching pool", path: "persistence.pool_util.matching", f: "pct", v: v)
            nbar(x: r.x + 12.0, y: r.y + 150.0, w: inner, path: "persistence.pool_util.matching", v: v)
            nline(x: r.x + 12.0, y: r.y + 172.0, w: inner, label: "visibility busy", path: "persistence.visibility_util", f: "pct", v: v)
            <text
                x=(n(r.x + 12.0))
                y=(n(r.y + r.h - 10.0))
                class="nreject"
                data-show="persistence.rejected_per_s"
                visibility=(if rejected > 0.0 { "visible" } else { "hidden" })
            >
                "QPS limit rejecting "<tspan data-bind="persistence.rejected_per_s|rate">(fmt_rate(rejected))</tspan>
            </text>
        </g>
    })
}

// --- pressure chain --------------------------------------------------------------------------------------

struct Line {
    label: &'static str,
    path: &'static str,
    f: &'static str,
    alarm: bool,
}

const fn l(label: &'static str, path: &'static str, f: &'static str) -> Line {
    Line {
        label,
        path,
        f,
        alarm: false,
    }
}

const fn a(label: &'static str, path: &'static str, f: &'static str) -> Line {
    Line {
        label,
        path,
        f,
        alarm: true,
    }
}

struct Stage {
    title: &'static str,
    primary_label: &'static str,
    primary: &'static str,
    primary_f: &'static str,
    /// heat-coloured when the primary value is a utilisation
    primary_heat: bool,
    lines: &'static [Line],
}

/// The two paths a workflow's work takes, stage by stage: where load turns into waiting, and
/// what each stage's saturation costs the next.
#[component]
async fn chain(v: &Value) -> Result<impl View> {
    const START: [Stage; 4] = [
        Stage {
            title: "Clients",
            primary_label: "starts + signals offered",
            primary: "workload.offered_per_s",
            primary_f: "rate",
            primary_heat: false,
            lines: &[
                l("started", "workload.started_per_s", "rate"),
                l("start p99", "latency.start.p99_ms", "ms"),
                l("signal p99", "latency.signal.p99_ms", "ms"),
                a("API errors", "latency.api_errors_per_s", "rate"),
            ],
        },
        Stage {
            title: "Frontend admission",
            primary_label: "busiest limiter",
            primary: "services.0.limit.util",
            primary_f: "pct",
            primary_heat: true,
            lines: &[
                l("limiter", "services.0.limit.name", "text"),
                l("CPU max", "services.0.cpu_max", "pct"),
                a("rejected", "services.0.rejected_per_s", "rate"),
            ],
        },
        Stage {
            title: "History",
            primary_label: "CPU max",
            primary: "services.1.cpu_max",
            primary_f: "pct",
            primary_heat: true,
            lines: &[
                l("history.rps", "services.1.limit.util", "pct"),
                l("lock wait p99", "history.lock_wait.p99_ms", "ms"),
                l("shard IO max", "history.shard_io_max", "pct"),
                a(
                    "BUSY_WORKFLOW retries",
                    "history.busy_retries_per_s",
                    "rate",
                ),
                a("rejected", "services.1.rejected_per_s", "rate"),
            ],
        },
        Stage {
            title: "Persistence",
            primary_label: "database busy",
            primary: "persistence.util",
            primary_f: "pct",
            primary_heat: true,
            lines: &[
                l("queue wait p99", "persistence.queue_wait.p99_ms", "ms"),
                l("history pool", "persistence.pool_util.history", "pct"),
                l("operations", "persistence.ops_per_s", "rate"),
                a("QPS-limit rejections", "persistence.rejected_per_s", "rate"),
            ],
        },
    ];
    const TASKS: [Stage; 4] = [
        Stage {
            title: "History task queues",
            primary_label: "tasks executed",
            primary: "history.tasks_per_s",
            primary_f: "rate",
            primary_heat: false,
            lines: &[
                l("pending", "history.pending_tasks", "int"),
                l("timers pending", "history.timers_pending", "int"),
                l("scheduler workers", "history.scheduler_util_max", "pct"),
                a(
                    "throttled retries",
                    "history.throttled_retries_per_s",
                    "rate",
                ),
            ],
        },
        Stage {
            title: "Matching",
            primary_label: "backlog",
            primary: "matching.backlog",
            primary_f: "int",
            primary_heat: false,
            lines: &[
                l("matching.rps", "services.2.limit.util", "pct"),
                l("sync match", "matching.sync_match_ratio", "pct"),
                l("dispatch p99", "matching.dispatch.p99_ms", "ms"),
                a("rejected", "services.2.rejected_per_s", "rate"),
                a("writer overflow", "matching.write_rejects_per_s", "rate"),
            ],
        },
        Stage {
            title: "Workers",
            primary_label: "WFT schedule→start p99",
            primary: "latency.wft_schedule_to_start.p99_ms",
            primary_f: "ms",
            primary_heat: false,
            lines: &[
                l(
                    "activity sched→start p99",
                    "latency.activity_schedule_to_start.p99_ms",
                    "ms",
                ),
                l("workflow slots", "workers.wft_slot_util", "pct"),
                l("activity slots", "workers.act_slot_util", "pct"),
                l("sticky hits", "workers.sticky_hit_ratio", "pct"),
                a("WFT timeouts", "workers.wft_timeouts_per_s", "rate"),
            ],
        },
        Stage {
            title: "Completion",
            primary_label: "workflows completed",
            primary: "workload.completed_per_s",
            primary_f: "rate",
            primary_heat: false,
            lines: &[
                l("running", "workload.running", "int"),
                l("end-to-end p50", "latency.e2e.p50_ms", "ms"),
                l("end-to-end p99", "latency.e2e.p99_ms", "ms"),
                a("start failures", "workload.start_failed_per_s", "rate"),
            ],
        },
    ];
    Ok(view! {
        <section class="chain-section" aria-label="pressure chain">
            <div class="section-head">
                <h2>"Where the load lands"</h2>
                <span class="muted">"each stage's saturation shows up as waiting, retries and rejections in the stages that depend on it"</span>
            </div>
            <div class="chain">
                <span class="chain-title">"Request path"</span>
                for s in &START {
                    stage(s: s, v: v)
                }
            </div>
            <div class="chain">
                <span class="chain-title">"Task path"</span>
                for s in &TASKS {
                    stage(s: s, v: v)
                }
            </div>
        </section>
    })
}

#[component]
async fn stage(s: &Stage, v: &Value) -> Result<impl View> {
    let pv = num_at(v, s.primary);
    Ok(view! {
        <div class="stage">
            <span class="stage-title">(s.title)</span>
            <span
                class=(if s.primary_heat { "primary heat" } else { "primary" })
                data-bind=(format!("{}|{}", s.primary, s.primary_f))
                data-heat=(if s.primary_heat { Some(s.primary) } else { None })
                data-level=(if s.primary_heat { Some(heat(pv).to_string()) } else { None })
            >
                (fmt_at(v, s.primary, s.primary_f))
            </span>
            <span class="primary-label">(s.primary_label)</span>
            <dl>
                for line in s.lines {
                    let val = num_at(v, line.path);
                    <div class=(if line.alarm { "line alarm" } else { "line" }) data-alarm=(if line.alarm { Some(line.path) } else { None }) data-on=(if line.alarm && val > 0.0 { "true" } else { "false" })>
                        <dt>(line.label)</dt>
                        <dd class="mono" data-bind=(format!("{}|{}", line.path, line.f))>(fmt_at(v, line.path, line.f))</dd>
                    </div>
                }
            </dl>
        </div>
    })
}

// --- charts ------------------------------------------------------------------------------------------------

struct Chart {
    key: &'static str,
    title: &'static str,
    /// frame path of the value shown next to the title
    current: &'static str,
    f: &'static str,
    legend: &'static str,
}

const CHARTS: [Chart; 8] = [
    Chart {
        key: "throughput",
        title: "Workflows per second",
        current: "workload.completed_per_s",
        f: "rate",
        legend: "completed · offered (dashed)",
    },
    Chart {
        key: "cpu",
        title: "Max pod CPU",
        current: "services.1.cpu_max",
        f: "pct",
        legend: "history · frontend (thin) · matching (dotted)",
    },
    Chart {
        key: "db",
        title: "Database busy",
        current: "persistence.util",
        f: "pct",
        legend: "",
    },
    Chart {
        key: "rejected",
        title: "Rate-limit rejections per second",
        current: "rejected_per_s",
        f: "rate",
        legend: "",
    },
    Chart {
        key: "backlog",
        title: "Matching backlog",
        current: "matching.backlog",
        f: "int",
        legend: "tasks waiting for a poller",
    },
    Chart {
        key: "wft",
        title: "Workflow task schedule→start p99",
        current: "latency.wft_schedule_to_start.p99_ms",
        f: "ms",
        legend: "",
    },
    Chart {
        key: "e2e",
        title: "Workflow end-to-end p99",
        current: "latency.e2e.p99_ms",
        f: "ms",
        legend: "",
    },
    Chart {
        key: "start",
        title: "StartWorkflowExecution p99",
        current: "latency.start.p99_ms",
        f: "ms",
        legend: "client-observed, retries included",
    },
];

#[component]
async fn charts(v: &Value) -> Result<impl View> {
    Ok(view! {
        <section aria-label="over time">
            <div class="section-head">
                <h2>"Over time"</h2>
                <span class="muted">"last three minutes of simulated time · dashed marks are events and live changes"</span>
            </div>
            <div class="charts">
                for c in &CHARTS {
                    <figure class="chart" data-chart=(c.key)>
                        <figcaption>
                            <span>(c.title)</span>
                            <b class="mono" data-bind=(format!("{}|{}", c.current, c.f))>(fmt_at(v, c.current, c.f))</b>
                        </figcaption>
                        <svg viewBox="0 0 320 90" preserveAspectRatio="none" role="img" aria-label=(c.title)></svg>
                        if !c.legend.is_empty() {
                            <span class="legend-note">(c.legend)</span>
                        }
                    </figure>
                }
            </div>
        </section>
    })
}

// --- hotspots ------------------------------------------------------------------------------------------------

#[component]
pub async fn hotspots(analysis: Option<&RunResult>, frame: &Frame) -> Result<impl View> {
    Ok(view! {
        match analysis {
            None => {
                <p class="muted">
                    (if frame.phase == Phase::Warmup {
                        format!("Warming up: the cluster fills from empty for {:.0} s of simulated time before measurement starts.", frame.warmup_s)
                    } else {
                        "Collecting the first measurements.".to_string()
                    })
                </p>
            },
            Some(r) => {
                if r.hotspots.is_empty() {
                    <p class="muted">"No resource has crossed the warning thresholds in the measurement window so far."</p>
                }
                for h in &r.hotspots {
                    hotspot(h: h)
                }
            },
        }
    })
}

#[component]
async fn hotspot(h: &Hotspot) -> Result<impl View> {
    let sev = match h.severity {
        Severity::Critical => "critical",
        Severity::Warning => "warning",
        Severity::Info => "info",
    };
    Ok(view! {
        <article class=(format!("hot {sev}"))>
            <div class="stripe"></div>
            <div class="body">
                <div class="title">
                    <span class=(format!("sev {sev}"))>(h.severity.as_str())</span>
                    <h3>(h.title.clone())</h3>
                    <span class="cat">(h.category.clone())</span>
                </div>
                <p>(h.detail.clone())</p>
                if !h.evidence.is_empty() {
                    <ul>
                        for e in &h.evidence {
                            <li>(e.clone())</li>
                        }
                    </ul>
                }
                if !h.metrics.is_empty() {
                    <div class="watch">
                        <span class="muted">"Watch"</span>
                        for m in &h.metrics {
                            <code>(m.clone())</code>
                        }
                    </div>
                }
                if !h.knobs.is_empty() {
                    <table class="knobs">
                        <tbody>
                            for k in &h.knobs {
                                <tr>
                                    <td>(k.key.clone())</td>
                                    <td>(k.current.clone())</td>
                                    <td>(k.hint.clone())</td>
                                </tr>
                            }
                        </tbody>
                    </table>
                }
            </div>
        </article>
    })
}

// --- detail tables --------------------------------------------------------------------------------------------

fn ms(v: f64) -> String {
    fmt_us(v * 1e3)
}

#[component]
async fn table(
    head: Vec<&'static str>,
    numeric_from: usize,
    rows: Vec<Vec<String>>,
) -> Result<impl View> {
    Ok(view! {
        <div class="tbl">
            <table class="data">
                <thead>
                    <tr>
                        for (i, h) in head.iter().enumerate() {
                            <th class=(if i >= numeric_from { Some("n") } else { None })>(h.to_string())</th>
                        }
                    </tr>
                </thead>
                <tbody>
                    for r in rows {
                        <tr>
                            for (i, c) in r.into_iter().enumerate() {
                                <td class=(if i >= numeric_from { "n" } else if i == 0 { "m" } else { "" })>(c)</td>
                            }
                        </tr>
                    }
                </tbody>
            </table>
        </div>
    })
}

/// Tables from the latest analysis (cumulative since warm-up), as in the HTML report.
#[component]
pub async fn detail(analysis: Option<&RunResult>) -> Result<impl View> {
    Ok(view! {
        match analysis {
            None => <p class="muted">"Tables appear with the first analysis."</p>,
            Some(r) => {
                let pod_rows: Vec<Vec<String>> = r
                    .services
                    .iter()
                    .flat_map(|s| s.pods.iter().filter(|p| p.alive))
                    .map(|p| {
                        vec![
                            p.name.clone(),
                            p.addr.clone(),
                            fmt_pct(p.cpu_util),
                            fmt_rate(p.requests_per_s),
                            fmt_rate(p.persistence_per_s),
                            fmt_pct(p.db_pool_util),
                            p.owned.to_string(),
                            p.top_limit().map(|(n, u)| format!("{n} {}", fmt_pct(u))).unwrap_or_else(|| "–".into()),
                            p.rejections.to_string(),
                        ]
                    })
                    .collect();
                let wf_rows: Vec<Vec<String>> = r
                    .workflows
                    .iter()
                    .filter(|w| w.started_per_s > 0.0 || w.offered_start_rate > 0.0)
                    .map(|w| {
                        vec![
                            w.workflow_type.clone(),
                            fmt_rate(w.offered_start_rate),
                            fmt_rate(w.started_per_s),
                            fmt_rate(w.completed_per_s),
                            fmt_rate(w.failed_per_s),
                            ms(w.e2e.p50_ms),
                            ms(w.e2e.p99_ms),
                            ms(w.wft_schedule_to_start.p99_ms),
                            ms(w.activity_schedule_to_start.p99_ms),
                            fmt_pct(w.sticky_hit_ratio),
                            w.wft_timeouts.to_string(),
                        ]
                    })
                    .collect();
                let api_rows: Vec<Vec<String>> = r
                    .apis
                    .iter()
                    .map(|a| {
                        vec![
                            a.api.clone(),
                            fmt_rate(a.per_s),
                            ms(a.latency.p50_ms),
                            ms(a.latency.p95_ms),
                            ms(a.latency.p99_ms),
                            if a.errors.is_empty() {
                                "–".into()
                            } else {
                                a.errors.iter().map(|(k, v)| format!("{k} {v}")).collect::<Vec<_>>().join(", ")
                            },
                        ]
                    })
                    .collect();
                let parts: Vec<&crate::report::PartitionResult> = {
                    let mut v: Vec<&crate::report::PartitionResult> = r
                        .matching
                        .partitions
                        .iter()
                        .filter(|p| !p.partition.starts_with("sticky"))
                        .collect();
                    v.sort_by(|a, b| b.adds_per_s.total_cmp(&a.adds_per_s));
                    v
                };
                let part_rows: Vec<Vec<String>> = parts
                    .iter()
                    .take(24)
                    .map(|p| {
                        vec![
                            format!("{} {} p{}", p.task_queue, p.kind, p.partition),
                            p.host.clone(),
                            fmt_rate(p.adds_per_s),
                            fmt_rate(p.polls_per_s),
                            fmt_pct(p.sync_match_ratio),
                            format!("{:.0}", p.backlog_mean),
                            format!("{:.1}", p.pollers_mean),
                            ms(p.task_wait.p99_ms),
                        ]
                    })
                    .collect();
                let op_rows: Vec<Vec<String>> = r
                    .persistence
                    .ops
                    .iter()
                    .take(12)
                    .map(|o| vec![o.op.clone(), fmt_rate(o.per_s), ms(o.latency.p50_ms), ms(o.latency.p99_ms), o.rejected.to_string()])
                    .collect();
                <details open="">
                    <summary><span>"Pods"</span><span class="muted">(format!("{} live", pod_rows.len()))</span></summary>
                    <div class="inner">
                        table(head: vec!["pod", "address", "CPU", "requests", "persistence", "pool", "owns", "busiest limiter", "rejected"], numeric_from: 2, rows: pod_rows)
                    </div>
                </details>
                <details>
                    <summary><span>"Workflows and client-observed APIs"</span><span class="muted">(format!("sync match {}", fmt_pct(r.matching.sync_match_ratio)))</span></summary>
                    <div class="inner">
                        table(head: vec!["workflow type", "offered", "started", "completed", "failed", "e2e p50", "e2e p99", "WFT s2s p99", "act s2s p99", "sticky hit", "WFT timeouts"], numeric_from: 1, rows: wf_rows)
                        table(head: vec!["API", "rate", "p50", "p95", "p99", "errors"], numeric_from: 1, rows: api_rows)
                    </div>
                </details>
                <details>
                    <summary><span>"Matching partitions"</span><span class="muted">(format!("{} partitions", r.matching.partitions.iter().filter(|p| !p.partition.starts_with("sticky")).count()))</span></summary>
                    <div class="inner">
                        table(head: vec!["partition", "host", "adds", "polls", "sync", "backlog", "pollers", "dispatch p99"], numeric_from: 2, rows: part_rows)
                    </div>
                </details>
                <details>
                    <summary><span>"Persistence operations"</span><span class="muted">(format!("database {} busy · lock wait p99 {} · cache hit {}", fmt_pct(r.persistence.utilization), ms(r.history.lock_wait.p99_ms), fmt_pct(r.history.cache_hit_ratio)))</span></summary>
                    <div class="inner">
                        table(head: vec!["operation", "rate", "p50", "p99", "rejected"], numeric_from: 1, rows: op_rows)
                    </div>
                </details>
            },
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn values_format_like_the_client_script() {
        let v = json!({
            "services": [{ "cpu_max": 0.997, "limit": { "name": "frontend.namespaceRPS[orders]", "util": 0.42 } }],
            "latency": { "start": { "p99_ms": 12.34 } },
            "t": 42.25,
            "speed": 0.0,
            "none": null
        });
        assert_eq!(fmt_at(&v, "services.0.cpu_max", "pct"), "99.7%");
        assert_eq!(fmt_at(&v, "services.0.limit.util", "pct"), "42%");
        assert_eq!(
            fmt_at(&v, "services.0.limit.name", "text"),
            "frontend.namespaceRPS[orders]"
        );
        assert_eq!(fmt_at(&v, "services.0.limit.name", "key"), "nsRPS[orders]");
        assert_eq!(format_value(Some(&json!("history.rps")), "key"), "rps");
        assert_eq!(fmt_at(&v, "latency.start.p99_ms", "ms"), "12.3ms");
        assert_eq!(fmt_at(&v, "t", "secs"), "42.2 s");
        assert_eq!(fmt_at(&v, "speed", "speed"), "max");
        assert_eq!(fmt_at(&v, "none", "rate"), "–");
        assert_eq!(fmt_at(&v, "services.3.cpu_max", "pct"), "–");
        assert_eq!(fmt_at(&v, "services.0.missing", "int"), "–");
        assert_eq!(format_value(Some(&json!(1234.5)), "rate"), "1234/s");
        assert_eq!(format_value(Some(&json!(2.5)), "mult"), "×2.50");
    }

    #[test]
    fn heat_levels_match_the_report() {
        assert_eq!(heat(0.0), 0);
        assert_eq!(heat(0.29), 1);
        assert_eq!(heat(0.5), 2);
        assert_eq!(heat(0.7), 3);
        assert_eq!(heat(0.89), 4);
        assert_eq!(heat(0.95), 5);
    }
}
