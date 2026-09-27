//! The Topcoat application: the page and its fragments, the frame stream, the JSON routes and
//! the control endpoint.

use std::sync::Arc;

use futures_util::{Stream, stream};
use serde::{Deserialize, Serialize};
use topcoat::{
    Result,
    context::{Cx, app_context},
    font::{Font, RouterBuilderFontExt, fontsource::fontsource_font},
    router::{
        HeaderName, HeaderValue, Router,
        content::{
            Css, Js, Json,
            sse::{Event, KeepAlive, Sse},
        },
        error::bad_request,
        header::CONTENT_TYPE,
        page, route,
    },
    view::{View, view},
};

use crate::config::dynamic::DcValue;
use crate::model::params::Params;
use crate::model::types::Service;

use super::engine::{Command, Engine, Point};
use super::views::{self, PageData};

/// IBM Plex, the report's typeface: a humanist sans for text and its mono for numbers.
pub const SANS: Font =
    fontsource_font!(IBM_PLEX_SANS, weight: [400, 500, 600], style: Normal, subset: Latin);
pub const MONO: Font =
    fontsource_font!(IBM_PLEX_MONO, weight: [400, 500], style: Normal, subset: Latin);

/// Application state shared by every request.
pub struct App {
    pub engine: Arc<Engine>,
    pub temporal_version: String,
}

/// Dynamic config keys the simulator applies to a running cluster (see
/// `model::build::apply_dc`), with the value each pod currently uses.
pub fn runtime_keys(p: &Params) -> Vec<(String, f64)> {
    let k = &p.k;
    let mut keys = vec![
        ("history.rps".to_string(), k.history_rps),
        ("matching.rps".to_string(), k.matching_rps),
        ("frontend.rps".to_string(), k.fe_rps),
        (
            "frontend.namespaceRPS".to_string(),
            p.namespaces.first().map(|n| n.fe_ns_rps).unwrap_or(0.0),
        ),
        (
            "history.persistenceMaxQPS".to_string(),
            k.history_persistence_max_qps,
        ),
        (
            "matching.persistenceMaxQPS".to_string(),
            k.matching_persistence_max_qps,
        ),
        (
            "frontend.persistenceMaxQPS".to_string(),
            k.fe_persistence_max_qps,
        ),
        (
            "history.shardIOConcurrency".to_string(),
            f64::from(k.shard_io_concurrency),
        ),
        (
            "history.transferProcessorSchedulerWorkerCount".to_string(),
            f64::from(k.scheduler_workers[0]),
        ),
        (
            "history.timerProcessorSchedulerWorkerCount".to_string(),
            f64::from(k.scheduler_workers[1]),
        ),
        (
            "history.visibilityProcessorSchedulerWorkerCount".to_string(),
            f64::from(k.scheduler_workers[2]),
        ),
    ];
    keys.retain(|(_, v)| v.is_finite());
    keys
}

pub fn router(engine: Arc<Engine>) -> Router {
    Router::builder()
        .page(index)
        .page(topology_fragment)
        .page(hotspots_fragment)
        .page(detail_fragment)
        .route(events)
        .route(frame_json)
        .route(history_json)
        .route(control)
        .route(stylesheet)
        .route(script)
        .font(SANS)
        .font(MONO)
        .app_context(App {
            engine,
            temporal_version: crate::config::dynamic::registry().temporal_version.clone(),
        })
        .build()
}

fn page_data(app: &App) -> Result<PageData> {
    let frame = app.engine.latest();
    let json = serde_json::to_value(&*frame)?;
    let (analysis, event_log) = {
        let sh = app.engine.shared();
        (sh.analysis.clone(), sh.events.clone())
    };
    Ok(PageData {
        dc_keys: runtime_keys(&app.engine.params),
        frame,
        json,
        params: app.engine.params.clone(),
        analysis,
        events: event_log,
        temporal_version: app.temporal_version.clone(),
    })
}

#[page("/")]
async fn index(cx: &Cx) -> Result<impl View> {
    let app: &App = app_context(cx);
    let d = page_data(app)?;
    Ok(view! {
        let d = d;
        views::document(d: &d)
    })
}

#[page("/fragment/topology")]
async fn topology_fragment(cx: &Cx) -> Result<impl View> {
    let app: &App = app_context(cx);
    let frame = app.engine.latest();
    let json = serde_json::to_value(&*frame)?;
    Ok(view! {
        let frame = frame;
        let json = json;
        views::topology(frame: &frame, v: &json)
    })
}

#[page("/fragment/hotspots")]
async fn hotspots_fragment(cx: &Cx) -> Result<impl View> {
    let app: &App = app_context(cx);
    let frame = app.engine.latest();
    let analysis = app.engine.shared().analysis.clone();
    Ok(view! {
        let frame = frame;
        let analysis = analysis;
        views::hotspots(analysis: analysis.as_deref(), frame: &frame)
    })
}

#[page("/fragment/detail")]
async fn detail_fragment(cx: &Cx) -> Result<impl View> {
    let app: &App = app_context(cx);
    let analysis = app.engine.shared().analysis.clone();
    Ok(view! {
        let analysis = analysis;
        views::detail(analysis: analysis.as_deref())
    })
}

/// The frame stream: the current frame on connect, then every new one.
#[route(GET "/events")]
async fn events(cx: &Cx) -> Result<Sse<impl Stream<Item = Result<Event>> + use<>>> {
    let app: &App = app_context(cx);
    let rx = app.engine.subscribe();
    let frames = stream::unfold((rx, true), |(mut rx, first)| async move {
        if !first && rx.changed().await.is_err() {
            return None;
        }
        let frame = rx.borrow_and_update().clone()?;
        let event = Event::new().event("frame").json_data(&*frame);
        Some((event, (rx, false)))
    });
    Ok(Sse::new(frames).keep_alive(KeepAlive::new()))
}

type JsonBody = ([(HeaderName, HeaderValue); 1], String);

fn json_body<T: Serialize + ?Sized>(value: &T) -> Result<JsonBody> {
    Ok((
        [(CONTENT_TYPE, HeaderValue::from_static("application/json"))],
        serde_json::to_string(value)?,
    ))
}

#[route(GET "/api/frame")]
async fn frame_json(cx: &Cx) -> Result<JsonBody> {
    let app: &App = app_context(cx);
    json_body(&*app.engine.latest())
}

/// The recent time series, for the charts of a page that just loaded.
#[route(GET "/api/history")]
async fn history_json(cx: &Cx) -> Result<JsonBody> {
    let app: &App = app_context(cx);
    let points: Vec<Point> = app.engine.shared().series.iter().cloned().collect();
    json_body(&points)
}

#[derive(Debug, Deserialize)]
pub struct ControlRequest {
    pub action: String,
    #[serde(default)]
    pub value: Option<f64>,
    #[serde(default)]
    pub service: Option<String>,
    #[serde(default)]
    pub key: Option<String>,
    #[serde(default)]
    pub seed: Option<u64>,
}

#[derive(Debug, Serialize)]
pub struct ControlResponse {
    pub ok: bool,
    pub message: String,
}

/// Turn a control request into an engine command, validating it as browser input.
pub fn parse_control(
    req: &ControlRequest,
    params: &Params,
) -> std::result::Result<Command, String> {
    let value = || {
        req.value
            .ok_or_else(|| format!("{} needs a value", req.action))
    };
    match req.action.as_str() {
        "pause" => Ok(Command::Pause),
        "resume" => Ok(Command::Resume),
        "speed" => {
            let s = value()?;
            if !s.is_finite() || !(s == 0.0 || (0.1..=100.0).contains(&s)) {
                return Err("speed must be 0 (unlimited) or between 0.1 and 100".into());
            }
            Ok(Command::Speed(s))
        }
        "load" => {
            let k = value()?;
            if !k.is_finite() || !(0.0..=20.0).contains(&k) {
                return Err("load must be between 0 and 20".into());
            }
            Ok(Command::Load(k))
        }
        "replicas" => {
            let svc = req
                .service
                .as_deref()
                .and_then(Service::parse)
                .ok_or_else(|| {
                    "replicas needs a service: frontend, history, matching or worker".to_string()
                })?;
            let n = value()?;
            if !n.is_finite() || !(1.0..=64.0).contains(&n) || n.fract() != 0.0 {
                return Err("replicas must be a whole number between 1 and 64".into());
            }
            Ok(Command::Replicas(svc, n as u32))
        }
        "dc" => {
            let key = req
                .key
                .clone()
                .ok_or_else(|| "dc needs a key".to_string())?;
            let known = runtime_keys(params);
            let key = known
                .iter()
                .map(|(k, _)| k)
                .find(|k| k.eq_ignore_ascii_case(&key))
                .ok_or_else(|| format!("{key} is not applied at runtime by the simulator"))?
                .clone();
            let v = value()?;
            if !v.is_finite() || v < 0.0 || v > 1e9 {
                return Err("the value must be a non-negative number".into());
            }
            let dc = if v.fract() == 0.0 {
                DcValue::Int(v as i64)
            } else {
                DcValue::Float(v)
            };
            Ok(Command::DynamicConfig(key, dc))
        }
        "restart" => Ok(Command::Restart(req.seed)),
        other => Err(format!("unknown action {other:?}")),
    }
}

#[route(POST "/api/control")]
async fn control(cx: &Cx, Json(req): Json<ControlRequest>) -> Result<Json<ControlResponse>> {
    let app: &App = app_context(cx);
    let cmd = parse_control(&req, &app.engine.params).map_err(bad_request)?;
    let message = format!("{cmd:?}");
    if !app.engine.send(cmd) {
        return Err(bad_request("the simulation has stopped").into());
    }
    Ok(Json(ControlResponse { ok: true, message }))
}

#[route(GET "/app.css")]
async fn stylesheet() -> Result<Css<&'static str>> {
    Ok(Css(include_str!("style.css")))
}

#[route(GET "/app.js")]
async fn script() -> Result<Js<&'static str>> {
    Ok(Js(include_str!("app.js")))
}
