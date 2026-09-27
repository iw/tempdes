//! `tempdes ui`: a live visualisation of the simulated cluster, served by the
//! [Topcoat](https://github.com/tokio-rs/topcoat) web framework.
//!
//! * [`engine`] runs the simulation on its own thread, paced to wall-clock time, and applies
//!   live changes (load, replica counts, dynamic config).
//! * [`frame`] is what the engine publishes every half second of simulated time: rates,
//!   utilisations and latencies over the interval, per service, per pod and per flow between
//!   services.
//! * [`views`] render the page and its fragments on the server; [`app`] is the Topcoat
//!   router with the server-sent event stream of frames and the JSON control route.
//!
//! Topcoat's optional browser runtime (signals, shards) is not used: its script is served from
//! an asset bundle that only the separate `topcoat` CLI produces, which a `cargo install`ed
//! tool cannot rely on. A small hand-written script applies the frames instead.

pub mod app;
pub mod engine;
pub mod frame;
pub mod views;

use std::process::Command;

use crate::model::params::Params;

/// Options of the `ui` command.
#[derive(Clone, Debug)]
pub struct UiOptions {
    pub host: String,
    pub port: u16,
    /// simulated seconds per wall second after warm-up; 0 runs as fast as possible
    pub speed: f64,
    pub open: bool,
}

/// Simulate `params` on a background thread and serve the live view until Ctrl-C.
pub fn serve(params: Params, opts: &UiOptions) -> anyhow::Result<()> {
    let name = params.name.clone();
    let engine = engine::Engine::spawn(
        params,
        engine::Options {
            speed: opts.speed,
            load: 1.0,
        },
    )?;
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    rt.block_on(async move {
        let listener = tokio::net::TcpListener::bind((opts.host.as_str(), opts.port)).await?;
        let addr = listener.local_addr()?;
        let url = format!("http://{addr}/");
        eprintln!("serving {name} at {url} (Ctrl-C to stop)");
        if opts.open {
            open_browser(&url);
        }
        topcoat::serve(listener, app::router(engine)).await?;
        Ok(())
    })
}

fn open_browser(url: &str) {
    let result = if cfg!(target_os = "macos") {
        Command::new("open").arg(url).spawn()
    } else if cfg!(target_os = "windows") {
        Command::new("cmd").args(["/C", "start", "", url]).spawn()
    } else {
        Command::new("xdg-open").arg(url).spawn()
    };
    if let Err(e) = result {
        eprintln!("could not open a browser ({e}); open {url} yourself");
    }
}
