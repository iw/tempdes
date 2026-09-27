//! The simulation thread behind `tempdes ui`.
//!
//! The simulator is single-threaded (`Rc<Sim>`), so one dedicated thread owns the built
//! cluster and its executor, advances simulated time in slices, and between slices applies the
//! commands the web app sends (pause, speed, load, replica counts, dynamic config, restart) and
//! publishes a [`Frame`] every [`FRAME_INTERVAL`] of simulated time. Warm-up runs as fast as
//! possible; after the warm-up reset the thread paces itself to the requested speed.
//!
//! Every few simulated seconds the thread also runs the report's hotspot analysis over the
//! measurement window so far, so the ranked hotspots update while the run is in progress.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use serde::Serialize;
use tokio::sync::watch;

use crate::config::dynamic::DcValue;
use crate::model::build::{self, RunInfo};
use crate::model::params::Params;
use crate::model::types::Service;
use crate::report::{self, RunResult, Severity};
use crate::sim::executor::{Time, now, spawn};

use super::frame::{AnalysisSummary, EventNote, Frame, Meta, Sampler};

/// Simulated time advanced per executor slice; commands are applied between slices.
pub const STEP: Time = 250_000;
/// Simulated time between frames.
pub const FRAME_INTERVAL: Time = 500_000;
/// Simulated time between hotspot analyses (also throttled to one per wall-clock second).
pub const ANALYSIS_INTERVAL: Time = 5_000_000;
/// Measured time needed before the first analysis: rates over a shorter window are noise.
pub const ANALYSIS_MIN_MEASURED: Time = 3_000_000;
/// Points kept for the charts (6 minutes at the frame interval).
pub const SERIES_LEN: usize = 720;
/// Event notes kept for the log.
pub const EVENTS_LEN: usize = 200;

/// A change requested from the web app, applied on the simulation thread between slices.
#[derive(Clone, Debug, PartialEq)]
pub enum Command {
    Pause,
    Resume,
    /// Simulated seconds per wall second; 0 means as fast as possible.
    Speed(f64),
    /// Multiplier on every start and signal rate.
    Load(f64),
    Replicas(Service, u32),
    DynamicConfig(String, DcValue),
    /// Start the run over, optionally with another seed.
    Restart(Option<u64>),
}

#[derive(Clone, Copy, Debug)]
pub struct Options {
    /// Simulated seconds per wall second after warm-up; 0 means as fast as possible.
    pub speed: f64,
    /// Initial multiplier on start and signal rates.
    pub load: f64,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            speed: 1.0,
            load: 1.0,
        }
    }
}

/// One point of the time series behind the charts.
#[derive(Clone, Debug, Default, Serialize)]
pub struct Point {
    pub t: f64,
    pub offered: f64,
    pub started: f64,
    pub completed: f64,
    /// max pod CPU per service, in `Service::ALL` order
    pub cpu: [f64; 4],
    pub db: f64,
    pub rejected: f64,
    pub backlog: u64,
    pub pending: u64,
    pub wft_s2s_p99: f64,
    pub e2e_p99: f64,
    pub start_p99: f64,
    pub lock_wait_p99: f64,
    pub shard_io_max: f64,
    pub sync_match: f64,
    pub running: u64,
    pub load: f64,
}

impl Point {
    fn of(f: &Frame) -> Point {
        let mut cpu = [0.0; 4];
        for (i, s) in f.services.iter().enumerate().take(4) {
            cpu[i] = s.cpu_max;
        }
        Point {
            t: f.t,
            offered: f.workload.offered_per_s,
            started: f.workload.started_per_s,
            completed: f.workload.completed_per_s,
            cpu,
            db: f.persistence.util,
            rejected: f.rejected_per_s,
            backlog: f.matching.backlog,
            pending: f.history.pending_tasks,
            wft_s2s_p99: f.latency.wft_schedule_to_start.p99_ms,
            e2e_p99: f.latency.e2e.p99_ms,
            start_p99: f.latency.start.p99_ms,
            lock_wait_p99: f.history.lock_wait.p99_ms,
            shard_io_max: f.history.shard_io_max,
            sync_match: f.matching.sync_match_ratio,
            running: f.workload.running,
            load: f.load_scale,
        }
    }
}

/// State shared with the web app besides the frame stream.
#[derive(Default)]
pub struct Shared {
    pub series: VecDeque<Point>,
    /// the latest hotspot analysis of the current run
    pub analysis: Option<Arc<RunResult>>,
    pub analysis_seq: u64,
    /// timeline events and live changes of the current run
    pub events: Vec<EventNote>,
}

/// Handle to the simulation thread.
pub struct Engine {
    cmds: mpsc::Sender<Command>,
    frames: watch::Receiver<Option<Arc<Frame>>>,
    shared: Arc<Mutex<Shared>>,
    /// the parameters every run starts from (the seed may change on restart)
    pub params: Arc<Params>,
}

impl Engine {
    /// Build the cluster on a new thread and start simulating. Returns once the first frame
    /// is available.
    pub fn spawn(params: Params, opts: Options) -> anyhow::Result<Arc<Engine>> {
        let (cmd_tx, cmd_rx) = mpsc::channel();
        let (frame_tx, frame_rx) = watch::channel(None);
        let (ready_tx, ready_rx) = mpsc::channel::<()>();
        let shared = Arc::new(Mutex::new(Shared::default()));
        let engine = Arc::new(Engine {
            cmds: cmd_tx,
            frames: frame_rx,
            shared: shared.clone(),
            params: Arc::new(params.clone()),
        });
        thread::Builder::new()
            .name("tempdes-sim".into())
            .spawn(move || run_loop(params, opts, &cmd_rx, &frame_tx, &shared, &ready_tx))?;
        ready_rx
            .recv()
            .map_err(|_| anyhow::anyhow!("the simulation thread stopped before its first frame"))?;
        Ok(engine)
    }

    pub fn send(&self, cmd: Command) -> bool {
        self.cmds.send(cmd).is_ok()
    }

    /// The most recent frame.
    pub fn latest(&self) -> Arc<Frame> {
        self.frames
            .borrow()
            .clone()
            .expect("the first frame is published before Engine::spawn returns")
    }

    pub fn subscribe(&self) -> watch::Receiver<Option<Arc<Frame>>> {
        self.frames.clone()
    }

    pub fn shared(&self) -> MutexGuard<'_, Shared> {
        self.shared.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// Wall-clock pacing of simulated time.
struct Pace {
    anchor_wall: Instant,
    anchor_sim: Time,
}

impl Pace {
    fn reset(&mut self) {
        self.anchor_wall = Instant::now();
        self.anchor_sim = now();
    }

    /// Sleep until simulated time `t` is due at `speed` simulated seconds per wall second.
    /// When the simulation cannot keep up, the anchor moves instead of trying to catch up.
    fn wait(&mut self, speed: f64, t: Time) {
        if speed <= 0.0 {
            return;
        }
        let due = self.anchor_wall
            + Duration::from_secs_f64(t.saturating_sub(self.anchor_sim) as f64 / 1e6 / speed);
        let wall = Instant::now();
        if due > wall {
            thread::sleep(due - wall);
        } else if wall - due > Duration::from_secs(2) {
            self.anchor_wall = wall;
            self.anchor_sim = t;
        }
    }
}

struct Live {
    paused: bool,
    speed: f64,
    load: f64,
    seq: u64,
    run: u32,
}

fn note(ctx: &crate::model::world::Ctx, text: String) {
    ctx.m
        .borrow_mut()
        .notes
        .push(format!("t={:.1}s {text}", now() as f64 / 1e6));
}

fn summary(r: &RunResult, seq: u64, measured_s: f64) -> AnalysisSummary {
    AnalysisSummary {
        seq,
        critical: r
            .hotspots
            .iter()
            .filter(|h| h.severity == Severity::Critical)
            .count(),
        warning: r
            .hotspots
            .iter()
            .filter(|h| h.severity == Severity::Warning)
            .count(),
        headline: r.headline.clone(),
        measured_s,
    }
}

#[allow(clippy::too_many_lines)]
fn run_loop(
    mut params: Params,
    opts: Options,
    cmds: &mpsc::Receiver<Command>,
    frames: &watch::Sender<Option<Arc<Frame>>>,
    shared: &Arc<Mutex<Shared>>,
    ready: &mpsc::Sender<()>,
) {
    let mut live = Live {
        paused: false,
        speed: opts.speed,
        load: opts.load,
        seq: 0,
        run: 0,
    };
    let mut ready = Some(ready);
    'runs: loop {
        live.run += 1;
        let wall_start = Instant::now();
        let (ctx, mut ex) = build::build(params.clone());
        let rates = build::start(&ctx, &mut ex);
        rates.borrow_mut().scale = live.load;
        let mut sampler = Sampler::new(&ctx);
        {
            let mut sh = shared.lock().unwrap_or_else(|e| e.into_inner());
            sh.series.clear();
            sh.analysis = None;
            sh.events.clear();
        }
        let mut analysis = AnalysisSummary::default();
        let mut pace = Pace {
            anchor_wall: Instant::now(),
            anchor_sim: 0,
        };
        let mut next_slice: Time = 0;
        let mut next_frame: Time = 0;
        let mut next_analysis: Time = ctx.p.warmup + ANALYSIS_MIN_MEASURED;
        let mut last_analysis_wall = Instant::now() - Duration::from_secs(10);
        let mut last_frame = (Instant::now(), 0 as Time);
        let mut actual_speed = 0.0;
        let mut in_warmup = true;
        let mut last_published: Option<Arc<Frame>> = None;

        let publish = |frame: Frame, last: &mut Option<Arc<Frame>>| {
            let frame = Arc::new(frame);
            *last = Some(frame.clone());
            let _ = frames.send(Some(frame));
        };

        loop {
            // --- commands ---------------------------------------------------------------------
            loop {
                let cmd = if live.paused {
                    match cmds.recv_timeout(Duration::from_millis(200)) {
                        Ok(c) => c,
                        Err(mpsc::RecvTimeoutError::Timeout) => break,
                        Err(mpsc::RecvTimeoutError::Disconnected) => return,
                    }
                } else {
                    match cmds.try_recv() {
                        Ok(c) => c,
                        Err(mpsc::TryRecvError::Empty) => break,
                        Err(mpsc::TryRecvError::Disconnected) => return,
                    }
                };
                let mut republish = false;
                match cmd {
                    Command::Pause => {
                        live.paused = true;
                        republish = true;
                    }
                    Command::Resume => {
                        live.paused = false;
                        pace.reset();
                        republish = true;
                    }
                    Command::Speed(s) => {
                        live.speed = s.max(0.0);
                        pace.reset();
                        republish = true;
                    }
                    Command::Load(k) => {
                        let k = k.clamp(0.0, 100.0);
                        live.load = k;
                        rates.borrow_mut().scale = k;
                        note(&ctx, format!("load ×{k:.2} (live change)"));
                    }
                    Command::Replicas(svc, n) => {
                        let n = n.clamp(1, 64) as usize;
                        note(&ctx, format!("scale {svc} to {n} (live change)"));
                        let c = ctx.clone();
                        spawn(async move { build::scale(&c, svc, n).await });
                    }
                    Command::DynamicConfig(key, value) => {
                        note(
                            &ctx,
                            format!("dynamic config {key} = {value} (live change)"),
                        );
                        build::apply_dc(&ctx, &key, &value);
                    }
                    Command::Restart(seed) => {
                        if let Some(s) = seed {
                            params.seed = s;
                        }
                        continue 'runs;
                    }
                }
                if republish && let Some(last) = &last_published {
                    let mut f = (**last).clone();
                    f.paused = live.paused;
                    f.speed = live.speed;
                    f.load_scale = live.load;
                    publish(f, &mut last_published);
                }
            }
            if live.paused && last_published.is_some() {
                continue;
            }

            // --- one slice of simulated time ------------------------------------------------
            // A slice ends exactly at the warm-up reset, so the counters the reset zeroes are
            // never differenced across it.
            next_slice = if in_warmup {
                (next_slice + STEP).min(ctx.p.warmup)
            } else {
                next_slice + STEP
            };
            ex.run_until(next_slice);
            let t = now();

            if in_warmup && t >= ctx.p.warmup {
                in_warmup = false;
                pace.reset();
                last_frame = (Instant::now(), t);
                sampler.resync(&ctx);
                next_frame = t + FRAME_INTERVAL;
                continue;
            }

            // --- hotspot analysis ---------------------------------------------------------
            if t >= next_analysis && last_analysis_wall.elapsed() >= Duration::from_secs(1) {
                let info = RunInfo {
                    polls: ex.polls(),
                    end: t,
                    wall_ms: wall_start.elapsed().as_millis(),
                };
                let measured_s = t.saturating_sub(ctx.p.warmup) as f64 / 1e6;
                let result = report::analyze_window(&ctx, &info, None, measured_s);
                let mut sh = shared.lock().unwrap_or_else(|e| e.into_inner());
                sh.analysis_seq += 1;
                analysis = summary(&result, sh.analysis_seq, measured_s);
                sh.analysis = Some(Arc::new(result));
                drop(sh);
                next_analysis = t + ANALYSIS_INTERVAL;
                last_analysis_wall = Instant::now();
            }

            // --- frame ---------------------------------------------------------------------
            if t >= next_frame {
                next_frame = t + FRAME_INTERVAL;
                let wall = Instant::now();
                let wall_dt = wall.duration_since(last_frame.0).as_secs_f64();
                if wall_dt > 0.0 {
                    let inst = (t - last_frame.1) as f64 / 1e6 / wall_dt;
                    actual_speed = if actual_speed == 0.0 {
                        inst
                    } else {
                        0.7 * actual_speed + 0.3 * inst
                    };
                }
                last_frame = (wall, t);
                live.seq += 1;
                let offered: f64 = {
                    let r = rates.borrow();
                    ctx.p
                        .wf_types
                        .iter()
                        .enumerate()
                        .filter(|(_, w)| !w.system_scheduler)
                        .map(|(i, _)| r.start[i] * r.scale)
                        .sum()
                };
                let meta = Meta {
                    seq: live.seq,
                    run: live.run,
                    paused: live.paused,
                    speed: live.speed,
                    actual_speed,
                    load_scale: live.load,
                    offered_per_s: offered,
                    analysis: analysis.clone(),
                };
                let frame = sampler.frame(&ctx, &meta);
                {
                    let mut sh = shared.lock().unwrap_or_else(|e| e.into_inner());
                    sh.series.push_back(Point::of(&frame));
                    while sh.series.len() > SERIES_LEN {
                        sh.series.pop_front();
                    }
                    sh.events.extend(frame.events.iter().cloned());
                    let overflow = sh.events.len().saturating_sub(EVENTS_LEN);
                    if overflow > 0 {
                        sh.events.drain(..overflow);
                    }
                }
                publish(frame, &mut last_published);
                if let Some(r) = ready.take() {
                    let _ = r.send(());
                }
            }

            // --- pacing --------------------------------------------------------------------
            if !in_warmup {
                pace.wait(live.speed, t);
            }
        }
    }
}
