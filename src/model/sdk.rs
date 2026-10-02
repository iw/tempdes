//! SDK side: client connections, retry policy, worker processes (pollers, slots, sticky cache),
//! workflow task processing (the workflow "program" interpreter) and activity execution.

use crate::config::scenario::{ClientLb, OnFailure};
use crate::sim::executor::{Time, now, sleep, spawn};
use crate::sim::sync::Permit;

use super::frontend;
use super::history::{
    self, ActTaskInfo, Commands, HISTORY_PAGE, ProgState, ScheduleActivities, StartOrigin, WftInfo,
};
use super::infra::*;
use super::matching::{self, Polled};
use super::params::{SCHEDULER_WF_TYPE, StepP};
use super::queues::history_call;
use super::types::*;
use super::world::*;

/// Which connection a request uses.
#[derive(Clone, Copy, Debug)]
pub enum Conn {
    Client(usize),
    Worker(usize),
}

/// The frontend pod that a request's next attempt goes to, following `network.client_lb`.
pub fn conn_pod(ctx: &Ctx, conn: Conn) -> PodId {
    match ctx.p.client_lb {
        ClientLb::Pinned => pinned_pod(ctx, conn),
        ClientLb::RoundRobin => round_robin_pod(ctx, conn),
        ClientLb::Proxy => proxy_pod(ctx),
    }
}

/// `pinned`: the process's single connection, reconnecting on GOAWAY (max connection age ±10%)
/// or when its pod is gone. New connections land on a uniformly random live frontend (NLB /
/// kube Service), so connection imbalance persists until connections age out.
fn pinned_pod(ctx: &Ctx, conn: Conn) -> PodId {
    let t = now();
    let (cur, exp) = match conn {
        Conn::Client(i) => {
            let c = &ctx.clients.borrow()[i];
            (c.conn, c.conn_expires)
        }
        Conn::Worker(i) => {
            let w = &ctx.workers.borrow()[i];
            (w.conn, w.conn_expires)
        }
    };
    let alive = ctx.pods.borrow().get(cur).map(|p| p.alive).unwrap_or(false);
    if alive && t < exp {
        return cur;
    }
    let live = ctx.live_pods(Service::Frontend);
    let pod = live[ctx.rand_index(live.len())];
    let exp = t.saturating_add(max_conn_age(ctx));
    {
        let mut pods = ctx.pods.borrow_mut();
        if let Some(fe) = pods.get_mut(cur).and_then(|p| p.fe.as_mut()) {
            fe.connections = fe.connections.saturating_sub(1);
        }
        if let Some(fe) = pods[pod].fe.as_mut() {
            fe.connections += 1;
        }
    }
    match conn {
        Conn::Client(i) => {
            let mut c = ctx.clients.borrow_mut();
            c[i].conn = pod;
            c[i].conn_expires = exp;
        }
        Conn::Worker(i) => {
            let mut w = ctx.workers.borrow_mut();
            w[i].conn = pod;
            w[i].conn_expires = exp;
        }
    }
    pod
}

/// grpc-go's DNS resolver re-resolves at most once per 30 s (`minDNSResRate`).
const DNS_RERESOLVE_MIN: Time = 30_000_000;

/// Connection lifetime until the server's GOAWAY: `frontend.keepAliveMaxConnectionAge` with
/// grpc-go's ±10% jitter (0 means connections never age out).
fn max_conn_age(ctx: &Ctx) -> Time {
    let age = ctx.p.k.keepalive_max_conn_age;
    if age == 0 {
        return Time::MAX;
    }
    (age as f64 * (0.9 + 0.2 * ctx.rand())) as Time
}

/// `round_robin`: gRPC client-side load balancing on a headless Service. The channel holds a
/// subchannel (connection) to every frontend pod that its last DNS resolution returned, and
/// rotates requests over the live ones. It re-resolves only when a subchannel closes (GOAWAY
/// at the max connection age, or its pod going away), and at most every 30 s. Pods added by
/// scaling therefore get traffic from a process only after its next re-resolution.
fn round_robin_pod(ctx: &Ctx, conn: Conn) -> PodId {
    let t = now();
    let mut rr = match conn {
        Conn::Client(i) => std::mem::take(&mut ctx.clients.borrow_mut()[i].rr),
        Conn::Worker(i) => std::mem::take(&mut ctx.workers.borrow_mut()[i].rr),
    };
    let pod = rr_pick(ctx, &mut rr, t);
    match conn {
        Conn::Client(i) => ctx.clients.borrow_mut()[i].rr = rr,
        Conn::Worker(i) => ctx.workers.borrow_mut()[i].rr = rr,
    }
    pod
}

fn rr_pick(ctx: &Ctx, rr: &mut RoundRobin, t: Time) -> PodId {
    let alive = |pod: PodId| ctx.pods.borrow()[pod].alive;
    match rr.resolved_at {
        None => rr_resolve(ctx, rr, t),
        Some(last) => {
            // a connection that reached its max age got GOAWAY: it reconnects to the same pod,
            // and the channel asks the resolver for fresh addresses
            for i in 0..rr.expires.len() {
                if rr.expires[i] <= t {
                    rr.resolve_pending = true;
                    while rr.expires[i] <= t {
                        rr.expires[i] = rr.expires[i].saturating_add(max_conn_age(ctx));
                    }
                }
            }
            if rr.subchannels.iter().any(|&p| !alive(p)) {
                rr.resolve_pending = true;
            }
            if rr.resolve_pending && t >= last + DNS_RERESOLVE_MIN {
                rr_resolve(ctx, rr, t);
            }
        }
    }
    for _ in 0..rr.subchannels.len() {
        let pod = rr.subchannels[rr.next];
        rr.next = (rr.next + 1) % rr.subchannels.len();
        if alive(pod) {
            return pod;
        }
    }
    // no live subchannel left: the channel re-resolves straight away
    rr_resolve(ctx, rr, t);
    rr.subchannels[rr.next]
}

/// Resolve the headless Service: every Ready frontend pod. Subchannels to pods that are still
/// there keep their connections, new pods get new ones, and gone pods are dropped.
fn rr_resolve(ctx: &Ctx, rr: &mut RoundRobin, t: Time) {
    let live = ctx.live_pods(Service::Frontend);
    {
        let mut pods = ctx.pods.borrow_mut();
        for &p in rr.subchannels.iter().filter(|p| !live.contains(p)) {
            if let Some(fe) = pods[p].fe.as_mut() {
                fe.connections = fe.connections.saturating_sub(1);
            }
        }
        for &p in live.iter().filter(|p| !rr.subchannels.contains(p)) {
            if let Some(fe) = pods[p].fe.as_mut() {
                fe.connections += 1;
            }
        }
    }
    let mut expires = Vec::with_capacity(live.len());
    for &p in &live {
        let exp = match rr.subchannels.iter().position(|&q| q == p) {
            Some(i) => rr.expires[i],
            None => t.saturating_add(max_conn_age(ctx)),
        };
        expires.push(exp);
    }
    rr.subchannels = live;
    rr.expires = expires;
    // grpc's round_robin picker starts at a random subchannel each time it is rebuilt
    rr.next = ctx.rand_index(rr.subchannels.len());
    rr.resolve_pending = false;
    rr.resolved_at = Some(t);
}

/// `proxy`: an L7 load balancer (ALB, Envoy, a service mesh) picks a frontend for every
/// request, round robin over the pods it considers healthy. A pod added by scaling joins after
/// `network.proxy_discovery`; a removed pod leaves at once.
fn proxy_pod(ctx: &Ctx) -> PodId {
    let t = now();
    let live = ctx.live_pods(Service::Frontend);
    let ready: Vec<PodId> = {
        let pods = ctx.pods.borrow();
        live.iter()
            .copied()
            .filter(|&p| pods[p].fe.as_ref().is_none_or(|fe| fe.ready_at <= t))
            .collect()
    };
    let pool = if ready.is_empty() { &live } else { &ready };
    let i = ctx.proxy_next.get();
    ctx.proxy_next.set(i.wrapping_add(1));
    pool[i % pool.len()]
}

/// How an SDK call is bounded and retried, after the Go SDK (sdk-go v1.36.0).
///
/// Every call gets one gRPC context with a deadline (`newGRPCContext` in `internal_utils.go`:
/// 10 s by default, 65 s for a history long poll), and the retry interceptor retries inside
/// that context (`internal/common/retry/interceptor.go`), so all attempts share the deadline.
/// The interceptor's 1-minute expiration only applies to a context without a deadline, which an
/// SDK call never has.
#[derive(Clone, Copy, Debug)]
pub struct Retry {
    /// the call's deadline, measured from its first attempt
    pub timeout: Time,
    /// retry retryable errors until the deadline (worker polls handle their own errors)
    pub retries: bool,
}

impl Retry {
    /// The Go SDK's default per-call timeout (`defaultRPCTimeout`).
    pub const DEFAULT_TIMEOUT: Time = 10_000_000;
    /// The Go SDK's timeout for a history long poll (`defaultGetHistoryTimeout`).
    pub const LONG_POLL_TIMEOUT: Time = 65_000_000;
    /// The Go SDK's deadline for each update call (`pollUpdateTimeout`).
    pub const UPDATE_TIMEOUT: Time = 60_000_000;

    /// A call retried within one deadline of `timeout`.
    pub fn call(timeout: Time) -> Retry {
        Retry {
            timeout,
            retries: true,
        }
    }

    /// A single attempt with a deadline of `timeout`.
    pub fn once(timeout: Time) -> Retry {
        Retry {
            timeout,
            retries: false,
        }
    }
}

/// gRPC codes the Go SDK retries (`IsRetryable`): Unavailable (which a lost shard also surfaces
/// as) and ResourceExhausted. A deadline is never retried.
fn retryable(e: Err) -> bool {
    matches!(
        e,
        Err::ResourceExhausted(..) | Err::Unavailable | Err::ShardOwnershipLost
    )
}

/// The Go SDK's wait before retry `n` (n ≥ 1): `createDynamicServiceRetryPolicy` starts at
/// 200 ms and doubles up to 6 s (a tenth of its 60 s expiration), and go-grpc-middleware waits
/// `initial × 2ⁿ` with ±20% jitter (`JitterUp`), so the first retry comes after about 400 ms.
fn sdk_backoff(ctx: &Ctx, n: u32) -> Time {
    let base = (200_000.0 * 2f64.powi(n as i32)).min(6_000_000.0);
    (base * (0.8 + 0.4 * ctx.rand())) as Time
}

/// Issue an API call from the SDK through the frontend, with retries inside one deadline.
/// `body` runs the frontend handler's work for an attempt and receives the call's deadline,
/// which the server propagates to history and matching. When the deadline passes, the client
/// gives up with `DeadlineExceeded` while the server may still finish the attempt. Records the
/// client-observed latency.
pub async fn sdk_call<T, F, Fut>(
    ctx: &Ctx,
    conn: Conn,
    ns: usize,
    api: Api,
    retry: Retry,
    extra_cpu: f64,
    body: F,
) -> Res<T>
where
    T: 'static,
    F: Fn(Ctx, PodId, Time) -> Fut + 'static,
    Fut: std::future::Future<Output = Res<T>> + 'static,
{
    let t0 = now();
    let deadline = t0.saturating_add(retry.timeout);
    let body = std::rc::Rc::new(body);
    let fail = |e: Err| {
        ctx.m.borrow_mut().client[api.idx()].record(now() - t0, Some(e));
        Err(e)
    };
    let mut attempt = 0u32;
    loop {
        if attempt > 0 {
            let d = sdk_backoff(ctx, attempt);
            if now().saturating_add(d) >= deadline {
                // the context expires during the backoff
                crate::sim::executor::sleep_until(deadline).await;
                return fail(Err::DeadlineExceeded);
            }
            sleep(d).await;
        }
        attempt += 1;
        let remaining = deadline.saturating_sub(now());
        if remaining == 0 {
            return fail(Err::DeadlineExceeded);
        }
        let fe = conn_pod(ctx, conn);
        let c = ctx.clone();
        let b = body.clone();
        let r = call_with_timeout(remaining, async move {
            client_hop(&c).await;
            if c.p.client_lb == ClientLb::Proxy && c.p.proxy_latency > 0 {
                sleep(c.p.proxy_latency).await;
            }
            let r = frontend::handle(&c, fe, ns, api, extra_cpu, move |c2, pod| {
                (*b)(c2, pod, deadline)
            })
            .await;
            client_hop(&c).await;
            r
        })
        .await;
        match r {
            Ok(v) => {
                ctx.m.borrow_mut().client[api.idx()].record(now() - t0, None);
                return Ok(v);
            }
            Err(e) if retry.retries && retryable(e) && attempt < 1_000 => {}
            Err(e) => return fail(e),
        }
    }
}

// --- worker processes -----------------------------------------------------------------------------

/// Start all pollers of a worker process.
pub fn start_worker(ctx: &Ctx, wk: usize) {
    let (fleet, sys) = {
        let w = &ctx.workers.borrow()[wk];
        (w.fleet, ctx.p.fleets[w.fleet].system)
    };
    let f = &ctx.p.fleets[fleet];
    let _ = sys;
    for i in 0..f.wf_pollers {
        let c = ctx.clone();
        spawn(async move { wft_poller(c, wk, i).await });
    }
    for _ in 0..f.act_pollers {
        let c = ctx.clone();
        spawn(async move { activity_poller(c, wk).await });
    }
}

async fn wft_poller(ctx: Ctx, wk: usize, idx: u32) {
    // stagger start
    sleep((ctx.rand() * 200_000.0) as Time).await;
    let (fleet, slots) = {
        let w = &ctx.workers.borrow()[wk];
        (w.fleet, w.wft_slots.clone())
    };
    let f = ctx.p.fleets[fleet].clone();
    let tq = f.tq;
    let sticky_enabled = f.sticky_cache > 0;
    let _ = idx;
    let mut failures = 0u32;
    loop {
        let permit = slots.acquire().await;
        // Go SDK: poll the sticky queue when it reported a backlog, or when there are no more
        // outstanding sticky polls than regular ones (internal_task_pollers.go).
        let sticky = {
            let mut ws = ctx.workers.borrow_mut();
            let w = &mut ws[wk];
            let s =
                sticky_enabled && (w.sticky_backlog > 0 || w.pending_sticky <= w.pending_regular);
            if s {
                w.pending_sticky += 1;
            } else {
                w.pending_regular += 1;
            }
            s
        };
        let sticky_worker = sticky.then_some(wk);
        let r = sdk_call(
            &ctx,
            Conn::Worker(wk),
            f.ns,
            Api::PollWorkflowTaskQueue,
            Retry::once(f.poll_timeout),
            0.0,
            move |c, fe, deadline| async move {
                matching_poll(&c, fe, tq, TqKind::Workflow, sticky_worker, deadline).await
            },
        )
        .await;
        {
            let backlog = if sticky {
                let pid = ctx.matching.borrow().sticky.get(&wk).copied();
                pid.map(|p| ctx.matching.borrow().parts[p].backlog_len())
                    .unwrap_or(0)
            } else {
                0
            };
            let mut ws = ctx.workers.borrow_mut();
            let w = &mut ws[wk];
            if sticky {
                w.pending_sticky = w.pending_sticky.saturating_sub(1);
                w.sticky_backlog = backlog;
            } else {
                w.pending_regular = w.pending_regular.saturating_sub(1);
            }
        }
        match r {
            Ok(Some(Polled::Wft(info))) => {
                failures = 0;
                let c = ctx.clone();
                spawn(async move { process_wft(c, wk, info, permit).await });
            }
            Ok(_) => {
                failures = 0;
                drop(permit);
            }
            Err(e) => {
                drop(permit);
                failures += 1;
                let d = match e {
                    Err::ResourceExhausted(..) => {
                        backoff(&ctx, 1_000_000, 2.0, 10_000_000, failures)
                    }
                    _ => backoff(&ctx, 200_000, 2.0, 10_000_000, failures),
                };
                sleep(d).await;
            }
        }
    }
}

async fn activity_poller(ctx: Ctx, wk: usize) {
    sleep((ctx.rand() * 200_000.0) as Time).await;
    let (fleet, slots) = {
        let w = &ctx.workers.borrow()[wk];
        (w.fleet, w.act_slots.clone())
    };
    let f = ctx.p.fleets[fleet].clone();
    let tq = f.tq;
    let mut failures = 0u32;
    loop {
        let permit = slots.acquire().await;
        let r = sdk_call(
            &ctx,
            Conn::Worker(wk),
            f.ns,
            Api::PollActivityTaskQueue,
            Retry::once(f.poll_timeout),
            0.0,
            move |c, fe, deadline| async move {
                matching_poll(&c, fe, tq, TqKind::Activity, None, deadline).await
            },
        )
        .await;
        match r {
            Ok(Some(Polled::Act(info))) => {
                failures = 0;
                let c = ctx.clone();
                spawn(async move { process_activity(c, wk, info, permit).await });
            }
            Ok(_) => {
                failures = 0;
                drop(permit);
            }
            Err(e) => {
                drop(permit);
                failures += 1;
                let d = match e {
                    Err::ResourceExhausted(..) => {
                        backoff(&ctx, 1_000_000, 2.0, 10_000_000, failures)
                    }
                    _ => backoff(&ctx, 200_000, 2.0, 10_000_000, failures),
                };
                sleep(d).await;
            }
        }
    }
}

/// Deadline of worker process `wk`'s SDK calls other than polls.
fn worker_rpc_timeout(ctx: &Ctx, wk: usize) -> Time {
    let fleet = ctx.workers.borrow()[wk].fleet;
    ctx.p.fleets[fleet].rpc_timeout
}

/// Frontend → matching poll with the matching client's retry (polls retried up to 1 min).
async fn matching_poll(
    ctx: &Ctx,
    fe: PodId,
    tq: usize,
    kind: TqKind,
    sticky: Option<usize>,
    deadline: Time,
) -> Res<Option<Polled>> {
    let mut attempt = 0;
    loop {
        attempt += 1;
        let r = matching::poll(ctx, fe, tq, kind, sticky, deadline).await;
        match r {
            Err(e) if retryable(e) && now() + 2_000_000 < deadline && attempt < 5 => {
                sleep(backoff(ctx, 1_000_000, 2.0, 10_000_000, attempt)).await;
            }
            other => return other,
        }
    }
}

/// Worker-side CPU time for workflow tasks (worker process CPU if bounded).
async fn worker_compute(ctx: &Ctx, wk: usize, us: f64) {
    let (host_pod, end) = {
        let mut ws = ctx.workers.borrow_mut();
        let w = &mut ws[wk];
        match (&mut w.cpu, w.host_pod) {
            (_, Some(pod)) => (Some(pod), None),
            (Some(cpu), None) => (None, Some(cpu.schedule(us))),
            (None, None) => (None, None),
        }
    };
    if let Some(pod) = host_pod {
        cpu(ctx, pod, us).await;
    } else if let Some(end) = end {
        crate::sim::executor::sleep_until(end).await;
    } else {
        sleep(us.round() as Time).await;
    }
}

/// Decide the next commands for a workflow from its program and the WFT snapshot.
pub fn decide(ctx: &Ctx, info: &WftInfo, entity: bool) -> (Commands, f64) {
    let tp = &ctx.p.wf_types[info.wf_type];
    let mut prog: ProgState = info.prog;
    let snap = info.snap;
    let mut cmds = Commands::default();
    let mut la_time = 0.0;
    // update handlers run first and don't block: each update is accepted and completed here
    cmds.updates = info.updates;
    if entity {
        prog.signals_consumed = snap.signals_received;
        cmds.new_prog = prog;
        return (cmds, 0.0);
    }
    let mut completed_in_step = snap.completed_in_step;
    let mut failed_in_step = snap.failed_in_step;
    let mut timer_fired = snap.timer_fired;
    let mut children_done = snap.children_done;
    let mut rng = ctx.rng.borrow_mut();
    loop {
        let Some(step) = tp.steps.get(prog.step) else {
            cmds.complete = true;
            prog.done = true;
            break;
        };
        let advance = |prog: &mut ProgState| {
            prog.step += 1;
            prog.step_started = false;
            prog.step_scheduled = 0;
        };
        match step {
            StepP::Activity {
                count,
                parallel,
                tq,
                on_failure,
                ..
            } => {
                if !prog.step_started {
                    prog.step_started = true;
                    let n = if *parallel { *count } else { 1 };
                    prog.step_scheduled = n;
                    cmds.schedule_activities.push(ScheduleActivities {
                        step: prog.step,
                        member: 0,
                        tq: *tq,
                        count: n,
                    });
                    break;
                }
                if failed_in_step > 0 && *on_failure == OnFailure::Fail {
                    // the workflow returns the activity's error: FailWorkflowExecution
                    cmds.complete = true;
                    cmds.fail = true;
                    prog.done = true;
                    break;
                }
                // with `on_failure: continue` a failed activity counts as done
                let done = completed_in_step + failed_in_step;
                if done >= *count {
                    advance(&mut prog);
                    completed_in_step = 0;
                    failed_in_step = 0;
                    timer_fired = false;
                    children_done = 0;
                    continue;
                }
                if !*parallel && done >= prog.step_scheduled && prog.step_scheduled < *count {
                    prog.step_scheduled += 1;
                    cmds.schedule_activities.push(ScheduleActivities {
                        step: prog.step,
                        member: 0,
                        tq: *tq,
                        count: 1,
                    });
                }
                break;
            }
            StepP::LocalActivity { count, duration } => {
                for _ in 0..*count {
                    la_time += duration.sample(&mut rng);
                }
                cmds.markers += count;
                advance(&mut prog);
                completed_in_step = 0;
                failed_in_step = 0;
                timer_fired = false;
                children_done = 0;
                continue;
            }
            StepP::Timer(d) => {
                if !prog.step_started {
                    prog.step_started = true;
                    cmds.start_timer = Some(d.sample_us(&mut rng).max(1_000));
                    break;
                }
                if timer_fired {
                    advance(&mut prog);
                    completed_in_step = 0;
                    failed_in_step = 0;
                    timer_fired = false;
                    children_done = 0;
                    continue;
                }
                break;
            }
            StepP::Child { wf_type, count } => {
                if !prog.step_started {
                    prog.step_started = true;
                    cmds.start_children.push((*wf_type, *count));
                    break;
                }
                if children_done >= *count {
                    advance(&mut prog);
                    completed_in_step = 0;
                    failed_in_step = 0;
                    timer_fired = false;
                    children_done = 0;
                    continue;
                }
                break;
            }
            StepP::Parallel {
                members,
                activities,
                children,
            } => {
                if !prog.step_started {
                    prog.step_started = true;
                    prog.step_scheduled = *activities;
                    for (m, member) in members.iter().enumerate() {
                        match member {
                            StepP::Activity { count, tq, .. } => {
                                cmds.schedule_activities.push(ScheduleActivities {
                                    step: prog.step,
                                    member: m as u8,
                                    tq: *tq,
                                    count: *count,
                                })
                            }
                            StepP::Child { wf_type, count } => {
                                cmds.start_children.push((*wf_type, *count))
                            }
                            _ => {}
                        }
                    }
                    break;
                }
                if failed_in_step > 0 {
                    // an activity whose member doesn't say `on_failure: continue` failed for
                    // good: the workflow returns its error
                    cmds.complete = true;
                    cmds.fail = true;
                    prog.done = true;
                    break;
                }
                if completed_in_step >= *activities && children_done >= *children {
                    advance(&mut prog);
                    completed_in_step = 0;
                    failed_in_step = 0;
                    timer_fired = false;
                    children_done = 0;
                    continue;
                }
                break;
            }
            StepP::WaitSignal { count, timeout } => {
                if snap.signals_received.saturating_sub(prog.signals_consumed) >= *count {
                    prog.signals_consumed += count;
                    advance(&mut prog);
                    completed_in_step = 0;
                    failed_in_step = 0;
                    timer_fired = false;
                    children_done = 0;
                    continue;
                }
                if !prog.step_started {
                    prog.step_started = true;
                    if let Some(t) = timeout {
                        cmds.start_timer = Some(*t);
                    }
                    break;
                }
                if timeout.is_some() && timer_fired {
                    advance(&mut prog);
                    completed_in_step = 0;
                    failed_in_step = 0;
                    timer_fired = false;
                    children_done = 0;
                    continue;
                }
                break;
            }
        }
    }
    let _ = (
        completed_in_step,
        failed_in_step,
        timer_fired,
        children_done,
    );
    cmds.new_prog = prog;
    (cmds, la_time)
}

/// Process a workflow task on worker `wk`, then keep processing inline follow-up tasks.
pub async fn process_wft(ctx: Ctx, wk: usize, first: WftInfo, permit: Permit) {
    let mut info = first;
    let fleet = ctx.workers.borrow()[wk].fleet;
    let f = ctx.p.fleets[fleet].clone();
    loop {
        let (entity, ns, closed) = {
            let wfs = ctx.wfs.borrow();
            match wfs.get(info.wf, info.wgen) {
                Some(w) => (w.entity, w.ns, w.status == WfStatus::Closed),
                None => (false, 0, true),
            }
        };
        if closed {
            break;
        }
        let tp = &ctx.p.wf_types[info.wf_type];
        // sticky cache
        let key = {
            let wfs = ctx.wfs.borrow();
            wfs.get(info.wf, info.wgen).map(|w| w.key).unwrap_or(0)
        };
        let hit = if f.sticky_cache > 0 {
            let mut ws = ctx.workers.borrow_mut();
            ws[wk].sticky_cache.access(key).0
        } else {
            false
        };
        let mut compute = tp.wft_processing.sample(&mut ctx.rng.borrow_mut());
        if !(info.sticky && hit) {
            // full replay; fetch remaining history pages
            let missing = if info.sticky {
                info.total_events
            } else {
                info.total_events.saturating_sub(info.events_in_response)
            };
            let pages = missing.div_ceil(HISTORY_PAGE);
            if pages > 0 {
                let (wf, wgen) = (info.wf, info.wgen);
                let shard = ctx.wf_shard(wf, wgen);
                if let Some(shard) = shard {
                    let _ = sdk_call(
                        &ctx,
                        Conn::Worker(wk),
                        ns,
                        Api::GetWorkflowExecutionHistory,
                        Retry::call(f.rpc_timeout),
                        0.0,
                        move |c, _fe, deadline| async move {
                            history_call(&c, shard, |c2, hp| {
                                let c2 = c2.clone();
                                async move {
                                    history::get_history(&c2, hp, wf, wgen, pages, false, deadline)
                                        .await
                                }
                            })
                            .await
                            .map(|_| ())
                        },
                    )
                    .await;
                    ctx.m.borrow_mut().wf[info.wf_type].history_pages_fetched += u64::from(pages);
                }
            }
            compute += tp.replay_per_event * f64::from(info.total_events);
            let mut m = ctx.m.borrow_mut();
            if info.sticky {
                m.wf[info.wf_type].sticky_misses += 1;
            } else {
                m.wf[info.wf_type].nonsticky_wfts += 1;
            }
        } else {
            ctx.m.borrow_mut().wf[info.wf_type].sticky_hits += 1;
        }
        // scheduler (system) workflows run their own logic
        if tp.name == SCHEDULER_WF_TYPE {
            let cmds = scheduler_decide(&ctx, wk, &info).await;
            let r = respond_wft(&ctx, wk, ns, info, cmds).await;
            match r {
                Ok(Some(next)) => {
                    info = next;
                    continue;
                }
                _ => break,
            }
        }
        let (mut cmds, la_time) = decide(&ctx, &info, entity);
        if la_time > 0.0 {
            sleep(la_time.round() as Time).await;
        }
        worker_compute(&ctx, wk, compute).await;
        if f.sticky_cache > 0 && !cmds.complete {
            cmds.sticky_worker = Some(wk);
        }
        if cmds.complete && f.sticky_cache > 0 {
            ctx.workers.borrow_mut()[wk].sticky_cache.remove(key);
        }
        // eager activities: only for activities on this worker's task queue, if slots free
        let mut eager_permits = Vec::new();
        if f.eager_activities && ctx.p.namespaces[ns].enable_eager_activity {
            let slots = ctx.workers.borrow()[wk].act_slots.clone();
            let mut want: u32 = cmds
                .schedule_activities
                .iter()
                .filter(|a| a.tq == f.tq)
                .map(|a| a.count)
                .sum();
            want = want.min(3); // SDK caps eager activities per workflow task
            for _ in 0..want {
                match slots.try_acquire(1) {
                    Some(p) => eager_permits.push(p),
                    None => break,
                }
            }
            cmds.eager_activities = eager_permits.len() as u32;
            cmds.eager_tq = f.tq;
        }
        let result = respond_wft_full(&ctx, wk, ns, info, cmds).await;
        match result {
            Ok(res) => {
                for (act, p) in res.eager.into_iter().zip(eager_permits) {
                    let c = ctx.clone();
                    spawn(async move { process_activity(c, wk, act, p).await });
                }
                match res.new_wft {
                    Some(next) => {
                        info = next;
                        continue;
                    }
                    None => break,
                }
            }
            Err(_) => break,
        }
    }
    drop(permit);
}

async fn respond_wft_full(
    ctx: &Ctx,
    wk: usize,
    ns: usize,
    info: WftInfo,
    cmds: Commands,
) -> Res<history::RespondResult> {
    let Some(shard) = ctx.wf_shard(info.wf, info.wgen) else {
        return Err(Err::NotFound);
    };
    let ncmd = cmds.schedule_activities.len()
        + usize::from(cmds.start_timer.is_some())
        + usize::from(cmds.complete);
    sdk_call(
        ctx,
        Conn::Worker(wk),
        ns,
        Api::RespondWorkflowTaskCompleted,
        Retry::call(worker_rpc_timeout(ctx, wk)),
        ctx.p.costs.frontend_per_command * ncmd as f64,
        move |c, _fe, deadline| {
            let cmds = cmds.clone();
            async move {
                history_call(&c, shard, |c2, hp| {
                    let c2 = c2.clone();
                    let cmds = cmds.clone();
                    async move { history::respond_wft_completed(&c2, hp, info, cmds, deadline).await }
                })
                .await
            }
        },
    )
    .await
}

async fn respond_wft(
    ctx: &Ctx,
    wk: usize,
    ns: usize,
    info: WftInfo,
    cmds: Commands,
) -> Res<Option<WftInfo>> {
    respond_wft_full(ctx, wk, ns, info, cmds)
        .await
        .map(|r| r.new_wft)
}

/// Execute an activity task: run for its duration (heartbeating), then respond.
///
/// The Go SDK runs the activity under a context whose deadline is the earlier of start-to-close
/// from now and schedule-to-close from the first schedule (`calculateActivityDeadline`). The
/// activity is assumed to honour it and stop there; a result past the deadline is dropped
/// without a response ("Activity complete after timeout" in `internal_task_handlers.go`), and
/// history's timeout task retries or fails the attempt.
pub async fn process_activity(ctx: Ctx, wk: usize, info: ActTaskInfo, permit: Permit) {
    // whether the attempt fails, and non-retryably: planned when the activity was scheduled, or
    // drawn at the failure rate, up front when a failed attempt has its own duration
    let (duration, heartbeat, outcome, failure_rate, timeouts) =
        match ctx.p.wf_types[info.wf_type].activity(info.step, info.member) {
            Some(StepP::Activity {
                duration,
                failed_duration,
                heartbeat,
                failure_rate,
                timeouts,
                ..
            }) => {
                let mut outcome = info.plan.outcome(info.attempt);
                if outcome.is_none() && failed_duration.is_some() {
                    outcome = Some((ctx.rand() < *failure_rate, false));
                }
                let d = match (outcome, failed_duration.as_deref()) {
                    (Some((true, _)), Some(f)) => f,
                    _ => duration,
                };
                (
                    d.sample_us(&mut ctx.rng.borrow_mut()),
                    *heartbeat,
                    outcome,
                    *failure_rate,
                    *timeouts,
                )
            }
            _ => (1_000, None, None, 0.0, Default::default()),
        };
    let ns = ctx.p.wf_types[info.wf_type].ns;
    let Some(shard) = ctx.wf_shard(info.wf, info.wgen) else {
        drop(permit);
        return;
    };
    let mut deadline = Time::MAX;
    if timeouts.start_to_close > 0 {
        deadline = now().saturating_add(timeouts.start_to_close);
    }
    if timeouts.schedule_to_close > 0 {
        deadline = deadline.min(info.first_scheduled_at + timeouts.schedule_to_close);
    }
    let end = (now() + duration).min(deadline);
    if let Some(hb) = heartbeat {
        while now() + hb < end {
            sleep(hb).await;
            let r = sdk_call(
                &ctx,
                Conn::Worker(wk),
                ns,
                Api::RecordActivityTaskHeartbeat,
                Retry::call(worker_rpc_timeout(&ctx, wk)),
                0.0,
                move |c, _fe, deadline| async move {
                    history_call(&c, shard, |c2, hp| {
                        let c2 = c2.clone();
                        async move { history::heartbeat(&c2, hp, info, deadline).await }
                    })
                    .await
                },
            )
            .await;
            if r == Err(Err::NotFound) {
                // the attempt timed out (or the workflow is gone): the Go SDK cancels the
                // activity's context, the activity returns, and its result is not recorded
                drop(permit);
                return;
            }
        }
    }
    crate::sim::executor::sleep_until(end).await;
    if end >= deadline {
        // timed out on the worker: no response
        drop(permit);
        return;
    }
    let (failed, non_retryable) = outcome.unwrap_or_else(|| (ctx.rand() < failure_rate, false));
    let api = if failed {
        Api::RespondActivityTaskFailed
    } else {
        Api::RespondActivityTaskCompleted
    };
    let _ = sdk_call(
        &ctx,
        Conn::Worker(wk),
        ns,
        api,
        Retry::call(worker_rpc_timeout(&ctx, wk)),
        0.0,
        move |c, _fe, deadline| async move {
            history_call(&c, shard, |c2, hp| {
                let c2 = c2.clone();
                async move {
                    history::respond_activity(&c2, hp, info, failed, non_retryable, deadline).await
                }
            })
            .await
        },
    )
    .await;
    drop(permit);
}

// --- scheduler workflows (worker service, per-namespace worker) -----------------------------------

/// Scheduler workflow task: start due actions through the per-(namespace, host) start-rate
/// limiter (`worker.schedulerNamespaceStartWorkflowRPS`). Short waits (≤
/// `worker.schedulerLocalActivitySleepLimit`) sleep inside the local activity; longer ones
/// return RateLimited and the workflow sleeps on a timer.
async fn scheduler_decide(ctx: &Ctx, wk: usize, info: &WftInfo) -> Commands {
    let (host, ns) = {
        let w = &ctx.workers.borrow()[wk];
        (w.host_pod, ctx.p.fleets[w.fleet].ns)
    };
    let host = host.unwrap_or(0);
    cpu(ctx, host, ctx.p.costs.worker_scheduler_wft).await;
    let state = {
        let wfs = ctx.wfs.borrow();
        wfs.get(info.wf, info.wgen).and_then(|w| w.schedule)
    };
    let Some(state) = state else {
        // schedule state not attached yet (first task raced the creation): check again shortly
        return Commands {
            new_prog: info.prog,
            start_timer: Some(1_000_000),
            ..Default::default()
        };
    };
    let (sched, due, next_fire, waiting) = (
        state.sched,
        state.due_actions,
        state.next_fire,
        state.waiting_rate_limit,
    );
    let sp = ctx.p.schedules[sched].clone();
    let mut cmds = Commands {
        new_prog: info.prog,
        ..Default::default()
    };
    let mut retry_at = None;
    let mut done = 0;
    let mut still_waiting = false;
    for _ in 0..due {
        // token from the namespace bucket on this host (share of the namespace limit)
        let delay = if waiting && done == 0 {
            0
        } else {
            let share = 1.0 / ctx.p.namespaces[ns].per_ns_worker_count.max(1) as f64;
            let rate = ctx.p.namespaces[ns].scheduler_start_rps * share;
            let mut b = ctx.schedule_buckets.borrow_mut();
            let bucket = b
                .entry((ns, host))
                .or_insert_with(|| super::ratelimit::TokenBucket::new(rate, rate.ceil().max(1.0)));
            bucket.reserve_delay()
        };
        if delay > ctx.p.namespaces[ns].scheduler_la_sleep_limit {
            ctx.m.borrow_mut().schedule_rate_limited += 1;
            retry_at = Some(now() + delay);
            still_waiting = true;
            break;
        }
        if delay > 0 {
            sleep(delay).await;
        }
        // StartWorkflowExecution through the frontend from the worker service
        let target = sp.wf_type;
        let key = history::alloc_key(ctx);
        let tns = ctx.p.wf_types[target].ns;
        let shard = history::shard_for(ctx, tns, target, key);
        let r = sdk_call(
            ctx,
            Conn::Worker(wk),
            tns,
            Api::StartWorkflowExecution,
            Retry::call(Retry::DEFAULT_TIMEOUT),
            0.0,
            move |c, _fe, deadline| async move {
                history_call(&c, shard, |c2, hp| {
                    let c2 = c2.clone();
                    async move {
                        history::start_workflow(
                            &c2,
                            hp,
                            shard,
                            key,
                            target,
                            StartOrigin::Schedule,
                            false,
                            deadline,
                        )
                        .await
                    }
                })
                .await
            },
        )
        .await;
        if r.is_ok() {
            done += 1;
            let mut m = ctx.m.borrow_mut();
            m.schedule_actions += 1;
            // delay relative to the nominal fire time of this action
            let nominal = next_fire.saturating_sub(sp.interval * u64::from(due - done + 1));
            m.schedule_delay.record(now().saturating_sub(nominal));
        }
    }
    cmds.actions_done = done;
    {
        let mut wfs = ctx.wfs.borrow_mut();
        if let Some(w) = wfs.get_mut(info.wf, info.wgen)
            && let Some(s) = w.schedule.as_mut()
        {
            s.waiting_rate_limit = still_waiting;
        }
    }
    let wake = match retry_at {
        Some(r) => r.min(next_fire),
        None => next_fire,
    };
    cmds.start_timer = Some(wake.saturating_sub(now()).max(1_000));
    cmds.markers = done;
    cmds
}
