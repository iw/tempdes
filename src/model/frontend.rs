//! Frontend service: admission (interceptor order ConcurrentRequestLimit → NamespaceRateLimit →
//! host RateLimit, `service/frontend/fx.go`) followed by the handler body.
//!
//! * `frontend.namespaceCount` limits concurrent long-running requests (polls, queries, history
//!   long polls, update-with-start) per namespace per API per instance (or
//!   `frontend.globalNamespaceCount` / #frontends).
//! * Namespace RPS (`frontend.namespaceRPS`, or `frontend.globalNamespaceRPS` / #frontends) and
//!   host RPS (`frontend.rps`) are priority limiters: P1 calls reserve tokens from lower
//!   priorities, so polls (P4) are throttled first under load, and history long polls (P5 in
//!   the namespace limiter) before them.
//! * Visibility APIs use separate buckets (`frontend.namespaceRPS.visibility` = 10/s default).

use std::future::Future;

use crate::sim::executor::{now, sleep};

use super::infra::cpu;
use super::ratelimit::{PriorityLimiter, TokenBucket};
use super::types::*;
use super::world::*;

/// Decrements the concurrent-request counter when the handler finishes.
pub struct ConcurrencyGuard {
    ctx: Ctx,
    pod: PodId,
    key: (usize, Api),
}

impl Drop for ConcurrencyGuard {
    fn drop(&mut self) {
        if let Ok(mut pods) = self.ctx.pods.try_borrow_mut()
            && let Some(fe) = pods[self.pod].fe.as_mut()
            && let Some(c) = fe.concurrent.get_mut(&self.key)
        {
            *c -= 1;
        }
    }
}

/// Per-instance share of a cluster-global limit.
pub fn per_instance(global: f64, per_instance: f64, n: usize) -> f64 {
    if global > 0.0 && n > 0 {
        global / n as f64
    } else {
        per_instance
    }
}

/// Build limiters for a frontend pod given the current number of frontends.
pub fn frontend_state(
    ctx_p: &super::params::Params,
    n_frontends: usize,
) -> (PriorityLimiter, FrontendState) {
    let k = &ctx_p.k;
    let rps = per_instance(k.fe_global_rps, k.fe_rps, n_frontends);
    let host = PriorityLimiter::new(6, rps, rps * 2.0, Some(k.operator_rps_ratio));
    let vis = PriorityLimiter::new(2, rps, rps * 2.0, Some(k.operator_rps_ratio));
    let ns_limiters = ctx_p
        .namespaces
        .iter()
        .map(|ns| {
            let r = per_instance(ns.fe_global_ns_rps, ns.fe_ns_rps, n_frontends);
            PriorityLimiter::new(
                6,
                r,
                (r * ns.fe_ns_burst_ratio).ceil().max(1.0),
                Some(k.operator_rps_ratio),
            )
        })
        .collect();
    let ns_vis_limiters = ctx_p
        .namespaces
        .iter()
        .map(|ns| {
            let r = per_instance(ns.fe_global_vis_rps, ns.fe_vis_rps, n_frontends);
            TokenBucket::new(r, (r * ns.fe_vis_burst_ratio).ceil().max(1.0))
        })
        .collect();
    (
        host,
        FrontendState {
            vis_limiter: vis,
            ns_limiters,
            ns_vis_limiters,
            concurrent: Default::default(),
            concurrent_max: Default::default(),
            poll_lb: Default::default(),
            connections: 0,
            ready_at: 0,
        },
    )
}

/// Recompute global-limit shares after a frontend membership change.
pub fn refresh_limits(ctx: &Ctx) {
    let n = ctx.n_live(Service::Frontend);
    let live = ctx.live_pods(Service::Frontend);
    let k = &ctx.p.k;
    let mut pods = ctx.pods.borrow_mut();
    for pod in live {
        let rps = per_instance(k.fe_global_rps, k.fe_rps, n);
        pods[pod]
            .rps_limiter
            .set_rate(rps, rps * 2.0, Some(k.operator_rps_ratio));
        let fe = pods[pod].fe.as_mut().unwrap();
        fe.vis_limiter
            .set_rate(rps, rps * 2.0, Some(k.operator_rps_ratio));
        for (i, ns) in ctx.p.namespaces.iter().enumerate() {
            let r = per_instance(ns.fe_global_ns_rps, ns.fe_ns_rps, n);
            fe.ns_limiters[i].set_rate(
                r,
                (r * ns.fe_ns_burst_ratio).ceil().max(1.0),
                Some(k.operator_rps_ratio),
            );
            let rv = per_instance(ns.fe_global_vis_rps, ns.fe_vis_rps, n);
            fe.ns_vis_limiters[i].set_rate(rv, (rv * ns.fe_vis_burst_ratio).ceil().max(1.0));
        }
    }
}

fn reject(ctx: &Ctx, limiter: &str, pod: PodId, ns: usize) {
    let addr = ctx.pods.borrow()[pod].addr.clone();
    ctx.m
        .borrow_mut()
        .reject(limiter, format!("{addr} ns={}", ctx.p.namespaces[ns].name));
}

/// Admission control. Returns a guard for long-running requests.
pub async fn admit(ctx: &Ctx, pod: PodId, ns: usize, api: Api) -> Res<Option<ConcurrencyGuard>> {
    let mut guard = None;
    if api.is_long_running() {
        let n = ctx.n_live(Service::Frontend);
        let nsp = &ctx.p.namespaces[ns];
        let quota = if nsp.fe_global_ns_count > 0 {
            ((nsp.fe_global_ns_count as f64) / n.max(1) as f64).ceil() as i64
        } else {
            nsp.fe_ns_count
        };
        let over = {
            let mut pods = ctx.pods.borrow_mut();
            let fe = pods[pod].fe.as_mut().unwrap();
            let c = fe.concurrent.entry((ns, api)).or_insert(0);
            *c += 1;
            let v = *c;
            let mx = fe.concurrent_max.entry((ns, api)).or_insert(0);
            *mx = (*mx).max(v);
            if v > quota {
                *fe.concurrent.get_mut(&(ns, api)).unwrap() -= 1;
                true
            } else {
                false
            }
        };
        if over {
            reject(ctx, "frontend.namespaceCount", pod, ns);
            return Err(Err::ResourceExhausted(
                ReCause::ConcurrentLimit,
                Scope::Namespace,
            ));
        }
        guard = Some(ConcurrencyGuard {
            ctx: ctx.clone(),
            pod,
            key: (ns, api),
        });
    }
    // namespace limiter
    let prio = api.namespace_priority();
    loop {
        let ok = {
            let mut pods = ctx.pods.borrow_mut();
            let fe = pods[pod].fe.as_mut().unwrap();
            if api.is_visibility() {
                fe.ns_vis_limiters[ns].allow()
            } else {
                fe.ns_limiters[ns].allow(prio)
            }
        };
        if ok {
            break;
        }
        let wait_allowed = ctx.p.k.fe_poll_wait_for_ns_token
            && matches!(api, Api::PollWorkflowTaskQueue | Api::PollActivityTaskQueue);
        if wait_allowed {
            // wait for a token (bounded by the poll deadline, approximated as 50s)
            sleep(50_000).await;
            continue;
        }
        let limiter = if api.is_visibility() {
            "frontend.namespaceRPS.visibility"
        } else {
            "frontend.namespaceRPS"
        };
        reject(ctx, limiter, pod, ns);
        return Err(Err::ResourceExhausted(ReCause::RpsLimit, Scope::Namespace));
    }
    // host limiter
    let ok = {
        let mut pods = ctx.pods.borrow_mut();
        if api.is_visibility() {
            pods[pod].fe.as_mut().unwrap().vis_limiter.allow(1)
        } else {
            pods[pod].rps_limiter.allow(api.host_priority())
        }
    };
    if !ok {
        reject(ctx, "frontend.rps", pod, ns);
        return Err(Err::ResourceExhausted(ReCause::RpsLimit, Scope::System));
    }
    Ok(guard)
}

/// Run an API on frontend `pod`: admission, CPU, then `body`. Records `service_*` metrics.
pub async fn handle<T, F, Fut>(
    ctx: &Ctx,
    pod: PodId,
    ns: usize,
    api: Api,
    extra_cpu: f64,
    body: F,
) -> Res<T>
where
    F: FnOnce(Ctx, PodId) -> Fut,
    Fut: Future<Output = Res<T>>,
{
    let t0 = now();
    *ctx.m
        .borrow_mut()
        .fe_ns_requests
        .entry((pod, ns, api.is_visibility()))
        .or_default() += 1;
    let r = async {
        let _guard = admit(ctx, pod, ns, api).await?;
        cpu(ctx, pod, ctx.p.costs.frontend[api.idx()] + extra_cpu).await;
        body(ctx.clone(), pod).await
    }
    .await;
    ctx.m
        .borrow_mut()
        .fe_op(pod, api)
        .record(now() - t0, r.as_ref().err().copied());
    r
}
