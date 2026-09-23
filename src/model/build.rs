//! Construct the simulated cluster from resolved parameters, run it, and apply timeline events
//! (replica changes → ring changes → shard / partition movement, dynamic config changes).

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;

use crate::sim::executor::{Executor, Time, now, sleep, sleep_until, spawn};
use crate::sim::rng::Rng;
use crate::sim::stats::TimeGauge;
use crate::sim::sync::Semaphore;
use crate::util::lru::Lru;

use super::frontend;
use super::infra::*;
use super::matching;
use super::metrics::Metrics;
use super::params::Params;
use super::ratelimit::{PriorityLimiter, TokenBucket};
use super::ring::HashRing;
use super::sdk;
use super::servers::FcfsServers;
use super::types::*;
use super::workload::{self, Rates};
use super::world::*;

fn service_replicas(p: &Params, svc: Service) -> usize {
    (match svc {
        Service::Frontend => p.replicas.frontend,
        Service::History => p.replicas.history,
        Service::Matching => p.replicas.matching,
        Service::Worker => p.replicas.worker.max(1),
    }) as usize
}

fn persistence_qps(p: &Params, svc: Service, n: usize) -> f64 {
    let k = &p.k;
    match svc {
        Service::Frontend => k.fe_persistence_max_qps,
        Service::History => frontend::per_instance(
            k.history_persistence_global_max_qps,
            k.history_persistence_max_qps,
            n,
        ),
        Service::Matching => frontend::per_instance(
            k.matching_persistence_global_max_qps,
            k.matching_persistence_max_qps,
            n,
        ),
        Service::Worker => k.worker_persistence_max_qps,
    }
}

pub fn make_pod(
    p: &Params,
    svc: Service,
    ordinal: usize,
    addr: String,
    n_same: usize,
    n_frontends: usize,
) -> Pod {
    let k = &p.k;
    let pq = persistence_qps(p, svc, n_same);
    let persist_limiter = if pq > 0.0 {
        PriorityLimiter::new(
            7,
            pq,
            pq * k.persistence_burst_ratio,
            Some(k.operator_rps_ratio),
        )
    } else {
        PriorityLimiter::unlimited(7)
    };
    let (rps_limiter, fe) = match svc {
        Service::Frontend => {
            let (host, st) = frontend::frontend_state(p, n_frontends);
            (host, Some(st))
        }
        Service::History => (
            PriorityLimiter::new(
                5,
                k.history_rps,
                k.history_rps * 2.0,
                Some(k.operator_rps_ratio),
            ),
            None,
        ),
        Service::Matching => (
            PriorityLimiter::new(
                2,
                k.matching_rps,
                k.matching_rps * 2.0,
                Some(k.operator_rps_ratio),
            ),
            None,
        ),
        Service::Worker => (PriorityLimiter::unlimited(2), None),
    };
    let hist = (svc == Service::History).then(|| HistoryHostState {
        cache: Lru::new(k.cache_max_size),
        schedulers: [
            Semaphore::new(k.scheduler_workers[0]),
            Semaphore::new(k.scheduler_workers[1]),
            Semaphore::new(k.scheduler_workers[2]),
        ],
        load_limiters: [
            TokenBucket::new(k.max_poll_host_rps[0], k.max_poll_host_rps[0]),
            TokenBucket::new(k.max_poll_host_rps[1], k.max_poll_host_rps[1]),
            TokenBucket::new(k.max_poll_host_rps[2], k.max_poll_host_rps[2]),
        ],
        pending_in_scheduler: [TimeGauge::new(), TimeGauge::new(), TimeGauge::new()],
        owned_shards: 0,
        shard_acquire: Semaphore::new(k.acquire_shard_concurrency),
    });
    Pod {
        svc,
        ordinal,
        addr,
        alive: true,
        cpu: FcfsServers::new(p.cpu[svc.idx()]),
        db_pool: Semaphore::new(p.max_conns[svc.idx()].max(1)),
        persist_limiter,
        rps_limiter,
        fe,
        hist,
    }
}

/// Parent partition in the forwarding tree (`common/tqid`): ceil(p/degree) - 1.
fn parent_of(part: u32, degree: u32) -> Option<u32> {
    if part == 0 {
        None
    } else {
        Some(part.div_ceil(degree.max(1)).saturating_sub(1))
    }
}

pub fn routing_key(ns_id: &str, tq: &str, part: u32, kind: TqKind) -> String {
    if part == 0 {
        format!("{ns_id}:{tq}:{}", kind.type_int())
    } else {
        format!("{ns_id}:/_sys/{tq}/{part}:{}", kind.type_int())
    }
}

/// Build the cluster. Creates the executor first: it resets this thread's simulated clock, which
/// everything constructed here (CPU servers, semaphores, gauges) reads.
pub fn build(p: Params) -> (Ctx, Executor) {
    let ex = Executor::new();
    let mut rng = Rng::new(p.seed);
    let mut pods: Vec<Pod> = Vec::new();
    let mut svc_pods: [Vec<PodId>; 4] = Default::default();
    let n_fe = service_replicas(&p, Service::Frontend);
    for svc in Service::ALL {
        let n = service_replicas(&p, svc);
        for (ordinal, addr) in p.addresses[svc.idx()].iter().take(n).enumerate() {
            let pod = make_pod(&p, svc, ordinal, addr.clone(), n, n_fe);
            svc_pods[svc.idx()].push(pods.len());
            pods.push(pod);
        }
    }
    let rp = p.k.ringpop_replica_points;
    let mk_ring = |svc: Service| -> (HashRing, Vec<PodId>) {
        let ids = svc_pods[svc.idx()].clone();
        let addrs: Vec<String> = ids.iter().map(|&i| pods[i].addr.clone()).collect();
        (HashRing::new(&addrs, rp), ids)
    };
    let (hring, hpods) = mk_ring(Service::History);
    let (mring, mpods) = mk_ring(Service::Matching);
    let (wring, wpods) = mk_ring(Service::Worker);

    // shards
    let mut shards = Vec::with_capacity(p.num_shards as usize);
    for id in 1..=p.num_shards {
        let owner = hpods[hring.lookup(&id.to_string()).unwrap()];
        if let Some(h) = pods[owner].hist.as_mut() {
            h.owned_shards += 1;
        }
        let ev_cap = (p.k.events_cache_max_bytes / 1024.0).max(8.0) as usize;
        shards.push(Shard {
            id,
            owner,
            epoch: 0,
            available_at: 0,
            io_sem: Semaphore::new(p.k.shard_io_concurrency),
            queues: [
                ShardQueue::new(p.k.max_poll_rps[0]),
                ShardQueue::new(p.k.max_poll_rps[1]),
                ShardQueue::new(p.k.max_poll_rps[2]),
            ],
            events_cache: Lru::new(ev_cap),
            tasks_completed_since_update: 0,
            last_shard_update: rng.below(300_000_000),
            writes: 0,
            api_requests: 0,
            persistence_ops: 0,
        });
    }

    // matching partitions
    let mut matching = MatchingState::default();
    let new_partition = |id: usize,
                         tq: usize,
                         kind: TqKind,
                         part: u32,
                         sticky_of: Option<usize>,
                         key: String,
                         host: PodId,
                         parent: Option<usize>,
                         fwd_rate: f64,
                         dispatch: Option<f64>| Partition {
        id,
        tq,
        kind,
        part,
        sticky_of,
        routing_key: key,
        host,
        parent,
        pollers: Default::default(),
        backlog_mem: Default::default(),
        backlog_db: Default::default(),
        write_queue: Default::default(),
        writer_active: false,
        reader_active: false,
        fwd_tasks_inflight: 0,
        fwd_polls_inflight: 0,
        fwd_limiter: TokenBucket::new(fwd_rate, fwd_rate.max(1.0)),
        last_poll: 0,
        dispatch_limiter: dispatch.map(|r| TokenBucket::new(r, r.max(1.0))),
        acked_since_delete: 0,
        last_delete: 0,
        range_left: 100_000,
        last_ack_update: 0,
        loaded: sticky_of.is_none(),
        sync_matches: 0,
        async_matches: 0,
        forwarded_tasks: 0,
        forwarded_polls: 0,
        remote_matches: 0,
        adds: 0,
        polls: 0,
        poll_timeouts: 0,
        writes: 0,
        write_rejects: 0,
        backlog_gauge: TimeGauge::new(),
        pollers_gauge: TimeGauge::new(),
        poll_wait: Default::default(),
        task_wait: Default::default(),
    };
    for (tqi, tq) in p.task_queues.iter().enumerate() {
        let ns_id = &p.namespaces[tq.ns].id;
        for kind in [TqKind::Workflow, TqKind::Activity] {
            let tpp = match kind {
                TqKind::Workflow => &tq.wf,
                TqKind::Activity => &tq.act,
            };
            // worker-set TaskQueueActivitiesPerSecond divides across read partitions
            let fleet_rate = p
                .fleets
                .iter()
                .filter(|f| f.tq == tqi)
                .filter_map(|f| f.activities_per_second)
                .reduce(f64::min);
            let dispatch = match (kind, fleet_rate) {
                (TqKind::Activity, Some(r)) => Some(
                    tpp.dispatch_rate
                        .unwrap_or(f64::MAX)
                        .min(r / f64::from(tpp.read_partitions)),
                ),
                _ => tpp.dispatch_rate,
            };
            let base = matching.parts.len();
            let mut ids = Vec::new();
            for part in 0..tpp.read_partitions {
                let key = routing_key(ns_id, &tq.name, part, kind);
                let host = mpods[mring.lookup(&key).unwrap()];
                let parent = parent_of(part, tpp.fwd_max_children).map(|pp| base + pp as usize);
                let id = matching.parts.len();
                matching.parts.push(new_partition(
                    id,
                    tqi,
                    kind,
                    part,
                    None,
                    key,
                    host,
                    parent,
                    tpp.fwd_max_rate,
                    dispatch,
                ));
                ids.push(id);
            }
            matching.by_tq.insert((tqi, kind), ids);
        }
    }

    // worker processes
    let mut workers = Vec::new();
    let mut per_ns_host: HashMap<usize, Vec<PodId>> = HashMap::new();
    for (fi, f) in p.fleets.iter().enumerate() {
        let hosts = if f.system {
            let ns_id = &p.namespaces[f.ns].id;
            let owners = wring.lookup_n(ns_id, f.processes as usize);
            let v: Vec<PodId> = owners.into_iter().map(|i| wpods[i]).collect();
            per_ns_host.insert(f.ns, v.clone());
            v
        } else {
            Vec::new()
        };
        for ord in 0..f.processes {
            let wk = workers.len();
            let host_pod = if f.system {
                Some(hosts[(ord as usize) % hosts.len().max(1)])
            } else {
                None
            };
            // sticky queue: one partition, routed by its unique name
            let sticky_name = format!("{}-{}-{}:{:x}", f.name, ord, wk, rng.next_u64());
            let key = routing_key(&p.namespaces[f.ns].id, &sticky_name, 0, TqKind::Workflow);
            let host = mpods[mring.lookup(&key).unwrap()];
            let pid = matching.parts.len();
            matching.parts.push(new_partition(
                pid,
                f.tq,
                TqKind::Workflow,
                0,
                Some(wk),
                key,
                host,
                None,
                10.0,
                None,
            ));
            matching.sticky.insert(wk, pid);
            workers.push(WorkerProc {
                fleet: fi,
                ordinal: ord,
                conn: usize::MAX,
                conn_expires: 0,
                rr: RoundRobin::default(),
                wft_slots: Semaphore::new(f.wf_slots),
                act_slots: Semaphore::new(f.act_slots),
                cpu: f.cpu.map(FcfsServers::new),
                sticky_cache: Lru::new((f.sticky_cache as usize).max(1)),
                sticky_partition: pid,
                last_sticky_poll: 0,
                poll_toggle: 0,
                pending_sticky: 0,
                pending_regular: 0,
                sticky_backlog: 0,
                host_pod,
            });
        }
    }

    // clients: starters per workflow type + signalers + query/visibility clients + system
    let mut n_clients = 0usize;
    for t in &p.wf_types {
        n_clients += t.starters as usize;
    }
    for s in &p.signals {
        n_clients += s.clients as usize;
    }
    n_clients += 4 + 1;
    let clients = (0..n_clients)
        .map(|_| Client {
            conn: usize::MAX,
            conn_expires: 0,
            rr: RoundRobin::default(),
        })
        .collect();

    let db = Db {
        servers: FcfsServers::new(f64::from(p.db_capacity)),
        vis_servers: FcfsServers::new(f64::from(p.vis.capacity)),
    };
    let n_types = p.wf_types.len();
    let seed = p.seed;
    let ctx = Rc::new(Sim {
        rng: RefCell::new(Rng::new(seed ^ 0xabcdef)),
        pods: RefCell::new(pods),
        svc_pods: RefCell::new(svc_pods),
        rings: RefCell::new(Rings {
            history: hring,
            matching: mring,
            worker: wring,
            history_pods: hpods,
            matching_pods: mpods,
            worker_pods: wpods,
        }),
        shards: RefCell::new(shards),
        wfs: RefCell::new(WfStore::default()),
        matching: RefCell::new(matching),
        workers: RefCell::new(workers),
        clients: RefCell::new(clients),
        db: RefCell::new(db),
        es: RefCell::new(Vec::new()),
        m: RefCell::new(Metrics::new(n_types)),
        timer_seq: RefCell::new(0),
        proxy_next: Cell::new(0),
        schedule_buckets: RefCell::new(HashMap::new()),
        measuring: RefCell::new(false),
        p,
    });
    (ctx, ex)
}

/// Run a built simulation to completion (warm-up + duration). Returns the executor stats.
pub struct RunInfo {
    pub polls: u64,
    pub end: Time,
    pub wall_ms: u128,
}

pub fn run(ctx: &Ctx, mut ex: Executor) -> RunInfo {
    let wall = std::time::Instant::now();
    let p = &ctx.p;
    // client index layout
    let mut client_base = Vec::new();
    let mut next = 0usize;
    for t in &p.wf_types {
        client_base.push(next);
        next += t.starters as usize;
    }
    let signal_base = next;
    for s in &p.signals {
        next += s.clients as usize;
    }
    let misc_base = next;
    let system_client = misc_base + 4;
    let rates = Rc::new(RefCell::new(Rates {
        start: p.wf_types.iter().map(|t| t.start_rate).collect(),
    }));

    {
        let c = ctx.clone();
        let rates = rates.clone();
        let client_base = client_base.clone();
        ex.spawn(async move {
            // workers first so pollers are waiting
            let n_workers = c.workers.borrow().len();
            for wk in 0..n_workers {
                sdk::start_worker(&c, wk);
            }
            let n_clients = c.clients.borrow().len();
            for i in 0..n_clients {
                workload::client_conn_init(&c, i);
            }
            sleep(300_000).await;
            spawn(workload::start_entities(c.clone(), system_client));
            spawn(workload::start_schedules(c.clone(), system_client));
            workload::start_generators(&c, &rates, &client_base);
            workload::start_signalers(&c, signal_base);
            workload::start_queries(&c, misc_base, 4);
            workload::start_visibility(&c, misc_base, 4);
            workload::start_sampler(&c, 5_000_000);
            workload::schedule_warmup_reset(&c, c.p.warmup);
            schedule_events(&c, &rates);
        });
    }
    let end = p.warmup + p.duration;
    let t = ex.run_until(end);
    RunInfo {
        polls: ex.polls(),
        end: t,
        wall_ms: wall.elapsed().as_millis(),
    }
}

fn schedule_events(ctx: &Ctx, rates: &Rc<RefCell<Rates>>) {
    for (i, e) in ctx.p.events.iter().enumerate() {
        let c = ctx.clone();
        let rates = rates.clone();
        let at = e.at;
        spawn(async move {
            sleep_until(at).await;
            apply_event(&c, i, &rates).await;
        });
    }
}

async fn apply_event(ctx: &Ctx, i: usize, rates: &Rc<RefCell<Rates>>) {
    let e = ctx.p.events[i].clone();
    let label = if e.label.is_empty() {
        format!("event #{}", i + 1)
    } else {
        e.label.clone()
    };
    ctx.m
        .borrow_mut()
        .notes
        .push(format!("t={:.1}s {label}", now() as f64 / 1e6));
    if let Some((t, r)) = e.start_rate {
        rates.borrow_mut().start[t] = r;
    }
    for (k, v) in &e.dc {
        apply_dc(ctx, k, v);
    }
    if let Some(r) = e.replicas {
        if let Some(n) = r.frontend {
            scale(ctx, Service::Frontend, n as usize).await;
        }
        if let Some(n) = r.matching {
            scale(ctx, Service::Matching, n as usize).await;
        }
        if let Some(n) = r.worker {
            scale(ctx, Service::Worker, n as usize).await;
        }
        if let Some(n) = r.history {
            scale(ctx, Service::History, n as usize).await;
        }
    }
}

/// Apply a dynamic config change at runtime for the limiter / capacity knobs that Temporal
/// reads dynamically.
fn apply_dc(ctx: &Ctx, key: &str, v: &crate::config::dynamic::DcValue) {
    use crate::config::dynamic::DcValue;
    let num = match v {
        DcValue::Int(i) => *i as f64,
        DcValue::Float(f) => *f,
        _ => {
            ctx.m.borrow_mut().notes.push(format!(
                "dynamic config {key}: non-numeric runtime change ignored"
            ));
            return;
        }
    };
    let k = key.to_ascii_lowercase();
    let mut applied = true;
    match k.as_str() {
        "history.shardiocconcurrency" | "history.shardioconcurrency" => {
            if ctx.p.store == crate::config::scenario::StoreKind::Cassandra {
                applied = false;
            } else {
                for s in ctx.shards.borrow().iter() {
                    s.io_sem.set_capacity(num as u32);
                }
            }
        }
        "history.rps" => {
            for pod in ctx.live_pods(Service::History) {
                ctx.pods.borrow_mut()[pod].rps_limiter.set_rate(
                    num,
                    num * 2.0,
                    Some(ctx.p.k.operator_rps_ratio),
                );
            }
        }
        "matching.rps" => {
            for pod in ctx.live_pods(Service::Matching) {
                ctx.pods.borrow_mut()[pod].rps_limiter.set_rate(
                    num,
                    num * 2.0,
                    Some(ctx.p.k.operator_rps_ratio),
                );
            }
        }
        "frontend.rps" => {
            for pod in ctx.live_pods(Service::Frontend) {
                ctx.pods.borrow_mut()[pod].rps_limiter.set_rate(
                    num,
                    num * 2.0,
                    Some(ctx.p.k.operator_rps_ratio),
                );
            }
        }
        "frontend.namespacerps" => {
            for pod in ctx.live_pods(Service::Frontend) {
                let mut pods = ctx.pods.borrow_mut();
                let fe = pods[pod].fe.as_mut().unwrap();
                for (ni, l) in fe.ns_limiters.iter_mut().enumerate() {
                    let br = ctx.p.namespaces[ni].fe_ns_burst_ratio;
                    l.set_rate(num, (num * br).ceil(), Some(ctx.p.k.operator_rps_ratio));
                }
            }
        }
        "history.persistencemaxqps"
        | "matching.persistencemaxqps"
        | "frontend.persistencemaxqps" => {
            let svc = match k.split('.').next().unwrap() {
                "history" => Service::History,
                "matching" => Service::Matching,
                _ => Service::Frontend,
            };
            for pod in ctx.live_pods(svc) {
                ctx.pods.borrow_mut()[pod].persist_limiter.set_rate(
                    num,
                    num * ctx.p.k.persistence_burst_ratio,
                    Some(ctx.p.k.operator_rps_ratio),
                );
            }
        }
        "history.transferprocessorschedulerworkercount"
        | "history.timerprocessorschedulerworkercount"
        | "history.visibilityprocessorschedulerworkercount" => {
            let c = if k.contains("transfer") {
                0
            } else if k.contains("timer") {
                1
            } else {
                2
            };
            for pod in ctx.live_pods(Service::History) {
                if let Some(h) = ctx.pods.borrow().get(pod).and_then(|p| p.hist.as_ref()) {
                    h.schedulers[c].set_capacity(num as u32);
                }
            }
        }
        _ => applied = false,
    }
    ctx.m.borrow_mut().notes.push(if applied {
        format!("  dynamic config {key} -> {v}")
    } else {
        format!("  dynamic config {key} -> {v}: not applied at runtime by the simulator")
    });
}

/// Change a service's replica count: create/remove pods, rebuild the ring and move ownership.
async fn scale(ctx: &Ctx, svc: Service, target: usize) {
    let current = ctx.live_pods(svc);
    if target == current.len() || target == 0 {
        return;
    }
    let n_fe = if svc == Service::Frontend {
        target
    } else {
        ctx.n_live(Service::Frontend)
    };
    if target > current.len() {
        for _ in current.len()..target {
            let addr = {
                let spare = &ctx.p.spare_addresses[svc.idx()];
                let used: Vec<String> = ctx.pods.borrow().iter().map(|p| p.addr.clone()).collect();
                spare.iter().find(|a| !used.contains(a)).cloned()
            };
            let Some(addr) = addr else { break };
            let ordinal = ctx.pods.borrow().iter().filter(|p| p.svc == svc).count();
            let mut pod = make_pod(&ctx.p, svc, ordinal, addr, target, n_fe);
            if let Some(fe) = pod.fe.as_mut() {
                fe.ready_at = now() + ctx.p.proxy_discovery;
            }
            let id = {
                let mut pods = ctx.pods.borrow_mut();
                pods.push(pod);
                pods.len() - 1
            };
            ctx.svc_pods.borrow_mut()[svc.idx()].push(id);
        }
    } else {
        // remove the highest ordinals (StatefulSet-style scale down)
        let remove: Vec<PodId> = current[target..].to_vec();
        for &pod in &remove {
            ctx.pods.borrow_mut()[pod].alive = false;
        }
        ctx.svc_pods.borrow_mut()[svc.idx()].retain(|p| !remove.contains(p));
    }
    ctx.m.borrow_mut().notes.push(format!(
        "  {svc} replicas {} -> {}",
        current.len(),
        ctx.n_live(svc)
    ));
    match svc {
        Service::Frontend => frontend::refresh_limits(ctx),
        Service::History => rebalance_history(ctx).await,
        Service::Matching => rebalance_matching(ctx),
        Service::Worker => rebalance_worker(ctx),
    }
    // persistence global limits depend on member counts
    let n = ctx.n_live(svc);
    let q = persistence_qps(&ctx.p, svc, n);
    for pod in ctx.live_pods(svc) {
        ctx.pods.borrow_mut()[pod].persist_limiter.set_rate(
            q,
            q * ctx.p.k.persistence_burst_ratio,
            Some(ctx.p.k.operator_rps_ratio),
        );
    }
}

fn new_ring(ctx: &Ctx, svc: Service) -> (HashRing, Vec<PodId>) {
    let ids = ctx.live_pods(svc);
    let addrs: Vec<String> = {
        let pods = ctx.pods.borrow();
        ids.iter().map(|&i| pods[i].addr.clone()).collect()
    };
    (HashRing::new(&addrs, ctx.p.k.ringpop_replica_points), ids)
}

async fn rebalance_history(ctx: &Ctx) {
    let (ring, ids) = new_ring(ctx, Service::History);
    // Old owners close moved shards as soon as they see the change; frontends and new owners
    // learn about it over ringpop gossip (system.ringpopApproximateMaxPropagationTime). Moved
    // shards serve nothing until the new owner has acquired them.
    let moving: Vec<ShardId> = {
        let shards = ctx.shards.borrow();
        shards
            .iter()
            .filter(|s| ids[ring.lookup(&s.id.to_string()).unwrap()] != s.owner)
            .map(|s| s.id)
            .collect()
    };
    {
        let mut shards = ctx.shards.borrow_mut();
        for &id in &moving {
            shards[(id - 1) as usize].available_at = Time::MAX / 4;
        }
    }
    sleep(ctx.p.k.membership_propagation).await;
    let mut moved: std::collections::BTreeMap<PodId, Vec<ShardId>> =
        std::collections::BTreeMap::new();
    {
        let mut shards = ctx.shards.borrow_mut();
        let mut pods = ctx.pods.borrow_mut();
        for s in shards.iter_mut() {
            let owner = ids[ring.lookup(&s.id.to_string()).unwrap()];
            if owner != s.owner {
                if let Some(h) = pods[s.owner].hist.as_mut() {
                    h.owned_shards = h.owned_shards.saturating_sub(1);
                }
                if let Some(h) = pods[owner].hist.as_mut() {
                    h.owned_shards += 1;
                }
                s.owner = owner;
                s.epoch += 1;
                s.available_at = Time::MAX / 4;
                moved.entry(owner).or_default().push(s.id);
            }
        }
    }
    {
        let mut r = ctx.rings.borrow_mut();
        r.history = ring;
        r.history_pods = ids;
    }
    let total: usize = moved.values().map(Vec::len).sum();
    ctx.m.borrow_mut().shard_moves += total as u64;
    ctx.m
        .borrow_mut()
        .notes
        .push(format!("  {total} history shards changed owner"));
    // each new owner acquires its shards `history.acquireShardConcurrency` at a time
    for (owner, list) in moved {
        let sem = ctx.pods.borrow()[owner]
            .hist
            .as_ref()
            .unwrap()
            .shard_acquire
            .clone();
        for shard in list {
            let c = ctx.clone();
            let sem = sem.clone();
            spawn(async move {
                let _p = sem.acquire().await;
                let _ = persist(&c, owner, PersistOp::GetOrCreateShard, Caller::ShardMgmt).await;
                let _ = persist(&c, owner, PersistOp::UpdateShard, Caller::ShardMgmt).await;
                // engine creation + queue processor start
                cpu(&c, owner, 2_000.0).await;
                sleep(c.p.k.shard_engine_start).await;
                {
                    let mut shards = c.shards.borrow_mut();
                    let s = &mut shards[(shard - 1) as usize];
                    if s.owner == owner {
                        s.available_at = now();
                    }
                }
                // restart readers for tasks left in the queues
                let has = {
                    let shards = c.shards.borrow();
                    let s = &shards[(shard - 1) as usize];
                    (
                        !s.queues[0].unloaded.is_empty(),
                        !s.queues[2].unloaded.is_empty(),
                        s.queues[1].next_timer_at(),
                    )
                };
                let _ = has;
                super::queues::commit_tasks(&c, shard, 0, 0, &[]);
            });
        }
    }
}

fn rebalance_matching(ctx: &Ctx) {
    let (ring, ids) = new_ring(ctx, Service::Matching);
    let mut moved = Vec::new();
    {
        let mut m = ctx.matching.borrow_mut();
        for p in m.parts.iter_mut() {
            let host = ids[ring.lookup(&p.routing_key).unwrap()];
            if host != p.host {
                p.host = host;
                moved.push(p.id);
            }
        }
    }
    {
        let mut r = ctx.rings.borrow_mut();
        r.matching = ring;
        r.matching_pods = ids;
    }
    ctx.m.borrow_mut().notes.push(format!(
        "  {} task queue partitions changed matching host",
        moved.len()
    ));
    matching::evict_partitions(ctx, &moved);
}

fn rebalance_worker(ctx: &Ctx) {
    let (ring, ids) = new_ring(ctx, Service::Worker);
    let mut ws = ctx.workers.borrow_mut();
    for (fi, f) in ctx.p.fleets.iter().enumerate() {
        if !f.system {
            continue;
        }
        let owners = ring.lookup_n(&ctx.p.namespaces[f.ns].id, f.processes as usize);
        let hosts: Vec<PodId> = owners.into_iter().map(|i| ids[i]).collect();
        for (ord, w) in ws.iter_mut().filter(|w| w.fleet == fi).enumerate() {
            w.host_pod = Some(hosts[ord % hosts.len().max(1)]);
        }
    }
    drop(ws);
    let mut r = ctx.rings.borrow_mut();
    r.worker = ring;
    r.worker_pods = ids;
}
