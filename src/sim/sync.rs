//! FIFO counting semaphore for simulated contention points (shard IO semaphore, workflow locks,
//! DB connection pools, scheduler worker pools, SDK slots).
//!
//! Waiters are served strictly in arrival order. A waiter that gives up (its future is dropped,
//! e.g. by [`crate::sim::executor::timeout`]) is skipped lazily when a permit is handed over.
//! Every semaphore keeps time-weighted utilisation and wait statistics, which is what the hotspot
//! report ranks.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;

use super::executor::{Elapsed, Sender, Time, now, oneshot, timeout};
use super::stats::Histogram;

#[derive(Default)]
struct State {
    capacity: u32,
    in_use: u32,
    /// High-priority waiters (API callers) are always served before low-priority ones (queue
    /// tasks), mirroring Temporal's `locks.PrioritySemaphore`.
    waiters: VecDeque<(Sender<()>, Time)>,
    low_waiters: VecDeque<(Sender<()>, Time)>,
    // statistics
    acquisitions: u64,
    timeouts: u64,
    busy_area: f64, // permit-microseconds
    queue_area: f64,
    last_change: Time,
    max_queue: usize,
    wait: Histogram,
    stats_start: Time,
}

impl State {
    fn account(&mut self) {
        let t = now();
        let dt = t.saturating_sub(self.last_change) as f64;
        self.busy_area += dt * f64::from(self.in_use);
        self.queue_area += dt * (self.waiters.len() + self.low_waiters.len()) as f64;
        self.last_change = t;
    }

    fn queued(&self) -> usize {
        self.waiters.len() + self.low_waiters.len()
    }
}

/// Waiter priority.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Prio {
    High,
    Low,
}

/// A FIFO semaphore. Cheap to clone (reference counted).
#[derive(Clone)]
pub struct Semaphore {
    st: Rc<RefCell<State>>,
}

/// RAII permit; releases on drop.
pub struct Permit {
    sem: Semaphore,
    count: u32,
}

impl Drop for Permit {
    fn drop(&mut self) {
        self.sem.release(self.count);
    }
}

impl Permit {
    /// Number of permits held.
    pub fn count(&self) -> u32 {
        self.count
    }
}

/// Snapshot of a semaphore's statistics over the measurement window.
#[derive(Clone, Debug, Default)]
pub struct SemStats {
    pub capacity: u32,
    pub acquisitions: u64,
    pub timeouts: u64,
    /// Mean number of permits in use divided by capacity.
    pub utilization: f64,
    /// Mean number of waiters.
    pub mean_queue: f64,
    pub max_queue: usize,
    pub wait_p50_us: u64,
    pub wait_p99_us: u64,
    pub wait_max_us: u64,
    pub wait_mean_us: f64,
    /// permit-microseconds held during the stats window
    pub busy_us: f64,
}

impl Semaphore {
    pub fn new(capacity: u32) -> Self {
        let st = State {
            capacity: capacity.max(1),
            last_change: now(),
            stats_start: now(),
            ..Default::default()
        };
        Semaphore {
            st: Rc::new(RefCell::new(st)),
        }
    }

    pub fn capacity(&self) -> u32 {
        self.st.borrow().capacity
    }

    pub fn in_use(&self) -> u32 {
        self.st.borrow().in_use
    }

    pub fn queue_len(&self) -> usize {
        self.st.borrow().queued()
    }

    /// Change capacity at runtime (dynamic config change, scale event).
    pub fn set_capacity(&self, capacity: u32) {
        {
            let mut st = self.st.borrow_mut();
            st.account();
            st.capacity = capacity.max(1);
        }
        self.dispatch();
    }

    /// Try to take `n` permits immediately (only if nobody is queued, preserving FIFO).
    pub fn try_acquire(&self, n: u32) -> Option<Permit> {
        let mut st = self.st.borrow_mut();
        if st.queued() == 0 && st.in_use + n <= st.capacity {
            st.account();
            st.in_use += n;
            st.acquisitions += 1;
            st.wait.record(0);
            drop(st);
            Some(Permit {
                sem: self.clone(),
                count: n,
            })
        } else {
            None
        }
    }

    /// Acquire one permit, waiting in FIFO order.
    pub async fn acquire(&self) -> Permit {
        self.acquire_prio(Prio::High).await
    }

    fn enqueue(&self, prio: Prio) -> super::executor::Receiver<()> {
        let (tx, rx) = oneshot();
        let mut st = self.st.borrow_mut();
        st.account();
        match prio {
            Prio::High => st.waiters.push_back((tx, now())),
            Prio::Low => st.low_waiters.push_back((tx, now())),
        }
        let q = st.queued();
        st.max_queue = st.max_queue.max(q);
        rx
    }

    /// Acquire one permit with the given priority (FIFO within a priority).
    pub async fn acquire_prio(&self, prio: Prio) -> Permit {
        if let Some(p) = self.try_acquire_prio(prio) {
            return p;
        }
        let rx = self.enqueue(prio);
        // The releaser transfers the permit to us before sending, so `in_use` already counts it.
        let _ = rx.await;
        Permit {
            sem: self.clone(),
            count: 1,
        }
    }

    /// Like `try_acquire(1)` but low-priority callers may not overtake queued high-priority
    /// ones, and high-priority callers only wait behind other high-priority waiters.
    fn try_acquire_prio(&self, prio: Prio) -> Option<Permit> {
        let mut st = self.st.borrow_mut();
        let blocked = match prio {
            Prio::High => !st.waiters.is_empty(),
            Prio::Low => st.queued() > 0,
        };
        if !blocked && st.in_use < st.capacity {
            st.account();
            st.in_use += 1;
            st.acquisitions += 1;
            st.wait.record(0);
            drop(st);
            Some(Permit {
                sem: self.clone(),
                count: 1,
            })
        } else {
            None
        }
    }

    /// Acquire with a deadline. On timeout the waiter is abandoned.
    pub async fn acquire_timeout(&self, dur: Time) -> Result<Permit, Elapsed> {
        self.acquire_timeout_prio(dur, Prio::High).await
    }

    /// Acquire with a deadline and priority.
    pub async fn acquire_timeout_prio(&self, dur: Time, prio: Prio) -> Result<Permit, Elapsed> {
        if let Some(p) = self.try_acquire_prio(prio) {
            return Ok(p);
        }
        if dur == 0 {
            let mut st = self.st.borrow_mut();
            st.timeouts += 1;
            return Err(Elapsed);
        }
        let rx = self.enqueue(prio);
        match timeout(dur, rx).await {
            Ok(_) => Ok(Permit {
                sem: self.clone(),
                count: 1,
            }),
            Err(e) => {
                let mut st = self.st.borrow_mut();
                st.timeouts += 1;
                st.wait.record(dur);
                Err(e)
            }
        }
    }

    fn release(&self, n: u32) {
        {
            let mut st = self.st.borrow_mut();
            st.account();
            st.in_use = st.in_use.saturating_sub(n);
        }
        self.dispatch();
    }

    /// Hand free permits to the longest-waiting live waiters.
    fn dispatch(&self) {
        loop {
            let mut st = self.st.borrow_mut();
            if st.in_use >= st.capacity {
                return;
            }
            let next = match st.waiters.pop_front() {
                Some(w) => Some(w),
                None => st.low_waiters.pop_front(),
            };
            let Some((tx, since)) = next else {
                return;
            };
            if tx.is_canceled() {
                st.account();
                continue;
            }
            st.account();
            st.in_use += 1;
            st.acquisitions += 1;
            let waited = now().saturating_sub(since);
            st.wait.record(waited);
            drop(st);
            if tx.send(()).is_err() {
                // Receiver vanished between the check and the send; return the permit.
                let mut st = self.st.borrow_mut();
                st.account();
                st.in_use -= 1;
                st.acquisitions -= 1;
            }
        }
    }

    /// Reset statistics (end of warm-up).
    pub fn reset_stats(&self) {
        let mut st = self.st.borrow_mut();
        st.account();
        st.acquisitions = 0;
        st.timeouts = 0;
        st.busy_area = 0.0;
        st.queue_area = 0.0;
        st.max_queue = st.queued();
        st.wait = Histogram::default();
        st.stats_start = now();
    }

    /// Permit-microseconds held since the statistics window started: a cheap monotonic
    /// counter for interval sampling (`tempdes ui`), without computing quantiles.
    pub fn busy_us(&self) -> f64 {
        let mut st = self.st.borrow_mut();
        st.account();
        st.busy_area
    }

    pub fn stats(&self) -> SemStats {
        let mut st = self.st.borrow_mut();
        st.account();
        let window = now().saturating_sub(st.stats_start).max(1) as f64;
        SemStats {
            capacity: st.capacity,
            acquisitions: st.acquisitions,
            timeouts: st.timeouts,
            utilization: st.busy_area / window / f64::from(st.capacity),
            mean_queue: st.queue_area / window,
            max_queue: st.max_queue,
            wait_p50_us: st.wait.quantile(0.50),
            wait_p99_us: st.wait.quantile(0.99),
            wait_max_us: st.wait.max(),
            wait_mean_us: st.wait.mean(),
            busy_us: st.busy_area,
        }
    }
}

/// A worker pool fed by interleaved weighted round robin over keyed channels, after Temporal's
/// host task scheduler (`common/tasks/interleaved_weighted_round_robin.go` in front of a FIFO
/// worker pool). A caller that finds a free worker and nobody waiting takes it at once, as
/// `TrySubmit` dispatches directly when no task is pending. Otherwise it waits in its key's
/// channel, and each freed worker goes to the next non-empty channel in the flattened IWRR
/// order: with weights high 10 and low 9, a saturated pool serves ten high tasks for every
/// nine low ones rather than all high tasks first. Cheap to clone (reference counted).
#[derive(Clone)]
pub struct WeightedSemaphore {
    st: Rc<RefCell<WState>>,
}

struct WChannel {
    key: u64,
    weight: u32,
    waiters: VecDeque<(Sender<()>, Time)>,
}

#[derive(Default)]
struct WState {
    capacity: u32,
    in_use: u32,
    channels: Vec<WChannel>,
    /// the flattened IWRR order, as indices into `channels`
    order: Vec<usize>,
    cursor: usize,
    queued: usize,
    // statistics
    acquisitions: u64,
    busy_area: f64,
    queue_area: f64,
    last_change: Time,
    max_queue: usize,
    wait: Histogram,
    stats_start: Time,
}

impl WState {
    fn account(&mut self) {
        let t = now();
        let dt = t.saturating_sub(self.last_change) as f64;
        self.busy_area += dt * f64::from(self.in_use);
        self.queue_area += dt * self.queued as f64;
        self.last_change = t;
    }

    /// `flattenWeightedChannelsLocked`: channels sorted by weight; in round `r` (from the
    /// largest weight down to 1) every channel heavier than `r - 1` gets a turn, heaviest first.
    /// Equal weights are ordered by key so runs stay deterministic.
    fn flatten(&mut self) {
        let mut by_weight: Vec<usize> = (0..self.channels.len()).collect();
        by_weight.sort_by_key(|&i| (self.channels[i].weight, self.channels[i].key));
        let max = by_weight.last().map_or(0, |&i| self.channels[i].weight);
        self.order.clear();
        for round in (0..max).rev() {
            for &i in by_weight.iter().rev() {
                if self.channels[i].weight <= round {
                    break;
                }
                self.order.push(i);
            }
        }
        self.cursor = 0;
    }

    fn channel(&mut self, key: u64, weight: u32) -> usize {
        match self.channels.iter().position(|c| c.key == key) {
            Some(i) => i,
            None => {
                self.channels.push(WChannel {
                    key,
                    weight: weight.max(1),
                    waiters: VecDeque::new(),
                });
                self.flatten();
                self.channels.len() - 1
            }
        }
    }
}

/// RAII permit of a [`WeightedSemaphore`]; releases on drop.
pub struct WeightedPermit {
    sem: WeightedSemaphore,
}

impl Drop for WeightedPermit {
    fn drop(&mut self) {
        self.sem.release();
    }
}

impl WeightedSemaphore {
    pub fn new(capacity: u32) -> Self {
        let st = WState {
            capacity: capacity.max(1),
            last_change: now(),
            stats_start: now(),
            ..Default::default()
        };
        WeightedSemaphore {
            st: Rc::new(RefCell::new(st)),
        }
    }

    pub fn capacity(&self) -> u32 {
        self.st.borrow().capacity
    }

    pub fn in_use(&self) -> u32 {
        self.st.borrow().in_use
    }

    pub fn queue_len(&self) -> usize {
        self.st.borrow().queued
    }

    /// Change the number of workers at runtime (dynamic config change).
    pub fn set_capacity(&self, capacity: u32) {
        {
            let mut st = self.st.borrow_mut();
            st.account();
            st.capacity = capacity.max(1);
        }
        self.dispatch();
    }

    /// Wait for a worker in channel `key`, whose weight is `weight` (used when the channel is
    /// first seen).
    pub async fn acquire(&self, key: u64, weight: u32) -> WeightedPermit {
        let rx = {
            let mut st = self.st.borrow_mut();
            if st.queued == 0 && st.in_use < st.capacity {
                st.account();
                st.in_use += 1;
                st.acquisitions += 1;
                st.wait.record(0);
                drop(st);
                return WeightedPermit { sem: self.clone() };
            }
            let (tx, rx) = oneshot();
            st.account();
            let i = st.channel(key, weight);
            st.channels[i].waiters.push_back((tx, now()));
            st.queued += 1;
            st.max_queue = st.max_queue.max(st.queued);
            rx
        };
        // the releaser counts the permit as ours before it sends
        let _ = rx.await;
        WeightedPermit { sem: self.clone() }
    }

    fn release(&self) {
        {
            let mut st = self.st.borrow_mut();
            st.account();
            st.in_use = st.in_use.saturating_sub(1);
        }
        self.dispatch();
    }

    /// Hand free workers to waiters in IWRR order.
    fn dispatch(&self) {
        loop {
            let mut st = self.st.borrow_mut();
            if st.in_use >= st.capacity || st.queued == 0 || st.order.is_empty() {
                return;
            }
            // the next channel in the IWRR order that has a waiter
            let n = st.order.len();
            let mut found = None;
            for k in 0..n {
                let pos = (st.cursor + k) % n;
                let ch = st.order[pos];
                if !st.channels[ch].waiters.is_empty() {
                    found = Some((pos, ch));
                    break;
                }
            }
            let Some((pos, ch)) = found else {
                st.queued = 0;
                st.cursor = 0;
                return;
            };
            st.cursor = (pos + 1) % n;
            let (tx, since) = st.channels[ch]
                .waiters
                .pop_front()
                .expect("non-empty channel");
            st.account();
            st.queued -= 1;
            if st.queued == 0 {
                // the dispatcher's pass ends when nothing is pending; the next starts afresh
                st.cursor = 0;
            }
            if tx.is_canceled() {
                continue;
            }
            st.in_use += 1;
            st.acquisitions += 1;
            let waited = now().saturating_sub(since);
            st.wait.record(waited);
            drop(st);
            if tx.send(()).is_err() {
                let mut st = self.st.borrow_mut();
                st.account();
                st.in_use -= 1;
                st.acquisitions -= 1;
            }
        }
    }

    /// Reset statistics (end of warm-up).
    pub fn reset_stats(&self) {
        let mut st = self.st.borrow_mut();
        st.account();
        st.acquisitions = 0;
        st.busy_area = 0.0;
        st.queue_area = 0.0;
        st.max_queue = st.queued;
        st.wait = Histogram::default();
        st.stats_start = now();
    }

    /// Worker-microseconds used since the statistics window started.
    pub fn busy_us(&self) -> f64 {
        let mut st = self.st.borrow_mut();
        st.account();
        st.busy_area
    }

    pub fn stats(&self) -> SemStats {
        let mut st = self.st.borrow_mut();
        st.account();
        let window = now().saturating_sub(st.stats_start).max(1) as f64;
        SemStats {
            capacity: st.capacity,
            acquisitions: st.acquisitions,
            timeouts: 0,
            utilization: st.busy_area / window / f64::from(st.capacity),
            mean_queue: st.queue_area / window,
            max_queue: st.max_queue,
            wait_p50_us: st.wait.quantile(0.50),
            wait_p99_us: st.wait.quantile(0.99),
            wait_max_us: st.wait.max(),
            wait_mean_us: st.wait.mean(),
            busy_us: st.busy_area,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sim::executor::{Executor, sleep, spawn};

    #[test]
    fn fifo_and_utilization() {
        let mut ex = Executor::new();
        let sem = Semaphore::new(1);
        let order = Rc::new(RefCell::new(Vec::new()));
        for i in 0..3u64 {
            let sem = sem.clone();
            let order = order.clone();
            ex.spawn(async move {
                sleep(i).await;
                let _p = sem.acquire().await;
                order.borrow_mut().push((i, now()));
                sleep(10).await;
            });
        }
        ex.run_until(100);
        assert_eq!(*order.borrow(), vec![(0, 0), (1, 10), (2, 20)]);
        let s = sem.stats();
        assert_eq!(s.acquisitions, 3);
        assert!((s.utilization - 0.30).abs() < 1e-9, "{}", s.utilization);
    }

    #[test]
    fn timeout_skips_abandoned_waiter() {
        let mut ex = Executor::new();
        let sem = Semaphore::new(1);
        let got = Rc::new(RefCell::new(Vec::new()));
        {
            let sem = sem.clone();
            ex.spawn(async move {
                let _p = sem.acquire().await;
                sleep(100).await;
            });
        }
        {
            let sem = sem.clone();
            let got = got.clone();
            spawn(async move {
                sleep(1).await;
                let r = sem.acquire_timeout(20).await;
                got.borrow_mut().push(("a", r.is_ok(), now()));
            });
        }
        {
            let sem = sem.clone();
            let got = got.clone();
            spawn(async move {
                sleep(2).await;
                let r = sem.acquire_timeout(500).await;
                got.borrow_mut().push(("b", r.is_ok(), now()));
            });
        }
        ex.run_until(1_000);
        assert_eq!(*got.borrow(), vec![("a", false, 21), ("b", true, 100)]);
        assert_eq!(sem.in_use(), 0);
    }

    #[test]
    fn weighted_pool_interleaves_channels_by_weight() {
        let mut ex = Executor::new();
        let pool = WeightedSemaphore::new(1);
        let order = Rc::new(RefCell::new(Vec::new()));
        // hold the only worker while 20 high (key 0, weight 10) and 20 low (key 1, weight 9)
        // tasks queue up
        {
            let pool = pool.clone();
            ex.spawn(async move {
                let _p = pool.acquire(0, 10).await;
                sleep(10).await;
            });
        }
        for i in 0..40u64 {
            let pool = pool.clone();
            let order = order.clone();
            spawn(async move {
                sleep(1).await;
                let (key, weight) = if i < 20 { (0, 10) } else { (1, 9) };
                let _p = pool.acquire(key, weight).await;
                order.borrow_mut().push(key);
                sleep(1).await;
            });
        }
        ex.run_until(1_000);
        let order = order.borrow();
        assert_eq!(order.len(), 40);
        // IWRR order for weights 10 and 9: high, then (high, low) nine times, then the rest
        let first_19: Vec<u64> = order[..19].to_vec();
        let mut expected = vec![0];
        for _ in 0..9 {
            expected.extend([0, 1]);
        }
        assert_eq!(first_19, expected);
        // over the first 19 dispatches low gets 9, not 0 as under strict priority
        assert_eq!(first_19.iter().filter(|&&k| k == 1).count(), 9);
        assert_eq!(pool.in_use(), 0);
        let s = pool.stats();
        assert_eq!(s.acquisitions, 41);
    }

    #[test]
    fn weighted_pool_dispatches_directly_when_idle() {
        let mut ex = Executor::new();
        let pool = WeightedSemaphore::new(2);
        let got = Rc::new(RefCell::new(Vec::new()));
        for i in 0..3u64 {
            let pool = pool.clone();
            let got = got.clone();
            ex.spawn(async move {
                sleep(i).await;
                let _p = pool.acquire(i, 1).await;
                got.borrow_mut().push((i, now()));
                sleep(10).await;
            });
        }
        ex.run_until(100);
        assert_eq!(*got.borrow(), vec![(0, 0), (1, 1), (2, 10)]);
    }
}
