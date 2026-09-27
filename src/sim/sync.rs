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
}
