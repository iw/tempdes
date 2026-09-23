//! First-come-first-served multi-server station with known service demand.
//!
//! Because service demands are known on arrival and the discipline is FCFS, the completion time
//! can be computed immediately (assign the earliest-free server), so each CPU burst or database
//! operation costs a single timer event. Used for pod CPU (servers = cores) and the database
//! (servers = concurrent operations it can serve).

use std::cmp::Reverse;
use std::collections::BinaryHeap;

use crate::sim::executor::{Time, now};
use crate::sim::stats::Histogram;

#[derive(Clone, Debug)]
pub struct FcfsServers {
    free_at: BinaryHeap<Reverse<Time>>,
    servers: u32,
    speed: f64,
    busy_us: f64,
    /// never reset: total scheduled work (for per-interval sampling)
    busy_abs: f64,
    window_start: Time,
    pub jobs: u64,
    pub wait: Histogram,
}

impl FcfsServers {
    /// `capacity` may be fractional (e.g. 1.5 CPU cores → 2 servers at 0.75 speed).
    pub fn new(capacity: f64) -> Self {
        let capacity = capacity.max(0.05);
        let servers = capacity.ceil().max(1.0) as u32;
        let speed = capacity / f64::from(servers);
        let t = now();
        FcfsServers {
            free_at: (0..servers).map(|_| Reverse(t)).collect(),
            servers,
            speed,
            busy_us: 0.0,
            busy_abs: 0.0,
            window_start: t,
            jobs: 0,
            wait: Histogram::default(),
        }
    }

    pub fn capacity(&self) -> f64 {
        f64::from(self.servers) * self.speed
    }

    /// Enqueue `work_us` of demand; returns the completion time.
    pub fn schedule(&mut self, work_us: f64) -> Time {
        let t = now();
        let Reverse(free) = self.free_at.pop().expect("at least one server");
        let start = free.max(t);
        let dur = (work_us / self.speed).max(0.0).round() as Time;
        let end = start + dur;
        self.free_at.push(Reverse(end));
        self.busy_us += dur as f64;
        self.busy_abs += dur as f64;
        self.jobs += 1;
        self.wait.record(start - t);
        end
    }

    /// Work (server-microseconds) scheduled but not yet executed.
    pub fn backlog_us(&self) -> f64 {
        let t = now();
        self.free_at
            .iter()
            .map(|Reverse(f)| f.saturating_sub(t) as f64)
            .sum()
    }

    /// Current queueing delay a new arrival would see.
    pub fn current_wait_us(&self) -> Time {
        let t = now();
        self.free_at
            .peek()
            .map(|Reverse(f)| f.saturating_sub(t))
            .unwrap_or(0)
    }

    /// Fraction of capacity used within the measurement window.
    pub fn utilization(&self) -> f64 {
        let w = now().saturating_sub(self.window_start) as f64;
        if w <= 0.0 {
            return 0.0;
        }
        let done = (self.busy_us - self.backlog_us()).max(0.0);
        (done / (w * f64::from(self.servers))).min(1.0)
    }

    /// CPU demand (server-microseconds scheduled) since the window started, including work still
    /// queued: the offered load even when saturated.
    pub fn demand_us(&self) -> f64 {
        self.busy_us
    }

    pub fn window_us(&self) -> f64 {
        now().saturating_sub(self.window_start) as f64
    }

    /// Server-microseconds of work completed so far (monotonic; for interval sampling).
    pub fn done_abs(&self) -> f64 {
        (self.busy_abs - self.backlog_us()).max(0.0)
    }

    pub fn servers(&self) -> u32 {
        self.servers
    }

    /// Mean busy servers (in capacity units, e.g. cores) within the window.
    pub fn mean_busy(&self) -> f64 {
        self.utilization() * self.capacity()
    }

    pub fn reset_stats(&mut self) {
        // work already queued beyond now belongs to the new window
        self.busy_us = self.backlog_us();
        self.window_start = now();
        self.jobs = 0;
        self.wait = Histogram::default();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sim::executor::{Executor, sleep, sleep_until};
    use std::cell::RefCell;
    use std::rc::Rc;

    #[test]
    fn fcfs_two_servers() {
        let mut ex = Executor::new();
        let srv = Rc::new(RefCell::new(FcfsServers::new(2.0)));
        let ends = Rc::new(RefCell::new(Vec::new()));
        for i in 0..4u64 {
            let srv = srv.clone();
            let ends = ends.clone();
            ex.spawn(async move {
                sleep(i).await;
                let end = srv.borrow_mut().schedule(10.0);
                sleep_until(end).await;
                ends.borrow_mut().push(end);
            });
        }
        ex.run_until(1_000);
        assert_eq!(*ends.borrow(), vec![10, 11, 20, 21]);
    }
}
