//! Token buckets and Temporal's priority rate limiter (`common/quotas/priority_rate_limiter_impl.go`).
//!
//! Temporal's limiters are `golang.org/x/time/rate` token buckets. The priority limiter keeps one
//! bucket per priority: a request at priority `p` is admitted by bucket `p` alone and, when
//! admitted, *reserves* a token from every lower-priority bucket `p+1..`, which can drive them
//! negative. High-priority traffic therefore starves low-priority traffic (e.g. frontend P1
//! Start/Respond calls starve P4 polls) — a classic hotspot mechanism that this reproduces.

use crate::sim::executor::{Time, now};

#[derive(Clone, Debug)]
pub struct TokenBucket {
    rate: f64,  // tokens per second (<= 0: unlimited)
    burst: f64, // max tokens
    tokens: f64,
    last: Time,
}

impl TokenBucket {
    pub fn new(rate: f64, burst: f64) -> Self {
        let burst = if rate > 0.0 {
            burst.max(1.0)
        } else {
            f64::INFINITY
        };
        TokenBucket {
            rate,
            burst,
            tokens: burst,
            last: now(),
        }
    }

    pub fn unlimited() -> Self {
        TokenBucket::new(0.0, 0.0)
    }

    pub fn is_unlimited(&self) -> bool {
        self.rate <= 0.0
    }

    pub fn rate(&self) -> f64 {
        self.rate
    }

    pub fn burst(&self) -> f64 {
        self.burst
    }

    fn refill(&mut self) {
        let t = now();
        if t > self.last {
            let dt = (t - self.last) as f64 / 1e6;
            self.tokens = (self.tokens + dt * self.rate).min(self.burst);
            self.last = t;
        }
    }

    pub fn set_rate(&mut self, rate: f64, burst: f64) {
        self.refill();
        self.rate = rate;
        self.burst = if rate > 0.0 {
            burst.max(1.0)
        } else {
            f64::INFINITY
        };
        if self.tokens > self.burst {
            self.tokens = self.burst;
        }
    }

    /// `Allow()`: take one token if available.
    pub fn allow(&mut self) -> bool {
        if self.rate <= 0.0 {
            return true;
        }
        self.refill();
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }

    /// Return a token taken by `allow()`, as a multi-stage limiter does when a later stage
    /// refuses (`MultiRequestRateLimiterImpl` cancels the earlier reservations).
    pub fn refund(&mut self) {
        if self.rate > 0.0 {
            self.tokens = (self.tokens + 1.0).min(self.burst);
        }
    }

    /// `Reserve()` without waiting: always takes the token (may go negative).
    pub fn reserve(&mut self) {
        if self.rate <= 0.0 {
            return;
        }
        self.refill();
        self.tokens -= 1.0;
    }

    /// `Reserve()` returning the delay until the reservation is usable (for waiting callers).
    pub fn reserve_delay(&mut self) -> Time {
        if self.rate <= 0.0 {
            return 0;
        }
        self.refill();
        self.tokens -= 1.0;
        if self.tokens >= 0.0 {
            0
        } else {
            ((-self.tokens) / self.rate * 1e6).ceil() as Time
        }
    }
}

/// Multi-priority limiter with Temporal's reserve-from-lower-priorities semantics.
#[derive(Clone, Debug)]
pub struct PriorityLimiter {
    buckets: Vec<TokenBucket>,
    pub allowed: Vec<u64>,
    pub rejected: Vec<u64>,
}

impl PriorityLimiter {
    /// `levels` buckets, each with `rate`/`burst`; priority 0 gets `p0_ratio × rate` (the
    /// operator bucket, `system.operatorRPSRatio`).
    pub fn new(levels: usize, rate: f64, burst: f64, p0_ratio: Option<f64>) -> Self {
        let mut buckets = Vec::with_capacity(levels);
        for p in 0..levels {
            let r = match (p, p0_ratio) {
                (0, Some(ratio)) => rate * ratio,
                _ => rate,
            };
            buckets.push(TokenBucket::new(r, burst));
        }
        PriorityLimiter {
            buckets,
            allowed: vec![0; levels],
            rejected: vec![0; levels],
        }
    }

    pub fn unlimited(levels: usize) -> Self {
        Self::new(levels, 0.0, 0.0, None)
    }

    pub fn levels(&self) -> usize {
        self.buckets.len()
    }

    pub fn is_unlimited(&self) -> bool {
        self.buckets.iter().all(TokenBucket::is_unlimited)
    }

    pub fn rate(&self) -> f64 {
        self.buckets.last().map(TokenBucket::rate).unwrap_or(0.0)
    }

    pub fn allow(&mut self, priority: usize) -> bool {
        let p = priority.min(self.buckets.len() - 1);
        if !self.buckets[p].allow() {
            self.rejected[p] += 1;
            return false;
        }
        for q in p + 1..self.buckets.len() {
            self.buckets[q].reserve();
        }
        self.allowed[p] += 1;
        true
    }

    pub fn set_rate(&mut self, rate: f64, burst: f64, p0_ratio: Option<f64>) {
        for (p, b) in self.buckets.iter_mut().enumerate() {
            let r = match (p, p0_ratio) {
                (0, Some(ratio)) => rate * ratio,
                _ => rate,
            };
            b.set_rate(r, burst);
        }
    }

    pub fn reset_stats(&mut self) {
        self.allowed.iter_mut().for_each(|v| *v = 0);
        self.rejected.iter_mut().for_each(|v| *v = 0);
    }

    pub fn total_rejected(&self) -> u64 {
        self.rejected.iter().sum()
    }

    pub fn total_allowed(&self) -> u64 {
        self.allowed.iter().sum()
    }
}

/// The history task scheduler's rate limiter (`service/history/queues/scheduler_quotas.go`).
/// Each task priority has its own limiter: the task's namespace bucket, then the pod's bucket,
/// and a task is admitted only when both have a token. As in `PriorityLimiter`, a task admitted
/// at priority `p` also reserves a token from every lower priority. Buckets burst to twice their
/// rate (`NewDefaultIncomingRateLimiter`).
#[derive(Clone, Debug)]
pub struct SchedulerLimiter {
    host: Vec<TokenBucket>,
    /// `[namespace][priority]`
    ns: Vec<Vec<TokenBucket>>,
    /// refusals by the namespace bucket and by the pod bucket
    pub refused_ns: u64,
    pub refused_host: u64,
}

impl SchedulerLimiter {
    /// High, low and preemptable (`tasks.Priority`).
    pub const LEVELS: usize = 3;

    /// `ns_rates` of 0 fall back to the pod rate (`newTaskRequestRateLimiter`); a pod rate of 0
    /// is unlimited.
    pub fn new(host_rate: f64, ns_rates: &[f64]) -> Self {
        let mut l = SchedulerLimiter {
            host: vec![TokenBucket::unlimited(); Self::LEVELS],
            ns: vec![vec![TokenBucket::unlimited(); Self::LEVELS]; ns_rates.len().max(1)],
            refused_ns: 0,
            refused_host: 0,
        };
        l.set_rates(host_rate, ns_rates);
        l
    }

    pub fn set_rates(&mut self, host_rate: f64, ns_rates: &[f64]) {
        for b in &mut self.host {
            b.set_rate(host_rate, 2.0 * host_rate);
        }
        for (i, buckets) in self.ns.iter_mut().enumerate() {
            let r = match ns_rates.get(i) {
                Some(&r) if r > 0.0 => r,
                _ => host_rate,
            };
            for b in buckets {
                b.set_rate(r, 2.0 * r);
            }
        }
    }

    pub fn host_rate(&self) -> f64 {
        self.host[0].rate()
    }

    pub fn ns_rate(&self, ns: usize) -> f64 {
        self.ns[ns.min(self.ns.len() - 1)][0].rate()
    }

    /// `TrySubmit`'s check: admit the task or refuse it without taking a token.
    pub fn allow(&mut self, priority: usize, ns: usize) -> bool {
        let p = priority.min(Self::LEVELS - 1);
        let ns = ns.min(self.ns.len() - 1);
        if !self.ns[ns][p].allow() {
            self.refused_ns += 1;
            return false;
        }
        if !self.host[p].allow() {
            self.ns[ns][p].refund();
            self.refused_host += 1;
            return false;
        }
        for q in p + 1..Self::LEVELS {
            self.ns[ns][q].reserve();
            self.host[q].reserve();
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sim::executor::{Executor, sleep};
    use std::cell::RefCell;
    use std::rc::Rc;

    #[test]
    fn bucket_rate_and_burst() {
        let mut ex = Executor::new();
        let out = Rc::new(RefCell::new((0u32, 0u32)));
        let o = out.clone();
        ex.spawn(async move {
            let mut b = TokenBucket::new(100.0, 10.0);
            // burst of 10 then ~100/s
            for _ in 0..1000 {
                if b.allow() {
                    o.borrow_mut().0 += 1;
                } else {
                    o.borrow_mut().1 += 1;
                }
                sleep(1_000).await; // 1ms apart -> 1000/s offered
            }
        });
        ex.run_until(2_000_000);
        let (ok, rej) = *out.borrow();
        assert!((105..=112).contains(&ok), "ok={ok} rej={rej}");
    }

    #[test]
    fn higher_priority_starves_lower() {
        let mut ex = Executor::new();
        let out = Rc::new(RefCell::new((0u32, 0u32)));
        let o = out.clone();
        ex.spawn(async move {
            let mut l = PriorityLimiter::new(5, 100.0, 100.0, Some(0.2));
            for _ in 0..2000 {
                // 1000/s of P1 traffic and 1000/s of P4 traffic
                if l.allow(1) {
                    o.borrow_mut().0 += 1;
                }
                if l.allow(4) {
                    o.borrow_mut().1 += 1;
                }
                sleep(1_000).await;
            }
        });
        ex.run_until(3_000_000);
        let (p1, p4) = *out.borrow();
        // P1 gets its full 100/s (plus burst); P4 is starved because P1 reserves its tokens.
        assert!(p1 >= 280, "p1={p1}");
        assert!(p4 < p1 / 3, "p1={p1} p4={p4}");
    }

    #[test]
    fn scheduler_limiter_checks_namespace_then_pod() {
        let mut ex = Executor::new();
        let out = Rc::new(RefCell::new(Vec::new()));
        let o = out.clone();
        ex.spawn(async move {
            // pod 100/s; namespace 0 capped at 20/s, namespace 1 falls back to the pod rate
            let mut l = SchedulerLimiter::new(100.0, &[20.0, 0.0]);
            let (mut ns0, mut ns1) = (0u32, 0u32);
            for _ in 0..1000 {
                // 500/s offered by each namespace, at high priority
                if l.allow(0, 0) {
                    ns0 += 1;
                }
                if l.allow(0, 1) {
                    ns1 += 1;
                }
                sleep(2_000).await;
            }
            o.borrow_mut()
                .extend([ns0, ns1, l.refused_ns as u32, l.refused_host as u32]);
        });
        ex.run_until(3_000_000);
        let v = out.borrow();
        let (ns0, ns1) = (v[0], v[1]);
        // ns0: 2s at 20/s plus a burst of 40. The pod's 400 tokens (2s at 100/s plus a burst of
        // 200) are shared, so ns1 gets the ~320 that ns0 leaves.
        assert!((70..=90).contains(&ns0), "ns0={ns0}");
        assert!((290..=345).contains(&ns1), "ns1={ns1}");
        assert!(v[2] > 0 && v[3] > 0, "refusals ns={} host={}", v[2], v[3]);
        // a namespace refusal takes no pod token, so ns0 + ns1 stays within the pod's budget
        assert!(ns0 + ns1 <= 2 * 100 + 200 + 5, "{ns0} + {ns1}");
    }

    #[test]
    fn scheduler_limiter_high_priority_reserves_from_low() {
        let mut ex = Executor::new();
        let out = Rc::new(RefCell::new((0u32, 0u32)));
        let o = out.clone();
        ex.spawn(async move {
            let mut l = SchedulerLimiter::new(100.0, &[0.0]);
            for _ in 0..2000 {
                if l.allow(0, 0) {
                    o.borrow_mut().0 += 1;
                }
                if l.allow(1, 0) {
                    o.borrow_mut().1 += 1;
                }
                sleep(1_000).await;
            }
        });
        ex.run_until(3_000_000);
        let (high, low) = *out.borrow();
        assert!(high >= 280, "high={high}");
        assert!(low < high / 3, "high={high} low={low}");
    }
}
