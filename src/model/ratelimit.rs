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
}
