//! Compact log-linear latency histogram (≈6% relative error) and small helpers.
//!
//! Buckets are allocated lazily so that tens of thousands of per-shard / per-workflow
//! histograms stay cheap.

use super::executor::{Time, now};

const SUB_BITS: u32 = 4;
const SUB: u64 = 1 << SUB_BITS; // 16 sub-buckets per power of two

#[derive(Clone, Debug, Default)]
pub struct Histogram {
    counts: Vec<u32>,
    count: u64,
    sum: f64,
    min: u64,
    max: u64,
}

#[inline]
fn bucket_of(v: u64) -> usize {
    if v < SUB {
        return v as usize;
    }
    let e = 63 - v.leading_zeros(); // >= SUB_BITS
    let m = (v >> (e - SUB_BITS)) & (SUB - 1);
    ((u64::from(e - SUB_BITS + 1) * SUB) + m) as usize
}

#[inline]
fn bucket_bounds(b: usize) -> (u64, u64) {
    let b = b as u64;
    if b < SUB {
        return (b, b);
    }
    let e = b / SUB + u64::from(SUB_BITS) - 1;
    let m = b % SUB;
    let shift = e - u64::from(SUB_BITS);
    let lower = (SUB + m) << shift;
    let width = 1u64 << shift;
    (lower, lower + width - 1)
}

impl Histogram {
    pub fn record(&mut self, v: u64) {
        let b = bucket_of(v);
        if b >= self.counts.len() {
            self.counts.resize(b + 1, 0);
        }
        self.counts[b] = self.counts[b].saturating_add(1);
        if self.count == 0 || v < self.min {
            self.min = v;
        }
        if v > self.max {
            self.max = v;
        }
        self.count += 1;
        self.sum += v as f64;
    }

    pub fn count(&self) -> u64 {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn mean(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.sum / self.count as f64
        }
    }

    pub fn sum(&self) -> f64 {
        self.sum
    }

    pub fn max(&self) -> u64 {
        self.max
    }

    pub fn min(&self) -> u64 {
        self.min
    }

    /// Approximate quantile `q` in `[0, 1]` (bucket midpoint, clamped to observed min/max).
    pub fn quantile(&self, q: f64) -> u64 {
        if self.count == 0 {
            return 0;
        }
        let target = ((q.clamp(0.0, 1.0) * self.count as f64).ceil() as u64).max(1);
        let mut acc = 0u64;
        for (b, &c) in self.counts.iter().enumerate() {
            acc += u64::from(c);
            if acc >= target {
                let (lo, hi) = bucket_bounds(b);
                let mid = lo + (hi - lo) / 2;
                return mid.clamp(self.min, self.max);
            }
        }
        self.max
    }

    pub fn merge(&mut self, other: &Histogram) {
        if other.count == 0 {
            return;
        }
        if other.counts.len() > self.counts.len() {
            self.counts.resize(other.counts.len(), 0);
        }
        for (a, b) in self.counts.iter_mut().zip(other.counts.iter()) {
            *a = a.saturating_add(*b);
        }
        if self.count == 0 || other.min < self.min {
            self.min = other.min;
        }
        self.max = self.max.max(other.max);
        self.count += other.count;
        self.sum += other.sum;
    }

    /// Cumulative counts at the given upper bounds (for Prometheus `_bucket` export).
    pub fn cumulative_at(&self, bounds: &[u64]) -> Vec<u64> {
        let mut out = Vec::with_capacity(bounds.len());
        for &ub in bounds {
            let mut acc = 0u64;
            for (b, &c) in self.counts.iter().enumerate() {
                let (lo, hi) = bucket_bounds(b);
                if hi <= ub {
                    acc += u64::from(c);
                } else if lo <= ub {
                    // partial bucket: assume uniform spread
                    let frac = (ub - lo + 1) as f64 / (hi - lo + 1) as f64;
                    acc += (f64::from(c) * frac).round() as u64;
                } else {
                    break;
                }
            }
            out.push(acc);
        }
        out
    }
}

/// Time-weighted gauge (e.g. queue length, backlog size) averaged over the measurement window.
#[derive(Clone, Debug, Default)]
pub struct TimeGauge {
    value: f64,
    area: f64,
    last: Time,
    start: Time,
    max: f64,
}

impl TimeGauge {
    pub fn new() -> Self {
        let t = now();
        TimeGauge {
            last: t,
            start: t,
            ..Default::default()
        }
    }

    fn account(&mut self) {
        let t = now();
        self.area += self.value * t.saturating_sub(self.last) as f64;
        self.last = t;
    }

    pub fn set(&mut self, v: f64) {
        self.account();
        self.value = v;
        if v > self.max {
            self.max = v;
        }
    }

    pub fn add(&mut self, dv: f64) {
        let v = self.value + dv;
        self.set(v);
    }

    pub fn value(&self) -> f64 {
        self.value
    }

    pub fn mean(&mut self) -> f64 {
        self.account();
        let w = now().saturating_sub(self.start);
        if w == 0 {
            self.value
        } else {
            self.area / w as f64
        }
    }

    pub fn max(&self) -> f64 {
        self.max
    }

    pub fn reset(&mut self) {
        self.account();
        self.area = 0.0;
        self.start = now();
        self.max = self.value;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_roundtrip() {
        for v in [
            0u64,
            1,
            15,
            16,
            17,
            31,
            32,
            33,
            100,
            1_000,
            12_345,
            1 << 30,
            u64::MAX / 3,
        ] {
            let b = bucket_of(v);
            let (lo, hi) = bucket_bounds(b);
            assert!(lo <= v && v <= hi, "v={v} b={b} lo={lo} hi={hi}");
            // relative width bound
            if v >= 16 {
                assert!((hi - lo) as f64 / lo as f64 <= 1.0 / 16.0 + 1e-12);
            }
        }
    }

    #[test]
    fn quantiles_are_close() {
        let mut h = Histogram::default();
        for v in 1..=10_000u64 {
            h.record(v);
        }
        let p50 = h.quantile(0.5) as f64;
        let p99 = h.quantile(0.99) as f64;
        assert!((p50 - 5_000.0).abs() / 5_000.0 < 0.07, "{p50}");
        assert!((p99 - 9_900.0).abs() / 9_900.0 < 0.07, "{p99}");
        assert_eq!(h.count(), 10_000);
        let c = h.cumulative_at(&[100, 1_000, 20_000]);
        assert!((c[0] as i64 - 100).abs() <= 8, "{c:?}");
        assert_eq!(c[2], 10_000);
    }
}
