//! Deterministic pseudo random numbers (xoshiro256++ seeded through splitmix64).

#[derive(Clone, Debug)]
pub struct Rng {
    s: [u64; 4],
}

fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

impl Rng {
    pub fn new(seed: u64) -> Self {
        let mut st = seed;
        let s = [
            splitmix64(&mut st),
            splitmix64(&mut st),
            splitmix64(&mut st),
            splitmix64(&mut st),
        ];
        Rng { s }
    }

    /// Derive an independent stream (e.g. per worker process) from this seed and a label.
    pub fn fork(&self, stream: u64) -> Rng {
        let mut st = self.s[0] ^ stream.wrapping_mul(0xd1b5_4a32_d192_ed03);
        Rng::new(splitmix64(&mut st))
    }

    #[inline]
    pub fn next_u64(&mut self) -> u64 {
        let result = self.s[0]
            .wrapping_add(self.s[3])
            .rotate_left(23)
            .wrapping_add(self.s[0]);
        let t = self.s[1] << 17;
        self.s[2] ^= self.s[0];
        self.s[3] ^= self.s[1];
        self.s[1] ^= self.s[2];
        self.s[0] ^= self.s[3];
        self.s[2] ^= t;
        self.s[3] = self.s[3].rotate_left(45);
        result
    }

    /// Uniform in [0, 1).
    #[inline]
    pub fn f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 * (1.0 / (1u64 << 53) as f64)
    }

    /// Uniform in (0, 1] (safe for logarithms).
    #[inline]
    pub fn f64_open0(&mut self) -> f64 {
        1.0 - self.f64()
    }

    /// Uniform integer in [0, n). `n` must be > 0.
    #[inline]
    pub fn below(&mut self, n: u64) -> u64 {
        debug_assert!(n > 0);
        // Lemire's multiply-shift (bias is negligible for simulation purposes).
        ((u128::from(self.next_u64()) * u128::from(n)) >> 64) as u64
    }

    #[inline]
    pub fn index(&mut self, len: usize) -> usize {
        self.below(len as u64) as usize
    }

    #[inline]
    pub fn chance(&mut self, p: f64) -> bool {
        p > 0.0 && (p >= 1.0 || self.f64() < p)
    }

    /// Exponential with the given mean.
    #[inline]
    pub fn exp(&mut self, mean: f64) -> f64 {
        -mean * self.f64_open0().ln()
    }

    /// Standard normal (Box-Muller, one value per call for simplicity).
    pub fn normal(&mut self) -> f64 {
        let u1 = self.f64_open0();
        let u2 = self.f64();
        (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos()
    }

    /// Zipf-like rank in [0, n) with exponent `s` using rejection-inversion free approximation
    /// (continuous power law inverse CDF). Good enough for skewed key popularity.
    pub fn zipf(&mut self, n: u64, s: f64) -> u64 {
        if n <= 1 {
            return 0;
        }
        if (s - 1.0).abs() < 1e-9 {
            let x = ((n as f64 + 1.0).ln() * self.f64()).exp() - 1.0;
            return (x as u64).min(n - 1);
        }
        let a = 1.0 - s;
        let u = self.f64();
        let hi = (n as f64 + 1.0).powf(a);
        let x = (1.0 + u * (hi - 1.0)).powf(1.0 / a) - 1.0;
        (x as u64).min(n - 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_and_uniformish() {
        let mut a = Rng::new(7);
        let mut b = Rng::new(7);
        for _ in 0..100 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
        let mut r = Rng::new(1);
        let n = 100_000;
        let mean: f64 = (0..n).map(|_| r.f64()).sum::<f64>() / n as f64;
        assert!((mean - 0.5).abs() < 0.01);
        let em: f64 = (0..n).map(|_| r.exp(3.0)).sum::<f64>() / n as f64;
        assert!((em - 3.0).abs() < 0.1, "{em}");
    }

    #[test]
    fn zipf_is_skewed() {
        let mut r = Rng::new(3);
        let mut c0 = 0;
        for _ in 0..10_000 {
            if r.zipf(1000, 1.1) == 0 {
                c0 += 1;
            }
        }
        assert!(c0 > 500, "{c0}");
    }
}
