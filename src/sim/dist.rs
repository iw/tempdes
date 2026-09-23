//! Service-time / think-time distributions.
//!
//! All distributions are represented internally as a piecewise-linear function of the standard
//! normal quantile `z` in log space. A two-point spec (p50, p99) is therefore exactly a
//! log-normal, and a many-point spec (e.g. quantiles read off a Temporal `persistence_latency`
//! histogram) reproduces the observed shape. Constant, exponential and uniform distributions are
//! handled natively.

use std::fmt;

use serde::Deserialize;

use super::rng::Rng;
use crate::util::units::{Dur, parse_duration_us};

#[derive(Clone, Debug)]
pub enum Dist {
    Const(f64),
    Exp {
        mean: f64,
    },
    Uniform {
        lo: f64,
        hi: f64,
    },
    /// ln(value) as a piecewise-linear function of z = Φ⁻¹(q); linear extrapolation beyond the
    /// ends.
    LogZ {
        zs: Vec<f64>,
        ys: Vec<f64>,
        cap: f64,
    },
}

/// Acklam's rational approximation of the inverse standard normal CDF.
pub fn inv_norm_cdf(p: f64) -> f64 {
    let p = p.clamp(1e-12, 1.0 - 1e-12);
    const A: [f64; 6] = [
        -3.969_683_028_665_376e1,
        2.209_460_984_245_205e2,
        -2.759_285_104_469_687e2,
        1.383_577_518_672_69e2,
        -3.066_479_806_614_716e1,
        2.506_628_277_459_239,
    ];
    const B: [f64; 5] = [
        -5.447_609_879_822_406e1,
        1.615_858_368_580_409e2,
        -1.556_989_798_598_866e2,
        6.680_131_188_771_972e1,
        -1.328_068_155_288_572e1,
    ];
    const C: [f64; 6] = [
        -7.784_894_002_430_293e-3,
        -3.223_964_580_411_365e-1,
        -2.400_758_277_161_838,
        -2.549_732_539_343_734,
        4.374_664_141_464_968,
        2.938_163_982_698_783,
    ];
    const D: [f64; 4] = [
        7.784_695_709_041_462e-3,
        3.224_671_290_700_398e-1,
        2.445_134_137_142_996,
        3.754_408_661_907_416,
    ];
    let plow = 0.02425;
    let phigh = 1.0 - plow;
    if p < plow {
        let q = (-2.0 * p.ln()).sqrt();
        (((((C[0] * q + C[1]) * q + C[2]) * q + C[3]) * q + C[4]) * q + C[5])
            / ((((D[0] * q + D[1]) * q + D[2]) * q + D[3]) * q + 1.0)
    } else if p <= phigh {
        let q = p - 0.5;
        let r = q * q;
        (((((A[0] * r + A[1]) * r + A[2]) * r + A[3]) * r + A[4]) * r + A[5]) * q
            / (((((B[0] * r + B[1]) * r + B[2]) * r + B[3]) * r + B[4]) * r + 1.0)
    } else {
        let q = (-2.0 * (1.0 - p).ln()).sqrt();
        -(((((C[0] * q + C[1]) * q + C[2]) * q + C[3]) * q + C[4]) * q + C[5])
            / ((((D[0] * q + D[1]) * q + D[2]) * q + D[3]) * q + 1.0)
    }
}

fn interp(zs: &[f64], ys: &[f64], z: f64) -> f64 {
    let n = zs.len();
    if n == 1 {
        return ys[0];
    }
    let seg = if z <= zs[0] {
        0
    } else if z >= zs[n - 1] {
        n - 2
    } else {
        match zs.binary_search_by(|v| v.partial_cmp(&z).unwrap()) {
            Ok(i) => return ys[i],
            Err(i) => i - 1,
        }
    };
    let (z0, z1, y0, y1) = (zs[seg], zs[seg + 1], ys[seg], ys[seg + 1]);
    if (z1 - z0).abs() < 1e-12 {
        return y0;
    }
    y0 + (y1 - y0) * (z - z0) / (z1 - z0)
}

impl Dist {
    pub fn constant(v: f64) -> Self {
        Dist::Const(v.max(0.0))
    }

    /// Log-normal fitted to a median and 99th percentile.
    pub fn lognormal_p50_p99(p50: f64, p99: f64) -> Self {
        Self::from_quantiles(&[(0.5, p50), (0.99, p99.max(p50))])
    }

    /// Build from (quantile, value) points. Values must be positive for the log transform; zeros
    /// are clamped to a tiny epsilon.
    pub fn from_quantiles(points: &[(f64, f64)]) -> Self {
        let mut pts: Vec<(f64, f64)> = points
            .iter()
            .filter(|(q, v)| *q > 0.0 && *q < 1.0 && v.is_finite())
            .map(|&(q, v)| (inv_norm_cdf(q), v.max(1e-3).ln()))
            .collect();
        pts.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        pts.dedup_by(|a, b| (a.0 - b.0).abs() < 1e-9);
        // enforce monotonic non-decreasing values
        for i in 1..pts.len() {
            if pts[i].1 < pts[i - 1].1 {
                pts[i].1 = pts[i - 1].1;
            }
        }
        if pts.is_empty() {
            return Dist::Const(0.0);
        }
        if pts.len() == 1 {
            return Dist::Const(pts[0].1.exp());
        }
        let max_y = pts.last().map(|p| p.1).unwrap_or(0.0);
        Dist::LogZ {
            zs: pts.iter().map(|p| p.0).collect(),
            ys: pts.iter().map(|p| p.1).collect(),
            // never sample more than 20x the highest supplied quantile
            cap: (max_y.exp() * 20.0).max(1.0),
        }
    }

    pub fn sample(&self, rng: &mut Rng) -> f64 {
        match self {
            Dist::Const(v) => *v,
            Dist::Exp { mean } => rng.exp(*mean),
            Dist::Uniform { lo, hi } => lo + (hi - lo) * rng.f64(),
            Dist::LogZ { zs, ys, cap } => {
                let z = rng.normal().clamp(-6.0, 6.0);
                interp(zs, ys, z).exp().min(*cap)
            }
        }
    }

    #[inline]
    pub fn sample_us(&self, rng: &mut Rng) -> u64 {
        self.sample(rng).max(0.0).round() as u64
    }

    pub fn quantile(&self, q: f64) -> f64 {
        match self {
            Dist::Const(v) => *v,
            Dist::Exp { mean } => -mean * (1.0 - q.clamp(0.0, 0.999_999)).ln(),
            Dist::Uniform { lo, hi } => lo + (hi - lo) * q.clamp(0.0, 1.0),
            Dist::LogZ { zs, ys, cap } => interp(zs, ys, inv_norm_cdf(q)).exp().min(*cap),
        }
    }

    /// Mean, computed numerically for the piecewise form.
    pub fn mean(&self) -> f64 {
        match self {
            Dist::Const(v) => *v,
            Dist::Exp { mean } => *mean,
            Dist::Uniform { lo, hi } => (lo + hi) / 2.0,
            Dist::LogZ { .. } => {
                let n = 2000;
                let mut acc = 0.0;
                for i in 0..n {
                    let q = (i as f64 + 0.5) / n as f64;
                    acc += self.quantile(q);
                }
                acc / n as f64
            }
        }
    }

    /// Multiply every value by `k` (used by calibration scaling).
    pub fn scaled(&self, k: f64) -> Dist {
        match self {
            Dist::Const(v) => Dist::Const(v * k),
            Dist::Exp { mean } => Dist::Exp { mean: mean * k },
            Dist::Uniform { lo, hi } => Dist::Uniform {
                lo: lo * k,
                hi: hi * k,
            },
            Dist::LogZ { zs, ys, cap } => Dist::LogZ {
                zs: zs.clone(),
                ys: ys.iter().map(|y| y + k.max(1e-9).ln()).collect(),
                cap: cap * k,
            },
        }
    }
}

impl fmt::Display for Dist {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use crate::util::units::fmt_us;
        match self {
            Dist::Const(v) => write!(f, "{}", fmt_us(*v)),
            Dist::Exp { mean } => write!(f, "exp(mean {})", fmt_us(*mean)),
            Dist::Uniform { lo, hi } => write!(f, "uniform({}..{})", fmt_us(*lo), fmt_us(*hi)),
            Dist::LogZ { .. } => write!(
                f,
                "p50 {} / p99 {}",
                fmt_us(self.quantile(0.5)),
                fmt_us(self.quantile(0.99))
            ),
        }
    }
}

// --- config representation ----------------------------------------------------------------------

/// A duration distribution as written in scenario files.
///
/// ```yaml
/// duration: 50ms                                 # constant
/// duration: { p50: 20ms, p99: 250ms }            # log-normal
/// duration: { mean: 10ms }                       # exponential
/// duration: { min: 1ms, max: 5ms }               # uniform
/// duration: { quantiles: { 0.5: 4ms, 0.9: 9ms, 0.99: 30ms, 0.999: 90ms } }
/// ```
#[derive(Clone, Debug)]
pub enum DurDist {
    Scalar(Dur),
    Spec(DurDistSpec),
}

impl<'de> Deserialize<'de> for DurDist {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> serde::de::Visitor<'de> for V {
            type Value = DurDist;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a duration (\"5ms\") or a distribution map ({ p50: 5ms, p99: 20ms })")
            }
            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<DurDist, E> {
                parse_duration_us(v)
                    .map(|us| DurDist::Scalar(Dur(us)))
                    .map_err(E::custom)
            }
            fn visit_f64<E: serde::de::Error>(self, v: f64) -> Result<DurDist, E> {
                Ok(DurDist::Scalar(Dur(v * 1e6)))
            }
            fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<DurDist, E> {
                Ok(DurDist::Scalar(Dur(v as f64 * 1e6)))
            }
            fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<DurDist, E> {
                Ok(DurDist::Scalar(Dur(v as f64 * 1e6)))
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(self, map: A) -> Result<DurDist, A::Error> {
                let spec =
                    DurDistSpec::deserialize(serde::de::value::MapAccessDeserializer::new(map))?;
                Ok(DurDist::Spec(spec))
            }
        }
        d.deserialize_any(V)
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DurDistSpec {
    #[serde(default)]
    pub dist: Option<String>,
    #[serde(default)]
    pub value: Option<Dur>,
    #[serde(default)]
    pub mean: Option<Dur>,
    #[serde(default)]
    pub p50: Option<Dur>,
    #[serde(default)]
    pub p90: Option<Dur>,
    #[serde(default)]
    pub p95: Option<Dur>,
    #[serde(default)]
    pub p99: Option<Dur>,
    #[serde(default)]
    pub p999: Option<Dur>,
    #[serde(default)]
    pub min: Option<Dur>,
    #[serde(default)]
    pub max: Option<Dur>,
    #[serde(default)]
    pub quantiles: Option<QuantileMap>,
}

/// `{ 0.5: 4ms, 0.99: 30ms }` or `{ p50: 4ms, p99: 30ms }`.
#[derive(Clone, Debug, Default)]
pub struct QuantileMap(pub Vec<(f64, Dur)>);

impl<'de> Deserialize<'de> for QuantileMap {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> serde::de::Visitor<'de> for V {
            type Value = QuantileMap;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a map of quantile -> duration")
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut map: A,
            ) -> Result<QuantileMap, A::Error> {
                let mut out = Vec::new();
                while let Some((k, v)) = map.next_entry::<QKey, Dur>()? {
                    out.push((k.0, v));
                }
                Ok(QuantileMap(out))
            }
        }
        d.deserialize_map(V)
    }
}

/// A quantile key written either as a number (`0.99`) or a string (`"p99"`, `"99"`).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct QKey(pub f64);

impl<'de> Deserialize<'de> for QKey {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl serde::de::Visitor<'_> for V {
            type Value = QKey;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a quantile such as 0.99 or \"p99\"")
            }
            fn visit_f64<E: serde::de::Error>(self, v: f64) -> Result<QKey, E> {
                Ok(QKey(if v > 1.0 { v / 100.0 } else { v }))
            }
            fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<QKey, E> {
                self.visit_f64(v as f64)
            }
            fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<QKey, E> {
                self.visit_f64(v as f64)
            }
            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<QKey, E> {
                let t = v.trim().trim_start_matches(['p', 'P']);
                let q: f64 = t
                    .parse()
                    .map_err(|_| E::custom(format!("invalid quantile {v:?}")))?;
                // "p999" means 0.999, "p99" means 0.99, "p50" means 0.5
                let q = if q > 1.0 {
                    let digits = t.len() as i32;
                    q / 10f64.powi(digits)
                } else {
                    q
                };
                Ok(QKey(q))
            }
        }
        d.deserialize_any(V)
    }
}

impl DurDist {
    pub fn constant(d: Dur) -> Self {
        DurDist::Scalar(d)
    }

    pub fn p50_p99(p50_ms: f64, p99_ms: f64) -> Self {
        DurDist::Spec(DurDistSpec {
            p50: Some(Dur::from_ms(p50_ms)),
            p99: Some(Dur::from_ms(p99_ms)),
            ..Default::default()
        })
    }

    pub fn build(&self) -> Result<Dist, String> {
        match self {
            DurDist::Scalar(d) => Ok(Dist::constant(d.0)),
            DurDist::Spec(s) => s.build(),
        }
    }
}

impl DurDistSpec {
    pub fn build(&self) -> Result<Dist, String> {
        let kind = self.dist.as_deref().map(str::to_ascii_lowercase);
        let mut points: Vec<(f64, f64)> = Vec::new();
        for (q, v) in [
            (0.5, self.p50),
            (0.9, self.p90),
            (0.95, self.p95),
            (0.99, self.p99),
            (0.999, self.p999),
        ] {
            if let Some(v) = v {
                points.push((q, v.0));
            }
        }
        if let Some(qs) = &self.quantiles {
            for &(q, v) in &qs.0 {
                if !(q > 0.0 && q < 1.0) {
                    return Err(format!("quantile {q} out of range (0,1)"));
                }
                points.push((q, v.0));
            }
        }
        match kind.as_deref() {
            Some("constant") | Some("const") => {
                let v = self
                    .value
                    .or(self.mean)
                    .or(self.p50)
                    .ok_or("constant distribution needs `value`")?;
                Ok(Dist::constant(v.0))
            }
            Some("exponential") | Some("exp") => {
                let m = self.mean.ok_or("exponential distribution needs `mean`")?;
                Ok(Dist::Exp { mean: m.0 })
            }
            Some("uniform") => {
                let (lo, hi) = (
                    self.min.ok_or("uniform needs `min`")?,
                    self.max.ok_or("uniform needs `max`")?,
                );
                if hi.0 < lo.0 {
                    return Err("uniform: max < min".into());
                }
                Ok(Dist::Uniform { lo: lo.0, hi: hi.0 })
            }
            Some("lognormal") | Some("quantiles") | None => {
                if !points.is_empty() {
                    if points.len() == 1 {
                        // single percentile: assume a moderately tailed log-normal (p99 = 4x p50)
                        let (q, v) = points[0];
                        let z = inv_norm_cdf(q);
                        let sigma = (4.0f64).ln() / inv_norm_cdf(0.99);
                        let median = v / (sigma * z).exp();
                        return Ok(Dist::lognormal_p50_p99(median, median * 4.0));
                    }
                    return Ok(Dist::from_quantiles(&points));
                }
                if let Some(v) = self.value {
                    return Ok(Dist::constant(v.0));
                }
                if let (Some(lo), Some(hi)) = (self.min, self.max) {
                    return Ok(Dist::Uniform { lo: lo.0, hi: hi.0 });
                }
                if let Some(m) = self.mean {
                    if kind.is_none() {
                        return Ok(Dist::Exp { mean: m.0 });
                    }
                    // lognormal with only a mean: assume p99 = 4x median
                    let sigma = (4.0f64).ln() / inv_norm_cdf(0.99);
                    let median = m.0 / (sigma * sigma / 2.0).exp();
                    return Ok(Dist::lognormal_p50_p99(median, median * 4.0));
                }
                Err("distribution needs one of: value, mean, p50/p99, quantiles, min/max".into())
            }
            Some(other) => Err(format!("unknown distribution {other:?}")),
        }
    }
}

/// Parse a compact textual distribution (used by CLI overrides), e.g. `5ms`, `p50=4ms,p99=20ms`.
pub fn parse_dist_str(s: &str) -> Result<Dist, String> {
    if !s.contains('=') {
        return Ok(Dist::constant(parse_duration_us(s)?));
    }
    let mut spec = DurDistSpec::default();
    for part in s.split(',') {
        let (k, v) = part
            .split_once('=')
            .ok_or_else(|| format!("expected key=value in {s:?}"))?;
        let d = Dur(parse_duration_us(v)?);
        match k.trim() {
            "p50" => spec.p50 = Some(d),
            "p90" => spec.p90 = Some(d),
            "p95" => spec.p95 = Some(d),
            "p99" => spec.p99 = Some(d),
            "p999" => spec.p999 = Some(d),
            "mean" => spec.mean = Some(d),
            "min" => spec.min = Some(d),
            "max" => spec.max = Some(d),
            other => return Err(format!("unknown distribution key {other:?}")),
        }
    }
    spec.build()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lognormal_quantiles_roundtrip() {
        let d = Dist::lognormal_p50_p99(4_000.0, 20_000.0);
        assert!((d.quantile(0.5) - 4_000.0).abs() < 1.0);
        assert!((d.quantile(0.99) - 20_000.0).abs() / 20_000.0 < 1e-3);
        let mut r = Rng::new(9);
        let n = 200_000;
        let mut v: Vec<f64> = (0..n).map(|_| d.sample(&mut r)).collect();
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let p50 = v[n / 2];
        let p99 = v[n * 99 / 100];
        assert!((p50 - 4_000.0).abs() / 4_000.0 < 0.03, "{p50}");
        assert!((p99 - 20_000.0).abs() / 20_000.0 < 0.05, "{p99}");
        let mean: f64 = v.iter().sum::<f64>() / n as f64;
        assert!(
            (mean - d.mean()).abs() / mean < 0.03,
            "{mean} vs {}",
            d.mean()
        );
    }

    #[test]
    fn parse_forms() {
        assert!(matches!(parse_dist_str("5ms").unwrap(), Dist::Const(v) if v == 5_000.0));
        let d = parse_dist_str("p50=2ms,p99=10ms").unwrap();
        assert!((d.quantile(0.5) - 2_000.0).abs() < 1.0);
        let spec: DurDist =
            serde_saphyr::from_str("{ quantiles: { 0.5: 4ms, 0.9: 9ms, 0.99: 30ms } }").unwrap();
        let d = spec.build().unwrap();
        assert!((d.quantile(0.9) - 9_000.0).abs() < 5.0);
    }
}
