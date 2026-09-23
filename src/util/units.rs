//! Parsing and formatting of human-friendly units used in scenario files:
//! durations (`250us`, `5ms`, `1.5s`, `2m`, `1h`, `3d`), rates (`200/s`, `12000/min`, `5/h`)
//! and byte sizes (`512`, `4KiB`, `2MB`).

use std::fmt;

use serde::Deserialize;
use serde::de::{self, Deserializer, Visitor};

use crate::sim::executor::Time;

/// Parse a duration into microseconds. A bare number is interpreted as `default_unit_us`.
pub fn parse_duration_us(s: &str) -> Result<f64, String> {
    let t = s.trim();
    if t.is_empty() {
        return Err("empty duration".into());
    }
    // Go-style compound durations like "1m30s" are accepted too.
    let mut total = 0.0;
    let mut rest = t;
    let mut parsed_any = false;
    while !rest.is_empty() {
        let num_end = rest
            .find(|c: char| {
                !(c.is_ascii_digit() || c == '.' || c == '-' || c == '+' || c == 'e' || c == 'E')
            })
            .unwrap_or(rest.len());
        // guard against unit letters that start with 'e' (none of ours do)
        let (num, after) = rest.split_at(num_end);
        if num.is_empty() {
            return Err(format!("invalid duration {s:?}"));
        }
        let v: f64 = num
            .parse()
            .map_err(|_| format!("invalid number in duration {s:?}"))?;
        let unit_end = after
            .find(|c: char| c.is_ascii_digit() || c == '.')
            .unwrap_or(after.len());
        let (unit, next) = after.split_at(unit_end);
        let mult = match unit.trim() {
            "ns" => 0.001,
            "us" | "µs" => 1.0,
            "ms" => 1_000.0,
            "s" | "" => 1_000_000.0,
            "m" | "min" => 60_000_000.0,
            "h" => 3_600_000_000.0,
            "d" => 86_400_000_000.0,
            other => return Err(format!("unknown duration unit {other:?} in {s:?}")),
        };
        if unit.trim().is_empty() && parsed_any {
            return Err(format!("missing unit in {s:?}"));
        }
        total += v * mult;
        parsed_any = true;
        rest = next;
    }
    if total < 0.0 {
        return Err(format!("negative duration {s:?}"));
    }
    Ok(total)
}

/// Parse a rate into events per second: `200/s`, `3000/min`, `10/h`, or a bare number (per s).
pub fn parse_rate_per_sec(s: &str) -> Result<f64, String> {
    let t = s.trim();
    let (num, unit) = match t.split_once('/') {
        Some((n, u)) => (n.trim(), u.trim()),
        None => (t, "s"),
    };
    let v: f64 = num
        .parse()
        .map_err(|_| format!("invalid rate {s:?} (expected e.g. 200/s)"))?;
    let div = match unit {
        "s" | "sec" | "second" => 1.0,
        "m" | "min" | "minute" => 60.0,
        "h" | "hour" => 3600.0,
        "d" | "day" => 86_400.0,
        other => return Err(format!("unknown rate unit {other:?} in {s:?}")),
    };
    if v < 0.0 {
        return Err(format!("negative rate {s:?}"));
    }
    Ok(v / div)
}

/// Parse a byte size: `512`, `4KiB`, `4KB`, `2MiB`, `1GB`.
pub fn parse_bytes(s: &str) -> Result<f64, String> {
    let t = s.trim();
    let idx = t.find(|c: char| c.is_ascii_alphabetic()).unwrap_or(t.len());
    let (num, unit) = t.split_at(idx);
    let v: f64 = num
        .trim()
        .parse()
        .map_err(|_| format!("invalid size {s:?}"))?;
    let mult = match unit.trim() {
        "" | "B" => 1.0,
        "KB" | "kB" => 1_000.0,
        "KiB" | "K" => 1024.0,
        "MB" => 1_000_000.0,
        "MiB" | "M" => 1024.0 * 1024.0,
        "GB" => 1e9,
        "GiB" | "G" => 1024.0 * 1024.0 * 1024.0,
        other => return Err(format!("unknown size unit {other:?}")),
    };
    Ok(v * mult)
}

/// Format microseconds for humans.
pub fn fmt_us(us: f64) -> String {
    if !us.is_finite() {
        return "∞".into();
    }
    if us < 1.0 {
        format!("{:.0}ns", us * 1000.0)
    } else if us < 1_000.0 {
        format!("{us:.0}µs")
    } else if us < 1_000_000.0 {
        let ms = us / 1_000.0;
        if ms < 10.0 {
            format!("{ms:.2}ms")
        } else if ms < 100.0 {
            format!("{ms:.1}ms")
        } else {
            format!("{ms:.0}ms")
        }
    } else if us < 60_000_000.0 {
        format!("{:.2}s", us / 1_000_000.0)
    } else if us < 3_600_000_000.0 {
        format!("{:.1}m", us / 60_000_000.0)
    } else {
        format!("{:.1}h", us / 3_600_000_000.0)
    }
}

pub fn fmt_rate(per_sec: f64) -> String {
    if per_sec == 0.0 {
        "0".into()
    } else if per_sec >= 10_000.0 {
        format!("{:.1}k/s", per_sec / 1000.0)
    } else if per_sec >= 100.0 {
        format!("{per_sec:.0}/s")
    } else if per_sec >= 1.0 {
        format!("{per_sec:.1}/s")
    } else {
        format!("{per_sec:.3}/s")
    }
}

pub fn fmt_pct(x: f64) -> String {
    if (0.995..1.0).contains(&x) {
        format!("{:.1}%", x * 100.0)
    } else {
        format!("{:.0}%", x * 100.0)
    }
}

// --- serde helpers -----------------------------------------------------------------------------

/// Duration value in a config file (string with unit, or number of seconds).
#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Default)]
pub struct Dur(pub f64); // microseconds

impl Dur {
    pub fn us(self) -> Time {
        self.0.round().max(0.0) as Time
    }
    pub fn secs(self) -> f64 {
        self.0 / 1e6
    }
    pub fn from_secs(s: f64) -> Self {
        Dur(s * 1e6)
    }
    pub fn from_ms(ms: f64) -> Self {
        Dur(ms * 1e3)
    }
}

impl fmt::Display for Dur {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&fmt_us(self.0))
    }
}

struct DurVisitor;

impl Visitor<'_> for DurVisitor {
    type Value = Dur;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a duration such as \"250ms\", \"1.5s\", \"5m\" or a number of seconds")
    }
    fn visit_str<E: de::Error>(self, v: &str) -> Result<Dur, E> {
        parse_duration_us(v).map(Dur).map_err(E::custom)
    }
    fn visit_f64<E: de::Error>(self, v: f64) -> Result<Dur, E> {
        Ok(Dur(v * 1e6))
    }
    fn visit_i64<E: de::Error>(self, v: i64) -> Result<Dur, E> {
        Ok(Dur(v as f64 * 1e6))
    }
    fn visit_u64<E: de::Error>(self, v: u64) -> Result<Dur, E> {
        Ok(Dur(v as f64 * 1e6))
    }
}

impl<'de> Deserialize<'de> for Dur {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        d.deserialize_any(DurVisitor)
    }
}

/// Rate value in a config file (`"200/s"` or a number per second).
#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Default)]
pub struct Rate(pub f64); // per second

struct RateVisitor;

impl Visitor<'_> for RateVisitor {
    type Value = Rate;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a rate such as \"200/s\", \"3000/min\" or a number per second")
    }
    fn visit_str<E: de::Error>(self, v: &str) -> Result<Rate, E> {
        parse_rate_per_sec(v).map(Rate).map_err(E::custom)
    }
    fn visit_f64<E: de::Error>(self, v: f64) -> Result<Rate, E> {
        Ok(Rate(v))
    }
    fn visit_i64<E: de::Error>(self, v: i64) -> Result<Rate, E> {
        Ok(Rate(v as f64))
    }
    fn visit_u64<E: de::Error>(self, v: u64) -> Result<Rate, E> {
        Ok(Rate(v as f64))
    }
}

impl<'de> Deserialize<'de> for Rate {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        d.deserialize_any(RateVisitor)
    }
}

/// Byte size in a config file.
#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Default)]
pub struct Bytes(pub f64);

struct BytesVisitor;

impl Visitor<'_> for BytesVisitor {
    type Value = Bytes;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a size such as \"4KiB\" or a number of bytes")
    }
    fn visit_str<E: de::Error>(self, v: &str) -> Result<Bytes, E> {
        parse_bytes(v).map(Bytes).map_err(E::custom)
    }
    fn visit_f64<E: de::Error>(self, v: f64) -> Result<Bytes, E> {
        Ok(Bytes(v))
    }
    fn visit_i64<E: de::Error>(self, v: i64) -> Result<Bytes, E> {
        Ok(Bytes(v as f64))
    }
    fn visit_u64<E: de::Error>(self, v: u64) -> Result<Bytes, E> {
        Ok(Bytes(v as f64))
    }
}

impl<'de> Deserialize<'de> for Bytes {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        d.deserialize_any(BytesVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations() {
        assert_eq!(parse_duration_us("250us").unwrap(), 250.0);
        assert_eq!(parse_duration_us("5ms").unwrap(), 5_000.0);
        assert_eq!(parse_duration_us("1.5s").unwrap(), 1_500_000.0);
        assert_eq!(parse_duration_us("2m").unwrap(), 120_000_000.0);
        assert_eq!(parse_duration_us("1m30s").unwrap(), 90_000_000.0);
        assert_eq!(parse_duration_us("3").unwrap(), 3_000_000.0);
        assert!(parse_duration_us("5 parsecs").is_err());
    }

    #[test]
    fn rates_and_sizes() {
        assert_eq!(parse_rate_per_sec("200/s").unwrap(), 200.0);
        assert_eq!(parse_rate_per_sec("600/min").unwrap(), 10.0);
        assert_eq!(parse_rate_per_sec("7").unwrap(), 7.0);
        assert_eq!(parse_bytes("4KiB").unwrap(), 4096.0);
        assert_eq!(parse_bytes("2MB").unwrap(), 2e6);
    }
}
