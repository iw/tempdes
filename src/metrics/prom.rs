//! Minimal Prometheus text exposition format parser (what `curl <pod>:9090/metrics` returns).
//!
//! Handles both Temporal metric naming schemes:
//! * tally reporter: `service_latency_bucket{le="0.005",...}` (seconds), counters without suffix;
//! * OpenTelemetry reporter: `service_latency_milliseconds_bucket`, `service_requests_total`.
//!
//! Names are normalised (prefix and unit/counter suffixes stripped) and the unit of each timer
//! histogram is recorded so later code always works in microseconds.

use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq)]
pub struct Sample {
    pub name: String,
    pub labels: BTreeMap<String, String>,
    pub value: f64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimeUnit {
    Seconds,
    Milliseconds,
    Unknown,
}

/// A parsed scrape, with names normalised.
#[derive(Clone, Debug, Default)]
pub struct Scrape {
    pub samples: Vec<Sample>,
    /// normalised histogram base name -> unit of its `le` boundaries and `_sum`
    pub units: BTreeMap<String, TimeUnit>,
}

fn parse_labels(s: &str) -> Result<BTreeMap<String, String>, String> {
    let mut out = BTreeMap::new();
    let b = s.as_bytes();
    let mut i = 0;
    while i < b.len() {
        while i < b.len() && (b[i] == b',' || b[i] == b' ') {
            i += 1;
        }
        if i >= b.len() {
            break;
        }
        let ks = i;
        while i < b.len() && b[i] != b'=' {
            i += 1;
        }
        let key = s[ks..i].trim().to_string();
        i += 1; // '='
        if i >= b.len() || b[i] != b'"' {
            return Err(format!("expected '\"' in labels {s:?}"));
        }
        i += 1;
        let mut val = String::new();
        while i < b.len() && b[i] != b'"' {
            if b[i] == b'\\' && i + 1 < b.len() {
                i += 1;
                val.push(match b[i] {
                    b'n' => '\n',
                    c => c as char,
                });
            } else {
                // labels are ASCII in practice; fall back to char boundary safe push
                let ch = s[i..].chars().next().unwrap();
                val.push(ch);
                i += ch.len_utf8() - 1;
            }
            i += 1;
        }
        i += 1; // closing quote
        out.insert(key, val);
    }
    Ok(out)
}

fn parse_value(s: &str) -> Option<f64> {
    match s {
        "+Inf" | "Inf" => Some(f64::INFINITY),
        "-Inf" => Some(f64::NEG_INFINITY),
        "NaN" => Some(f64::NAN),
        _ => s.parse().ok(),
    }
}

/// Strip an optional global prefix and OTEL unit / counter suffixes.
pub fn normalise_name(raw: &str, prefix: &str) -> (String, Option<TimeUnit>) {
    let mut n = raw;
    if !prefix.is_empty() {
        n = n.strip_prefix(prefix).unwrap_or(n);
        n = n.strip_prefix('_').unwrap_or(n);
    }
    let mut unit = None;
    // histogram parts keep their suffix for now
    let (base, part) = ["_bucket", "_sum", "_count"]
        .iter()
        .find_map(|p| n.strip_suffix(p).map(|b| (b, *p)))
        .unwrap_or((n, ""));
    let mut base = base.to_string();
    if let Some(b) = base.strip_suffix("_total") {
        base = b.to_string();
    }
    if let Some(b) = base.strip_suffix("_milliseconds") {
        base = b.to_string();
        unit = Some(TimeUnit::Milliseconds);
    } else if let Some(b) = base.strip_suffix("_seconds") {
        // keep Temporal's own gauges that legitimately end in _seconds
        if !b.ends_with("_age") {
            base = b.to_string();
            unit = Some(TimeUnit::Seconds);
        }
    } else if let Some(b) = base.strip_suffix("_bytes") {
        base = b.to_string();
    } else if let Some(b) = base.strip_suffix("_ratio") {
        base = b.to_string();
    }
    (format!("{base}{part}"), unit)
}

impl Scrape {
    pub fn parse(text: &str, prefix: &str) -> Result<Scrape, String> {
        let mut scrape = Scrape::default();
        for (lineno, line) in text.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let (name_labels, rest) = if let Some(open) = line.find('{') {
                let close = line[open..]
                    .find('}')
                    .map(|c| c + open)
                    .ok_or_else(|| format!("line {}: unterminated labels", lineno + 1))?;
                (&line[..=close], line[close + 1..].trim())
            } else {
                let sp = line
                    .find(char::is_whitespace)
                    .ok_or_else(|| format!("line {}: missing value", lineno + 1))?;
                (&line[..sp], line[sp..].trim())
            };
            let (raw_name, labels) = match name_labels.find('{') {
                Some(open) => (
                    &name_labels[..open],
                    parse_labels(&name_labels[open + 1..name_labels.len() - 1])
                        .map_err(|e| format!("line {}: {e}", lineno + 1))?,
                ),
                None => (name_labels, BTreeMap::new()),
            };
            let value_str = rest.split_whitespace().next().unwrap_or("");
            let Some(value) = parse_value(value_str) else {
                return Err(format!("line {}: bad value {value_str:?}", lineno + 1));
            };
            let (name, unit) = normalise_name(raw_name, prefix);
            let base = name
                .trim_end_matches("_bucket")
                .trim_end_matches("_sum")
                .trim_end_matches("_count")
                .to_string();
            if name.ends_with("_bucket") || name.ends_with("_sum") {
                let entry = scrape.units.entry(base).or_insert(TimeUnit::Unknown);
                if let Some(u) = unit {
                    *entry = u;
                }
            }
            scrape.samples.push(Sample {
                name,
                labels,
                value,
            });
        }
        // Infer units for tally-style histograms: boundaries in seconds top out at 1000,
        // milliseconds at 1e6.
        let bases: Vec<String> = scrape
            .units
            .iter()
            .filter(|(_, u)| **u == TimeUnit::Unknown)
            .map(|(b, _)| b.clone())
            .collect();
        for base in bases {
            let bucket_name = format!("{base}_bucket");
            let max_le = scrape
                .samples
                .iter()
                .filter(|s| s.name == bucket_name)
                .filter_map(|s| s.labels.get("le").and_then(|v| parse_value(v)))
                .filter(|v| v.is_finite())
                .fold(0.0f64, f64::max);
            let u = if max_le <= 0.0 {
                TimeUnit::Unknown
            } else if max_le <= 5_000.0 {
                TimeUnit::Seconds
            } else {
                TimeUnit::Milliseconds
            };
            scrape.units.insert(base, u);
        }
        Ok(scrape)
    }

    /// Element-wise `after - before` for counters/histograms (gauges keep `after`).
    pub fn delta(before: &Scrape, after: &Scrape, gauges: &dyn Fn(&str) -> bool) -> Scrape {
        let mut prev: BTreeMap<(String, Vec<(String, String)>), f64> = BTreeMap::new();
        for s in &before.samples {
            let k = (
                s.name.clone(),
                s.labels
                    .iter()
                    .map(|(a, b)| (a.clone(), b.clone()))
                    .collect(),
            );
            *prev.entry(k).or_default() += s.value;
        }
        let mut out = Scrape {
            samples: Vec::with_capacity(after.samples.len()),
            units: after.units.clone(),
        };
        for s in &after.samples {
            let k = (
                s.name.clone(),
                s.labels
                    .iter()
                    .map(|(a, b)| (a.clone(), b.clone()))
                    .collect::<Vec<_>>(),
            );
            let v = if gauges(&s.name) {
                s.value
            } else {
                let p = prev.get(&k).copied().unwrap_or(0.0);
                // counter reset (pod restart): use the raw value
                if s.value >= p { s.value - p } else { s.value }
            };
            out.samples.push(Sample {
                name: s.name.clone(),
                labels: s.labels.clone(),
                value: v,
            });
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEXT: &str = r#"
# HELP persistence_latency persistence_latency histogram
# TYPE persistence_latency histogram
persistence_latency_bucket{operation="UpdateWorkflowExecution",le="0.001"} 10
persistence_latency_bucket{operation="UpdateWorkflowExecution",le="0.005"} 80
persistence_latency_bucket{operation="UpdateWorkflowExecution",le="0.01"} 95
persistence_latency_bucket{operation="UpdateWorkflowExecution",le="1000"} 100
persistence_latency_bucket{operation="UpdateWorkflowExecution",le="+Inf"} 100
persistence_latency_sum{operation="UpdateWorkflowExecution"} 0.4
persistence_latency_count{operation="UpdateWorkflowExecution"} 100
temporal_service_requests_total{operation="StartWorkflowExecution",service_name="frontend"} 1234
service_latency_milliseconds_bucket{operation="X",le="5"} 3
"#;

    #[test]
    fn parses_and_normalises() {
        let s = Scrape::parse(TEXT, "temporal").unwrap();
        assert_eq!(s.samples.len(), 9);
        assert!(
            s.samples
                .iter()
                .any(|x| x.name == "service_requests" && x.value == 1234.0)
        );
        assert_eq!(s.units.get("persistence_latency"), Some(&TimeUnit::Seconds));
        assert_eq!(
            s.units.get("service_latency"),
            Some(&TimeUnit::Milliseconds)
        );
        let inf = s
            .samples
            .iter()
            .find(|x| x.labels.get("le").map(String::as_str) == Some("+Inf"))
            .unwrap();
        assert!(inf.value == 100.0);
    }
}
