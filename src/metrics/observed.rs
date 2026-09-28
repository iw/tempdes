//! Observed Temporal metrics used to calibrate and validate the simulation.
//!
//! Three ways to provide them:
//! 1. A YAML observations file where each entry names a Temporal metric, a label filter and a
//!    value (rate, gauge value, quantiles or raw histogram buckets). Typically filled in from
//!    Grafana / PromQL (`tempdes metrics queries` prints the queries).
//! 2. One Prometheus scrape (`/metrics` text). Histograms are usable directly (cumulative since
//!    process start); counters need a second scrape.
//! 3. Two Prometheus scrapes plus the time between them: counters become rates.
//!
//! Everything is normalised into [`Observations`], queried by metric name + label subset.

use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{Context, bail};
use serde::Deserialize;

use super::prom::{Scrape, TimeUnit};
use crate::sim::dist::{Dist, QuantileMap};
use crate::util::units::{Dur, Rate};

/// One observation of a metric series (or of an aggregate over several series).
#[derive(Clone, Debug, Default)]
pub struct Observation {
    pub name: String,
    pub labels: BTreeMap<String, String>,
    /// events per second (counters)
    pub rate: Option<f64>,
    /// instantaneous value (gauges, ratios)
    pub value: Option<f64>,
    /// (quantile, microseconds) for timers
    pub quantiles: Vec<(f64, f64)>,
    /// mean in microseconds (timers) when known
    pub mean_us: Option<f64>,
    /// raw histogram: (upper bound in microseconds, cumulative count)
    pub buckets: Vec<(f64, f64)>,
    pub source: String,
}

impl Observation {
    /// Quantile in microseconds from explicit quantiles or histogram buckets.
    pub fn quantile_us(&self, q: f64) -> Option<f64> {
        if let Some(&(_, v)) = self.quantiles.iter().find(|(qq, _)| (qq - q).abs() < 1e-9) {
            return Some(v);
        }
        if self.buckets.len() >= 2 {
            return bucket_quantile(&self.buckets, q);
        }
        if self.quantiles.len() >= 2 {
            return Some(Dist::from_quantiles(&self.quantiles).quantile(q));
        }
        None
    }

    /// Build a latency distribution from whatever the observation carries.
    pub fn to_dist(&self) -> Option<Dist> {
        if self.quantiles.len() >= 2 {
            return Some(Dist::from_quantiles(&self.quantiles));
        }
        if self.buckets.len() >= 2 {
            let qs = [0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999];
            let pts: Vec<(f64, f64)> = qs
                .iter()
                .filter_map(|&q| bucket_quantile(&self.buckets, q).map(|v| (q, v)))
                .collect();
            if pts.len() >= 2 {
                return Some(Dist::from_quantiles(&pts));
            }
        }
        if let Some(&(q, v)) = self.quantiles.first() {
            // single percentile: assume p99 = 4x p50 shape
            let z = crate::sim::dist::inv_norm_cdf(q);
            let sigma = 4.0f64.ln() / crate::sim::dist::inv_norm_cdf(0.99);
            let median = v / (sigma * z).exp();
            return Some(Dist::lognormal_p50_p99(median, median * 4.0));
        }
        if let Some(m) = self.mean_us {
            let sigma = 4.0f64.ln() / crate::sim::dist::inv_norm_cdf(0.99);
            let median = m / (sigma * sigma / 2.0).exp();
            return Some(Dist::lognormal_p50_p99(median, median * 4.0));
        }
        None
    }

    /// Total count of histogram observations, if known.
    pub fn count(&self) -> Option<f64> {
        self.buckets.last().map(|b| b.1)
    }
}

/// `histogram_quantile` semantics (linear interpolation inside the bucket).
pub fn bucket_quantile(buckets: &[(f64, f64)], q: f64) -> Option<f64> {
    let total = buckets.last()?.1;
    if total <= 0.0 {
        return None;
    }
    let rank = q * total;
    let mut prev_ub = 0.0;
    let mut prev_c = 0.0;
    for &(ub, c) in buckets {
        if c >= rank {
            if !ub.is_finite() {
                return Some(prev_ub);
            }
            let in_bucket = c - prev_c;
            if in_bucket <= 0.0 {
                return Some(ub);
            }
            return Some(prev_ub + (ub - prev_ub) * (rank - prev_c) / in_bucket);
        }
        prev_ub = ub;
        prev_c = c;
    }
    Some(prev_ub)
}

#[derive(Clone, Debug, Default)]
pub struct Observations {
    pub items: Vec<Observation>,
    pub notes: Vec<String>,
}

// --- YAML observations file ------------------------------------------------------------------

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ObsFile {
    #[serde(default)]
    #[allow(dead_code)]
    description: Option<String>,
    /// Observation window (informational; used when entries give `increase` instead of `rate`).
    #[serde(default)]
    window: Option<Dur>,
    /// Strip this prefix from metric names (e.g. `temporal`).
    #[serde(default)]
    prefix: Option<String>,
    /// Prometheus scrape files relative to this file.
    #[serde(default)]
    prometheus: Option<PromFiles>,
    #[serde(default)]
    metrics: Vec<ObsEntry>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PromFiles {
    #[serde(default)]
    before: Option<String>,
    after: String,
    #[serde(default)]
    interval: Option<Dur>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ObsEntry {
    name: String,
    #[serde(default)]
    labels: BTreeMap<String, String>,
    #[serde(default)]
    rate: Option<Rate>,
    #[serde(default)]
    increase: Option<f64>,
    #[serde(default)]
    value: Option<f64>,
    #[serde(default)]
    mean: Option<Dur>,
    #[serde(default)]
    p50: Option<Dur>,
    #[serde(default)]
    p90: Option<Dur>,
    #[serde(default)]
    p95: Option<Dur>,
    #[serde(default)]
    p99: Option<Dur>,
    #[serde(default)]
    p999: Option<Dur>,
    #[serde(default)]
    quantiles: Option<QuantileMap>,
    /// cumulative histogram buckets `{ "0.005": 120, "0.01": 180, "+Inf": 200 }` (le in seconds
    /// unless `bucket_unit: ms`)
    #[serde(default)]
    buckets: Option<BTreeMap<String, f64>>,
    #[serde(default)]
    bucket_unit: Option<String>,
}

impl Observations {
    /// Prometheus scrape files a YAML observations file refers to (`prometheus.before` /
    /// `prometheus.after`), as written, relative to the file's folder unless absolute. Prometheus
    /// text files refer to nothing.
    pub fn referenced_files(path: &Path) -> anyhow::Result<Vec<String>> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading observations {}", path.display()))?;
        let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
        if ext == "prom" || ext == "txt" || looks_like_exposition(&text) {
            return Ok(Vec::new());
        }
        let f: ObsFile = serde_saphyr::from_str(&text)
            .map_err(|e| anyhow::anyhow!("{}: {e}", path.display()))?;
        Ok(f.prometheus
            .map(|p| {
                p.before
                    .into_iter()
                    .chain(std::iter::once(p.after))
                    .collect()
            })
            .unwrap_or_default())
    }

    pub fn load(path: &Path) -> anyhow::Result<Observations> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading observations {}", path.display()))?;
        let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
        if ext == "prom" || ext == "txt" || looks_like_exposition(&text) {
            let scrape = Scrape::parse(&text, "temporal").map_err(|e| anyhow::anyhow!(e))?;
            let mut o = Observations::default();
            o.ingest_scrape(&scrape, None, &path.display().to_string());
            return Ok(o);
        }
        let f: ObsFile = serde_saphyr::from_str(&text)
            .map_err(|e| anyhow::anyhow!("{}: {e}", path.display()))?;
        let mut o = Observations::default();
        let base = path.parent().unwrap_or(Path::new("."));
        let prefix = f.prefix.clone().unwrap_or_else(|| "temporal".into());
        if let Some(p) = &f.prometheus {
            let after_path = base.join(&p.after);
            let after = Scrape::parse(
                &std::fs::read_to_string(&after_path)
                    .with_context(|| format!("reading {}", after_path.display()))?,
                &prefix,
            )
            .map_err(|e| anyhow::anyhow!(e))?;
            match &p.before {
                Some(b) => {
                    let before_path = base.join(b);
                    let before = Scrape::parse(
                        &std::fs::read_to_string(&before_path)
                            .with_context(|| format!("reading {}", before_path.display()))?,
                        &prefix,
                    )
                    .map_err(|e| anyhow::anyhow!(e))?;
                    let Some(interval) = p.interval else {
                        bail!(
                            "prometheus.before given without prometheus.interval (time between the scrapes)"
                        );
                    };
                    let delta = Scrape::delta(&before, &after, &is_gauge);
                    o.ingest_scrape(
                        &delta,
                        Some(interval.secs()),
                        &format!("{} - {}", p.after, b),
                    );
                }
                // a single file whose counters are increases over `interval` (e.g. the output of
                // `tempdes run --prom` or a recording of `increase(...[interval])`)
                None => o.ingest_scrape(&after, p.interval.map(|i| i.secs()), &p.after),
            }
        }
        for e in f.metrics {
            o.items
                .push(e.into_observation(f.window, &path.display().to_string())?);
        }
        Ok(o)
    }

    fn ingest_scrape(&mut self, scrape: &Scrape, interval_s: Option<f64>, source: &str) {
        // group histogram buckets by (base name, labels without le)
        type Key = (String, BTreeMap<String, String>);
        let mut hists: BTreeMap<Key, Vec<(f64, f64)>> = BTreeMap::new();
        let mut sums: BTreeMap<Key, f64> = BTreeMap::new();
        let mut counters: BTreeMap<Key, f64> = BTreeMap::new();
        for s in &scrape.samples {
            if let Some(base) = s.name.strip_suffix("_bucket") {
                let mut labels = s.labels.clone();
                let le = labels.remove("le").unwrap_or_default();
                let unit = scrape.units.get(base).copied().unwrap_or(TimeUnit::Unknown);
                let ub = match le.as_str() {
                    "+Inf" | "Inf" => f64::INFINITY,
                    v => v.parse::<f64>().unwrap_or(f64::NAN) * unit_to_us(unit),
                };
                if ub.is_nan() {
                    continue;
                }
                hists
                    .entry((base.to_string(), labels))
                    .or_default()
                    .push((ub, s.value));
            } else if let Some(base) = s.name.strip_suffix("_sum") {
                let unit = scrape.units.get(base).copied().unwrap_or(TimeUnit::Unknown);
                *sums
                    .entry((base.to_string(), s.labels.clone()))
                    .or_default() += s.value * unit_to_us(unit);
            } else if s.name.ends_with("_count") {
                continue;
            } else {
                *counters
                    .entry((s.name.clone(), s.labels.clone()))
                    .or_default() += s.value;
            }
        }
        for ((name, labels), mut b) in hists {
            b.sort_by(|x, y| x.0.partial_cmp(&y.0).unwrap());
            let count = b.last().map(|x| x.1).unwrap_or(0.0);
            let mean_us = sums
                .get(&(name.clone(), labels.clone()))
                .filter(|_| count > 0.0)
                .map(|s| s / count);
            let rate = interval_s.map(|iv| count / iv.max(1e-9));
            self.items.push(Observation {
                name,
                labels,
                buckets: b,
                mean_us,
                rate,
                source: source.to_string(),
                ..Default::default()
            });
        }
        for ((name, labels), v) in counters {
            let gauge = is_gauge(&name);
            let (rate, value) = if gauge {
                (None, Some(v))
            } else {
                match interval_s {
                    Some(iv) => (Some(v / iv.max(1e-9)), None),
                    None => (None, Some(v)), // cumulative total; only useful as a ratio
                }
            };
            self.items.push(Observation {
                name,
                labels,
                rate,
                value,
                source: source.to_string(),
                ..Default::default()
            });
        }
        if interval_s.is_none() {
            self.notes.push(format!(
                "{source}: single scrape — histogram shapes are used, counters are process-lifetime totals (use a before/after pair for rates)"
            ));
        }
    }

    pub fn merge(&mut self, other: Observations) {
        self.items.extend(other.items);
        self.notes.extend(other.notes);
    }

    /// Series of `name` whose labels contain all of `filter`.
    pub fn select<'a>(
        &'a self,
        name: &'a str,
        filter: &'a [(&'a str, &'a str)],
    ) -> impl Iterator<Item = &'a Observation> + 'a {
        self.items.iter().filter(move |o| {
            o.name == name
                && filter.iter().all(|(k, v)| {
                    o.labels
                        .get(*k)
                        .map(|lv| lv.eq_ignore_ascii_case(v) || lv.ends_with(v))
                        .unwrap_or(false)
                })
        })
    }

    /// Sum of rates across matching series.
    pub fn rate(&self, name: &str, filter: &[(&str, &str)]) -> Option<f64> {
        let mut any = false;
        let mut total = 0.0;
        for o in self.select(name, filter) {
            if let Some(r) = o.rate {
                any = true;
                total += r;
            }
        }
        any.then_some(total)
    }

    /// Sum of values (gauges / totals) across matching series.
    pub fn value_sum(&self, name: &str, filter: &[(&str, &str)]) -> Option<f64> {
        let mut any = false;
        let mut total = 0.0;
        for o in self.select(name, filter) {
            if let Some(v) = o.value {
                any = true;
                total += v;
            }
        }
        any.then_some(total)
    }

    /// Merge histograms / quantiles of matching series into one latency observation.
    pub fn latency(&self, name: &str, filter: &[(&str, &str)]) -> Option<Observation> {
        let matches: Vec<&Observation> = self.select(name, filter).collect();
        if matches.is_empty() {
            return None;
        }
        if matches.len() == 1 {
            return Some(matches[0].clone());
        }
        // merge bucket histograms when all have buckets with identical bounds
        let with_buckets: Vec<&&Observation> =
            matches.iter().filter(|o| o.buckets.len() >= 2).collect();
        if with_buckets.len() == matches.len() {
            let mut merged: BTreeMap<u64, f64> = BTreeMap::new();
            let mut mean_num = 0.0;
            let mut mean_den = 0.0;
            for o in &with_buckets {
                for &(ub, c) in &o.buckets {
                    *merged
                        .entry(if ub.is_finite() {
                            ub.to_bits()
                        } else {
                            f64::INFINITY.to_bits()
                        })
                        .or_default() += c;
                }
                if let (Some(m), Some(c)) = (o.mean_us, o.count()) {
                    mean_num += m * c;
                    mean_den += c;
                }
            }
            let mut b: Vec<(f64, f64)> = merged
                .into_iter()
                .map(|(k, c)| (f64::from_bits(k), c))
                .collect();
            b.sort_by(|x, y| x.0.partial_cmp(&y.0).unwrap());
            return Some(Observation {
                name: name.to_string(),
                buckets: b,
                mean_us: (mean_den > 0.0).then(|| mean_num / mean_den),
                source: "merged".into(),
                ..Default::default()
            });
        }
        // otherwise take the one with the most information
        matches
            .into_iter()
            .max_by_key(|o| o.quantiles.len() + o.buckets.len())
            .cloned()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}

impl ObsEntry {
    fn into_observation(self, window: Option<Dur>, source: &str) -> anyhow::Result<Observation> {
        let mut o = Observation {
            name: normalise(&self.name),
            labels: self.labels,
            value: self.value,
            mean_us: self.mean.map(|d| d.0),
            source: source.to_string(),
            ..Default::default()
        };
        if let Some(r) = self.rate {
            o.rate = Some(r.0);
        } else if let Some(inc) = self.increase {
            let Some(w) = window else {
                bail!("{}: `increase` needs a top-level `window`", o.name);
            };
            o.rate = Some(inc / w.secs().max(1e-9));
        }
        for (q, v) in [
            (0.5, self.p50),
            (0.9, self.p90),
            (0.95, self.p95),
            (0.99, self.p99),
            (0.999, self.p999),
        ] {
            if let Some(v) = v {
                o.quantiles.push((q, v.0));
            }
        }
        if let Some(qs) = self.quantiles {
            for (q, v) in qs.0 {
                o.quantiles.push((q, v.0));
            }
        }
        o.quantiles.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        if let Some(b) = self.buckets {
            let mult = match self.bucket_unit.as_deref() {
                Some("ms") | Some("milliseconds") => 1_000.0,
                Some("us") => 1.0,
                _ => 1_000_000.0,
            };
            let mut v: Vec<(f64, f64)> = b
                .into_iter()
                .map(|(le, c)| {
                    let ub = if le.contains("Inf") {
                        f64::INFINITY
                    } else {
                        le.parse::<f64>().unwrap_or(f64::INFINITY) * mult
                    };
                    (ub, c)
                })
                .collect();
            v.sort_by(|x, y| x.0.partial_cmp(&y.0).unwrap());
            o.buckets = v;
        }
        Ok(o)
    }
}

fn unit_to_us(u: TimeUnit) -> f64 {
    match u {
        TimeUnit::Seconds => 1_000_000.0,
        TimeUnit::Milliseconds => 1_000.0,
        // tally default is seconds
        TimeUnit::Unknown => 1_000_000.0,
    }
}

fn normalise(name: &str) -> String {
    super::prom::normalise_name(name, "temporal").0
}

fn looks_like_exposition(text: &str) -> bool {
    text.lines()
        .map(str::trim)
        .find(|l| !l.is_empty())
        .map(|l| l.starts_with("# HELP") || l.starts_with("# TYPE"))
        .unwrap_or(false)
}

/// Temporal gauges (never differenced between scrapes).
pub fn is_gauge(name: &str) -> bool {
    const GAUGES: &[&str] = &[
        "service_pending_requests",
        "numshards_gauge",
        "cache_size",
        "cache_usage",
        "cache_pinned_usage",
        "approximate_backlog_count",
        "approximate_backlog_age_seconds",
        "physical_approximate_backlog_count",
        "physical_approximate_backlog_age_seconds",
        "loaded_task_queue_partition_count",
        "loaded_task_queue_count",
        "loaded_physical_task_queue_count",
        "loaded_task_queue_family_count",
        "host_rps_limit",
        "persistence_sql_max_open_conn",
        "persistence_sql_open_conn",
        "persistence_sql_idle_conn",
        "persistence_sql_in_use",
        "num_goroutines",
        "gomaxprocs",
        "memory_heap",
        "memory_allocated",
        "task_lag_per_tl",
        "dynamic_worker_pool_scheduler_active_workers",
        "dynamic_worker_pool_scheduler_buffer_size",
        "container_cpu_utilization",
    ];
    GAUGES.contains(&name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_quantile_matches_promql() {
        let b = vec![
            (1_000.0, 10.0),
            (5_000.0, 80.0),
            (10_000.0, 95.0),
            (f64::INFINITY, 100.0),
        ];
        let p50 = bucket_quantile(&b, 0.5).unwrap();
        // rank 50 falls in (1ms, 5ms]: 1000 + 4000 * (50-10)/70
        assert!((p50 - (1_000.0 + 4_000.0 * 40.0 / 70.0)).abs() < 1e-6);
        assert_eq!(bucket_quantile(&b, 0.99).unwrap(), 10_000.0);
    }

    #[test]
    fn yaml_observations() {
        let dir = std::env::temp_dir().join(format!("tempdes-obs-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("obs.yaml");
        std::fs::write(
            &p,
            r#"
window: 10m
metrics:
  - name: temporal_persistence_latency
    labels: { operation: UpdateWorkflowExecution }
    p50: 3ms
    p99: 20ms
  - name: service_requests_total
    labels: { service_name: frontend, operation: StartWorkflowExecution }
    rate: 120/s
  - name: service_requests
    labels: { service_name: frontend, operation: SignalWorkflowExecution }
    increase: 6000
"#,
        )
        .unwrap();
        let o = Observations::load(&p).unwrap();
        let l = o
            .latency(
                "persistence_latency",
                &[("operation", "UpdateWorkflowExecution")],
            )
            .unwrap();
        assert!((l.to_dist().unwrap().quantile(0.5) - 3_000.0).abs() < 1.0);
        assert_eq!(
            o.rate(
                "service_requests",
                &[("operation", "StartWorkflowExecution")]
            ),
            Some(120.0)
        );
        assert_eq!(
            o.rate(
                "service_requests",
                &[("operation", "SignalWorkflowExecution")]
            ),
            Some(10.0)
        );
    }
}
