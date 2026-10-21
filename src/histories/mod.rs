//! Workloads from exported workflow histories (`tempdes workload fetch` and `import`).
//!
//! Reads histories as the Temporal CLI, Web UI and tctl export them, or as `workload fetch`
//! writes them ([`fetch`]), turns each into a sequence of steps ([`trace`]), and pools executions
//! that took the same steps into the scenario's `workflows:` entries ([`program`]): step
//! durations, attempts, retry policies and timeouts as recorded, with waits in the cluster left
//! out, and each type's payload size from the history sizes the server recorded
//! (`historySizeBytes`); and a commented worker fleet per task queue, with the processes seen.
//! Payloads are never read.

#[cfg(feature = "fetch")]
pub mod fetch;
pub mod parse;
pub mod program;
pub mod trace;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::Context;
use serde::{Deserialize, Serialize};
use serde_saphyr::FlowSeq;

pub use program::{Counted, Options, Program};

/// The manifest `workload fetch` writes beside the histories.
pub const MANIFEST: &str = "fetch.yaml";

/// What `workload fetch` listed and fetched: the executions started in a window, by type.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Manifest {
    /// the namespace they were listed in
    pub namespace: String,
    /// the window, RFC 3339
    pub start: String,
    pub end: String,
    /// its length in seconds
    pub window_s: f64,
    /// the length of the slices it was listed in, one at a time, in seconds (0 when written
    /// before slicing)
    #[serde(default)]
    pub slice_s: f64,
    /// the visibility query that listed them
    pub query: String,
    pub types: Vec<TypeCount>,
}

/// A workflow type's executions in a [`Manifest`].
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct TypeCount {
    #[serde(rename = "type")]
    pub name: String,
    /// started in the window
    pub started: u64,
    /// of them, started by another workflow
    pub children: u64,
    /// histories written
    pub fetched: u64,
    /// started in each slice of the window, in order
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_by_slice: Option<FlowSeq<Vec<u64>>>,
    /// closed in each slice of the window (those that closed after it aren't in any)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub closed_by_slice: Option<FlowSeq<Vec<u64>>>,
    /// how they stood when listed: `Running`, or how they closed (`Completed`, `Failed`, ...)
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub status: BTreeMap<String, u64>,
    /// start to close of those closed when listed
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_s: Option<Durations>,
}

/// A distribution of durations, in seconds.
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Durations {
    pub p50: f64,
    pub p90: f64,
    pub p99: f64,
    pub max: f64,
}

impl Durations {
    /// Nearest-rank quantiles of `v`, to the millisecond; None when it is empty.
    pub fn of(mut v: Vec<f64>) -> Option<Durations> {
        if v.is_empty() {
            return None;
        }
        v.sort_by(f64::total_cmp);
        let ms = |x: f64| (x * 1000.0).round() / 1000.0;
        let q = |p: f64| ms(v[((p * v.len() as f64).ceil() as usize).clamp(1, v.len()) - 1]);
        Some(Durations {
            p50: q(0.5),
            p90: q(0.9),
            p99: q(0.99),
            max: ms(v[v.len() - 1]),
        })
    }
}

/// Read a manifest that `workload fetch` wrote.
pub fn read_manifest(path: &Path) -> anyhow::Result<Manifest> {
    let text =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    serde_saphyr::from_str(&text).map_err(|e| anyhow::anyhow!("reading {}: {e}", path.display()))
}

/// Import the histories in `paths` (files, or directories of `*.json` files). A directory that
/// `workload fetch` wrote gives each type's start rate from its manifest's counts.
pub fn import(paths: &[PathBuf], opts: &Options) -> anyhow::Result<Program> {
    let mut opts = opts.clone();
    let mut files: Vec<PathBuf> = Vec::new();
    for p in paths {
        if p.is_dir() {
            let manifest = p.join(MANIFEST);
            if manifest.is_file() {
                let m = read_manifest(&manifest)?;
                for t in m.types {
                    let c = opts.counted.entry(t.name).or_default();
                    c.started += t.started;
                    c.children += t.children;
                    c.window_s += m.window_s;
                }
            }
            let mut in_dir: Vec<PathBuf> = std::fs::read_dir(p)?
                .filter_map(|e| e.ok().map(|e| e.path()))
                .filter(|f| {
                    f.extension()
                        .is_some_and(|x| x.eq_ignore_ascii_case("json"))
                })
                .collect();
            in_dir.sort();
            files.extend(in_dir);
        } else {
            files.push(p.clone());
        }
    }
    anyhow::ensure!(!files.is_empty(), "no history files given");
    let mut traces = Vec::new();
    let mut unreadable = Vec::new();
    for f in &files {
        match read(f) {
            Ok(t) => traces.push(t),
            Err(e) => unreadable.push(format!("{}: {e}", f.display())),
        }
    }
    anyhow::ensure!(
        !traces.is_empty(),
        "no readable histories among {} files:\n  {}",
        files.len(),
        unreadable.join("\n  ")
    );
    let mut program = program::build(&traces, &opts);
    if !unreadable.is_empty() {
        program.summary.push_str(&format!(
            "skipped {} unreadable files:\n  {}\n",
            unreadable.len(),
            unreadable.join("\n  ")
        ));
    }
    Ok(program)
}

fn read(f: &Path) -> Result<trace::Trace, String> {
    let text = std::fs::read_to_string(f).map_err(|e| e.to_string())?;
    let events = parse::parse_history(&text)?;
    trace::trace(&events)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations_take_nearest_rank_quantiles() {
        let d = Durations::of((1..=100).map(f64::from).rev().collect()).unwrap();
        assert_eq!((d.p50, d.p90, d.p99, d.max), (50.0, 90.0, 99.0, 100.0));
        let one = Durations::of(vec![1.234_567]).unwrap();
        assert_eq!((one.p50, one.max), (1.235, 1.235));
        assert_eq!(Durations::of(Vec::new()), None);
    }

    #[test]
    fn a_manifest_from_before_slicing_still_reads() {
        let m: Manifest = serde_saphyr::from_str(
            "namespace: orders\nstart: a\nend: b\nwindow_s: 900.0\nquery: q\ntypes:\n- type: Order\n  started: 4\n  children: 0\n  fetched: 2\n",
        )
        .unwrap();
        assert_eq!(m.slice_s, 0.0);
        assert_eq!(m.types[0].started_by_slice, None);
        // and a new one reads back as written
        let mut new = m.clone();
        new.slice_s = 60.0;
        new.types[0].started_by_slice = Some(FlowSeq(vec![1, 0, 3]));
        new.types[0].duration_s = Durations::of(vec![2.0]);
        let yaml = serde_saphyr::to_string(&new).unwrap();
        assert!(yaml.contains("started_by_slice: [1, 0, 3]"), "{yaml}");
        assert_eq!(serde_saphyr::from_str::<Manifest>(&yaml).unwrap(), new);
    }
}
