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

use std::path::{Path, PathBuf};

use anyhow::Context;
use serde::{Deserialize, Serialize};

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
