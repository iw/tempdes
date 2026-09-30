//! Workloads from exported workflow histories (`tempdes workload import`).
//!
//! Reads histories as the Temporal CLI, Web UI and tctl export them, turns each into a sequence
//! of steps ([`trace`]), and pools executions that took the same steps into the scenario's
//! `workflows:` entries ([`program`]): step durations, attempts, retry policies and timeouts
//! as recorded, with waits in the cluster left out, and each type's payload size from the
//! history sizes the server recorded (`historySizeBytes`); and a commented worker fleet per
//! task queue, with the processes seen. Payloads are never read.

pub mod parse;
pub mod program;
pub mod trace;

use std::path::{Path, PathBuf};

pub use program::{Options, Program};

/// Import the histories in `paths` (files, or directories of `*.json` files).
pub fn import(paths: &[PathBuf], opts: &Options) -> anyhow::Result<Program> {
    let mut files: Vec<PathBuf> = Vec::new();
    for p in paths {
        if p.is_dir() {
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
    let mut program = program::build(&traces, opts);
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
