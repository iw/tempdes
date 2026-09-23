//! Regenerate the embedded dynamic config registry from a Temporal server source checkout.
//!
//! ```text
//! git clone --depth 1 --branch v1.31.0 https://github.com/temporalio/temporal ../temporal
//! cargo run --release --bin gen-dc-registry -- ../temporal -o data/dynamicconfig-1.31.0.json
//! cargo run --release --bin gen-dc-registry -- ../temporal --check data/dynamicconfig-1.31.0.json
//! ```

use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::Context;
use clap::Parser;

use tempdes::config::dynamic::RegistryFile;
use tempdes::config::registry_gen;

/// Generate tempdes' dynamic config registry (every setting's key, scope, type, default and
/// description) from a Temporal server source checkout.
#[derive(Parser)]
#[command(name = "gen-dc-registry", version)]
struct Args {
    /// Temporal server source checkout (github.com/temporalio/temporal at a release tag)
    source: PathBuf,
    /// Write the registry to this file instead of stdout
    #[arg(short, long, value_name = "FILE", conflicts_with = "check")]
    output: Option<PathBuf>,
    /// Compare with an existing registry file instead of writing; exit 1 when it is out of date
    #[arg(long, value_name = "FILE")]
    check: Option<PathBuf>,
    /// Temporal version to record [default: ServerVersion in common/headers/version_checker.go]
    #[arg(long, value_name = "VERSION")]
    temporal_version: Option<String>,
}

fn main() -> anyhow::Result<ExitCode> {
    let args = Args::parse();
    let g = registry_gen::generate(&args.source, args.temporal_version)?;
    for w in &g.warnings {
        eprintln!("warning: {w}");
    }
    eprintln!(
        "{} settings for Temporal {} from {} Go files",
        g.registry.settings.len(),
        g.registry.temporal_version,
        g.files_scanned
    );
    if !g.unevaluated.is_empty() {
        eprintln!(
            "{} defaults are not constant expressions and are recorded as Go source:",
            g.unevaluated.len()
        );
        for u in &g.unevaluated {
            eprintln!("  {u}");
        }
    }

    if let Some(path) = &args.check {
        let text =
            std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        let existing: RegistryFile = serde_json::from_str(&text)
            .with_context(|| format!("{} is not a registry file", path.display()))?;
        let changes = registry_gen::diff(&existing, &g.registry);
        if changes.is_empty() && text == registry_gen::to_json(&g.registry) {
            eprintln!("{} is up to date", path.display());
            return Ok(ExitCode::SUCCESS);
        }
        for c in &changes {
            println!("{c}");
        }
        eprintln!(
            "{} is out of date ({}); regenerate it with -o",
            path.display(),
            if changes.is_empty() {
                "formatting only".to_string()
            } else {
                format!("{} differences", changes.len())
            }
        );
        return Ok(ExitCode::FAILURE);
    }

    let json = registry_gen::to_json(&g.registry);
    match &args.output {
        Some(path) => {
            std::fs::write(path, json).with_context(|| format!("writing {}", path.display()))?;
            eprintln!("wrote {}", path.display());
        }
        None => print!("{json}"),
    }
    Ok(ExitCode::SUCCESS)
}
