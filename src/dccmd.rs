//! `tempdes dc …` — inspect and validate Temporal 1.31.0 dynamic config.

use std::path::PathBuf;
use std::process::ExitCode;

use crate::config::dynamic::{DynamicConfig, registry};
use crate::model::params::MODELED_KEYS;

pub enum Cmd {
    Modeled,
    Search(String),
    Explain(String),
    Validate(PathBuf),
}

fn one_line(s: &str, n: usize) -> String {
    let t: String = s.split_whitespace().collect::<Vec<_>>().join(" ");
    if t.chars().count() > n {
        format!("{}…", t.chars().take(n).collect::<String>())
    } else {
        t
    }
}

fn default_str(key: &str) -> String {
    registry()
        .get(key)
        .map(|d| d.default_display.to_string().trim_matches('"').to_string())
        .unwrap_or_else(|| "?".into())
}

pub fn run(cmd: Cmd) -> anyhow::Result<ExitCode> {
    let reg = registry();
    match cmd {
        Cmd::Modeled => {
            println!(
                "Dynamic config keys modelled by tempdes (Temporal {}):\n",
                reg.temporal_version
            );
            // group by prefix, keeping the order in which prefixes first appear
            let prefix = |k: &str| k.split('.').next().unwrap_or("").to_string();
            let mut groups: Vec<String> = Vec::new();
            for k in MODELED_KEYS {
                if !groups.contains(&prefix(k)) {
                    groups.push(prefix(k));
                }
            }
            let ordered = groups
                .iter()
                .flat_map(|g| MODELED_KEYS.iter().filter(move |k| prefix(k) == *g));
            let mut last = String::new();
            for k in ordered {
                if prefix(k) != last {
                    last = prefix(k);
                    println!("{last}");
                }
                let d = reg.get(k);
                println!(
                    "  {:<58} {:<10} default {:<10} {}",
                    k,
                    d.map(|d| d.scope.as_str()).unwrap_or("?"),
                    default_str(k),
                    d.map(|d| one_line(&d.description, 70)).unwrap_or_default()
                );
            }
            println!(
                "\n{} of {} registered keys are modelled; others are validated and reported but do not change the simulation.",
                MODELED_KEYS.len(),
                reg.settings.len()
            );
            Ok(ExitCode::SUCCESS)
        }
        Cmd::Search(pat) => {
            let p = pat.to_ascii_lowercase();
            let mut n = 0;
            for s in &reg.settings {
                if s.key.to_ascii_lowercase().contains(&p)
                    || s.description.to_ascii_lowercase().contains(&p)
                {
                    n += 1;
                    let modeled = MODELED_KEYS.iter().any(|m| m.eq_ignore_ascii_case(&s.key));
                    println!(
                        "{}{:<60} {:<10} {:<8} default {}",
                        if modeled { "* " } else { "  " },
                        s.key,
                        s.scope,
                        s.typ,
                        default_str(&s.key)
                    );
                }
            }
            println!("\n{n} match(es); * = modelled by the simulator");
            Ok(ExitCode::SUCCESS)
        }
        Cmd::Explain(key) => match reg.get(&key) {
            Some(d) => {
                let modeled = MODELED_KEYS.iter().any(|m| m.eq_ignore_ascii_case(&d.key));
                println!("{}", d.key);
                println!(
                    "  scope    {} (constraints: {})",
                    d.scope,
                    scope_constraints(&d.scope)
                );
                println!("  type     {}", d.typ);
                println!("  default  {}", default_str(&d.key));
                if d.default.is_none() {
                    println!("  default (Go) {}", one_line(&d.default_go, 200));
                }
                println!(
                    "  source   {} (temporal v{})",
                    d.source, reg.temporal_version
                );
                println!(
                    "  modelled {}",
                    if modeled {
                        "yes"
                    } else {
                        "no (validated only)"
                    }
                );
                println!("\n{}", textwrap(&d.description, 96));
                Ok(ExitCode::SUCCESS)
            }
            None => {
                let s = reg.suggest(&key, 5);
                eprintln!(
                    "{key:?} is not a Temporal {} dynamic config key.",
                    reg.temporal_version
                );
                if !s.is_empty() {
                    eprintln!("did you mean: {}", s.join(", "));
                }
                Ok(ExitCode::from(1))
            }
        },
        Cmd::Validate(file) => {
            let text = std::fs::read_to_string(&file)?;
            let dc = DynamicConfig::from_yaml_str(&text, &file.display().to_string())?;
            let keys = dc.configured_keys();
            println!("{}: {} keys", file.display(), keys.len());
            for k in &keys {
                let modeled = MODELED_KEYS.iter().any(|m| m.eq_ignore_ascii_case(k));
                let known = reg.get(k).is_some();
                println!(
                    "  {} {:<60} {}",
                    if !known {
                        "?"
                    } else if modeled {
                        "*"
                    } else {
                        " "
                    },
                    k,
                    dc.describe(k)
                );
            }
            if dc.warnings.is_empty() {
                println!("\nno problems found (* = modelled, ? = unknown)");
                Ok(ExitCode::SUCCESS)
            } else {
                println!("\n{} problem(s):", dc.warnings.len());
                for w in &dc.warnings {
                    println!("  ! {w}");
                }
                Ok(ExitCode::from(1))
            }
        }
    }
}

fn scope_constraints(scope: &str) -> &'static str {
    match scope {
        "Global" => "none",
        "Namespace" => "namespace",
        "NamespaceID" => "namespaceID",
        "TaskQueue" => "namespace, taskQueueName, taskType",
        "ShardID" => "shardID",
        "TaskType" => "historyTaskType",
        "Destination" => "namespace, destination",
        "ChasmTaskType" => "chasmTaskType",
        _ => "?",
    }
}

fn textwrap(s: &str, width: usize) -> String {
    let mut out = String::new();
    let mut line = String::new();
    for w in s.split_whitespace() {
        if line.len() + w.len() + 1 > width && !line.is_empty() {
            out.push_str("  ");
            out.push_str(&line);
            out.push('\n');
            line.clear();
        }
        if !line.is_empty() {
            line.push(' ');
        }
        line.push_str(w);
    }
    if !line.is_empty() {
        out.push_str("  ");
        out.push_str(&line);
    }
    out
}
