//! Import deployment settings from a `temporalio/helm-charts` values file, the usual source of
//! truth for Temporal on EKS:
//!
//! | Helm value                                                          | Scenario field                  |
//! |---------------------------------------------------------------------|---------------------------------|
//! | `server.<svc>.replicaCount` (fallback `server.replicaCount`)        | `cluster.replicas.<svc>`         |
//! | `server.<svc>.resources.limits.cpu` (fallback `requests.cpu`)       | `cluster.resources.<svc>.cpu`    |
//! | `server.config.persistence.numHistoryShards` (or `config.numHistoryShards`) | `cluster.num_history_shards` |
//! | `server.config.persistence.datastores.default.{sql,cassandra}`      | store kind, `max_conns`          |
//! | `server.dynamicConfig`                                              | dynamic config (Temporal format) |

use std::collections::BTreeMap;
use std::path::Path;

use anyhow::Context;

use super::dynamic::{DcValue, InlineCv};
use super::scenario::{MaxConns, Scenario, StoreKind};

fn get<'a>(v: &'a DcValue, path: &[&str]) -> Option<&'a DcValue> {
    let mut cur = v;
    for p in path {
        match cur {
            DcValue::Map(m) => cur = m.get(*p)?,
            _ => return None,
        }
    }
    Some(cur)
}

fn as_u32(v: &DcValue) -> Option<u32> {
    match v {
        DcValue::Int(i) if *i >= 0 => Some(*i as u32),
        DcValue::Str(s) => s.trim().parse().ok(),
        _ => None,
    }
}

/// Kubernetes CPU quantity: `2`, `1.5`, `2500m`.
pub fn parse_cpu(v: &DcValue) -> Option<f64> {
    match v {
        DcValue::Int(i) => Some(*i as f64),
        DcValue::Float(f) => Some(*f),
        DcValue::Str(s) => {
            let t = s.trim();
            if let Some(m) = t.strip_suffix('m') {
                m.parse::<f64>().ok().map(|x| x / 1000.0)
            } else {
                t.parse().ok()
            }
        }
        _ => None,
    }
}

/// Apply Helm values to a scenario. Returns notes describing what was taken from the file.
pub fn apply(sc: &mut Scenario, path: &Path) -> anyhow::Result<Vec<String>> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("reading Helm values {}", path.display()))?;
    let root: DcValue = serde_saphyr::from_str(&text)
        .map_err(|e| anyhow::anyhow!("{}: invalid YAML: {e}", path.display()))?;
    let Some(server) = get(&root, &["server"]) else {
        anyhow::bail!(
            "{}: no `server:` section (is this a temporalio/helm-charts values file?)",
            path.display()
        );
    };
    let mut notes = Vec::new();
    let global_replicas = get(server, &["replicaCount"]).and_then(as_u32);
    for (svc, key) in [
        ("frontend", "frontend"),
        ("history", "history"),
        ("matching", "matching"),
        ("worker", "worker"),
    ] {
        let enabled = !matches!(get(server, &[key, "enabled"]), Some(DcValue::Bool(false)));
        let replicas = get(server, &[key, "replicaCount"])
            .and_then(as_u32)
            .or(global_replicas);
        if let Some(n) = replicas {
            let n = if enabled { n } else { 0 };
            match svc {
                "frontend" => sc.cluster.replicas.frontend = n,
                "history" => sc.cluster.replicas.history = n,
                "matching" => sc.cluster.replicas.matching = n,
                _ => sc.cluster.replicas.worker = n.max(1),
            }
            notes.push(format!("helm: {svc} replicas = {n}"));
        }
        let cpu = get(server, &[key, "resources", "limits", "cpu"])
            .or_else(|| get(server, &[key, "resources", "requests", "cpu"]))
            .and_then(parse_cpu);
        if let Some(c) = cpu {
            let r = &mut sc.cluster.resources;
            match svc {
                "frontend" => r.frontend.cpu = c,
                "history" => r.history.cpu = c,
                "matching" => r.matching.cpu = c,
                _ => r.worker.cpu = c,
            }
            notes.push(format!("helm: {svc} cpu = {c}"));
        } else if replicas.is_some() {
            notes.push(format!(
                "helm: {svc} has no CPU limit — GOMAXPROCS follows the node's cores and CPU is shared; the scenario value is used"
            ));
        }
    }
    let shards = get(server, &["config", "persistence", "numHistoryShards"])
        .or_else(|| get(server, &["config", "numHistoryShards"]))
        .and_then(as_u32);
    if let Some(s) = shards {
        sc.cluster.num_history_shards = s;
        notes.push(format!("helm: numHistoryShards = {s}"));
    }
    let default_store = get(server, &["config", "persistence", "defaultStore"])
        .and_then(|v| {
            if let DcValue::Str(s) = v {
                Some(s.clone())
            } else {
                None
            }
        })
        .unwrap_or_else(|| "default".into());
    let store = get(
        server,
        &["config", "persistence", "datastores", &default_store],
    )
    .or_else(|| get(server, &["config", "persistence", &default_store]));
    if let Some(store) = store {
        if let Some(sql) = get(store, &["sql"]) {
            let plugin = get(sql, &["pluginName"])
                .or_else(|| get(sql, &["driver"]))
                .or_else(|| get(sql, &["driverName"]))
                .and_then(|v| {
                    if let DcValue::Str(s) = v {
                        Some(s.to_ascii_lowercase())
                    } else {
                        None
                    }
                })
                .unwrap_or_default();
            sc.cluster.persistence.store = if plugin.contains("postgres") {
                StoreKind::Postgresql
            } else if plugin.contains("sqlite") {
                StoreKind::Sqlite
            } else {
                StoreKind::Mysql
            };
            if let Some(mc) = get(sql, &["maxConns"]).and_then(as_u32) {
                let per = sc.cluster.persistence.max_conns.unwrap_or(MaxConns {
                    frontend: mc,
                    history: mc,
                    matching: mc,
                    worker: mc,
                });
                // the chart renders one server config for all services: same maxConns everywhere
                sc.cluster.persistence.max_conns = Some(MaxConns {
                    frontend: mc,
                    history: mc,
                    matching: mc,
                    worker: per.worker.min(mc),
                });
                notes.push(format!("helm: SQL maxConns = {mc} per pod ({plugin})"));
            }
        } else if get(store, &["cassandra"]).is_some() {
            sc.cluster.persistence.store = StoreKind::Cassandra;
            notes.push("helm: Cassandra default store".into());
        }
    }
    if let Some(DcValue::Map(dc)) = get(server, &["dynamicConfig"]) {
        let mut n = 0;
        for (k, v) in dc {
            let DcValue::List(items) = v else { continue };
            let mut cvs = Vec::new();
            for it in items {
                let DcValue::Map(m) = it else { continue };
                let value = m.get("value").cloned().unwrap_or(DcValue::Null);
                let constraints = match m.get("constraints") {
                    Some(DcValue::Map(c)) => c.clone(),
                    _ => BTreeMap::new(),
                };
                cvs.push(InlineCv { value, constraints });
            }
            // scenario inline values keep precedence over the chart's
            sc.dynamic_config.entry(k.clone()).or_insert_with(|| {
                n += 1;
                cvs
            });
        }
        notes.push(format!(
            "helm: {n} dynamic config keys from server.dynamicConfig"
        ));
    }
    Ok(notes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cpu_quantities() {
        assert_eq!(parse_cpu(&DcValue::Str("2500m".into())), Some(2.5));
        assert_eq!(parse_cpu(&DcValue::Int(4)), Some(4.0));
        assert_eq!(parse_cpu(&DcValue::Str("1.5".into())), Some(1.5));
    }
}
