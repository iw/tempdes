//! Temporal dynamic config, as understood by server 1.31.0.
//!
//! * Keys are case-insensitive (`dynamicconfig.MakeKey` lower-cases them).
//! * Each key holds a list of `{value, constraints}` entries.
//! * Lookups use the precedence lists from `common/dynamicconfig/setting_gen.go`; a constrained
//!   value only matches if its constraint set is *exactly* equal to one of the precedence entries.
//! * Integer settings do not accept floats, durations accept strings (`"5s"`) or numbers
//!   (seconds). Values that fail conversion fall back to the default — we warn about those,
//!   because Temporal does so silently.
//!
//! The full 1.31.0 key registry (613 settings, generated from the server source by the
//! `gen-dc-registry` binary, see [`super::registry_gen`]) is embedded so that user files can be
//! validated.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::OnceLock;

use serde::{Deserialize, Serialize};

use crate::util::units::parse_duration_us;

// --- registry ------------------------------------------------------------------------------------

/// One registered setting, as recorded in `data/dynamicconfig-<version>.json`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SettingDef {
    pub key: String,
    pub scope: String,
    #[serde(rename = "type")]
    pub typ: String,
    /// Evaluated default (durations in microseconds); null when it is not a constant.
    pub default: Option<serde_json::Value>,
    /// Default for people: `"5m"` for durations, the Go source when not a constant.
    pub default_display: serde_json::Value,
    /// The default expression as written in the Go source.
    pub default_go: String,
    pub description: String,
    /// `path/to/file.go:line` of the registration in the Temporal source.
    pub source: String,
}

/// The registry file: settings sorted by lower-cased key.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RegistryFile {
    pub temporal_version: String,
    pub settings: Vec<SettingDef>,
}

pub struct Registry {
    pub temporal_version: String,
    pub settings: Vec<SettingDef>,
    by_key: HashMap<String, usize>,
}

pub(crate) static REGISTRY_JSON: &str = include_str!("../../data/dynamicconfig-1.31.0.json");

pub fn registry() -> &'static Registry {
    static REG: OnceLock<Registry> = OnceLock::new();
    REG.get_or_init(|| {
        let f: RegistryFile =
            serde_json::from_str(REGISTRY_JSON).expect("embedded dynamic config registry is valid");
        let by_key = f
            .settings
            .iter()
            .enumerate()
            .map(|(i, s)| (s.key.to_ascii_lowercase(), i))
            .collect();
        Registry {
            temporal_version: f.temporal_version,
            settings: f.settings,
            by_key,
        }
    })
}

impl Registry {
    pub fn get(&self, key: &str) -> Option<&SettingDef> {
        self.by_key
            .get(&key.to_ascii_lowercase())
            .map(|&i| &self.settings[i])
    }

    /// Closest known keys by edit distance (for "did you mean" hints).
    pub fn suggest(&self, key: &str, n: usize) -> Vec<&str> {
        let lk = key.to_ascii_lowercase();
        let mut scored: Vec<(usize, &str)> = self
            .settings
            .iter()
            .map(|s| {
                (
                    levenshtein(&lk, &s.key.to_ascii_lowercase()),
                    s.key.as_str(),
                )
            })
            .collect();
        scored.sort();
        scored
            .into_iter()
            .filter(|(d, _)| *d <= (lk.len() / 3).max(3))
            .take(n)
            .map(|(_, k)| k)
            .collect()
    }
}

fn levenshtein(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let mut prev: Vec<usize> = (0..=b.len()).collect();
    let mut cur = vec![0; b.len() + 1];
    for i in 1..=a.len() {
        cur[0] = i;
        for j in 1..=b.len() {
            let cost = usize::from(a[i - 1] != b[j - 1]);
            cur[j] = (prev[j] + 1).min(cur[j - 1] + 1).min(prev[j - 1] + cost);
        }
        std::mem::swap(&mut prev, &mut cur);
    }
    prev[b.len()]
}

// --- values ---------------------------------------------------------------------------------------

/// A raw dynamic config value as found in YAML.
#[derive(Clone, Debug, PartialEq)]
pub enum DcValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    List(Vec<DcValue>),
    Map(BTreeMap<String, DcValue>),
}

impl<'de> Deserialize<'de> for DcValue {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> serde::de::Visitor<'de> for V {
            type Value = DcValue;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a dynamic config value")
            }
            fn visit_unit<E>(self) -> Result<DcValue, E> {
                Ok(DcValue::Null)
            }
            fn visit_none<E>(self) -> Result<DcValue, E> {
                Ok(DcValue::Null)
            }
            fn visit_bool<E>(self, v: bool) -> Result<DcValue, E> {
                Ok(DcValue::Bool(v))
            }
            fn visit_i64<E>(self, v: i64) -> Result<DcValue, E> {
                Ok(DcValue::Int(v))
            }
            fn visit_u64<E>(self, v: u64) -> Result<DcValue, E> {
                Ok(DcValue::Int(v as i64))
            }
            fn visit_f64<E>(self, v: f64) -> Result<DcValue, E> {
                Ok(DcValue::Float(v))
            }
            fn visit_str<E>(self, v: &str) -> Result<DcValue, E> {
                Ok(DcValue::Str(v.to_string()))
            }
            fn visit_string<E>(self, v: String) -> Result<DcValue, E> {
                Ok(DcValue::Str(v))
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> Result<DcValue, A::Error> {
                let mut out = Vec::new();
                while let Some(v) = seq.next_element()? {
                    out.push(v);
                }
                Ok(DcValue::List(out))
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut map: A,
            ) -> Result<DcValue, A::Error> {
                let mut out = BTreeMap::new();
                while let Some((k, v)) = map.next_entry::<DcKey, DcValue>()? {
                    out.insert(k.0, v);
                }
                Ok(DcValue::Map(out))
            }
        }
        d.deserialize_any(V)
    }
}

/// Map keys may be written as numbers in YAML; normalise to strings.
struct DcKey(String);

impl<'de> Deserialize<'de> for DcKey {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        match DcValue::deserialize(d)? {
            DcValue::Str(s) => Ok(DcKey(s)),
            DcValue::Int(i) => Ok(DcKey(i.to_string())),
            DcValue::Float(f) => Ok(DcKey(f.to_string())),
            DcValue::Bool(b) => Ok(DcKey(b.to_string())),
            other => Err(serde::de::Error::custom(format!(
                "unsupported map key {other:?}"
            ))),
        }
    }
}

impl fmt::Display for DcValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DcValue::Null => f.write_str("null"),
            DcValue::Bool(b) => write!(f, "{b}"),
            DcValue::Int(i) => write!(f, "{i}"),
            DcValue::Float(x) => {
                if x.fract() == 0.0 && x.abs() < 1e15 {
                    write!(f, "{x:.1}")
                } else {
                    write!(f, "{x}")
                }
            }
            DcValue::Str(s) => write!(f, "{s:?}"),
            DcValue::List(l) => write!(f, "[{} items]", l.len()),
            DcValue::Map(m) => write!(f, "{{{} keys}}", m.len()),
        }
    }
}

impl DcValue {
    /// Parse a value given on the command line (`--dc key=value`).
    pub fn parse_cli(s: &str) -> DcValue {
        let t = s.trim();
        if t.eq_ignore_ascii_case("true") {
            return DcValue::Bool(true);
        }
        if t.eq_ignore_ascii_case("false") {
            return DcValue::Bool(false);
        }
        if let Ok(i) = t.parse::<i64>() {
            return DcValue::Int(i);
        }
        if let Ok(f) = t.parse::<f64>() {
            return DcValue::Float(f);
        }
        DcValue::Str(t.to_string())
    }

    fn from_json(v: &serde_json::Value) -> DcValue {
        match v {
            serde_json::Value::Null => DcValue::Null,
            serde_json::Value::Bool(b) => DcValue::Bool(*b),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    DcValue::Int(i)
                } else {
                    DcValue::Float(n.as_f64().unwrap_or(0.0))
                }
            }
            serde_json::Value::String(s) => DcValue::Str(s.clone()),
            serde_json::Value::Array(a) => {
                DcValue::List(a.iter().map(DcValue::from_json).collect())
            }
            serde_json::Value::Object(o) => DcValue::Map(
                o.iter()
                    .map(|(k, v)| (k.clone(), DcValue::from_json(v)))
                    .collect(),
            ),
        }
    }
}

// --- constraints ----------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TaskQueueType {
    Workflow = 1,
    Activity = 2,
    Nexus = 3,
}

impl TaskQueueType {
    pub fn parse(v: &DcValue) -> Option<TaskQueueType> {
        match v {
            DcValue::Int(1) => Some(TaskQueueType::Workflow),
            DcValue::Int(2) => Some(TaskQueueType::Activity),
            DcValue::Int(3) => Some(TaskQueueType::Nexus),
            DcValue::Str(s) => {
                let s = s.to_ascii_lowercase();
                let s = s.trim_start_matches("task_queue_type_");
                match s {
                    "workflow" => Some(TaskQueueType::Workflow),
                    "activity" => Some(TaskQueueType::Activity),
                    "nexus" => Some(TaskQueueType::Nexus),
                    _ => None,
                }
            }
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            TaskQueueType::Workflow => "Workflow",
            TaskQueueType::Activity => "Activity",
            TaskQueueType::Nexus => "Nexus",
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct Constraints {
    pub namespace: Option<String>,
    pub namespace_id: Option<String>,
    pub task_queue_name: Option<String>,
    pub task_queue_type: Option<TaskQueueType>,
    pub shard_id: Option<i64>,
    pub history_task_type: Option<String>,
    pub destination: Option<String>,
    pub chasm_task_type: Option<String>,
}

impl fmt::Display for Constraints {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut parts = Vec::new();
        if let Some(v) = &self.namespace {
            parts.push(format!("namespace={v}"));
        }
        if let Some(v) = &self.namespace_id {
            parts.push(format!("namespaceID={v}"));
        }
        if let Some(v) = &self.task_queue_name {
            parts.push(format!("taskQueueName={v}"));
        }
        if let Some(v) = &self.task_queue_type {
            parts.push(format!("taskType={}", v.as_str()));
        }
        if let Some(v) = &self.shard_id {
            parts.push(format!("shardID={v}"));
        }
        if let Some(v) = &self.history_task_type {
            parts.push(format!("historyTaskType={v}"));
        }
        if let Some(v) = &self.destination {
            parts.push(format!("destination={v}"));
        }
        if let Some(v) = &self.chasm_task_type {
            parts.push(format!("chasmTaskType={v}"));
        }
        if parts.is_empty() {
            f.write_str("(global)")
        } else {
            f.write_str(&parts.join(","))
        }
    }
}

#[derive(Clone, Debug)]
pub struct ConstrainedValue {
    pub value: DcValue,
    pub constraints: Constraints,
}

#[derive(Deserialize)]
struct YamlCv {
    value: DcValue,
    #[serde(default)]
    constraints: BTreeMap<String, DcValue>,
}

// --- the collection -------------------------------------------------------------------------------

#[derive(Clone, Debug, Default)]
pub struct DynamicConfig {
    values: HashMap<String, Vec<ConstrainedValue>>,
    /// Original spelling of keys for reporting.
    spelling: HashMap<String, String>,
    pub warnings: Vec<String>,
}

impl DynamicConfig {
    pub fn new() -> Self {
        Self::default()
    }

    /// Parse a Temporal dynamic config YAML document (the same format as the server's
    /// `dynamicconfig/*.yaml` file or the Helm chart's `server.dynamicConfig` values).
    pub fn from_yaml_str(src: &str, origin: &str) -> anyhow::Result<Self> {
        let mut dc = DynamicConfig::new();
        dc.merge_yaml_str(src, origin)?;
        Ok(dc)
    }

    pub fn merge_yaml_str(&mut self, src: &str, origin: &str) -> anyhow::Result<()> {
        if src.trim().is_empty() {
            return Ok(());
        }
        let raw: BTreeMap<String, Vec<YamlCv>> = serde_saphyr::from_str(src)
            .map_err(|e| anyhow::anyhow!("{origin}: invalid dynamic config: {e}"))?;
        for (key, cvs) in raw {
            let list = cvs
                .into_iter()
                .map(|cv| (cv.value, cv.constraints))
                .collect::<Vec<_>>();
            self.set_raw(&key, list, origin);
        }
        self.lint(origin);
        Ok(())
    }

    /// Cross-key checks Temporal does not do for you.
    fn lint(&mut self, origin: &str) {
        let reads = self.entries("matching.numTaskqueueReadPartitions");
        for w in self.entries("matching.numTaskqueueWritePartitions") {
            let DcValue::Int(wv) = w.value else { continue };
            // read partitions for the same constraints, else the default 4
            let rv = reads
                .iter()
                .find(|r| r.constraints == w.constraints)
                .and_then(|r| {
                    if let DcValue::Int(v) = r.value {
                        Some(v)
                    } else {
                        None
                    }
                })
                .unwrap_or(4);
            if wv > rv {
                self.warnings.push(format!(
                    "{origin}: matching.numTaskqueueWritePartitions={wv} exceeds numTaskqueueReadPartitions={rv} for {} — tasks written to partitions {rv}..{} are never polled; always raise read partitions first and lower them last",
                    w.constraints,
                    wv - 1
                ));
            }
        }
    }

    /// Merge values from a scenario's inline `dynamic_config:` map (already deserialised).
    pub fn merge_inline(&mut self, inline: &BTreeMap<String, Vec<InlineCv>>, origin: &str) {
        for (key, cvs) in inline {
            let list = cvs
                .iter()
                .map(|cv| (cv.value.clone(), cv.constraints.clone()))
                .collect::<Vec<_>>();
            self.set_raw(key, list, origin);
        }
    }

    /// Set a single unconstrained (or constrained) value, replacing an existing entry with the
    /// same constraints. Used by CLI overrides and sweeps.
    pub fn set(&mut self, key: &str, value: DcValue, constraints: Constraints) {
        let lk = key.to_ascii_lowercase();
        self.validate_key(key, "override");
        self.spelling
            .entry(lk.clone())
            .or_insert_with(|| key.to_string());
        let list = self.values.entry(lk.clone()).or_default();
        if let Some(existing) = list.iter_mut().find(|cv| cv.constraints == constraints) {
            existing.value = value;
        } else {
            // put the most specific entries first (order only matters for duplicates)
            list.insert(0, ConstrainedValue { value, constraints });
        }
        let cvs = self.values[&lk].clone();
        for cv in &cvs {
            self.validate_value(key, &cv.value, "override");
        }
    }

    fn set_raw(
        &mut self,
        key: &str,
        list: Vec<(DcValue, BTreeMap<String, DcValue>)>,
        origin: &str,
    ) {
        let lk = key.to_ascii_lowercase();
        let def = registry().get(key);
        self.validate_key(key, origin);
        let mut out = Vec::with_capacity(list.len());
        for (value, raw_constraints) in list {
            let constraints = self.convert_constraints(key, def, &raw_constraints, origin);
            self.validate_value(key, &value, origin);
            out.push(ConstrainedValue { value, constraints });
        }
        self.spelling.insert(lk.clone(), key.to_string());
        // Later files override earlier ones key-by-key, like Temporal's file client does when the
        // same key appears in a later document.
        self.values.insert(lk, out);
    }

    fn validate_key(&mut self, key: &str, origin: &str) {
        if registry().get(key).is_none() {
            let sugg = registry().suggest(key, 3);
            let hint = if sugg.is_empty() {
                String::new()
            } else {
                format!(" (did you mean {}?)", sugg.join(", "))
            };
            self.warnings.push(format!(
                "{origin}: unknown dynamic config key {key:?} for Temporal 1.31.0 — the server logs \"unregistered key\" and ignores it{hint}"
            ));
        }
    }

    fn validate_value(&mut self, key: &str, value: &DcValue, origin: &str) {
        let Some(def) = registry().get(key) else {
            return;
        };
        let ok = match def.typ.as_str() {
            "Int" => matches!(value, DcValue::Int(_)),
            "Float" => matches!(value, DcValue::Int(_) | DcValue::Float(_)),
            "Bool" => matches!(value, DcValue::Bool(_)),
            "String" => matches!(value, DcValue::Str(_)),
            "Duration" => match value {
                DcValue::Int(_) | DcValue::Float(_) => true,
                DcValue::Str(s) => parse_temporal_duration(s).is_some(),
                _ => false,
            },
            "Map" => matches!(value, DcValue::Map(_)),
            _ => true,
        };
        if !ok {
            // "an int", "a float", "a bool", ...
            let typ = def.typ.to_ascii_lowercase();
            let article = if typ.starts_with(['a', 'e', 'i', 'o', 'u']) {
                "an"
            } else {
                "a"
            };
            self.warnings.push(format!(
                "{origin}: {key} expects {article} {typ} value but got {value}; Temporal fails the conversion and falls back to the default ({})",
                def.default_display
            ));
        }
    }

    fn convert_constraints(
        &mut self,
        key: &str,
        def: Option<&SettingDef>,
        raw: &BTreeMap<String, DcValue>,
        origin: &str,
    ) -> Constraints {
        let mut c = Constraints::default();
        let scope = def.map(|d| d.scope.as_str()).unwrap_or("Unknown");
        for (k, v) in raw {
            let valid = match k.to_ascii_lowercase().as_str() {
                "namespace" => {
                    c.namespace = as_string(v);
                    matches!(scope, "Namespace" | "TaskQueue" | "Destination")
                }
                "namespaceid" => {
                    c.namespace_id = as_string(v);
                    scope == "NamespaceID"
                }
                "taskqueuename" => {
                    c.task_queue_name = as_string(v);
                    scope == "TaskQueue"
                }
                "tasktype" => {
                    c.task_queue_type = TaskQueueType::parse(v);
                    if c.task_queue_type.is_none() {
                        self.warnings.push(format!(
                            "{origin}: {key}: taskType constraint must be Workflow/Activity/Nexus, got {v}"
                        ));
                    }
                    scope == "TaskQueue"
                }
                "historytasktype" => {
                    c.history_task_type = as_string(v);
                    scope == "TaskType"
                }
                "shardid" => {
                    c.shard_id = match v {
                        DcValue::Int(i) => Some(*i),
                        _ => {
                            self.warnings.push(format!(
                                "{origin}: {key}: shardID constraint must be an integer"
                            ));
                            None
                        }
                    };
                    scope == "ShardID"
                }
                "destination" => {
                    c.destination = as_string(v);
                    scope == "Destination"
                }
                "chasmtasktype" => {
                    c.chasm_task_type = as_string(v);
                    scope == "ChasmTaskType"
                }
                other => {
                    self.warnings
                        .push(format!("{origin}: {key}: unknown constraint {other:?}"));
                    true
                }
            };
            if !valid && def.is_some() {
                self.warnings.push(format!(
                    "{origin}: {key} is a {scope}-scoped setting; constraint {k:?} can never match, so this value is ignored by the server"
                ));
            }
        }
        c
    }

    fn find(&self, key: &str, precedence: &[Constraints]) -> Option<&DcValue> {
        let list = self.values.get(&key.to_ascii_lowercase())?;
        for want in precedence {
            if let Some(cv) = list.iter().find(|cv| &cv.constraints == want) {
                return Some(&cv.value);
            }
        }
        None
    }

    fn default_of(key: &str) -> Option<DcValue> {
        registry()
            .get(key)
            .and_then(|d| d.default.as_ref())
            .map(DcValue::from_json)
    }

    pub fn is_set(&self, key: &str) -> bool {
        self.values.contains_key(&key.to_ascii_lowercase())
    }

    /// All keys explicitly configured (original spelling).
    pub fn configured_keys(&self) -> Vec<String> {
        let mut v: Vec<String> = self.spelling.values().cloned().collect();
        v.sort();
        v
    }

    pub fn entries(&self, key: &str) -> Vec<ConstrainedValue> {
        self.values
            .get(&key.to_ascii_lowercase())
            .cloned()
            .unwrap_or_default()
    }

    // precedence builders (mirroring setting_gen.go)
    pub fn prec_global() -> Vec<Constraints> {
        vec![Constraints::default()]
    }

    pub fn prec_namespace(ns: &str) -> Vec<Constraints> {
        vec![
            Constraints {
                namespace: Some(ns.to_string()),
                ..Default::default()
            },
            Constraints::default(),
        ]
    }

    pub fn prec_task_queue(ns: &str, tq: &str, tq_type: TaskQueueType) -> Vec<Constraints> {
        vec![
            Constraints {
                namespace: Some(ns.to_string()),
                task_queue_name: Some(tq.to_string()),
                task_queue_type: Some(tq_type),
                ..Default::default()
            },
            Constraints {
                namespace: Some(ns.to_string()),
                task_queue_name: Some(tq.to_string()),
                ..Default::default()
            },
            Constraints {
                task_queue_name: Some(tq.to_string()),
                ..Default::default()
            },
            Constraints {
                namespace: Some(ns.to_string()),
                ..Default::default()
            },
            Constraints::default(),
        ]
    }

    pub fn prec_shard(shard: i64) -> Vec<Constraints> {
        vec![
            Constraints {
                shard_id: Some(shard),
                ..Default::default()
            },
            Constraints::default(),
        ]
    }

    fn lookup(&self, key: &str, prec: &[Constraints]) -> Option<DcValue> {
        self.find(key, prec)
            .cloned()
            .or_else(|| Self::default_of(key))
    }

    pub fn int(&self, key: &str, prec: &[Constraints], fallback: i64) -> i64 {
        let default = Self::default_of(key);
        match self.find(key, prec) {
            Some(DcValue::Int(i)) => *i,
            // Temporal: wrong type -> conversion error -> default
            _ => match default {
                Some(DcValue::Int(i)) => i,
                Some(DcValue::Float(f)) => f as i64,
                _ => fallback,
            },
        }
    }

    pub fn float(&self, key: &str, prec: &[Constraints], fallback: f64) -> f64 {
        match self.lookup(key, prec) {
            Some(DcValue::Int(i)) => i as f64,
            Some(DcValue::Float(f)) => f,
            _ => fallback,
        }
    }

    pub fn boolean(&self, key: &str, prec: &[Constraints], fallback: bool) -> bool {
        match self.find(key, prec) {
            Some(DcValue::Bool(b)) => *b,
            // GradualChange-typed booleans (matching.useNewMatcher) may be written as maps
            Some(DcValue::Map(m)) => {
                matches!(m.get("new").or(m.get("New")), Some(DcValue::Bool(true)))
            }
            _ => match Self::default_of(key) {
                Some(DcValue::Bool(b)) => b,
                _ => fallback,
            },
        }
    }

    /// Duration in microseconds.
    pub fn duration_us(&self, key: &str, prec: &[Constraints], fallback_us: f64) -> f64 {
        let conv = |v: &DcValue| -> Option<f64> {
            match v {
                DcValue::Int(i) => Some(*i as f64 * 1e6),
                DcValue::Float(f) => Some(f * 1e6),
                DcValue::Str(s) => parse_temporal_duration(s),
                _ => None,
            }
        };
        if let Some(v) = self.find(key, prec).and_then(conv) {
            return v;
        }
        match Self::default_of(key) {
            // registry stores durations as microseconds
            Some(DcValue::Float(f)) => f,
            Some(DcValue::Int(i)) => i as f64,
            _ => fallback_us,
        }
    }

    pub fn string(&self, key: &str, prec: &[Constraints], fallback: &str) -> String {
        match self.lookup(key, prec) {
            Some(DcValue::Str(s)) => s,
            _ => fallback.to_string(),
        }
    }

    /// Render the effective value of a key for reports.
    pub fn describe(&self, key: &str) -> String {
        let entries = self.entries(key);
        if entries.is_empty() {
            let d = registry()
                .get(key)
                .map(|d| d.default_display.to_string())
                .unwrap_or_else(|| "?".into());
            return format!("{} (default)", d.trim_matches('"'));
        }
        entries
            .iter()
            .map(|cv| {
                if cv.constraints == Constraints::default() {
                    cv.value.to_string()
                } else {
                    format!("{} [{}]", cv.value, cv.constraints)
                }
            })
            .collect::<Vec<_>>()
            .join("; ")
    }
}

/// Inline dynamic config entry in a scenario file (same shape as Temporal's YAML).
#[derive(Clone, Debug, Deserialize)]
pub struct InlineCv {
    pub value: DcValue,
    #[serde(default)]
    pub constraints: BTreeMap<String, DcValue>,
}

fn as_string(v: &DcValue) -> Option<String> {
    match v {
        DcValue::Str(s) => Some(s.clone()),
        DcValue::Int(i) => Some(i.to_string()),
        _ => None,
    }
}

/// `timestamp.ParseDurationDefaultSeconds`: bare numbers are seconds; Go units plus `d`.
pub fn parse_temporal_duration(s: &str) -> Option<f64> {
    let t = s.trim();
    if let Ok(secs) = t.parse::<f64>() {
        return Some(secs * 1e6);
    }
    parse_duration_us(t).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"
history.shardIOConcurrency:
  - value: 2
frontend.namespaceRPS:
  - value: 500
    constraints:
      namespace: orders
  - value: 3000
matching.numTaskqueueReadPartitions:
  - value: 8
    constraints: {namespace: orders, taskQueueName: payments, taskType: Activity}
  - value: 16
    constraints: {taskQueueName: payments}
frontend.namespaceCount:
  - value: 10.5
matching.longPollExpirationInterval:
  - value: "30s"
history.bogusKey:
  - value: 1
frontend.rps:
  - value: 100
    constraints: {namespace: orders}
"#;

    #[test]
    fn registry_loads() {
        let r = registry();
        assert_eq!(r.temporal_version, "1.31.0");
        assert!(r.settings.len() > 500);
        let d = r.get("HISTORY.SHARDIOCONCURRENCY").unwrap();
        assert_eq!(d.scope, "Global");
    }

    #[test]
    fn precedence_matches_temporal() {
        let dc = DynamicConfig::from_yaml_str(SAMPLE, "test").unwrap();
        assert_eq!(
            dc.int(
                "history.shardIOConcurrency",
                &DynamicConfig::prec_global(),
                0
            ),
            2
        );
        assert_eq!(
            dc.int(
                "frontend.namespaceRPS",
                &DynamicConfig::prec_namespace("orders"),
                0
            ),
            500
        );
        assert_eq!(
            dc.int(
                "frontend.namespaceRPS",
                &DynamicConfig::prec_namespace("other"),
                0
            ),
            3000
        );
        let tq = |ns: &str, tq: &str, t| DynamicConfig::prec_task_queue(ns, tq, t);
        assert_eq!(
            dc.int(
                "matching.numTaskqueueReadPartitions",
                &tq("orders", "payments", TaskQueueType::Activity),
                0
            ),
            8
        );
        assert_eq!(
            dc.int(
                "matching.numTaskqueueReadPartitions",
                &tq("orders", "payments", TaskQueueType::Workflow),
                0
            ),
            16
        );
        assert_eq!(
            dc.int(
                "matching.numTaskqueueReadPartitions",
                &tq("orders", "other", TaskQueueType::Workflow),
                0
            ),
            4
        );
        // float given for an int setting -> default
        assert_eq!(
            dc.int(
                "frontend.namespaceCount",
                &DynamicConfig::prec_namespace("x"),
                0
            ),
            1200
        );
        assert_eq!(
            dc.duration_us(
                "matching.longPollExpirationInterval",
                &tq("a", "b", TaskQueueType::Workflow),
                0.0
            ),
            30e6
        );
        // global-scoped key with a namespace constraint never matches
        assert_eq!(
            dc.int("frontend.rps", &DynamicConfig::prec_global(), 0),
            2400
        );
        let w = dc.warnings.join("\n");
        assert!(w.contains("history.bogusKey"), "{w}");
        assert!(w.contains("frontend.namespaceCount"), "{w}");
        assert!(w.contains("frontend.rps is a Global-scoped"), "{w}");
    }

    #[test]
    fn suggestions() {
        let s = registry().suggest("history.shardIoConcurency", 3);
        assert!(s.contains(&"history.shardIOConcurrency"), "{s:?}");
    }
}
