//! Generator for the embedded dynamic config registry (`data/dynamicconfig-<version>.json`).
//! The `gen-dc-registry` binary (`src/bin/gen-dc-registry.rs`) is its command-line front end.
//!
//! Temporal registers every dynamic config setting with a constructor call:
//!
//! ```go
//! HistoryRPS = NewGlobalIntSetting(
//!     "history.rps",
//!     3000,
//!     `HistoryRPS is request rate per second for each history host`,
//! )
//! ```
//!
//! `common/dynamicconfig/setting_gen.go` generates one constructor per scope (precedence) and
//! value type, plus `WithConstrainedDefault` and `WithConverter` variants. Components register
//! their own settings the same way (`dynamicconfig.NewNamespaceIntSetting(...)`).
//!
//! The generator walks a server source checkout and finds those calls with a small Go lexer that
//! skips comments and string / rune literals. It splits each call's arguments at top-level
//! commas and records the setting's key, scope, value type, default and description. Defaults
//! that are constant expressions (`5*time.Minute-10*time.Second`, `4*1024*1024`, `true`, `"off"`)
//! are evaluated with Go's rules for untyped constants. Other defaults are kept as Go source.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::Context;
use serde::Serialize;
use serde_json::{Number, Value};

use super::dynamic::{RegistryFile, SettingDef};

/// Scope part of the constructor names, `New<Scope><Type>Setting`.
const SCOPES: [&str; 8] = [
    "Global",
    "Namespace",
    "NamespaceID",
    "TaskQueue",
    "ShardID",
    "TaskType",
    "Destination",
    "ChasmTaskType",
];

/// Value-type part of the constructor names.
const TYPES: [&str; 7] = ["Bool", "Int", "Float", "String", "Duration", "Map", "Typed"];

/// Identifiers that defaults refer to. `time` units are in nanoseconds, like `time.Duration`.
/// A default that uses any other identifier is kept as Go source.
const CONSTANTS: [(&str, &str); 13] = [
    ("time.Nanosecond", "1"),
    ("time.Microsecond", "1000"),
    ("time.Millisecond", "1000000"),
    ("time.Second", "1000000000"),
    ("time.Minute", "60000000000"),
    ("time.Hour", "3600000000000"),
    // common/debug/not_debug.go (100 only in TEMPORAL_DEBUG builds)
    ("debug.TimeoutMultiplier", "1"),
    // common/primitives/constants.go
    ("primitives.DefaultTransactionSizeLimit", "4 * 1024 * 1024"),
    ("primitives.DefaultWorkflowTaskTimeout", "10 * time.Second"),
    ("primitives.GetHistoryMaxPageSize", "256"),
    ("primitives.DefaultHistoryMaxAutoResetPoints", "20"),
    // common/dynamicconfig/shared_constants.go
    ("GlobalDefaultNumTaskQueuePartitions", "4"),
    // a constrained default: 1 for the per-namespace worker queues, 4 for everything else
    ("defaultNumTaskQueuePartitions", "4"),
];

/// Where the server records its version (`ServerVersion = "1.31.0"`).
pub const VERSION_FILE: &str = "common/headers/version_checker.go";

/// A generated registry, plus diagnostics for whoever runs the generator.
#[derive(Debug)]
pub struct Generated {
    pub registry: RegistryFile,
    /// Non-test Go files read.
    pub files_scanned: usize,
    /// `key = <Go expression>` for scalar settings whose default is not a constant expression.
    pub unevaluated: Vec<String>,
    pub warnings: Vec<String>,
}

/// Build the registry from a Temporal server source checkout. `version` overrides the
/// `ServerVersion` found in the source.
pub fn generate(root: &Path, version: Option<String>) -> anyhow::Result<Generated> {
    anyhow::ensure!(
        root.join("common/dynamicconfig").is_dir(),
        "{} is not a Temporal server checkout (it has no common/dynamicconfig)",
        root.display()
    );
    let temporal_version = match version {
        Some(v) => v,
        None => detect_version(root).with_context(|| {
            format!(
                "no ServerVersion in {}; pass --temporal-version",
                root.join(VERSION_FILE).display()
            )
        })?,
    };
    let mut files = Vec::new();
    collect_go_files(root, &mut files)?;
    files.sort();

    let mut by_key: BTreeMap<String, SettingDef> = BTreeMap::new();
    let mut warnings = Vec::new();
    for path in &files {
        let bytes = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
        let src = String::from_utf8_lossy(&bytes);
        if !src.contains("Setting") {
            continue;
        }
        for def in scan_file(&relative(root, path), &src) {
            let lk = def.key.to_ascii_lowercase();
            if let Some(prev) = by_key.get(&lk) {
                warnings.push(format!(
                    "{} is registered twice: {} and {}",
                    def.key, prev.source, def.source
                ));
                // the central registry wins; otherwise the later file does
                if prev.source.starts_with("common/dynamicconfig/") {
                    continue;
                }
            }
            by_key.insert(lk, def);
        }
    }
    // keys are case-insensitive: sorted by their lower-cased form
    let settings: Vec<SettingDef> = by_key.into_values().collect();
    let unevaluated = settings
        .iter()
        .filter(|s| s.default.is_none() && !matches!(s.typ.as_str(), "Map" | "Typed"))
        .map(|s| format!("{} = {}", s.key, one_line(&s.default_go)))
        .collect();
    Ok(Generated {
        registry: RegistryFile {
            temporal_version,
            settings,
        },
        files_scanned: files.len(),
        unevaluated,
        warnings,
    })
}

/// `ServerVersion` from `common/headers/version_checker.go`.
pub fn detect_version(root: &Path) -> Option<String> {
    let src = std::fs::read_to_string(root.join(VERSION_FILE)).ok()?;
    src.match_indices("ServerVersion").find_map(|(at, name)| {
        let rest = src[at + name.len()..]
            .trim_start()
            .strip_prefix('=')?
            .trim_start();
        let end = quoted_end(rest.as_bytes(), 0)?;
        unquote(rest.get(1..end)?)
    })
}

/// The registry file's text, with the one-space indentation of the committed file.
pub fn to_json(reg: &RegistryFile) -> String {
    let mut buf = Vec::new();
    let fmt = serde_json::ser::PrettyFormatter::with_indent(b" ");
    let mut ser = serde_json::Serializer::with_formatter(&mut buf, fmt);
    reg.serialize(&mut ser)
        .expect("the registry serialises to JSON");
    let mut text = String::from_utf8(buf).expect("serde_json writes UTF-8");
    text.push('\n');
    text
}

/// What changed from `old` to `new`, one line per difference: `+` added, `-` removed,
/// `~` changed.
pub fn diff(old: &RegistryFile, new: &RegistryFile) -> Vec<String> {
    let mut out = Vec::new();
    if old.temporal_version != new.temporal_version {
        out.push(format!(
            "~ temporal_version: {} → {}",
            old.temporal_version, new.temporal_version
        ));
    }
    let index = |r: &RegistryFile| -> BTreeMap<String, Value> {
        r.settings
            .iter()
            .map(|s| {
                let v = serde_json::to_value(s).expect("settings serialise to JSON");
                (s.key.to_ascii_lowercase(), v)
            })
            .collect()
    };
    let (before, after) = (index(old), index(new));
    for (k, now) in &after {
        let key = now["key"].as_str().unwrap_or(k);
        let Some(Value::Object(was)) = before.get(k) else {
            out.push(format!(
                "+ {key} ({} {}, default {})",
                now["scope"].as_str().unwrap_or("?"),
                now["type"].as_str().unwrap_or("?"),
                short(&now["default_display"])
            ));
            continue;
        };
        if let Value::Object(now) = now {
            for (field, v) in now {
                let prev = was.get(field).unwrap_or(&Value::Null);
                if prev != v {
                    out.push(format!("~ {key}: {field} {} → {}", short(prev), short(v)));
                }
            }
        }
    }
    for (k, was) in &before {
        if !after.contains_key(k) {
            out.push(format!("- {}", was["key"].as_str().unwrap_or(k)));
        }
    }
    out
}

fn short(v: &Value) -> String {
    let s = v.to_string();
    if s.chars().count() > 80 {
        format!("{}…", s.chars().take(79).collect::<String>())
    } else {
        s
    }
}

fn one_line(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

// --- walking the checkout -------------------------------------------------------------------------

/// Non-test Go files under `dir`. Test-only trees (`tests*`, `temporaltest*`) and hidden
/// directories are skipped.
fn collect_go_files(dir: &Path, out: &mut Vec<PathBuf>) -> anyhow::Result<()> {
    let entries = std::fs::read_dir(dir).with_context(|| format!("reading {}", dir.display()))?;
    for entry in entries {
        let entry = entry.with_context(|| format!("reading {}", dir.display()))?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let kind = entry.file_type()?;
        if kind.is_dir() {
            let skip = name.starts_with('.')
                || name.starts_with("tests")
                || name.starts_with("temporaltest");
            if !skip {
                collect_go_files(&entry.path(), out)?;
            }
        } else if kind.is_file()
            && name.ends_with(".go")
            && !name.ends_with("_test.go")
            && !name.ends_with("_mock.go")
        {
            out.push(entry.path());
        }
    }
    Ok(())
}

/// `path` relative to the checkout, with `/` separators.
fn relative(root: &Path, path: &Path) -> String {
    let rel = path.strip_prefix(root).unwrap_or(path);
    rel.components()
        .map(|c| c.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

// --- finding registrations ------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Variant {
    Plain,
    /// `(key, []TypedConstrainedValue[T]{...}, description)`
    ConstrainedDefault,
    /// `(key, converter, default, description)`
    Converter,
}

/// A matched `New<Scope><Type>Setting...(`.
#[derive(Debug)]
struct Ctor {
    scope: &'static str,
    typ: &'static str,
    variant: Variant,
    /// Just past the call's `(`.
    args_start: usize,
}

/// The settings registered in one Go file. `rel` is the file's path in the checkout.
pub fn scan_file(rel: &str, src: &str) -> Vec<SettingDef> {
    let b = src.as_bytes();
    let line_starts: Vec<usize> = std::iter::once(0)
        .chain(src.match_indices('\n').map(|(i, _)| i + 1))
        .collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        if let Some(next) = skip_opaque(b, i) {
            i = next;
            continue;
        }
        if b[i] == b'N'
            && (i == 0 || !is_ident(b[i - 1]))
            && let Some(ctor) = match_ctor(b, i)
        {
            // skip the constructors' own definitions in setting_gen.go
            if !is_func_decl(src, i)
                && let Some(args) = split_args(src, ctor.args_start)
            {
                let line = line_starts.partition_point(|&s| s <= i);
                if let Some(def) = setting(&ctor, &args, format!("{rel}:{line}")) {
                    out.push(def);
                }
            }
            i = ctor.args_start;
            continue;
        }
        i += 1;
    }
    out
}

/// A constructor call starting at `at`:
/// `New<Scope><Type>Setting[WithConstrainedDefault|WithConverter]`, optional type arguments,
/// then `(`.
fn match_ctor(b: &[u8], at: usize) -> Option<Ctor> {
    let rest = b[at..].strip_prefix(b"New")?;
    for scope in SCOPES {
        let Some(rest) = rest.strip_prefix(scope.as_bytes()) else {
            continue;
        };
        for typ in TYPES {
            let Some(rest) = rest
                .strip_prefix(typ.as_bytes())
                .and_then(|r| r.strip_prefix(b"Setting"))
            else {
                continue;
            };
            let (variant, rest) = if let Some(r) = rest.strip_prefix(b"WithConstrainedDefault") {
                (Variant::ConstrainedDefault, r)
            } else if let Some(r) = rest.strip_prefix(b"WithConverter") {
                (Variant::Converter, r)
            } else {
                (Variant::Plain, rest)
            };
            let mut k = b.len() - rest.len();
            while k < b.len() && is_space(b[k]) {
                k += 1;
            }
            if b.get(k) == Some(&b'[') {
                k = closing(b, k)? + 1; // explicit type arguments, e.g. [[]string]
            }
            if b.get(k) == Some(&b'(') {
                return Some(Ctor {
                    scope,
                    typ,
                    variant,
                    args_start: k + 1,
                });
            }
        }
    }
    None
}

/// `func NewGlobalIntSetting(...)`: the text before `at` on its line starts with `func`.
fn is_func_decl(src: &str, at: usize) -> bool {
    let line_start = src[..at].rfind('\n').map_or(0, |p| p + 1);
    src[line_start..at].trim_start().starts_with("func")
}

/// Turn one constructor call into a registry entry. `None` when the first argument is not a
/// string constant naming a dotted key.
fn setting(ctor: &Ctor, args: &[String], source: String) -> Option<SettingDef> {
    let key = string_const(args.first()?)?;
    if !key.contains('.') {
        return None;
    }
    let default_go = match ctor.variant {
        Variant::Converter => args.get(2),
        _ => args.get(1),
    }
    .map_or("", String::as_str);
    let description = match args.len() {
        0..=2 => String::new(),
        n => string_const(&args[n - 1]).unwrap_or_else(|| args[n - 1].clone()),
    };
    let default = match ctor.variant {
        Variant::Plain => eval_default(default_go, ctor.typ),
        Variant::ConstrainedDefault => constrained_default(default_go, ctor.typ),
        Variant::Converter => converter_default(default_go),
    };
    let default_display = match &default {
        Some(Value::Number(n)) if ctor.typ == "Duration" => Value::String(fmt_us(n.as_f64()?)),
        Some(v) => v.clone(),
        None => Value::String(default_go.chars().take(200).collect()),
    };
    Some(SettingDef {
        key,
        scope: ctor.scope.to_string(),
        typ: ctor.typ.to_string(),
        default,
        default_display,
        default_go: default_go.chars().take(400).collect(),
        description: one_line(&description),
        source,
    })
}

// --- Go lexing ------------------------------------------------------------------------------------

fn is_space(c: u8) -> bool {
    matches!(c, b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c)
}

fn is_ident(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_' || c >= 0x80
}

fn find(b: &[u8], from: usize, pat: &[u8]) -> Option<usize> {
    b.get(from..)?
        .windows(pat.len())
        .position(|w| w == pat)
        .map(|p| from + p)
}

/// If a comment or a string / rune literal starts at `i`, the index just past it.
fn skip_opaque(b: &[u8], i: usize) -> Option<usize> {
    match b[i] {
        b'/' => match b.get(i + 1) {
            Some(b'/') => Some(find(b, i + 2, b"\n").unwrap_or(b.len())),
            Some(b'*') => Some(find(b, i + 2, b"*/").map_or(b.len(), |j| j + 2)),
            _ => None,
        },
        b'`' => Some(find(b, i + 1, b"`").map_or(b.len(), |j| j + 1)),
        // an unterminated literal (not valid Go) ends at the end of its line
        b'"' | b'\'' => Some(
            quoted_end(b, i)
                .map(|j| j + 1)
                .unwrap_or_else(|| find(b, i, b"\n").unwrap_or(b.len())),
        ),
        _ => None,
    }
}

/// Index of the quote that closes the interpreted string or rune literal opened at `i`.
fn quoted_end(b: &[u8], i: usize) -> Option<usize> {
    let q = *b.get(i)?;
    if q != b'"' && q != b'\'' {
        return None;
    }
    let mut j = i + 1;
    while j < b.len() {
        match b[j] {
            b'\\' => j += 2,
            b'\n' => return None,
            c if c == q => return Some(j),
            _ => j += 1,
        }
    }
    None
}

/// Index of the bracket that closes the one opened at `open`, skipping literals and comments.
fn closing(b: &[u8], open: usize) -> Option<usize> {
    let mut depth = 0usize;
    let mut i = open;
    while i < b.len() {
        if let Some(next) = skip_opaque(b, i) {
            i = next;
            continue;
        }
        match b[i] {
            b'(' | b'[' | b'{' => depth += 1,
            b')' | b']' | b'}' => {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// Arguments of the call whose `(` ends just before `start`: split at top-level commas, with
/// comments removed and whitespace trimmed. Empty arguments (a trailing comma) are dropped.
fn split_args(src: &str, start: usize) -> Option<Vec<String>> {
    let b = src.as_bytes();
    let mut args = Vec::new();
    let mut cur = String::new();
    let mut seg = start; // start of the text not yet copied into `cur`
    let mut depth = 0usize;
    let mut i = start;
    while i < b.len() {
        if let Some(next) = skip_opaque(b, i) {
            if b[i] == b'/' {
                seg = drop_comment(src, &mut cur, seg, i, next);
            }
            i = next;
            continue;
        }
        match b[i] {
            b'(' | b'[' | b'{' => depth += 1,
            b')' | b']' | b'}' if depth > 0 => depth -= 1,
            b')' | b']' | b'}' => {
                cur.push_str(&src[seg..i]);
                args.push(cur);
                let args = args
                    .into_iter()
                    .map(|a| a.trim().to_string())
                    .filter(|a| !a.is_empty())
                    .collect();
                return Some(args);
            }
            b',' if depth == 0 => {
                cur.push_str(&src[seg..i]);
                args.push(std::mem::take(&mut cur));
                seg = i + 1;
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// Copy the text before the comment at `at..end` into `cur`; returns where copying resumes.
/// A comment on a line of its own takes the whole line with it.
fn drop_comment(src: &str, cur: &mut String, seg: usize, at: usize, end: usize) -> usize {
    let b = src.as_bytes();
    let line_start = src[..at].rfind('\n').map_or(0, |p| p + 1);
    let line_end = src[end..].find('\n').map_or(src.len(), |p| end + p);
    let alone = line_start >= seg
        && b[line_start..at].iter().all(|&c| is_space(c))
        && b[end..line_end].iter().all(|&c| is_space(c));
    if alone {
        cur.push_str(&src[seg..line_start]);
        (line_end + 1).min(src.len())
    } else {
        cur.push_str(&src[seg..at]);
        end
    }
}

/// The value of a Go string constant: literals (interpreted or raw) joined by `+`.
fn string_const(expr: &str) -> Option<String> {
    let b = expr.as_bytes();
    let mut out = String::new();
    let mut i = 0;
    loop {
        while i < b.len() && is_space(b[i]) {
            i += 1;
        }
        match b.get(i)? {
            b'`' => {
                let end = find(b, i + 1, b"`")?;
                // carriage returns are discarded from raw string values
                out.extend(expr[i + 1..end].chars().filter(|&c| c != '\r'));
                i = end + 1;
            }
            b'"' => {
                let end = quoted_end(b, i)?;
                out.push_str(&unquote(&expr[i + 1..end])?);
                i = end + 1;
            }
            _ => return None,
        }
        while i < b.len() && is_space(b[i]) {
            i += 1;
        }
        match b.get(i) {
            None => return Some(out),
            Some(b'+') => i += 1,
            Some(_) => return None,
        }
    }
}

/// Decode the escapes of an interpreted string literal's body.
fn unquote(s: &str) -> Option<String> {
    let b = s.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] != b'\\' {
            out.push(b[i]);
            i += 1;
            continue;
        }
        let c = *b.get(i + 1)?;
        i += 2;
        match c {
            b'a' => out.push(0x07),
            b'b' => out.push(0x08),
            b'f' => out.push(0x0c),
            b'n' => out.push(b'\n'),
            b'r' => out.push(b'\r'),
            b't' => out.push(b'\t'),
            b'v' => out.push(0x0b),
            b'\\' | b'"' | b'\'' => out.push(c),
            b'x' => {
                out.push(u8::from_str_radix(s.get(i..i + 2)?, 16).ok()?);
                i += 2;
            }
            b'0'..=b'7' => {
                out.push(u8::from_str_radix(s.get(i - 1..i + 2)?, 8).ok()?);
                i += 2;
            }
            b'u' | b'U' => {
                let n = if c == b'u' { 4 } else { 8 };
                let ch = char::from_u32(u32::from_str_radix(s.get(i..i + n)?, 16).ok()?)?;
                out.extend_from_slice(ch.encode_utf8(&mut [0; 4]).as_bytes());
                i += n;
            }
            _ => return None,
        }
    }
    Some(String::from_utf8_lossy(&out).into_owned())
}

// --- defaults -------------------------------------------------------------------------------------

/// The default of a plain constructor, as JSON: durations in microseconds.
fn eval_default(expr: &str, typ: &str) -> Option<Value> {
    let e = expr.trim();
    match typ {
        "Bool" => match e {
            "true" => Some(Value::Bool(true)),
            "false" => Some(Value::Bool(false)),
            _ => None,
        },
        "String" => string_const(e).map(Value::String),
        "Int" => match eval_const(e)? {
            Num::Int(v) => int_value(v),
            // an untyped float constant converts to int when it is integral
            Num::Float(f) if f.is_finite() && f.fract() == 0.0 => int_value(f as i128),
            Num::Float(_) => None,
        },
        "Float" => match eval_const(e)? {
            Num::Int(v) => int_value(v),
            Num::Float(f) => Number::from_f64(f).map(Value::Number),
        },
        "Duration" => Number::from_f64(eval_const(e)?.float() / 1000.0).map(Value::Number),
        _ => None,
    }
}

/// `[]TypedConstrainedValue[T]{{Constraints: ..., Value: a}, {Value: b}}`: the last `Value:`
/// is the unconstrained fallback.
fn constrained_default(expr: &str, typ: &str) -> Option<Value> {
    match expr.rfind("Value:") {
        Some(p) => {
            let rest = expr[p + "Value:".len()..].trim_start();
            let end = rest.find([',', '\n', '}']).unwrap_or(rest.len());
            eval_default(&rest[..end], typ)
        }
        // a named default, e.g. defaultNumTaskQueuePartitions
        None => eval_default(expr, typ),
    }
}

/// `StaticGradualChange(true)` / `StaticGradualChange[int](0)` defaults of converter settings.
fn converter_default(expr: &str) -> Option<Value> {
    let name = "StaticGradualChange";
    let b = expr.as_bytes();
    let mut k = expr.find(name)? + name.len();
    if b.get(k) == Some(&b'[') {
        k = closing(b, k)? + 1;
    }
    if b.get(k) != Some(&b'(') {
        return None;
    }
    let inner = expr[k + 1..closing(b, k)?].trim();
    match inner {
        "true" | "false" => eval_default(inner, "Bool"),
        _ => eval_default(inner, "Int"),
    }
}

fn int_value(v: i128) -> Option<Value> {
    if let Ok(v) = i64::try_from(v) {
        Some(Value::from(v))
    } else {
        u64::try_from(v).ok().map(Value::from)
    }
}

/// An untyped Go constant.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Num {
    Int(i128),
    Float(f64),
}

impl Num {
    fn float(self) -> f64 {
        match self {
            Num::Int(v) => v as f64,
            Num::Float(f) => f,
        }
    }
}

/// Evaluate a Go constant expression: numeric literals, [`CONSTANTS`], `+ - * / % << >> & | ^
/// &^`, unary `+ - ^`, parentheses and numeric conversions such as `time.Duration(x)`. Integer
/// arithmetic is exact and `/` truncates between integers; a float operand makes a float.
fn eval_const(expr: &str) -> Option<Num> {
    eval_nested(expr, 0)
}

fn eval_nested(expr: &str, depth: usize) -> Option<Num> {
    if depth > 8 {
        return None; // constants referring to constants: no cycles in the table, but be safe
    }
    let mut p = ConstParser {
        s: expr,
        i: 0,
        depth,
    };
    let v = p.binary(1)?;
    p.skip_space();
    (p.i == expr.len()).then_some(v)
}

struct ConstParser<'a> {
    s: &'a str,
    i: usize,
    depth: usize,
}

impl ConstParser<'_> {
    fn peek(&self) -> Option<u8> {
        self.s.as_bytes().get(self.i).copied()
    }

    fn skip_space(&mut self) {
        while self.peek().is_some_and(is_space) {
            self.i += 1;
        }
    }

    /// The binary operator at the cursor and its Go precedence.
    fn operator(&mut self) -> Option<(&'static str, u8)> {
        self.skip_space();
        let rest = &self.s[self.i..];
        const OPS: [(&str, u8); 11] = [
            ("&^", 5),
            ("<<", 5),
            (">>", 5),
            ("*", 5),
            ("/", 5),
            ("%", 5),
            ("&", 5),
            ("+", 4),
            ("-", 4),
            ("|", 4),
            ("^", 4),
        ];
        OPS.into_iter().find(|(op, _)| rest.starts_with(op))
    }

    /// Precedence climbing: operators of equal precedence associate to the left.
    fn binary(&mut self, min_prec: u8) -> Option<Num> {
        let mut lhs = self.unary()?;
        while let Some((op, prec)) = self.operator() {
            if prec < min_prec {
                break;
            }
            self.i += op.len();
            let rhs = self.binary(prec + 1)?;
            lhs = apply(op, lhs, rhs)?;
        }
        Some(lhs)
    }

    fn unary(&mut self) -> Option<Num> {
        self.skip_space();
        match self.peek()? {
            b'+' => {
                self.i += 1;
                self.unary()
            }
            b'-' => {
                self.i += 1;
                match self.unary()? {
                    Num::Int(v) => Some(Num::Int(v.checked_neg()?)),
                    Num::Float(f) => Some(Num::Float(-f)),
                }
            }
            b'^' => {
                self.i += 1;
                match self.unary()? {
                    Num::Int(v) => Some(Num::Int(!v)),
                    Num::Float(_) => None,
                }
            }
            _ => self.primary(),
        }
    }

    fn primary(&mut self) -> Option<Num> {
        let c = self.peek()?;
        if c == b'(' {
            self.i += 1;
            let v = self.binary(1)?;
            return self.close_paren().then_some(v);
        }
        let digit_next = self
            .s
            .as_bytes()
            .get(self.i + 1)
            .is_some_and(u8::is_ascii_digit);
        if c.is_ascii_digit() || (c == b'.' && digit_next) {
            return self.number();
        }
        if !(c.is_ascii_alphabetic() || c == b'_') {
            return None;
        }
        // a (qualified) identifier: a known constant, or a conversion like int64(x)
        let start = self.i;
        while self.peek().is_some_and(|c| is_ident(c) || c == b'.') {
            self.i += 1;
        }
        let name = &self.s[start..self.i];
        self.skip_space();
        if self.peek() == Some(b'(') {
            self.i += 1;
            let v = self.binary(1)?;
            return if self.close_paren() {
                convert(name, v)
            } else {
                None
            };
        }
        let (_, value) = CONSTANTS.iter().find(|(n, _)| *n == name)?;
        eval_nested(value, self.depth + 1)
    }

    fn close_paren(&mut self) -> bool {
        self.skip_space();
        if self.peek() == Some(b')') {
            self.i += 1;
            true
        } else {
            false
        }
    }

    /// A Go integer or floating-point literal (underscores allowed; no hex floats).
    fn number(&mut self) -> Option<Num> {
        let b = self.s.as_bytes();
        let start = self.i;
        let radix = match (b[start], b.get(start + 1).map(u8::to_ascii_lowercase)) {
            (b'0', Some(b'x')) => 16,
            (b'0', Some(b'b')) => 2,
            (b'0', Some(b'o')) => 8,
            _ => 10,
        };
        let v = if radix != 10 {
            self.i += 2;
            let digits_start = self.i;
            while self
                .peek()
                .is_some_and(|c| c.is_ascii_hexdigit() || c == b'_')
            {
                self.i += 1;
            }
            let digits = self.s[digits_start..self.i].replace('_', "");
            Num::Int(i128::from_str_radix(&digits, radix).ok()?)
        } else {
            let digits = |p: &mut Self| {
                while p.peek().is_some_and(|c| c.is_ascii_digit() || c == b'_') {
                    p.i += 1;
                }
            };
            digits(self);
            let mut float = false;
            if self.peek() == Some(b'.') {
                float = true;
                self.i += 1;
                digits(self);
            }
            if matches!(self.peek(), Some(b'e' | b'E')) {
                float = true;
                self.i += 1;
                if matches!(self.peek(), Some(b'+' | b'-')) {
                    self.i += 1;
                }
                digits(self);
            }
            let text = self.s[start..self.i].replace('_', "");
            if float {
                Num::Float(text.parse().ok()?)
            } else if text.len() > 1 && text.starts_with('0') {
                Num::Int(i128::from_str_radix(&text[1..], 8).ok()?) // legacy octal: 0755
            } else {
                Num::Int(text.parse().ok()?)
            }
        };
        // `10ms` or `1i` are not constants we understand
        if self.peek().is_some_and(is_ident) {
            return None;
        }
        Some(v)
    }
}

/// A binary operation on untyped constants.
fn apply(op: &str, a: Num, b: Num) -> Option<Num> {
    Some(match (a, b) {
        (Num::Int(x), Num::Int(y)) => Num::Int(match op {
            "+" => x.checked_add(y)?,
            "-" => x.checked_sub(y)?,
            "*" => x.checked_mul(y)?,
            "/" => x.checked_div(y)?,
            "%" => x.checked_rem(y)?,
            "<<" => {
                let r = x.checked_shl(u32::try_from(y).ok()?)?;
                if r >> y != x {
                    return None; // overflow
                }
                r
            }
            ">>" => x >> u32::try_from(y).ok()?.min(127),
            "&" => x & y,
            "|" => x | y,
            "^" => x ^ y,
            "&^" => x & !y,
            _ => return None,
        }),
        _ => {
            let (x, y) = (a.float(), b.float());
            Num::Float(match op {
                "+" => x + y,
                "-" => x - y,
                "*" => x * y,
                "/" if y != 0.0 => x / y,
                _ => return None,
            })
        }
    })
}

/// Numeric conversions: `time.Duration(x)`, `int64(x)`, `float64(x)`, ...
fn convert(name: &str, v: Num) -> Option<Num> {
    match name {
        "time.Duration" | "int" | "int8" | "int16" | "int32" | "int64" | "uint" | "uint8"
        | "uint16" | "uint32" | "uint64" => match v {
            Num::Int(_) => Some(v),
            Num::Float(f) if f.is_finite() && f.fract() == 0.0 => Some(Num::Int(f as i128)),
            Num::Float(_) => None,
        },
        "float32" | "float64" => Some(Num::Float(v.float())),
        _ => None,
    }
}

/// A duration in microseconds, the way Temporal's docs write it: `1h`, `5m`, `500ms`.
fn fmt_us(us: f64) -> String {
    if us == 0.0 {
        return "0s".into();
    }
    for (unit, mult) in [
        ("h", 3600e6),
        ("m", 60e6),
        ("s", 1e6),
        ("ms", 1e3),
        ("us", 1.0),
    ] {
        let q = us / mult;
        if us >= mult && (q - q.round()).abs() < 1e-9 {
            return format!("{}{unit}", q.round() as i64);
        }
    }
    format!("{}s", float_repr(us / 1e6))
}

/// Shortest round-trip form of a float, with `.0` on whole numbers and exponent notation
/// outside 1e-4..1e16 (`5e-07`), as the registry has always written it.
fn float_repr(x: f64) -> String {
    let a = x.abs();
    if a == 0.0 || (1e-4..1e16).contains(&a) {
        let s = format!("{x}");
        if s.contains('.') { s } else { format!("{s}.0") }
    } else {
        let s = format!("{x:e}");
        let (mantissa, exp) = s.split_once('e').unwrap_or((s.as_str(), "0"));
        let (sign, digits) = match exp.strip_prefix('-') {
            Some(d) => ('-', d),
            None => ('+', exp),
        };
        format!("{mantissa}e{sign}{digits:0>2}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"package dynamicconfig

import "time"

var (
	HistoryRPS = NewGlobalIntSetting(
		"history.rps",
		3000,
		`HistoryRPS is request rate
per second for each history host`,
	)
	// NewGlobalIntSetting("commented.out", 1, "ignored")
	Timeout = NewNamespaceDurationSetting(
		"history.someTimeout",
		5*time.Minute-10*time.Second,
		"A timeout. " +
			"Second sentence.",
	)
	Limit = dynamicconfig.NewNamespaceIntSetting(
		"component.limit",
		// Temporary limit, this isn't final: see issue #1, #2.
		30,
		`Limits things.`,
	)
	Partitions = NewTaskQueueIntSettingWithConstrainedDefault(
		"matching.parts",
		[]TypedConstrainedValue[int]{
			// one partition for system queues
			{Constraints: Constraints{TaskQueueName: "x"}, Value: 1},
			{Value: 4},
		},
		`Partitions.`,
	)
	UseNew = NewTaskQueueTypedSettingWithConverter(
		"matching.useNewMatcher",
		ConvertGradualChange(false),
		StaticGradualChange(true),
		`Use the new matcher.`,
	)
	Batch = NewTaskQueueTypedSettingWithConverter(
		"matching.batch",
		ConvertGradualChange(0),
		StaticGradualChange[int](0),
		`Batch size.`,
	)
	Headers = NewGlobalTypedSetting[[]string](
		"frontend.headers",
		[]string{"a", "b"},
		`Headers.`,
	)
	Size = NewGlobalIntSetting("limit.size", primitives.DefaultTransactionSizeLimit, "Size.")
	Shift = NewGlobalIntSetting("limit.shift", 1 << 20, "Shift.")
	Ratio = NewNamespaceFloatSetting("frontend.ratio", 0.2, "Ratio.")
	Mode = NewGlobalStringSetting("system.mode", "o\x66f", "Mode.")
	Flag = NewNamespaceBoolSetting("system.flag", true, "Flag.")
	Slow = NewGlobalDurationSetting("history.slow", 5*time.Second*debug.TimeoutMultiplier, "Slow.")
	Env = NewGlobalBoolSetting("system.env", os.Getenv("X") == "", "Env.")
	Rune = NewGlobalStringSetting("system.rune", "it's", "Has ' in a string.")
	NoDot = NewGlobalIntSetting("nodot", 1, "not a dynamic config key")
)

func NewGlobalIntSetting(key Key, def int, description string) GlobalIntSetting {
	return GlobalIntSetting{}
}
"#;

    fn by_key(defs: &[SettingDef]) -> BTreeMap<&str, &SettingDef> {
        defs.iter().map(|d| (d.key.as_str(), d)).collect()
    }

    #[test]
    fn finds_every_registration() {
        let defs = scan_file("common/dynamicconfig/constants.go", SAMPLE);
        let keys: Vec<&str> = defs.iter().map(|d| d.key.as_str()).collect();
        assert_eq!(
            keys,
            [
                "history.rps",
                "history.someTimeout",
                "component.limit",
                "matching.parts",
                "matching.useNewMatcher",
                "matching.batch",
                "frontend.headers",
                "limit.size",
                "limit.shift",
                "frontend.ratio",
                "system.mode",
                "system.flag",
                "history.slow",
                "system.env",
                "system.rune",
            ]
        );
        let d = by_key(&defs);
        let rps = d["history.rps"];
        assert_eq!((rps.scope.as_str(), rps.typ.as_str()), ("Global", "Int"));
        assert_eq!(rps.default, Some(Value::from(3000)));
        assert_eq!(
            rps.description,
            "HistoryRPS is request rate per second for each history host"
        );
        assert_eq!(rps.source, "common/dynamicconfig/constants.go:6");
        assert_eq!(
            d["component.limit"].source,
            "common/dynamicconfig/constants.go:19"
        );
    }

    #[test]
    fn comments_inside_calls_are_ignored() {
        let defs = scan_file("x.go", SAMPLE);
        let limit = by_key(&defs)["component.limit"];
        // the comment has an apostrophe and commas: neither may split or swallow arguments
        assert_eq!(limit.default, Some(Value::from(30)));
        assert_eq!(limit.default_go, "30");
        assert_eq!(limit.description, "Limits things.");
        let parts = by_key(&defs)["matching.parts"];
        assert!(!parts.default_go.contains("//"), "{}", parts.default_go);
        assert_eq!(parts.default, Some(Value::from(4)));
    }

    #[test]
    fn defaults_are_evaluated() {
        let defs = scan_file("x.go", SAMPLE);
        let d = by_key(&defs);
        let timeout = d["history.someTimeout"];
        assert_eq!(timeout.default, Some(Value::from(290_000_000.0)));
        assert_eq!(timeout.default_display, Value::from("290s"));
        assert_eq!(timeout.description, "A timeout. Second sentence.");
        assert_eq!(d["history.slow"].default_display, Value::from("5s"));
        assert_eq!(d["matching.useNewMatcher"].default, Some(Value::Bool(true)));
        assert_eq!(d["matching.batch"].default, Some(Value::from(0)));
        assert_eq!(d["limit.size"].default, Some(Value::from(4_194_304)));
        assert_eq!(d["limit.shift"].default, Some(Value::from(1_048_576)));
        assert_eq!(d["frontend.ratio"].default, Some(Value::from(0.2)));
        assert_eq!(d["system.mode"].default, Some(Value::from("off")));
        assert_eq!(d["system.flag"].default, Some(Value::Bool(true)));
        assert_eq!(d["system.rune"].default, Some(Value::from("it's")));
        // not constants: kept as Go source
        let env = d["system.env"];
        assert_eq!(env.default, None);
        assert_eq!(env.default_display, Value::from(r#"os.Getenv("X") == """#));
        let headers = d["frontend.headers"];
        assert_eq!(
            (headers.typ.as_str(), headers.default.as_ref()),
            ("Typed", None)
        );
        assert_eq!(headers.default_go, r#"[]string{"a", "b"}"#);
    }

    #[test]
    fn go_constant_arithmetic() {
        let ev = |s: &str| eval_const(s);
        assert_eq!(ev("7 / 2"), Some(Num::Int(3)));
        assert_eq!(ev("-7 % 3"), Some(Num::Int(-1)));
        assert_eq!(ev("7.0 / 2"), Some(Num::Float(3.5)));
        assert_eq!(ev("(2 + 3) * 4"), Some(Num::Int(20)));
        assert_eq!(ev("2 + 3 * 4"), Some(Num::Int(14)));
        assert_eq!(ev("10 - 4 - 3"), Some(Num::Int(3)));
        assert_eq!(ev("0x10 | 0b1 | 0o10"), Some(Num::Int(25)));
        assert_eq!(ev("1_000 &^ 8"), Some(Num::Int(992)));
        assert_eq!(ev("^0"), Some(Num::Int(-1)));
        assert_eq!(ev("1e6"), Some(Num::Float(1e6)));
        assert_eq!(
            ev("time.Duration(1.5 * float64(time.Second))"),
            Some(Num::Int(1_500_000_000))
        );
        assert_eq!(ev("14*24*time.Hour"), Some(Num::Int(1_209_600_000_000_000)));
        assert_eq!(ev("1 / 0"), None);
        assert_eq!(ev("math.MaxInt64"), None);
        assert_eq!(ev("10ms"), None);
        assert_eq!(eval_default("1e6", "Int"), Some(Value::from(1_000_000)));
    }

    #[test]
    fn string_constants() {
        assert_eq!(string_const(r#""a\tbé""#).as_deref(), Some("a\tb\u{e9}"));
        assert_eq!(string_const("`raw\\n` + \"x\"").as_deref(), Some("raw\\nx"));
        assert_eq!(string_const("name"), None);
        assert_eq!(string_const(r#""a" + b"#), None);
    }

    #[test]
    fn duration_display() {
        assert_eq!(fmt_us(0.0), "0s");
        assert_eq!(fmt_us(3600e6), "1h");
        assert_eq!(fmt_us(90e6), "90s");
        assert_eq!(fmt_us(1500.0), "1500us");
        assert_eq!(fmt_us(0.5), "5e-07s");
        assert_eq!(float_repr(2.0), "2.0");
        assert_eq!(float_repr(1.5e16), "1.5e+16");
    }

    #[test]
    fn diff_reports_changes() {
        let defs = scan_file("x.go", SAMPLE);
        let old = RegistryFile {
            temporal_version: "1.31.0".into(),
            settings: defs[..3].to_vec(),
        };
        let mut new = RegistryFile {
            temporal_version: "1.31.0".into(),
            settings: defs[1..4].to_vec(),
        };
        new.settings[0].default = Some(Value::from(1.0));
        let d = diff(&old, &new);
        assert!(d.iter().any(|l| l.starts_with("+ matching.parts")), "{d:?}");
        assert!(d.iter().any(|l| l == "- history.rps"), "{d:?}");
        assert!(
            d.iter()
                .any(|l| l.starts_with("~ history.someTimeout: default 290000000.0 → 1.0")),
            "{d:?}"
        );
        assert!(diff(&old, &old).is_empty());
    }

    /// The committed registry is exactly what `to_json` writes, so regenerating it only
    /// changes what the Temporal source changed.
    #[test]
    fn embedded_registry_is_in_generator_format() {
        let embedded = crate::config::dynamic::REGISTRY_JSON;
        let reg: RegistryFile = serde_json::from_str(embedded).expect("registry parses");
        assert!(
            to_json(&reg) == embedded,
            "data/dynamicconfig-*.json was not written by gen-dc-registry; regenerate it"
        );
    }
}
