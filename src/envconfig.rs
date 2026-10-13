//! Temporal client profiles: the `temporal.toml` file and `TEMPORAL_*` environment variables
//! that the Temporal CLI and SDKs connect with.
//!
//! A profile is resolved as sdk-core's `envconfig` resolves it. The file is `--temporal-config`,
//! else `TEMPORAL_CONFIG_FILE`, else `temporalio/temporal.toml` in the user's config folder
//! (`~/.config` on Linux, `~/Library/Application Support` on macOS, `%APPDATA%` on Windows). The
//! profile is `--temporal-profile`, else `TEMPORAL_PROFILE`, else `default`. A profile named
//! either way must exist; a missing `default` is an empty profile. Environment variables then
//! override the file, and the command line overrides both:
//!
//! ```toml
//! [profile.prod]
//! address = "temporal.example.com:7233"
//! namespace = "orders"
//! api_key = "..."                  # sent as `Authorization: Bearer ...`
//!
//! [profile.prod.tls]               # TLS is on with a `tls` table, or with an API key,
//! disabled = false                 # unless `disabled = true`
//! client_cert_path = "client.pem"  # or client_cert_data, the PEM itself; likewise the key
//! client_key_path = "client.key"
//! server_ca_cert_path = "ca.pem"   # trusted instead of the system's certificates
//! server_name = "temporal.example.com"
//! disable_host_verification = false
//!
//! [profile.prod.grpc_meta]         # request headers
//! authorization = "Bearer ..."
//! ```
//!
//! The variables are `TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE`, `TEMPORAL_API_KEY`,
//! `TEMPORAL_TLS` (`true` or `false`), `TEMPORAL_TLS_CLIENT_CERT_PATH` and `_DATA`,
//! `TEMPORAL_TLS_CLIENT_KEY_PATH` and `_DATA`, `TEMPORAL_TLS_SERVER_CA_CERT_PATH` and `_DATA`,
//! `TEMPORAL_TLS_SERVER_NAME`, `TEMPORAL_TLS_DISABLE_HOST_VERIFICATION`, and
//! `TEMPORAL_GRPC_META_<NAME>` for each header (`TEMPORAL_GRPC_META_MY_KEY` is `my-key`; an
//! empty value removes it). The `codec` table and variables are for tools that decode payloads.
//! tempdes never decodes them, so it ignores those.

use std::collections::BTreeMap;
use std::path::PathBuf;

use anyhow::{Context, bail};
use serde::Deserialize;

/// The address when neither the profile nor the environment gives one.
pub const DEFAULT_ADDRESS: &str = "localhost:7233";
/// The namespace when neither the profile nor the environment gives one.
pub const DEFAULT_NAMESPACE: &str = "default";
const DEFAULT_PROFILE: &str = "default";

/// A certificate or key: a file, or the PEM itself.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Source {
    Path(PathBuf),
    Data(Vec<u8>),
}

impl Source {
    /// The PEM.
    pub fn read(&self) -> anyhow::Result<Vec<u8>> {
        match self {
            Source::Path(p) => std::fs::read(p).with_context(|| format!("reading {}", p.display())),
            Source::Data(d) => Ok(d.clone()),
        }
    }
}

/// A profile's `tls` table.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Tls {
    /// `Some(true)` turns TLS off and `Some(false)` on; unset, the table turns it on
    pub disabled: Option<bool>,
    pub client_cert: Option<Source>,
    pub client_key: Option<Source>,
    /// trusted instead of the system's certificates
    pub server_ca_cert: Option<Source>,
    /// the name the server's certificate is verified against, and sent as SNI
    pub server_name: Option<String>,
    pub disable_host_verification: bool,
}

/// A client profile, resolved.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Profile {
    /// the frontend's gRPC address, `host:port`
    pub address: Option<String>,
    pub namespace: Option<String>,
    pub api_key: Option<String>,
    pub tls: Option<Tls>,
    /// gRPC metadata (request headers), keys in lower case with hyphens for underscores
    pub grpc_meta: BTreeMap<String, String>,
}

impl Profile {
    pub fn address(&self) -> &str {
        self.address
            .as_deref()
            .filter(|a| !a.is_empty())
            .unwrap_or(DEFAULT_ADDRESS)
    }

    pub fn namespace(&self) -> &str {
        self.namespace
            .as_deref()
            .filter(|n| !n.is_empty())
            .unwrap_or(DEFAULT_NAMESPACE)
    }

    /// The API key, if it isn't empty.
    pub fn api_key(&self) -> Option<&str> {
        self.api_key.as_deref().filter(|k| !k.is_empty())
    }

    /// TLS is on with a `tls` table that isn't disabled, or with an API key and no table.
    pub fn tls_enabled(&self) -> bool {
        match &self.tls {
            Some(t) => t.disabled != Some(true),
            None => self.api_key().is_some(),
        }
    }
}

/// What the command line chose, which wins over the environment.
#[derive(Clone, Debug, Default)]
pub struct Choice {
    /// `--temporal-config`
    pub file: Option<PathBuf>,
    /// `--temporal-profile`
    pub profile: Option<String>,
}

/// A profile and where it came from.
#[derive(Clone, Debug)]
pub struct Loaded {
    pub profile: Profile,
    /// e.g. `profile prod in ~/.config/temporalio/temporal.toml, with TEMPORAL_NAMESPACE`
    pub origin: String,
}

/// Load the chosen profile, with the process's environment variables applied.
pub fn load(choice: &Choice) -> anyhow::Result<Loaded> {
    let env: BTreeMap<String, String> = std::env::vars_os()
        .filter_map(|(k, v)| Some((k.into_string().ok()?, v.into_string().ok()?)))
        .collect();
    load_with(choice, &env)
}

/// [`load`] with the environment given.
pub fn load_with(choice: &Choice, env: &BTreeMap<String, String>) -> anyhow::Result<Loaded> {
    let set = |k: &str| env.get(k).filter(|v| !v.is_empty()).cloned();
    // a file named on the command line must exist; one from the environment, or the default,
    // may not
    let (file, named_file) = match (&choice.file, set("TEMPORAL_CONFIG_FILE")) {
        (Some(f), _) => (Some(f.clone()), true),
        (None, Some(f)) => (Some(PathBuf::from(f)), false),
        (None, None) => (default_file(env), false),
    };
    let text = match &file {
        Some(f) => match std::fs::read_to_string(f) {
            Ok(t) => Some(t),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound && !named_file => None,
            Err(e) => return Err(e).with_context(|| format!("reading {}", f.display())),
        },
        None => None,
    };
    let shown = file.as_ref().map(|f| f.display().to_string());
    let profiles = match &text {
        Some(t) => {
            parse(t).with_context(|| format!("reading {}", shown.as_deref().unwrap_or("")))?
        }
        None => BTreeMap::new(),
    };
    let (name, named) = match (&choice.profile, set("TEMPORAL_PROFILE")) {
        (Some(p), _) => (p.clone(), true),
        (None, Some(p)) => (p, true),
        (None, None) => (DEFAULT_PROFILE.to_string(), false),
    };
    let found = profiles.get(&name).cloned();
    let mut origin = match (&found, &text, &shown) {
        (Some(_), _, Some(f)) => format!("profile {name} in {f}"),
        (None, _, _) if named => {
            let what = match (&text, &shown) {
                (Some(_), Some(f)) if profiles.is_empty() => format!("{f} has no profiles"),
                (Some(_), Some(f)) => format!(
                    "{f} has {}",
                    profiles.keys().cloned().collect::<Vec<_>>().join(", ")
                ),
                (_, Some(f)) => format!("there is no {f}"),
                _ => "there is no config file".into(),
            };
            bail!("no profile {name:?}: {what}")
        }
        (None, Some(_), Some(f)) => format!("{f} (no {name} profile)"),
        (_, _, Some(f)) => format!("no config file at {f}"),
        _ => "no config file".into(),
    };
    let mut profile = found.unwrap_or_default();
    let used = apply_env(&mut profile, env)?;
    if !used.is_empty() {
        origin = format!("{origin}, with {}", used.join(", "));
    }
    Ok(Loaded { profile, origin })
}

/// `temporalio/temporal.toml` in the user's config folder, where the Temporal CLI and SDKs look.
pub fn default_file(env: &BTreeMap<String, String>) -> Option<PathBuf> {
    let abs = |k: &str| env.get(k).map(PathBuf::from).filter(|p| p.is_absolute());
    let dir = if cfg!(target_os = "macos") {
        abs("HOME").map(|h| h.join("Library").join("Application Support"))
    } else if cfg!(windows) {
        abs("APPDATA")
    } else {
        abs("XDG_CONFIG_HOME").or_else(|| abs("HOME").map(|h| h.join(".config")))
    };
    Some(dir?.join("temporalio").join("temporal.toml"))
}

// --- the file ---------------------------------------------------------------------------------

/// The file as written. Keys it doesn't know (such as `codec`) are ignored, as sdk-core
/// ignores them unless asked to be strict.
#[derive(Deserialize)]
struct File {
    #[serde(default)]
    profile: BTreeMap<String, FileProfile>,
}

#[derive(Deserialize)]
struct FileProfile {
    address: Option<String>,
    namespace: Option<String>,
    api_key: Option<String>,
    tls: Option<FileTls>,
    #[serde(default)]
    grpc_meta: BTreeMap<String, String>,
}

#[derive(Deserialize)]
struct FileTls {
    disabled: Option<bool>,
    client_cert_path: Option<String>,
    client_cert_data: Option<String>,
    client_key_path: Option<String>,
    client_key_data: Option<String>,
    server_ca_cert_path: Option<String>,
    server_ca_cert_data: Option<String>,
    server_name: Option<String>,
    #[serde(default)]
    disable_host_verification: bool,
}

/// The profiles in a file.
pub fn parse(text: &str) -> anyhow::Result<BTreeMap<String, Profile>> {
    let file: File = toml::from_str(text)?;
    let mut profiles = BTreeMap::new();
    for (name, p) in file.profile {
        let tls = match p.tls {
            Some(t) => {
                let source =
                    |what: &str, path: Option<String>, data: Option<String>| match (path, data) {
                        (Some(_), Some(_)) => {
                            bail!("profile {name}: give {what}_path or {what}_data, not both")
                        }
                        (Some(p), None) => Ok(Some(Source::Path(p.into()))),
                        (None, Some(d)) => {
                            Ok((!d.is_empty()).then(|| Source::Data(d.into_bytes())))
                        }
                        (None, None) => Ok(None),
                    };
                Some(Tls {
                    disabled: t.disabled,
                    client_cert: source("client_cert", t.client_cert_path, t.client_cert_data)?,
                    client_key: source("client_key", t.client_key_path, t.client_key_data)?,
                    server_ca_cert: source(
                        "server_ca_cert",
                        t.server_ca_cert_path,
                        t.server_ca_cert_data,
                    )?,
                    server_name: t.server_name,
                    disable_host_verification: t.disable_host_verification,
                })
            }
            None => None,
        };
        let grpc_meta = p
            .grpc_meta
            .into_iter()
            .map(|(k, v)| (meta_key(&k), v))
            .collect();
        profiles.insert(
            name,
            Profile {
                address: p.address,
                namespace: p.namespace,
                api_key: p.api_key,
                tls,
                grpc_meta,
            },
        );
    }
    Ok(profiles)
}

/// A header name as sdk-core normalises it: lower case, with hyphens for underscores.
fn meta_key(k: &str) -> String {
    k.to_lowercase().replace('_', "-")
}

// --- the environment --------------------------------------------------------------------------

const TLS_VARS: [&str; 9] = [
    "TEMPORAL_TLS",
    "TEMPORAL_TLS_CLIENT_CERT_PATH",
    "TEMPORAL_TLS_CLIENT_CERT_DATA",
    "TEMPORAL_TLS_CLIENT_KEY_PATH",
    "TEMPORAL_TLS_CLIENT_KEY_DATA",
    "TEMPORAL_TLS_SERVER_CA_CERT_PATH",
    "TEMPORAL_TLS_SERVER_CA_CERT_DATA",
    "TEMPORAL_TLS_SERVER_NAME",
    "TEMPORAL_TLS_DISABLE_HOST_VERIFICATION",
];

/// Override `p` with the environment's `TEMPORAL_*` variables; the names of those used.
fn apply_env(p: &mut Profile, env: &BTreeMap<String, String>) -> anyhow::Result<Vec<String>> {
    let mut used: Vec<String> = Vec::new();
    let mut var = |k: &str| {
        let v = env.get(k).cloned();
        if v.is_some() {
            used.push(k.to_string());
        }
        v
    };
    if let Some(v) = var("TEMPORAL_ADDRESS") {
        p.address = Some(v);
    }
    if let Some(v) = var("TEMPORAL_NAMESPACE") {
        p.namespace = Some(v);
    }
    if let Some(v) = var("TEMPORAL_API_KEY") {
        p.api_key = Some(v);
    }
    if p.tls.is_none() && TLS_VARS.iter().any(|k| env.contains_key(*k)) {
        p.tls = Some(Tls::default());
    }
    if let Some(t) = &mut p.tls {
        if let Some(on) = var("TEMPORAL_TLS").as_deref().and_then(boolean) {
            t.disabled = Some(!on);
        }
        let pairs: [(&str, &str, &str, &mut Option<Source>); 3] = [
            (
                "client certificate",
                "TEMPORAL_TLS_CLIENT_CERT_PATH",
                "TEMPORAL_TLS_CLIENT_CERT_DATA",
                &mut t.client_cert,
            ),
            (
                "client key",
                "TEMPORAL_TLS_CLIENT_KEY_PATH",
                "TEMPORAL_TLS_CLIENT_KEY_DATA",
                &mut t.client_key,
            ),
            (
                "server CA certificate",
                "TEMPORAL_TLS_SERVER_CA_CERT_PATH",
                "TEMPORAL_TLS_SERVER_CA_CERT_DATA",
                &mut t.server_ca_cert,
            ),
        ];
        for (what, path_var, data_var, dest) in pairs {
            match (var(path_var), var(data_var)) {
                (Some(_), Some(_)) => bail!("set {path_var} or {data_var}, not both"),
                (Some(path), None) => {
                    if matches!(dest, Some(Source::Data(_))) {
                        bail!(
                            "{path_var} gives the {what}'s path, but the profile has its data already"
                        );
                    }
                    *dest = Some(Source::Path(path.into()));
                }
                (None, Some(data)) => {
                    if matches!(dest, Some(Source::Path(_))) {
                        bail!(
                            "{data_var} gives the {what}'s data, but the profile has its path already"
                        );
                    }
                    *dest = Some(Source::Data(data.into_bytes()));
                }
                (None, None) => {}
            }
        }
        if let Some(v) = var("TEMPORAL_TLS_SERVER_NAME") {
            t.server_name = Some(v);
        }
        if let Some(off) = var("TEMPORAL_TLS_DISABLE_HOST_VERIFICATION")
            .as_deref()
            .and_then(boolean)
        {
            t.disable_host_verification = off;
        }
    }
    for (k, v) in env {
        if let Some(header) = k.strip_prefix("TEMPORAL_GRPC_META_") {
            used.push(k.clone());
            let key = meta_key(header);
            if v.is_empty() {
                p.grpc_meta.remove(&key);
            } else {
                p.grpc_meta.insert(key, v.clone());
            }
        }
    }
    Ok(used)
}

/// `true`/`1` or `false`/`0`, in any case; anything else is ignored, as sdk-core ignores it.
fn boolean(s: &str) -> Option<bool> {
    match s.to_ascii_lowercase().as_str() {
        "true" | "1" => Some(true),
        "false" | "0" => Some(false),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    /// A config file in a fresh directory.
    fn file(name: &str, text: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("tempdes-envconfig-{name}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let f = dir.join("temporal.toml");
        std::fs::write(&f, text).unwrap();
        f
    }

    const FILE: &str = r#"
[profile.default]
address = "localhost:7233"

[profile.prod]
address = "temporal.example.com:7233"
namespace = "orders"
api_key = "secret"

[profile.prod.tls]
client_cert_path = "/certs/client.pem"
client_key_data = "-----BEGIN PRIVATE KEY-----"
server_name = "frontend.example.com"

[profile.prod.grpc_meta]
My_Header = "x"

[profile.prod.codec]
endpoint = "http://localhost:8080"
"#;

    #[test]
    fn profiles_read_as_sdk_core_reads_them() {
        let p = &parse(FILE).unwrap()["prod"];
        assert_eq!(p.address(), "temporal.example.com:7233");
        assert_eq!(p.namespace(), "orders");
        assert_eq!(p.api_key(), Some("secret"));
        let t = p.tls.as_ref().unwrap();
        assert_eq!(
            t.client_cert,
            Some(Source::Path("/certs/client.pem".into()))
        );
        assert_eq!(
            t.client_key,
            Some(Source::Data(b"-----BEGIN PRIVATE KEY-----".to_vec()))
        );
        assert_eq!(t.server_name.as_deref(), Some("frontend.example.com"));
        // header names are normalised; the codec table is ignored
        assert_eq!(p.grpc_meta["my-header"], "x");
        assert!(p.tls_enabled());
        let both = "[profile.x.tls]\nclient_cert_path = \"a\"\nclient_cert_data = \"b\"\n";
        assert!(
            parse(both)
                .unwrap_err()
                .to_string()
                .contains("client_cert_path or client_cert_data")
        );
    }

    #[test]
    fn tls_is_on_with_a_table_or_an_api_key() {
        let mut p = Profile::default();
        assert!(!p.tls_enabled());
        p.api_key = Some("k".into());
        assert!(p.tls_enabled());
        p.tls = Some(Tls {
            disabled: Some(true),
            ..Tls::default()
        });
        assert!(!p.tls_enabled());
        p.api_key = None;
        p.tls = Some(Tls::default());
        assert!(p.tls_enabled());
        // an empty key is no key
        let p = Profile {
            api_key: Some(String::new()),
            ..Profile::default()
        };
        assert!(!p.tls_enabled());
    }

    #[test]
    fn the_profile_comes_from_the_command_line_then_the_environment() {
        let f = file("choose", FILE);
        let choice = Choice {
            file: Some(f.clone()),
            profile: None,
        };
        // no profile named: `default`
        let l = load_with(&choice, &env(&[])).unwrap();
        assert_eq!(l.profile.address(), "localhost:7233");
        assert!(l.origin.starts_with("profile default in "), "{}", l.origin);
        // TEMPORAL_PROFILE names one, and --temporal-profile wins over it
        let l = load_with(&choice, &env(&[("TEMPORAL_PROFILE", "prod")])).unwrap();
        assert_eq!(l.profile.namespace(), "orders");
        let choice = Choice {
            file: Some(f.clone()),
            profile: Some("default".into()),
        };
        let l = load_with(&choice, &env(&[("TEMPORAL_PROFILE", "prod")])).unwrap();
        assert_eq!(l.profile.namespace(), "default");
        // a named profile must exist
        let choice = Choice {
            file: Some(f.clone()),
            profile: Some("staging".into()),
        };
        let err = load_with(&choice, &env(&[])).unwrap_err().to_string();
        assert!(
            err.contains("no profile \"staging\"") && err.contains("default, prod"),
            "{err}"
        );
        // TEMPORAL_CONFIG_FILE names the file
        let l = load_with(
            &Choice::default(),
            &env(&[
                ("TEMPORAL_CONFIG_FILE", f.to_str().unwrap()),
                ("TEMPORAL_PROFILE", "prod"),
            ]),
        )
        .unwrap();
        assert_eq!(l.profile.namespace(), "orders");
    }

    #[test]
    fn missing_files_are_empty_unless_named_on_the_command_line() {
        let missing = std::env::temp_dir().join("tempdes-envconfig-none/temporal.toml");
        let l = load_with(
            &Choice::default(),
            &env(&[("TEMPORAL_CONFIG_FILE", missing.to_str().unwrap())]),
        )
        .unwrap();
        assert_eq!(l.profile, Profile::default());
        assert!(l.origin.starts_with("no config file at "), "{}", l.origin);
        let named = Choice {
            file: Some(missing.clone()),
            profile: None,
        };
        assert!(load_with(&named, &env(&[])).is_err());
        // a profile named without a file to hold it
        let err = load_with(
            &Choice {
                file: None,
                profile: Some("prod".into()),
            },
            &env(&[("TEMPORAL_CONFIG_FILE", missing.to_str().unwrap())]),
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("there is no"), "{err}");
    }

    #[test]
    fn environment_variables_override_the_file() {
        let f = file("env", FILE);
        let choice = Choice {
            file: Some(f),
            profile: Some("prod".into()),
        };
        let l = load_with(
            &choice,
            &env(&[
                ("TEMPORAL_ADDRESS", "127.0.0.1:7233"),
                ("TEMPORAL_NAMESPACE", "orders-2"),
                ("TEMPORAL_TLS", "false"),
                ("TEMPORAL_TLS_SERVER_CA_CERT_PATH", "/certs/ca.pem"),
                ("TEMPORAL_GRPC_META_MY_HEADER", ""),
                ("TEMPORAL_GRPC_META_TENANT_ID", "t1"),
            ]),
        )
        .unwrap();
        let p = &l.profile;
        assert_eq!(p.address(), "127.0.0.1:7233");
        assert_eq!(p.namespace(), "orders-2");
        // TEMPORAL_TLS=false turns TLS off even with an API key
        assert!(!p.tls_enabled());
        let t = p.tls.as_ref().unwrap();
        assert_eq!(t.server_ca_cert, Some(Source::Path("/certs/ca.pem".into())));
        assert!(!p.grpc_meta.contains_key("my-header"));
        assert_eq!(p.grpc_meta["tenant-id"], "t1");
        // the variables used are named, never their values
        assert!(l.origin.contains("TEMPORAL_ADDRESS"), "{}", l.origin);
        assert!(!l.origin.contains("orders-2"), "{}", l.origin);
        // a TLS variable makes a table; path and data can't both be given
        let mut p = Profile::default();
        apply_env(&mut p, &env(&[("TEMPORAL_TLS_SERVER_NAME", "x")])).unwrap();
        assert!(p.tls_enabled());
        let mut p = parse(FILE).unwrap()["prod"].clone();
        let err = apply_env(&mut p, &env(&[("TEMPORAL_TLS_CLIENT_KEY_PATH", "/k")]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("has its data already"), "{err}");
        let err = apply_env(
            &mut Profile::default(),
            &env(&[
                ("TEMPORAL_TLS_CLIENT_CERT_PATH", "/c"),
                ("TEMPORAL_TLS_CLIENT_CERT_DATA", "pem"),
            ]),
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("not both"), "{err}");
    }

    #[test]
    fn the_default_file_is_where_the_temporal_cli_looks() {
        let home = if cfg!(windows) { "C:\\h" } else { "/h" };
        let f = default_file(&env(&[("HOME", home), ("APPDATA", "C:\\a")]));
        let expected: PathBuf = if cfg!(target_os = "macos") {
            "/h/Library/Application Support/temporalio/temporal.toml".into()
        } else if cfg!(windows) {
            "C:\\a\\temporalio\\temporal.toml".into()
        } else {
            "/h/.config/temporalio/temporal.toml".into()
        };
        assert_eq!(f, Some(expected));
        if !cfg!(target_os = "macos") && !cfg!(windows) {
            let f = default_file(&env(&[("HOME", "/h"), ("XDG_CONFIG_HOME", "/x")]));
            assert_eq!(f, Some("/x/temporalio/temporal.toml".into()));
        }
    }
}
