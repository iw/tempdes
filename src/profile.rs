//! Saved run profiles: a scenario plus the options of one run (replica counts, dynamic config,
//! observed metrics, load, client load balancing, duration, warm-up, seed), saved under a name
//! so the run can be repeated with `--profile NAME`.
//!
//! A profile usually describes a real cluster, so profiles live in a private store outside any
//! repository, one directory per profile:
//!
//! ```text
//! ~/.config/tempdes/profiles/prod/
//!   profile.yaml         the run options
//!   scenario.yaml        a copy of the scenario
//!   observed/1/…         copies of the observed-metrics files (and their scrape files)
//! ```
//!
//! A scenario file placed in the store as `<name>.yaml` is a profile too: the scenario is the
//! whole run.
//!
//! The store is `$TEMPDES_PROFILES`, else `$XDG_CONFIG_HOME/tempdes/profiles`, else
//! `~/.config/tempdes/profiles`. On Unix its directories are created `0700` and its files
//! `0600`. Files the scenario itself refers to (Helm values, dynamic config files, calibration
//! observations) are not copied: they still resolve against the scenario's original folder.

use std::path::{Component, Path, PathBuf};

use anyhow::{Context, bail};
use serde::{Deserialize, Serialize};

use crate::config::scenario::{ClientLb, Scenario};
use crate::metrics::observed::Observations;
use crate::run::Overrides;

/// The options of one run, as given on the command line or saved in a profile.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RunOptions {
    /// Observed-metrics files (`--observed`).
    pub observed: Vec<PathBuf>,
    /// Replica overrides as written on the command line, e.g. `history=4`.
    pub replicas: Vec<String>,
    /// Dynamic config overrides as written on the command line, e.g. `history.rps=4500`.
    pub dc: Vec<String>,
    pub load: Option<f64>,
    pub client_lb: Option<ClientLb>,
    pub duration: Option<f64>,
    pub warmup: Option<f64>,
    pub seed: Option<u64>,
}

impl RunOptions {
    /// Apply `top` over these options: lists grow (so a later replica or dynamic config value
    /// for the same key wins), and the single-valued options `top` sets replace these.
    pub fn layer(&mut self, top: &RunOptions) {
        self.observed.extend(top.observed.iter().cloned());
        self.replicas.extend(top.replicas.iter().cloned());
        self.dc.extend(top.dc.iter().cloned());
        self.load = top.load.or(self.load);
        self.client_lb = top.client_lb.or(self.client_lb);
        self.duration = top.duration.or(self.duration);
        self.warmup = top.warmup.or(self.warmup);
        self.seed = top.seed.or(self.seed);
    }

    /// The simulator overrides these options stand for.
    pub fn overrides(&self) -> anyhow::Result<Overrides> {
        let mut ov = Overrides {
            duration_s: self.duration,
            warmup_s: self.warmup,
            seed: self.seed,
            start_rate_scale: self.load,
            client_lb: self.client_lb,
            ..Default::default()
        };
        for r in &self.replicas {
            let (s, n) = r
                .split_once('=')
                .ok_or_else(|| anyhow::anyhow!("--replicas expects SERVICE=N, got {r:?}"))?;
            let n = n
                .trim()
                .parse()
                .with_context(|| format!("--replicas {r:?}: not a replica count"))?;
            ov.replicas.push((s.trim().to_ascii_lowercase(), n));
        }
        for d in &self.dc {
            ov.dc.push(crate::cli::parse_dc_override(d)?);
        }
        Ok(ov)
    }

    /// The observed-metrics files, as the calibration loader takes them.
    pub fn observed_args(&self) -> Vec<String> {
        self.observed
            .iter()
            .map(|p| p.display().to_string())
            .collect()
    }

    /// The same options with repeated replica and dynamic config settings collapsed to the
    /// value that takes effect (the last one for each service or key and constraints).
    pub fn normalized(&self) -> RunOptions {
        fn last_wins(items: &[String], key: impl Fn(&str) -> String) -> Vec<String> {
            let mut out: Vec<String> = Vec::new();
            for item in items {
                let k = key(item);
                out.retain(|o| key(o) != k);
                out.push(item.clone());
            }
            out
        }
        RunOptions {
            replicas: last_wins(&self.replicas, |r| {
                r.split('=').next().unwrap_or(r).trim().to_ascii_lowercase()
            }),
            dc: last_wins(&self.dc, |d| {
                d.rsplit_once('=')
                    .map_or(d, |(k, _)| k)
                    .trim()
                    .to_ascii_lowercase()
            }),
            ..self.clone()
        }
    }

    /// A one-line summary, e.g. `history=4 history.rps=4500 client_lb=round_robin`.
    pub fn summary(&self) -> String {
        let n = self.normalized();
        let mut parts: Vec<String> = n.replicas.clone();
        parts.extend(n.dc.iter().cloned());
        if let Some(l) = self.load {
            parts.push(format!("load×{l}"));
        }
        if let Some(lb) = self.client_lb {
            parts.push(format!("client_lb={lb}"));
        }
        if let Some(d) = self.duration {
            parts.push(format!("duration={d}s"));
        }
        if let Some(w) = self.warmup {
            parts.push(format!("warmup={w}s"));
        }
        if let Some(s) = self.seed {
            parts.push(format!("seed={s}"));
        }
        if !self.observed.is_empty() {
            parts.push(format!("{} observed file(s)", self.observed.len()));
        }
        parts.join(" ")
    }
}

/// Everything one run needs: the scenario and the options.
#[derive(Clone, Debug, PartialEq)]
pub struct RunSpec {
    /// The scenario file to read.
    pub scenario: PathBuf,
    /// The folder the scenario's own relative paths resolve against.
    pub scenario_dir: PathBuf,
    pub options: RunOptions,
    /// The profile this came from.
    pub profile: Option<String>,
}

impl RunSpec {
    /// A run of the scenario at `path`.
    pub fn for_scenario(path: &Path) -> RunSpec {
        RunSpec {
            scenario: path.to_path_buf(),
            scenario_dir: path
                .parent()
                .map(Path::to_path_buf)
                .unwrap_or_else(|| PathBuf::from(".")),
            options: RunOptions::default(),
            profile: None,
        }
    }

    pub fn load_scenario(&self) -> anyhow::Result<Scenario> {
        Scenario::load_with_base(&self.scenario, &self.scenario_dir)
    }
}

/// `profile.yaml`: the saved options of a run. Paths are relative to the profile directory,
/// except `scenario_dir`.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Profile {
    /// One line describing the profile.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// The scenario copy.
    pub scenario: PathBuf,
    /// Where the scenario was saved from. Its own relative paths (Helm values, dynamic config
    /// files, calibration observations) resolve against this folder.
    pub scenario_dir: PathBuf,
    /// Copies of the observed-metrics files.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub observed: Vec<PathBuf>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub replicas: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dc: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub load: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_lb: Option<ClientLb>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub warmup: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seed: Option<u64>,
}

impl Profile {
    fn options(&self, dir: &Path) -> RunOptions {
        RunOptions {
            observed: self.observed.iter().map(|p| dir.join(p)).collect(),
            replicas: self.replicas.clone(),
            dc: self.dc.clone(),
            load: self.load,
            client_lb: self.client_lb,
            duration: self.duration,
            warmup: self.warmup,
            seed: self.seed,
        }
    }
}

/// What `Store::save` did, for the user.
#[derive(Debug)]
pub struct Saved {
    pub dir: PathBuf,
    /// Files copied into the profile.
    pub copied: Vec<PathBuf>,
    /// Files the scenario refers to, read from their original place on every run.
    pub external: Vec<PathBuf>,
}

/// How a profile is kept in the store.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// A directory written by `tempdes profile save`: the options, plus copies of the files.
    Saved,
    /// A scenario file placed in the store as `<name>.yaml`: the scenario is the whole run.
    Scenario,
}

/// A profile found in the store.
#[derive(Clone, Debug)]
pub struct Entry {
    pub name: String,
    pub kind: Kind,
    /// The profile's directory (saved) or file (scenario).
    pub path: PathBuf,
    pub description: Option<String>,
    pub spec: RunSpec,
}

/// The profile store: a directory of profiles.
#[derive(Clone, Debug)]
pub struct Store {
    dir: PathBuf,
}

/// The file inside each profile directory.
const PROFILE_FILE: &str = "profile.yaml";

impl Store {
    /// The store at its default location (see the module docs). Nothing is created until a
    /// profile is saved.
    pub fn open() -> anyhow::Result<Store> {
        let dir = default_dir(|k| std::env::var_os(k).map(PathBuf::from))
            .context("can't find a home directory for the profile store; set TEMPDES_PROFILES")?;
        Ok(Store { dir })
    }

    pub fn at(dir: impl Into<PathBuf>) -> Store {
        Store { dir: dir.into() }
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    fn profile_dir(&self, name: &str) -> anyhow::Result<PathBuf> {
        check_name(name)?;
        Ok(self.dir.join(name))
    }

    /// Where profile `name` is kept, if it exists.
    fn locate(&self, name: &str) -> anyhow::Result<Option<(Kind, PathBuf)>> {
        let dir = self.profile_dir(name)?;
        let saved = dir.join(PROFILE_FILE).is_file();
        let file = ["yaml", "yml"]
            .iter()
            .map(|ext| self.dir.join(format!("{name}.{ext}")))
            .find(|p| p.is_file());
        match (saved, file) {
            (true, Some(f)) => bail!(
                "both {} and {} define profile {name:?}; remove one of them",
                dir.display(),
                f.display()
            ),
            (true, None) => Ok(Some((Kind::Saved, dir))),
            (false, Some(f)) => Ok(Some((Kind::Scenario, f))),
            (false, None) => Ok(None),
        }
    }

    /// Load profile `name` as a run.
    pub fn load(&self, name: &str) -> anyhow::Result<Entry> {
        let Some((kind, path)) = self.locate(name)? else {
            bail!(
                "no profile {name:?} in {} (see `tempdes profile list`)",
                self.dir.display()
            );
        };
        match kind {
            Kind::Scenario => Ok(Entry {
                name: name.to_string(),
                kind,
                description: None,
                spec: RunSpec {
                    scenario: path.clone(),
                    scenario_dir: self.dir.clone(),
                    options: RunOptions::default(),
                    profile: Some(name.to_string()),
                },
                path,
            }),
            Kind::Saved => {
                let file = path.join(PROFILE_FILE);
                let text = std::fs::read_to_string(&file)
                    .with_context(|| format!("reading {}", file.display()))?;
                let profile: Profile = serde_saphyr::from_str(&text)
                    .map_err(|e| anyhow::anyhow!("{}: {e}", file.display()))?;
                Ok(Entry {
                    name: name.to_string(),
                    kind,
                    description: profile.description.clone(),
                    spec: RunSpec {
                        scenario: path.join(&profile.scenario),
                        scenario_dir: profile.scenario_dir.clone(),
                        options: profile.options(&path),
                        profile: Some(name.to_string()),
                    },
                    path,
                })
            }
        }
    }

    /// Every profile in the store, sorted by name, with the error for any that doesn't load.
    pub fn list(&self) -> anyhow::Result<Vec<(String, anyhow::Result<Entry>)>> {
        let entries = match std::fs::read_dir(&self.dir) {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e).with_context(|| format!("reading {}", self.dir.display())),
        };
        let mut names = std::collections::BTreeSet::new();
        for entry in entries {
            let path = entry?.path();
            let name = if path.join(PROFILE_FILE).is_file() {
                path.file_name()
            } else if path.is_file()
                && matches!(
                    path.extension().and_then(|e| e.to_str()),
                    Some("yaml" | "yml")
                )
            {
                path.file_stem()
            } else {
                None
            };
            if let Some(name) = name.map(|n| n.to_string_lossy().into_owned())
                && check_name(&name).is_ok()
            {
                names.insert(name);
            }
        }
        Ok(names
            .into_iter()
            .map(|name| {
                let entry = self.load(&name);
                (name, entry)
            })
            .collect())
    }

    /// Save `spec` as profile `name`: copy the scenario and the observed-metrics files (with
    /// the scrape files they refer to) into the profile, and write its options.
    pub fn save(
        &self,
        name: &str,
        spec: &RunSpec,
        description: Option<String>,
        force: bool,
    ) -> anyhow::Result<Saved> {
        let target = self.profile_dir(name)?;
        if let Some((Kind::Scenario, file)) = self.locate(name)? {
            bail!(
                "{} already defines profile {name:?}; save under another name, for example `tempdes profile save {name}-2 --profile {name} ...`",
                file.display()
            );
        }
        if target.exists() && !force {
            bail!("profile {name:?} already exists; pass --force to replace it");
        }
        // refuse broken profiles: the scenario, the options and the observations must load
        let scenario = spec.load_scenario()?;
        spec.options.overrides()?;
        for f in &spec.options.observed {
            Observations::load(f)?;
        }

        create_private_dir(&self.dir)?;
        let staging = self
            .dir
            .join(format!(".{name}.saving-{}", std::process::id()));
        if staging.exists() {
            std::fs::remove_dir_all(&staging)?;
        }
        create_private_dir(&staging)?;
        let copied = match write_profile(&staging, spec, description) {
            Ok(copied) => copied,
            Err(e) => {
                let _ = std::fs::remove_dir_all(&staging);
                return Err(e);
            }
        };
        if target.exists() {
            std::fs::remove_dir_all(&target)
                .with_context(|| format!("replacing {}", target.display()))?;
        }
        std::fs::rename(&staging, &target)
            .with_context(|| format!("saving {}", target.display()))?;
        let external = scenario
            .referenced_files()
            .iter()
            .map(|f| {
                let p = scenario.resolve_path(f);
                std::fs::canonicalize(&p).unwrap_or(p)
            })
            .collect();
        Ok(Saved {
            dir: target,
            copied,
            external,
        })
    }

    /// Delete profile `name`: a saved profile's directory, or a scenario profile's file.
    pub fn remove(&self, name: &str) -> anyhow::Result<PathBuf> {
        let Some((kind, path)) = self.locate(name)? else {
            bail!("no profile {name:?} in {}", self.dir.display());
        };
        match kind {
            Kind::Saved => std::fs::remove_dir_all(&path),
            Kind::Scenario => std::fs::remove_file(&path),
        }
        .with_context(|| format!("removing {}", path.display()))?;
        Ok(path)
    }
}

/// Write a profile's files into `dir`; returns the files copied into it.
fn write_profile(
    dir: &Path,
    spec: &RunSpec,
    description: Option<String>,
) -> anyhow::Result<Vec<PathBuf>> {
    let mut copied = vec![spec.scenario.clone()];
    copy_private(&spec.scenario, &dir.join("scenario.yaml"))?;
    let mut observed = Vec::new();
    for (i, f) in spec.options.observed.iter().enumerate() {
        let sub = PathBuf::from("observed").join((i + 1).to_string());
        create_private_dir(&dir.join("observed"))?;
        create_private_dir(&dir.join(&sub))?;
        let file_name = f
            .file_name()
            .with_context(|| format!("{} is not a file", f.display()))?;
        copy_private(f, &dir.join(&sub).join(file_name))?;
        copied.push(f.clone());
        // scrape files the observations refer to, kept at the same relative place
        let base = f.parent().unwrap_or_else(|| Path::new("."));
        for r in Observations::referenced_files(f)? {
            let rel = Path::new(&r);
            if rel.is_absolute() {
                continue; // read from where it is
            }
            if rel
                .components()
                .any(|c| !matches!(c, Component::Normal(_) | Component::CurDir))
            {
                bail!(
                    "{}: scrape file {r:?} is outside the observations file's folder; keep scrape files next to it (or below it) so the profile can copy them",
                    f.display()
                );
            }
            let to = dir.join(&sub).join(rel);
            if let Some(parent) = to.parent() {
                create_private_dir(parent)?;
            }
            copy_private(&base.join(rel), &to)?;
            copied.push(base.join(rel));
        }
        observed.push(sub.join(file_name));
    }
    let o = &spec.options.normalized();
    let profile = Profile {
        description,
        scenario: PathBuf::from("scenario.yaml"),
        scenario_dir: absolute(&spec.scenario_dir),
        observed,
        replicas: o.replicas.clone(),
        dc: o.dc.clone(),
        load: o.load,
        client_lb: o.client_lb,
        duration: o.duration,
        warmup: o.warmup,
        seed: o.seed,
    };
    let yaml =
        serde_saphyr::to_string(&profile).map_err(|e| anyhow::anyhow!("writing profile: {e}"))?;
    let text = format!(
        "# tempdes run profile: private, keep it out of version control.\n# Run it with `tempdes run --profile <name>`.\n{yaml}"
    );
    write_private(&dir.join(PROFILE_FILE), text.as_bytes())?;
    Ok(copied)
}

/// The default store directory, given a way to read environment variables.
pub fn default_dir(env: impl Fn(&str) -> Option<PathBuf>) -> Option<PathBuf> {
    if let Some(d) = env("TEMPDES_PROFILES").filter(|d| !d.as_os_str().is_empty()) {
        return Some(d);
    }
    let config = env("XDG_CONFIG_HOME")
        .filter(|d| d.is_absolute())
        .or_else(|| env("HOME").map(|h| h.join(".config")))?;
    Some(config.join("tempdes").join("profiles"))
}

/// Profile names are one path component: letters, digits, `-`, `_` and `.`, not starting with
/// a dot.
pub fn check_name(name: &str) -> anyhow::Result<()> {
    let ok = !name.is_empty()
        && name.len() <= 64
        && !name.starts_with('.')
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'));
    if !ok {
        bail!(
            "invalid profile name {name:?}: use letters, digits, '-', '_' and '.', not starting with '.'"
        );
    }
    Ok(())
}

fn absolute(p: &Path) -> PathBuf {
    std::path::absolute(p).unwrap_or_else(|_| p.to_path_buf())
}

/// Create `dir` (and missing parents), and make `dir` itself private to the user.
fn create_private_dir(dir: &Path) -> anyhow::Result<()> {
    std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))
            .with_context(|| format!("restricting {}", dir.display()))?;
    }
    Ok(())
}

/// Write a file readable only by the user.
fn write_private(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    use std::io::Write;
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let mut f = opts
        .open(path)
        .with_context(|| format!("writing {}", path.display()))?;
    f.write_all(bytes)
        .with_context(|| format!("writing {}", path.display()))?;
    Ok(())
}

fn copy_private(from: &Path, to: &Path) -> anyhow::Result<()> {
    let bytes = std::fs::read(from).with_context(|| format!("reading {}", from.display()))?;
    write_private(to, &bytes)
}

/// On Unix, a note when `path` (a profile's file or directory) can be read by other users,
/// with the command that makes it private.
pub fn readable_by_others(path: &Path) -> Option<String> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(path).ok()?.permissions().mode();
        if mode & 0o077 != 0 {
            let fix = if path.is_dir() {
                "chmod -R go-rwx"
            } else {
                "chmod 600"
            };
            return Some(format!(
                "{} can be read by other users on this machine (mode {:o}); `{fix} {}` makes it private",
                path.display(),
                mode & 0o777,
                path.display()
            ));
        }
        None
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        None
    }
}

/// The root of the git working tree in which `path` (a file, or a directory's files) is
/// untracked and not ignored, so a `git add -A` would pick it up. `None` when it isn't in a
/// working tree, is tracked or ignored, or git can't be asked.
pub fn untracked_in_git(path: &Path) -> Option<PathBuf> {
    let abs = std::fs::canonicalize(path).unwrap_or_else(|_| absolute(path));
    let root = abs
        .ancestors()
        .find(|a| a.join(".git").exists())?
        .to_path_buf();
    let out = std::process::Command::new("git")
        .arg("-C")
        .arg(&root)
        .args(["ls-files", "--others", "--exclude-standard", "--"])
        .arg(&abs)
        .stderr(std::process::Stdio::null())
        .output()
        .ok()?;
    (out.status.success() && !out.stdout.is_empty()).then_some(root)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fresh directory under the system temp dir.
    fn temp_dir(tag: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let d = std::env::temp_dir().join(format!(
            "tempdes-profile-test-{tag}-{}-{nanos}",
            std::process::id()
        ));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn scenario_copy(dir: &Path) -> PathBuf {
        let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/scenarios/baseline.yaml");
        let to = dir.join("private-cluster.yaml");
        std::fs::copy(src, &to).unwrap();
        to
    }

    #[test]
    fn default_location() {
        let env = |vars: &'static [(&'static str, &'static str)]| {
            move |k: &str| {
                vars.iter()
                    .find(|(n, _)| *n == k)
                    .map(|(_, v)| PathBuf::from(v))
            }
        };
        assert_eq!(
            default_dir(env(&[("TEMPDES_PROFILES", "/p"), ("HOME", "/h")])),
            Some(PathBuf::from("/p"))
        );
        assert_eq!(
            default_dir(env(&[("XDG_CONFIG_HOME", "/x"), ("HOME", "/h")])),
            Some(PathBuf::from("/x/tempdes/profiles"))
        );
        assert_eq!(
            default_dir(env(&[("HOME", "/h")])),
            Some(PathBuf::from("/h/.config/tempdes/profiles"))
        );
        assert_eq!(default_dir(env(&[])), None);
    }

    #[test]
    fn names_are_single_safe_components() {
        for ok in ["prod", "prod-500", "eu_west.2"] {
            assert!(check_name(ok).is_ok(), "{ok}");
        }
        for bad in ["", ".hidden", "../x", "a/b", "a b", "..", "x\\y"] {
            assert!(check_name(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn save_load_derive_and_remove() {
        let src = temp_dir("src");
        let store = Store::at(temp_dir("store").join("profiles"));
        let scenario = scenario_copy(&src);
        // observations that refer to a scrape file next to them
        std::fs::write(src.join("scrape.prom"), "temporal_service_requests_total{operation=\"StartWorkflowExecution\",service_name=\"frontend\"} 10\n").unwrap();
        std::fs::write(
            src.join("observed.yaml"),
            "prometheus:\n  after: scrape.prom\n  interval: 60s\nmetrics: []\n",
        )
        .unwrap();

        let mut spec = RunSpec::for_scenario(&scenario);
        spec.options = RunOptions {
            observed: vec![src.join("observed.yaml")],
            replicas: vec!["history=4".into()],
            dc: vec!["history.rps=4500".into()],
            client_lb: Some(ClientLb::RoundRobin),
            load: Some(1.5),
            ..Default::default()
        };
        let saved = store
            .save("prod", &spec, Some("private cluster".into()), false)
            .unwrap();
        assert!(saved.dir.join("scenario.yaml").is_file());
        assert!(saved.dir.join("observed/1/observed.yaml").is_file());
        assert!(saved.dir.join("observed/1/scrape.prom").is_file());
        assert_eq!(saved.copied.len(), 3);

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = |p: &Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode(store.dir()), 0o700);
            assert_eq!(mode(&saved.dir), 0o700);
            assert_eq!(mode(&saved.dir.join("profile.yaml")), 0o600);
            assert_eq!(mode(&saved.dir.join("scenario.yaml")), 0o600);
        }

        // the profile runs without the original files
        std::fs::remove_file(&scenario).unwrap();
        std::fs::remove_file(src.join("observed.yaml")).unwrap();
        std::fs::remove_file(src.join("scrape.prom")).unwrap();
        let entry = store.load("prod").unwrap();
        assert_eq!(entry.kind, Kind::Saved);
        assert_eq!(entry.description.as_deref(), Some("private cluster"));
        let loaded = entry.spec;
        assert_eq!(loaded.profile.as_deref(), Some("prod"));
        assert_eq!(loaded.options.replicas, ["history=4"]);
        assert_eq!(loaded.options.client_lb, Some(ClientLb::RoundRobin));
        let sc = loaded.load_scenario().unwrap();
        let ov = loaded.options.overrides().unwrap();
        let p = crate::run::prepare(&sc, &ov, None).unwrap();
        assert_eq!(p.replicas.history, 4);
        assert_eq!(p.client_lb, ClientLb::RoundRobin);
        Observations::load(&loaded.options.observed[0]).unwrap();

        // no silent overwrite
        assert!(store.save("prod", &loaded, None, false).is_err());

        // a derived profile: the saved one plus another option
        let mut derived = loaded.clone();
        derived.options.layer(&RunOptions {
            replicas: vec!["history=5".into()],
            ..Default::default()
        });
        store.save("prod-5", &derived, None, false).unwrap();
        let d = store.load("prod-5").unwrap().spec;
        let p = crate::run::prepare(
            &d.load_scenario().unwrap(),
            &d.options.overrides().unwrap(),
            None,
        )
        .unwrap();
        assert_eq!(p.replicas.history, 5, "the later replica value wins");

        let names: Vec<String> = store.list().unwrap().into_iter().map(|(n, _)| n).collect();
        assert_eq!(names, ["prod", "prod-5"]);
        store.remove("prod").unwrap();
        assert!(store.load("prod").is_err());
        assert_eq!(store.list().unwrap().len(), 1);
    }

    #[test]
    fn scenario_files_in_the_store_are_profiles() {
        let store_dir = temp_dir("scenario-store");
        let store = Store::at(&store_dir);
        let file = scenario_copy(&store_dir);
        std::fs::rename(&file, store_dir.join("staging.yaml")).unwrap();

        let entry = store.load("staging").unwrap();
        assert_eq!(entry.kind, Kind::Scenario);
        assert_eq!(entry.spec.scenario, store_dir.join("staging.yaml"));
        entry.spec.load_scenario().unwrap();
        let listed: Vec<String> = store.list().unwrap().into_iter().map(|(n, _)| n).collect();
        assert_eq!(listed, ["staging"]);

        // a saved profile can't shadow it, even with --force
        let spec = RunSpec::for_scenario(&store_dir.join("staging.yaml"));
        let err = store.save("staging", &spec, None, true).unwrap_err();
        assert!(err.to_string().contains("already defines profile"), "{err}");
        // but it can be the base of one
        let mut derived = entry.spec.clone();
        derived.options.replicas = vec!["history=4".into()];
        store.save("staging-4h", &derived, None, false).unwrap();
        assert_eq!(store.load("staging-4h").unwrap().kind, Kind::Saved);

        // one name defined both ways is an error, not a guess
        std::fs::write(store_dir.join("staging-4h.yaml"), "name: clash\n").unwrap();
        assert!(store.load("staging-4h").is_err());
        std::fs::remove_file(store_dir.join("staging-4h.yaml")).unwrap();

        assert_eq!(
            store.remove("staging").unwrap(),
            store_dir.join("staging.yaml")
        );
        assert!(!store_dir.join("staging.yaml").exists());
    }

    #[test]
    fn repeated_settings_collapse_to_the_effective_value() {
        let o = RunOptions {
            replicas: vec!["history=4".into(), "matching=6".into(), "History=5".into()],
            dc: vec![
                "history.rps=3000".into(),
                "frontend.namespaceRPS[namespace=orders]=500".into(),
                "history.rps=4500".into(),
            ],
            ..Default::default()
        };
        let n = o.normalized();
        assert_eq!(n.replicas, ["matching=6", "History=5"]);
        assert_eq!(
            n.dc,
            [
                "frontend.namespaceRPS[namespace=orders]=500",
                "history.rps=4500"
            ]
        );
        assert_eq!(
            o.summary(),
            "matching=6 History=5 frontend.namespaceRPS[namespace=orders]=500 history.rps=4500"
        );
    }

    #[test]
    fn broken_runs_are_not_saved() {
        let src = temp_dir("broken");
        let store = Store::at(temp_dir("broken-store"));
        let mut spec = RunSpec::for_scenario(&scenario_copy(&src));
        spec.options.replicas = vec!["history".into()];
        assert!(store.save("bad", &spec, None, false).is_err());
        assert!(store.list().unwrap().is_empty());
    }
}
