//! The profile `tempdes profile init` writes: a `profile.yaml` and a `scenario.yaml` with every
//! setting commented, the optional ones commented out with their defaults or an example.
//!
//! The sources mark what is commented out. Lines between `#{off` and `#}off` are commented out
//! in what `init` writes ([`Variant::Template`]) and switched on in [`Variant::Full`], which the
//! tests load so that every setting the template shows stays valid. Lines between `#{alt` and
//! `#}alt` are alternatives to settings that are on, commented out in both.

const PROFILE: &str = include_str!("profile.src.yaml");
const SCENARIO: &str = include_str!("scenario.src.yaml");

/// The description when `init` isn't given one.
const DESCRIPTION: &str = "what this profile models";

/// Which settings are on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Variant {
    /// What `profile init` writes.
    Template,
    /// Every commented-out setting switched on, except the alternatives.
    Full,
}

/// The files of profile `name`: (file name, contents). They name no folder, so the profile can
/// be moved or copied.
pub fn render(
    variant: Variant,
    name: &str,
    description: Option<&str>,
) -> [(&'static str, String); 2] {
    // a JSON string is a YAML double-quoted string
    let quote = |s: &str| serde_json::to_string(s).expect("a string");
    let fill = |src: &str| {
        markers(src, variant)
            .replace("__NAME_Q__", &quote(name))
            .replace("__NAME__", name)
            .replace(
                "__DESCRIPTION__",
                &quote(description.unwrap_or(DESCRIPTION)),
            )
    };
    [
        ("profile.yaml", fill(PROFILE)),
        ("scenario.yaml", fill(SCENARIO)),
    ]
}

/// `src` with its marked regions commented out, or switched on, and the markers dropped. A
/// region is commented out at its least-indented line, so that removing each `# ` restores it.
fn markers(src: &str, variant: Variant) -> String {
    let mut out = String::with_capacity(src.len() + 1024);
    let mut region: Option<(bool, Vec<&str>)> = None;
    for line in src.lines() {
        match line.trim() {
            "#{off" | "#{alt" => {
                let alt = line.trim() == "#{alt";
                region = Some((variant == Variant::Template || alt, Vec::new()));
            }
            "#}off" | "#}alt" => {
                let (comment, block) = region.take().unwrap_or_default();
                let indent = block
                    .iter()
                    .filter(|l| !l.trim().is_empty())
                    .map(|l| l.len() - l.trim_start().len())
                    .min()
                    .unwrap_or(0);
                for l in block {
                    if comment && !l.trim().is_empty() {
                        out.push_str(&l[..indent]);
                        out.push_str("# ");
                        out.push_str(&l[indent..]);
                    } else {
                        out.push_str(l);
                    }
                    out.push('\n');
                }
            }
            _ => match &mut region {
                Some((_, block)) => block.push(line),
                None => {
                    out.push_str(line);
                    out.push('\n');
                }
            },
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn regions_are_commented_out_at_their_indent_or_switched_on() {
        let src = "a: 1\n#{off\n  b:\n    c: 2\n#}off\n#{alt\n  d: 3\n#}alt\n";
        assert_eq!(
            markers(src, Variant::Template),
            "a: 1\n  # b:\n  #   c: 2\n  # d: 3\n"
        );
        assert_eq!(
            markers(src, Variant::Full),
            "a: 1\n  b:\n    c: 2\n  # d: 3\n"
        );
    }

    #[test]
    fn the_sources_are_balanced_and_fully_filled() {
        for (name, src) in [("profile", PROFILE), ("scenario", SCENARIO)] {
            let opens = src.lines().filter(|l| l.trim().starts_with("#{")).count();
            let closes = src.lines().filter(|l| l.trim().starts_with("#}")).count();
            assert_eq!(opens, closes, "{name}");
        }
        for (_, text) in render(Variant::Full, "p-1", Some("it's \"q\"")) {
            assert!(!text.contains("__"), "{text}");
            assert!(!text.contains("#{") && !text.contains("#}"), "{text}");
        }
    }
}
