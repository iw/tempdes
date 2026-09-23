# Contributing to tempdes

Thanks for your interest in improving tempdes. It is a simulator, so it is only as useful as its
model is faithful to Temporal. The most valuable contributions are:

* **production evidence**: scenarios and observed metrics where the simulation disagrees with a
  real cluster;
* **model fixes**: changes grounded in the Temporal source;
* **hotspot rules** that point at the right metric and knob.

Everyone taking part is expected to follow the [code of conduct](CODE_OF_CONDUCT.md). Please
report security issues privately, as described in [SECURITY.md](SECURITY.md), not in public
issues.

## Getting started

You need [rustup](https://rustup.rs). The repository pins its toolchain (Rust 1.98.1, with
clippy and rustfmt) in `rust-toolchain.toml`, and rustup installs it the first time you run
`cargo`.

```bash
git clone https://github.com/iw/tempdes && cd tempdes
cargo run --release -- run examples/scenarios/baseline.yaml
```

## Before you open a pull request

CI runs these checks on Linux, macOS and Windows. Run them locally first:

```bash
cargo fmt --all --check
cargo clippy --all-targets --locked
cargo test --locked
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --locked --document-private-items
cargo deny check      # optional locally: cargo install --locked cargo-deny
```

Warnings are errors in CI.

Keep pull requests focused, and describe the behaviour they change. Note user-visible changes
under *Unreleased* in [CHANGELOG.md](CHANGELOG.md).

## Changing the model

[docs/MODEL.md](docs/MODEL.md) describes each simulated mechanism and names the Temporal 1.31.0
code it follows. When you add or change a mechanism:

1. **Cite the source.** Link or name the Temporal code (`service/history/...`, file and line at
   the `v1.31.0` tag) in the pull request and in the code comment or `docs/MODEL.md`. Behaviour
   that can't be traced to the Temporal source or to documented SDK behaviour needs a
   production measurement to justify it.
2. **Keep runs deterministic.**
   * A given seed must reproduce a run exactly on every platform.
   * Use `BTreeMap`/`BTreeSet` wherever iteration order can affect the simulation.
   * Draw randomness only from the component's `Rng`.
   * Never read the wall clock inside the model.
   * Never hold a `RefCell` borrow across an `.await`.
3. **Test the effect.** `tests/scenarios.rs` holds the end-to-end checks. Each example scenario
   must keep surfacing the hotspot it was built to show, and parameter changes must move results
   in the expected direction. Unit tests live next to the code.
4. **Mind the cost.** Simulations should stay fast enough to sweep. Measure before and after
   with `tempdes run` on `examples/scenarios/cassandra-large.yaml`.

### Hotspot rules

Rules live in `src/report/rules.rs`. A new rule should:

* name the Temporal metrics that show the problem in production;
* name the dynamic config keys (or replica counts) that change it;
* come with a scenario or test that triggers it.

When a rule reports a symptom of another hotspot, extend the causal ranking in `causal()` so
the root cause ranks first.

### Example scenarios

Scenarios in `examples/scenarios/` start with a comment that explains what they show. Add a
test for any new scenario.

## The dynamic config registry

`data/dynamicconfig-1.31.0.json` is generated from the Temporal source by the `gen-dc-registry`
binary, so don't edit it by hand. A unit test fails if it wasn't written by the generator.

```bash
git clone --depth 1 --branch v1.31.0 https://github.com/temporalio/temporal ../temporal
cargo run --release --bin gen-dc-registry -- ../temporal -o data/dynamicconfig-1.31.0.json
```

To support another Temporal release:

1. Generate a registry for that release.
2. Review the model against the release's changes.
3. Update `docs/MODEL.md` and the version references.

## Dependencies

tempdes depends only on `serde`, `serde_json`, `serde-saphyr`, `clap` and `anyhow`. Please open
an issue before adding a dependency. `cargo deny check` enforces the license allow-list in
`deny.toml`.

## License

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in
the work by you, as defined in the Apache-2.0 license, shall be dual licensed under the
[MIT](LICENSE-MIT) and [Apache-2.0](LICENSE-APACHE) licenses, without any additional terms or
conditions.
