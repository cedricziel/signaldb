# workspace-hack

Managed by [`cargo hakari`](https://docs.rs/cargo-hakari). Every other workspace
member (except `logql-parser`, `traceql-parser`, `query-ir`, and `ql-ir`, which
must stay free of the FDAP stack — see `scripts/check-leaf-purity.sh` and
`.config/hakari.toml`'s `final-excludes`) depends on this crate so that shared
dependencies like DataFusion, Arrow, and tokio get built with one unified
feature set, instead of cargo potentially resolving a different set per
invocation depending on which subset of crates that invocation touches.

Nothing in this crate is ever imported — its only job is to exist as a pinned
dependency. `cargo machete` would otherwise flag its entire `[dependencies]`
list as unused; that's expected and pre-ignored in this crate's own
`Cargo.toml` (`[package.metadata.cargo-machete]`).

## Regenerating

After adding, removing, or changing a dependency anywhere in the workspace:

```bash
cargo hakari generate    # update workspace-hack/Cargo.toml
cargo hakari manage-deps # add/remove the workspace-hack dependency on members
cargo hakari verify      # what CI runs — confirms the above is up to date
```
