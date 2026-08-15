---
audience: operator
type: reference
status: living
sources:
  - .cargo/config.toml
  - Dockerfile
---

# Binary Runtime Characteristics

What the released binaries and container images assume about the machine they
run on, how a service is selected, and how they allocate memory.

## One server executable

SignalDB ships one server executable, `signaldb`, plus the separate operator
CLI `signaldb-cli`. Run with no subcommand it is the monolith (every service in
one process); run with a service subcommand it is that one service:

```bash
signaldb --config signaldb.toml            # monolith
signaldb router --config signaldb.toml     # only the router
signaldb writer --flight-port 50051        # only the writer, with its own flags
signaldb --config signaldb.toml -v router  # shared options work before or after
signaldb compactor validate                # common commands nest under a service
signaldb router --help                     # that service's flags + shared options
```

Services: `acceptor`, `router`, `writer`, `querier`, `compactor`, `mcp`. Each
keeps the flags, environment variables, ports and configuration keys of the
former per-service binaries; only the way it is invoked changed. The
executable's file name is never consulted — renaming or copying it does not
change what it runs.

Container images follow the same shape: every per-service image
(`ghcr.io/cedricziel/signaldb/<service>`) contains the same `signaldb` binary
and uses `["/usr/local/bin/signaldb", "<service>"]` as its entrypoint, so
arguments given to the container append to that service's arguments and
`docker run … router --version` prints the router's version. Deployments that
rely on the image entrypoint (compose, Kubernetes manifests, the hive app) need
no change; anything that used to exec `signaldb-<service>` by name runs
`signaldb <service>` instead. Release archives contain `signaldb`
(`signaldb.exe` on Windows) as the only server executable — the former
monolithic / microservices / mcp archives collapsed into one per target.

Why one binary: the release profile links with thin LTO, and cargo runs LTO
once per binary over the whole DataFusion/Arrow/Iceberg graph (~7 minutes each
on the release runners). Eight binaries meant eight such links for the same
code; two (`signaldb`, `signaldb-cli`) is what the images and archives need.

## CPU baseline

Every build of `common` runs a build script that embeds the bundled schema
registries: it parses `vendor/otel-semconv/` (the OpenTelemetry semantic
conventions at the self-monitoring pin) and `otel/registry/` and bakes the
resolved definitions into the binary, so the Dockerfile copies both trees
into the source builder alongside `src/`. A missing or drifted vendor tree
fails the build rather than the process at runtime.

Release builds are compiled with CPU target features enabled
(`.cargo/config.toml`): `aes`, `sse2`, `ssse3`, `sse4.1`, `sse4.2` on x86-64
and `aes`, `neon` on aarch64. This hardware-accelerates the hash paths used by
query execution (DataFusion group-by, joins, repartitioning) and SIMD paths in
Arrow kernels.

The resulting requirement:

- **x86-64**: Intel Westmere (2010) / AMD Bulldozer (2011) or newer — any CPU
  with SSE4.2 and AES-NI. This includes low-power homelab parts (Intel N100
  class, old Xeons from the same era onward).
- **aarch64**: ARMv8 with the crypto extension — Raspberry Pi 5, Apple
  Silicon, and all server ARM cores qualify.

On an older CPU the binaries fail immediately with an illegal-instruction
fault (`SIGILL`), not a graceful error. If you must run on such hardware,
build from source without the target-feature flags.

These flags reach the compiler only while `RUSTFLAGS` is unset, because cargo
picks a single source for flags and an environment value replaces the
per-target `rustflags` in `.cargo/config.toml` rather than merging with it.
The release jobs therefore pass `rustflags: ""` to
`actions-rust-lang/setup-rust-toolchain`, whose default is `-D warnings`. If
that is ever dropped, the published artifacts silently fall back to ahash's
scalar mixer and the default linker while still passing CI — the build stays
green and only gets slower, so nothing surfaces the regression.

One carve-out on aarch64 musl builds (the Linux arm64 container images and
release binaries): their C dependencies — jemalloc included — are compiled
with `-mno-outline-atomics` (`.cargo/config.toml` sets
`CFLAGS_aarch64_unknown_linux_musl`), because Ubuntu's musl-tools gcc links
the glibc-built libgcc whose outline-atomics runtime dispatch requires
`__getauxval`, a symbol musl doesn't provide. C code in these binaries uses
LL/SC atomics even on CPUs with LSE; Rust code is unaffected.

## Memory allocator

Service binaries in the container images (and CI-built musl release binaries)
run with **jemalloc** as the global allocator — the `jemalloc` cargo feature,
enabled by the Dockerfile and the musl build workflows. musl's built-in
allocator serializes multithreaded allocation and collapses under the
allocation churn of Arrow batch processing; jemalloc restores per-thread
caching. `signaldb-cli` and the macOS/Windows release binaries use the system
allocator.

jemalloc returns freed memory to the OS lazily, so container RSS can sit above
actual usage after load spikes. To make it decay promptly, enable jemalloc's
background purging thread:

```bash
MALLOC_CONF=background_thread:true
```

## Heap profiling and the glibc image

The default container images and the musl release binaries do **not** support
heap profiling. They are CPU-profiling only.

jemalloc's heap profiler walks the stack with `_Unwind_Backtrace`, which is
only safe when the unwinder and the libc it is linked against come from the
same toolchain. The musl targets are cross-compiled with a toolchain that is a
spec wrapper around the glibc gcc (see the aarch64 note above for the other
symptom of the same wrapper), so the unwinder that gets linked in is
ABI-mismatched with the musl runtime around it. Asking a musl binary for heap
profiles crashes the process — historically an immediate `SIGSEGV` on the
first sampled allocation, before the binary reached its own `--help` output.
The `jemalloc-profiling` cargo feature is therefore off for every musl build.

Heap profiling lives in a separate image instead:

```text
ghcr.io/cedricziel/signaldb:main-glibc-profiling
```

This is the monolithic `signaldb` binary built for
`x86_64-unknown-linux-gnu` by a native glibc toolchain — no cross wrapper, no
ABI mismatch — on a `debian:trixie-slim` runtime whose glibc matches the
builder's. It bundles the same entrypoint, ports, and Explore UI as the
monolithic image, plus `signaldb-cli` for parity. **amd64 only** — no arm64
build, no per-microservice variant, and only branch/PR tags (`main-`,
`pr-<n>-`) are published; tagged releases do not yet produce a version-pinned
profiling image, so pin the image by commit SHA if you need reproducibility.

Enable profiling on that image with both jemalloc's sampler and the
`[self_monitoring].heap_profiles_enabled` setting. `MALLOC_CONF` takes one
combined value, so if background purging is also enabled, append rather than
replace it:

```bash
MALLOC_CONF=background_thread:true,prof:true
```

Setting `MALLOC_CONF=prof:true` on a default (musl) image is not merely
useless — treat it as unsupported.

See the profiling configuration in `signaldb.dist.toml` and
[Profiles](../users/profiles.md#heap-profiles).
