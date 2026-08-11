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
run on, and how they allocate memory.

## CPU baseline

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

Heap self-profiling (the `jemalloc-profiling` cargo feature) builds on the
same allocator and ships by default in the container images and CI-built musl
binaries for acceptor, router, writer, querier, and the monolithic
`signaldb` binary (`compactor` and `mcp-server` don't define the feature).
Compiling it in adds no allocator overhead — the `jemalloc` feature already
builds jemalloc with `--enable-prof` — it just wires up the Rust-side
Pyroscope/jemalloc_pprof glue, which stays inert until `prof:true` and
`[self_monitoring].heap_profiles_enabled` are both set at runtime.
`MALLOC_CONF` takes one combined value, so if background purging is also
enabled, append rather than replace it:

```bash
MALLOC_CONF=background_thread:true,prof:true
```

See the profiling configuration in `signaldb.dist.toml` and
[Profiles](../users/profiles.md#heap-profiles).
