# Changelog

## [0.5.0](https://github.com/cedricziel/signaldb/compare/writer-v0.4.1...writer-v0.5.0) (2026-10-03)


### ⚠ BREAKING CHANGES

* **tests-integration:** metric tables are recreated as metrics/metric_exemplars; pre-cutover metric data is dropped, not migrated.
* metric tables are recreated as metrics/metric_exemplars; pre-cutover metric data is dropped, not migrated.
* remove the legacy attribute map write path ([#1793](https://github.com/cedricziel/signaldb/issues/1793))
* drop the legacy attr_tokens column ([#1794](https://github.com/cedricziel/signaldb/issues/1794))
* existing tables still in the legacy map<string,string> attribute layout are dropped and recreated in the typed layout the next time they are loaded; pre-cutover data in those tables is not migrated.

### Features

* **acceptor:** target the wide metrics table under the wide layout ([#1907](https://github.com/cedricziel/signaldb/issues/1907)) ([e6fb521](https://github.com/cedricziel/signaldb/commit/e6fb521164fa99ba4ebf987de022696576916cf9))
* **common:** declare the typed metrics and metric_exemplars schemas ([#1886](https://github.com/cedricziel/signaldb/issues/1886)) ([9095c9f](https://github.com/cedricziel/signaldb/commit/9095c9f252f6bfead8ea76d7ee8f2c996caa69bd))
* cut attribute storage over to the typed layout ([#1791](https://github.com/cedricziel/signaldb/issues/1791)) ([79b1fff](https://github.com/cedricziel/signaldb/commit/79b1fff7184ee1b3a97dfaf6fbf2d994a6199b39))
* cut metrics over to the typed metrics layout ([#1928](https://github.com/cedricziel/signaldb/issues/1928)) ([e416d6f](https://github.com/cedricziel/signaldb/commit/e416d6f79029d36930b0b98a288517ac18a0b7ff))
* **writer:** commit metric exemplars alongside their metrics batch ([#1895](https://github.com/cedricziel/signaldb/issues/1895)) ([5536644](https://github.com/cedricziel/signaldb/commit/55366442e01832a8f331f37797bc54aab543234f))
* **writer:** drop legacy metric tables from the reconciler under the wide layout ([#1908](https://github.com/cedricziel/signaldb/issues/1908)) ([ed8a311](https://github.com/cedricziel/signaldb/commit/ed8a311d9d124e7a754f06c9503c61e75ed61896))
* **writer:** place typed attributes in every writer deployment ([#1790](https://github.com/cedricziel/signaldb/issues/1790)) ([49551e8](https://github.com/cedricziel/signaldb/commit/49551e8fd811514c222b1643ecbc2cca3c83495f))
* **writer:** place typed attributes through the type authority ([#1764](https://github.com/cedricziel/signaldb/issues/1764)) ([afd07b6](https://github.com/cedricziel/signaldb/commit/afd07b6241cfd2d33054aec7dfde2ed9b9ee7c04))
* **writer:** surface off-type attribute values and type-pin conflicts ([#1829](https://github.com/cedricziel/signaldb/issues/1829)) ([b4bbfb4](https://github.com/cedricziel/signaldb/commit/b4bbfb464e9cb3e96c5afe82cc543b9f5beaf401))
* **writer:** transform wire exemplars into metric_exemplars rows ([#1894](https://github.com/cedricziel/signaldb/issues/1894)) ([27ac405](https://github.com/cedricziel/signaldb/commit/27ac4054d78fa3464ac953c4682ec5030d421ed2))
* **writer:** transform wire metrics into the typed metrics layout ([#1893](https://github.com/cedricziel/signaldb/issues/1893)) ([ef96961](https://github.com/cedricziel/signaldb/commit/ef969619aa00fb944cdafcfbedbc6a47a73c28fd))
* **writer:** wire the type authority into the writer service ([#1765](https://github.com/cedricziel/signaldb/issues/1765)) ([3f6c1cf](https://github.com/cedricziel/signaldb/commit/3f6c1cfcfdb7f977465806cd95f09750b4ef1c50))
* **writer:** write warm-index tokens for opted-in typed tables ([#1783](https://github.com/cedricziel/signaldb/issues/1783)) ([c2c1dba](https://github.com/cedricziel/signaldb/commit/c2c1dbab6de9d9175bd4f06615d556f67ed4fe82))


### Bug Fixes

* **acceptor:** acknowledge an exporter's resend of an already-durable batch ([#1814](https://github.com/cedricziel/signaldb/issues/1814)) ([ff52ded](https://github.com/cedricziel/signaldb/commit/ff52deda64b5b803c4040cee0db249a6beb5d7f4))
* **acceptor:** dedup client resends across acceptor replicas and restarts ([#1821](https://github.com/cedricziel/signaldb/issues/1821)) ([98cf40e](https://github.com/cedricziel/signaldb/commit/98cf40e5e6bc7d97f3b43564591df2039b9c8070))
* **compactor:** advertise COMPACTOR_ADVERTISE_ADDR instead of the bind address ([#2107](https://github.com/cedricziel/signaldb/issues/2107)) ([2b8adb7](https://github.com/cedricziel/signaldb/commit/2b8adb7f23df02ee44a7d7f72dde53bcdd436254)), closes [#1844](https://github.com/cedricziel/signaldb/issues/1844)
* **config:** merge a tenant's schema block over the global [schema] ([#2086](https://github.com/cedricziel/signaldb/issues/2086)) ([0832345](https://github.com/cedricziel/signaldb/commit/0832345521808cf865d627580dc1a0621692adcc))
* **querier:** resolve label columns by their origin-key doc ([#2183](https://github.com/cedricziel/signaldb/issues/2183)) ([73768e9](https://github.com/cedricziel/signaldb/commit/73768e96e645b50157c04864dac913637f2d1068))
* **telemetry:** namespace bare log fields flagged by weaver live-check ([#1879](https://github.com/cedricziel/signaldb/issues/1879)) ([90dbf09](https://github.com/cedricziel/signaldb/commit/90dbf09c31f7181f4f97c4a0bdb6c79f0f78aca3)), closes [#912](https://github.com/cedricziel/signaldb/issues/912)
* **writer:** keep skipped exemplar ids in the metric_exemplars marker ([#1948](https://github.com/cedricziel/signaldb/issues/1948)) ([a36f3e6](https://github.com/cedricziel/signaldb/commit/a36f3e697552a72386c081668db50706f556c30c))
* **writer:** retry the legacy metric table purge on converged datasets ([#1949](https://github.com/cedricziel/signaldb/issues/1949)) ([1ec8d3f](https://github.com/cedricziel/signaldb/commit/1ec8d3f9a87f165e2cf7e69bfbe2b4bcd145a1a5))


### Code Refactoring

* build SchemaConfig literals with struct update syntax ([#1779](https://github.com/cedricziel/signaldb/issues/1779)) ([9b46397](https://github.com/cedricziel/signaldb/commit/9b46397790ee6edc1f5ed0757ee57c89f2d1df09))
* **common:** let ServiceBootstrap resolve the advertised address ([#2121](https://github.com/cedricziel/signaldb/issues/2121)) ([6bef83b](https://github.com/cedricziel/signaldb/commit/6bef83b68b873ea7407d0f92981665a2b4cb420e))
* drop the dead MetricsLayout switch and *_with_layout helpers ([#1961](https://github.com/cedricziel/signaldb/issues/1961)) ([4a98be3](https://github.com/cedricziel/signaldb/commit/4a98be3e108b2c67be847db23de0f768c1ad2a58))
* drop the legacy attr_tokens column ([#1794](https://github.com/cedricziel/signaldb/issues/1794)) ([a4b84a7](https://github.com/cedricziel/signaldb/commit/a4b84a7b7128e822524b5379036ded18873f4fa9))
* remove the legacy attribute map write path ([#1793](https://github.com/cedricziel/signaldb/issues/1793)) ([9962ba9](https://github.com/cedricziel/signaldb/commit/9962ba962ca3da95107442dd0d7140cb0b2fd98e))
* **writer:** delete the gauge and sum metric transforms ([#1935](https://github.com/cedricziel/signaldb/issues/1935)) ([e920fc1](https://github.com/cedricziel/signaldb/commit/e920fc154792bddce29659c0ceb341590cbbf408))
* **writer:** delete the gauge schema builder and move its fixtures onto the wire format ([#1934](https://github.com/cedricziel/signaldb/issues/1934)) ([bc74e50](https://github.com/cedricziel/signaldb/commit/bc74e50459199b3a2a38456efe845ec417e97bc8))
* **writer:** delete the histogram, exponential-histogram, and summary metric transforms ([#1932](https://github.com/cedricziel/signaldb/issues/1932)) ([f7276e2](https://github.com/cedricziel/signaldb/commit/f7276e2832de355cc77912d2f683fe6e7252f4c2))
* **writer:** delete the sum, histogram, exponential-histogram, and summary schema builders ([#1933](https://github.com/cedricziel/signaldb/issues/1933)) ([cd762cf](https://github.com/cedricziel/signaldb/commit/cd762cf8e93b068ee4dbbc97203523fb7e79bcec))


### Tests

* derive per-tenant table counts and switch metrics fixtures to wire-format batches ([#1927](https://github.com/cedricziel/signaldb/issues/1927)) ([dbeddc5](https://github.com/cedricziel/signaldb/commit/dbeddc5b78665446fbc7dc2c151816a3bd21feb6))
* move metric fixtures off the legacy per-type tables ([#1966](https://github.com/cedricziel/signaldb/issues/1966)) ([4f239c7](https://github.com/cedricziel/signaldb/commit/4f239c7da68a4a7db76667044a724b19fe691127))
* **tests-integration:** add an end-to-end metrics cutover test ([#1929](https://github.com/cedricziel/signaldb/issues/1929)) ([bb47677](https://github.com/cedricziel/signaldb/commit/bb4767732f40a82ca1af1e4eab88a8dc11580829))


### Build System

* fix the beta test leg for cargo's unused-dependency lints ([#2047](https://github.com/cedricziel/signaldb/issues/2047)) ([6867d69](https://github.com/cedricziel/signaldb/commit/6867d69dceb26f2e55ccfac31e60ae42aad76418))


### Continuous Integration

* run every Criterion bench once on core PRs ([#2167](https://github.com/cedricziel/signaldb/issues/2167)) ([918f9c5](https://github.com/cedricziel/signaldb/commit/918f9c558ab655cc4b71071ce981d9ba5ed6ec8a))

## [0.4.1](https://github.com/cedricziel/signaldb/compare/writer-v0.4.0...writer-v0.4.1) (2026-09-23)


### Features

* **writer:** match WAL label columns by stamped origin key ([#1640](https://github.com/cedricziel/signaldb/issues/1640)) ([ec23d82](https://github.com/cedricziel/signaldb/commit/ec23d82325ccecdd4694ee9af32fb09c7cd73593))


### Bug Fixes

* **writer:** retire stale WAL markers on dormant tables from the reconciler ([#1635](https://github.com/cedricziel/signaldb/issues/1635)) ([d0ce9e5](https://github.com/cedricziel/signaldb/commit/d0ce9e52f0ec740541339bd741b10e0e1c3f371e))


### Code Refactoring

* **writer:** drop unused object_store from WalProcessor ([#1665](https://github.com/cedricziel/signaldb/issues/1665)) ([b34a85d](https://github.com/cedricziel/signaldb/commit/b34a85d14c584bfd2c3215c78fe78f0ad3484b35))
* **writer:** drop unused object_store handle from IcebergTableWriter ([#1650](https://github.com/cedricziel/signaldb/issues/1650)) ([c2e3112](https://github.com/cedricziel/signaldb/commit/c2e31121ce10d9175ba5a9b54d67d3cbf9035ca8))

## [0.4.0](https://github.com/cedricziel/signaldb/compare/writer-v0.3.0...writer-v0.4.0) (2026-09-12)


### Features

* **iceberg:** Postgres backend for the Iceberg SQL catalog ([#1529](https://github.com/cedricziel/signaldb/issues/1529)) ([53f7b7e](https://github.com/cedricziel/signaldb/commit/53f7b7e064c839587d3a4470868012d1051fbcd4))
* sort every producer's rows by the declared key and attest it per file ([#1313](https://github.com/cedricziel/signaldb/issues/1313)) ([c667eda](https://github.com/cedricziel/signaldb/commit/c667eda0c05752ff51fb1ad6ba37cf4594455c6f))
* **storage:** persist resource_identity on metrics and profiles ([#1503](https://github.com/cedricziel/signaldb/issues/1503)) ([1177c01](https://github.com/cedricziel/signaldb/commit/1177c01556dcdb5f5c2569eb2cf73a5745c3b177))
* **storage:** persist resource_identity on traces and logs ([#1497](https://github.com/cedricziel/signaldb/issues/1497)) ([b8a3b47](https://github.com/cedricziel/signaldb/commit/b8a3b47f09edf23eb54a9d4674c4fb9bba5122a7))
* **wal:** frame every record with a length and CRC-32 ([#1294](https://github.com/cedricziel/signaldb/issues/1294)) ([50ab64a](https://github.com/cedricziel/signaldb/commit/50ab64aefb041d471e0668f86255970ab0e12840)), closes [#946](https://github.com/cedricziel/signaldb/issues/946)


### Bug Fixes

* **wal:** add signaldb wal dead-letter replay/list/purge ([#1528](https://github.com/cedricziel/signaldb/issues/1528)) ([563199f](https://github.com/cedricziel/signaldb/commit/563199f9785f16bb70b29d8861437e6af3deb490))
* **wal:** cap concurrently active WAL instances against RLIMIT_NOFILE ([#1437](https://github.com/cedricziel/signaldb/issues/1437)) ([2c14e7e](https://github.com/cedricziel/signaldb/commit/2c14e7e203c114572bd786a835802512ac7e1067))
* **wal:** reclaim processed segments from the service drain loops ([#1338](https://github.com/cedricziel/signaldb/issues/1338)) ([e2b3da6](https://github.com/cedricziel/signaldb/commit/e2b3da636d773b20457e92e2a9938da68d14b712))
* **wal:** report and expire dead-lettered WAL entries ([#1525](https://github.com/cedricziel/signaldb/issues/1525)) ([c8e580b](https://github.com/cedricziel/signaldb/commit/c8e580beef3cfc2ab15f58e048c412dfb01bd12f)), closes [#1494](https://github.com/cedricziel/signaldb/issues/1494)
* **wal:** self-heal entries_pending gauge drift with per-directory attribution ([#1523](https://github.com/cedricziel/signaldb/issues/1523)) ([586889d](https://github.com/cedricziel/signaldb/commit/586889d2f16e3b5eddb2a7f990c8a347e19394c2))
* **writer:** bound WAL drain decode to a per-cycle byte budget ([#1396](https://github.com/cedricziel/signaldb/issues/1396)) ([5568037](https://github.com/cedricziel/signaldb/commit/55680371195cb472b076b429ce2a553d2402eefa))
* **writer:** classify commit failures as permanent or transient ([#1399](https://github.com/cedricziel/signaldb/issues/1399)) ([07c0d9a](https://github.com/cedricziel/signaldb/commit/07c0d9a395cf35ccca06ea3e18201e6d7fa2ab82))
* **writer:** collision-collapsing materialized_labels naming, same bug class as [#814](https://github.com/cedricziel/signaldb/issues/814) ([#1532](https://github.com/cedricziel/signaldb/issues/1532)) ([d25034c](https://github.com/cedricziel/signaldb/commit/d25034c3228e4f067a197089470db4551057bfbf))
* **writer:** give each tenant its own WAL instead of one global WAL ([#1299](https://github.com/cedricziel/signaldb/issues/1299)) ([830900e](https://github.com/cedricziel/signaldb/commit/830900ebaddf46dff5ac9eb0748d8fb63e7b35b2))
* **writer:** hygiene follow-ups from writer review (W8, W10, W11) ([#1409](https://github.com/cedricziel/signaldb/issues/1409)) ([43422e6](https://github.com/cedricziel/signaldb/commit/43422e6f4ac08af152109e8160e0c3dfce40e703))
* **writer:** isolate a poison entry to itself, not its commit group ([#1402](https://github.com/cedricziel/signaldb/issues/1402)) ([d9c861e](https://github.com/cedricziel/signaldb/commit/d9c861ec4f3bf372dc94ad658be4c70d993eafff))
* **writer:** normalise tenant id before suppression and label lookup ([#1430](https://github.com/cedricziel/signaldb/issues/1430)) ([ab83f5f](https://github.com/cedricziel/signaldb/commit/ab83f5fc9dbb9c235a42e03aac4aa4503eec2c9a)), closes [#1334](https://github.com/cedricziel/signaldb/issues/1334)
* **writer:** reject deterministic do_put decode and routing faults ([#1398](https://github.com/cedricziel/signaldb/issues/1398)) ([e09fefb](https://github.com/cedricziel/signaldb/commit/e09fefbc771f5f362660180368383ed8a3b79723)), closes [#1060](https://github.com/cedricziel/signaldb/issues/1060)
* **writer:** retire Iceberg WAL markers left by writer ids past retention ([#1346](https://github.com/cedricziel/signaldb/issues/1346)) ([61c9f1b](https://github.com/cedricziel/signaldb/commit/61c9f1bc426edc34516922368ddcfe598d1e62f2))
* **writer:** route ingest and replay through one shared function ([#1333](https://github.com/cedricziel/signaldb/issues/1333)) ([452ec09](https://github.com/cedricziel/signaldb/commit/452ec09495f275117072713a7163eb8183680561))
* **writer:** tighten processor observability (metrics, logging, do_action span) ([#1397](https://github.com/cedricziel/signaldb/issues/1397)) ([7bf404b](https://github.com/cedricziel/signaldb/commit/7bf404b2142adbfc33fdf0a6362f543a4d831519))
* **writer:** wrap group commit in an explicit timeout ([#1408](https://github.com/cedricziel/signaldb/issues/1408)) ([1041a9d](https://github.com/cedricziel/signaldb/commit/1041a9de54081f2fca9fc7d35d796639df1b8c5e)), closes [#1400](https://github.com/cedricziel/signaldb/issues/1400)


### Performance Improvements

* **writer:** commit tenants' WAL groups concurrently, not one at a time ([#1344](https://github.com/cedricziel/signaldb/issues/1344)) ([414982c](https://github.com/cedricziel/signaldb/commit/414982cab6c0e243302ead1519ef18f9ae6685e3))


### Code Refactoring

* quality cleanups across writer, mcp-server, schema-model, and tests-integration ([#1330](https://github.com/cedricziel/signaldb/issues/1330)) ([cee4018](https://github.com/cedricziel/signaldb/commit/cee401872f96e2a6961edc1dd3714fa394a56c31))


### Tests

* **bench:** measure the declared sort order win and archive declared-sort-orders ([#1467](https://github.com/cedricziel/signaldb/issues/1467)) ([33aaa38](https://github.com/cedricziel/signaldb/commit/33aaa38ff54b38d6cd10952585b616f20d9e94ca))

## [0.3.0](https://github.com/cedricziel/signaldb/compare/writer-v0.2.1...writer-v0.3.0) (2026-08-17)


### Features

* one signaldb binary with the services as subcommands ([#1204](https://github.com/cedricziel/signaldb/issues/1204)) ([77f3278](https://github.com/cedricziel/signaldb/commit/77f3278ca445ac9b28bf955b0e482d4366a27c07))
* **otel-native-schema:** Layer 2 logical schema foundation ([#1104](https://github.com/cedricziel/signaldb/issues/1104)) ([af66060](https://github.com/cedricziel/signaldb/commit/af6606016430645693a0d524d3f15d9db4a52ead))
* semconv RPC server spans on Flight boundaries ([#904](https://github.com/cedricziel/signaldb/issues/904)) ([a791f45](https://github.com/cedricziel/signaldb/commit/a791f45edf5b1650cc9091d1acf481175060628a))
* **tracing:** add server.address and network.peer to RPC spans ([#1111](https://github.com/cedricziel/signaldb/issues/1111)) ([4e64934](https://github.com/cedricziel/signaldb/commit/4e64934814762c25226a3a7529bc9d695035d578))
* **writer:** ack ingest on WAL flush, commit to Iceberg asynchronously ([#893](https://github.com/cedricziel/signaldb/issues/893)) ([fffdbb1](https://github.com/cedricziel/signaldb/commit/fffdbb109c48893bb2725a8afd3e2e740968a152))
* **writer:** bound Iceberg metadata growth via delete-after-commit ([#895](https://github.com/cedricziel/signaldb/issues/895)) ([35ce5c7](https://github.com/cedricziel/signaldb/commit/35ce5c7aa18aa4f12d3e62c4f34221c849f973f3))
* **writer:** coalesce Iceberg commits with a per-table floor + force-commit primitive ([#891](https://github.com/cedricziel/signaldb/issues/891)) ([ad47bb6](https://github.com/cedricziel/signaldb/commit/ad47bb6867dd5cf622701b5778ef9f94e7b60923))


### Bug Fixes

* **acceptor:** dead-letter writer-rejected WAL entries instead of wedging the retry pass ([#1063](https://github.com/cedricziel/signaldb/issues/1063)) ([7fc6ada](https://github.com/cedricziel/signaldb/commit/7fc6ada1ea922784220789f304fb3f8448ff8ef1)), closes [#1060](https://github.com/cedricziel/signaldb/issues/1060)
* **build:** stop jemalloc heap profiling from crashing musl images ([#1126](https://github.com/cedricziel/signaldb/issues/1126)) ([98b2996](https://github.com/cedricziel/signaldb/commit/98b299660ef31b56d73e079a2477166b415e736e))
* **common:** resolve a tenant's default dataset even without a dataset row ([#1082](https://github.com/cedricziel/signaldb/issues/1082)) ([055733f](https://github.com/cedricziel/signaldb/commit/055733f7e2d0e016091a987836fab2e788540e82))
* metrics without service.name land as 'unknown'; boot log flood demoted to debug ([#1227](https://github.com/cedricziel/signaldb/issues/1227)) ([7b5ea34](https://github.com/cedricziel/signaldb/commit/7b5ea343096ea8a7c0f62575029ac1e838ec514c))
* **metrics:** carry NaN/±Inf values through the wire format instead of dead-lettering ([#1239](https://github.com/cedricziel/signaldb/issues/1239)) ([9e38b3a](https://github.com/cedricziel/signaldb/commit/9e38b3a993b6d632d7c67f498f2f489ea97e6636)), closes [#1061](https://github.com/cedricziel/signaldb/issues/1061)
* provision signal tables for every registered dataset, and read an absent one as empty ([#1074](https://github.com/cedricziel/signaldb/issues/1074)) ([9a50ffa](https://github.com/cedricziel/signaldb/commit/9a50ffaa7e404a96cb80d7d3b0cc0850ede00f49))
* **telemetry:** emit int-typed registry attributes as i64 ([#1013](https://github.com/cedricziel/signaldb/issues/1013)) ([be67718](https://github.com/cedricziel/signaldb/commit/be677184819e5cbe700d253a03e59cd2bffa7ba8))
* **traces:** span_kind/status_code numeric source of truth + schema evolution engine ([#1235](https://github.com/cedricziel/signaldb/issues/1235)) ([0f8603b](https://github.com/cedricziel/signaldb/commit/0f8603bdb1f39254c83af0c631653a65c8a85e3f))
* **wal:** carry tenant/dataset/signal on WAL failure telemetry ([#866](https://github.com/cedricziel/signaldb/issues/866)) ([a023dbb](https://github.com/cedricziel/signaldb/commit/a023dbb54822964d44f7c22864391eb2af957a58))
* **writer,tempo-api:** stop leaking Option Debug into logs; accept lowercase Tempo tag scopes ([#1149](https://github.com/cedricziel/signaldb/issues/1149)) ([4a83388](https://github.com/cedricziel/signaldb/commit/4a8338801252c36a948efa10d1a5cfe0d4f7de5a))
* **writer:** derive flush scope from request metadata, not the action body ([#897](https://github.com/cedricziel/signaldb/issues/897)) ([cd94186](https://github.com/cedricziel/signaldb/commit/cd9418653c1f90812ffee4a0688dd947039dbbeb))
* **writer:** table-schema-consistency check across all built-in tables ([#1241](https://github.com/cedricziel/signaldb/issues/1241)) ([4a392a8](https://github.com/cedricziel/signaldb/commit/4a392a8e6059c3f9b9798b4def4894ebb1d8e97a))
* **writer:** use registry attribute names on reconciler provisioning counters ([#1152](https://github.com/cedricziel/signaldb/issues/1152)) ([aa15a68](https://github.com/cedricziel/signaldb/commit/aa15a684317399e8356498f0d44c9f8bb3e9d98f))


### Performance Improvements

* CPU target features and jemalloc allocator for release builds ([#970](https://github.com/cedricziel/signaldb/issues/970)) ([766e2d1](https://github.com/cedricziel/signaldb/commit/766e2d1c82dad65a674184edaf2e8d67cb4083dd))
* **flight,wal:** compress Flight IPC payloads and WAL entries ([#945](https://github.com/cedricziel/signaldb/issues/945)) ([#998](https://github.com/cedricziel/signaldb/issues/998)) ([efb5ef4](https://github.com/cedricziel/signaldb/commit/efb5ef4bc85e2e77483f4546255b50c564015827))
* **wal:** batch index persistence in mark_processed_many ([#943](https://github.com/cedricziel/signaldb/issues/943)) ([#984](https://github.com/cedricziel/signaldb/issues/984)) ([41a91cd](https://github.com/cedricziel/signaldb/commit/41a91cd4938286a39c120e642f0b11261b813ab7))
* **writer:** compile trace v1-&gt;v2 materialization into a resolved-once plan ([#1245](https://github.com/cedricziel/signaldb/issues/1245)) ([a8910ee](https://github.com/cedricziel/signaldb/commit/a8910eee63c470b277629b8c3ed586acec8467b1))


### Code Refactoring

* **flight:** decode Flight data dictionary-aware ([#1004](https://github.com/cedricziel/signaldb/issues/1004)) ([94a7a30](https://github.com/cedricziel/signaldb/commit/94a7a30edd81060f2bfc5147dbf3b53307d2de72))
* **logging:** forbid log:: macros in favor of tracing:: ([#1006](https://github.com/cedricziel/signaldb/issues/1006)) ([071ebb4](https://github.com/cedricziel/signaldb/commit/071ebb47d02f2d6e43ccfb60380c00e3be929248))
* simplify backend workspace (dedup, dead code, redundant clones) ([#1168](https://github.com/cedricziel/signaldb/issues/1168)) ([409b778](https://github.com/cedricziel/signaldb/commit/409b778686a1cea5c54edfba7778c3e9ed3aa29c))
* span hygiene sweep and construction guard ([#907](https://github.com/cedricziel/signaldb/issues/907)) ([c1f7b81](https://github.com/cedricziel/signaldb/commit/c1f7b81fbc00ae5fd6c9b948f9fb35c9d5a27d26))
* **writer:** simplify pass ([#1173](https://github.com/cedricziel/signaldb/issues/1173)) ([162985e](https://github.com/cedricziel/signaldb/commit/162985e3e249658e08c145bb33624f537177a013))


### Tests

* delete tautological tests and rewrite salvageable ones as contract tests ([#961](https://github.com/cedricziel/signaldb/issues/961)) ([b3e884a](https://github.com/cedricziel/signaldb/commit/b3e884ad59b4df853429133d5eef2724a8adcada))
* make swallow-and-fallback integration tests fail on real failures ([#965](https://github.com/cedricziel/signaldb/issues/965)) ([a6720ba](https://github.com/cedricziel/signaldb/commit/a6720ba4d84b933e59f14490a2aca41f19d38779))
* polish medium/low audit findings across the workspace ([#969](https://github.com/cedricziel/signaldb/issues/969)) ([8962f6d](https://github.com/cedricziel/signaldb/commit/8962f6d1d22c8a176d4a1d99376d61b42b1da258))

## [0.2.1](https://github.com/cedricziel/signaldb/compare/writer-v0.2.0...writer-v0.2.1) (2026-07-30)


### Features

* **writer:** propagate trace context through the WAL write path ([#836](https://github.com/cedricziel/signaldb/issues/836)) ([455c58f](https://github.com/cedricziel/signaldb/commit/455c58f79a3329449a66d9e5004f22203ad30c2c))

## [0.2.0](https://github.com/cedricziel/signaldb/compare/writer-v0.1.0...writer-v0.2.0) (2026-07-30)


### ⚠ BREAKING CHANGES

* Minimum supported Rust version is now 1.85.0

### Features

* add Grafana datasource plugin and Docker infrastructure ([#253](https://github.com/cedricziel/signaldb/issues/253)) ([a95cdfe](https://github.com/cedricziel/signaldb/commit/a95cdfe038e0667bc9b563c3b2f7a8bd7b280069))
* Add schema module with Iceberg integration and DSN-based storage ([#162](https://github.com/cedricziel/signaldb/issues/162)) ([60bbb8d](https://github.com/cedricziel/signaldb/commit/60bbb8d09a5ff63e2114c6383e7650c9dfef0d24))
* attr_tokens key=value column with bloom for arbitrary attribute equality ([#777](https://github.com/cedricziel/signaldb/issues/777)) ([b305438](https://github.com/cedricziel/signaldb/commit/b30543823d4f1d20f489c1b4c097d2fe7c448fe0))
* **auth:** add tenant ID validation and naming consistency ([#180](https://github.com/cedricziel/signaldb/issues/180)) ([#318](https://github.com/cedricziel/signaldb/issues/318)) ([2c2146a](https://github.com/cedricziel/signaldb/commit/2c2146a579e978842b0af48f2445485d3fb7a1e4))
* **cli:** add HTTP admin API client for TUI ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** add terminal UI with traces, logs, metrics, admin, and dashboard tabs ([#458](https://github.com/cedricziel/signaldb/issues/458)) ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** implement Admin tab with tenant/key/dataset CRUD and confirmations ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** implement Logs tab with Flight SQL query interface ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** implement Metrics tab with sparklines and Flight SQL ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** integrate TUI tabs with help overlay and error handling ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **compactor:** complete epic [#432](https://github.com/cedricziel/signaldb/issues/432) — real compaction, multi-instance tests, observability ([#540](https://github.com/cedricziel/signaldb/issues/540)) ([ed95e20](https://github.com/cedricziel/signaldb/commit/ed95e2062a05b7386d05188c89a754a3606fc428))
* **compactor:** Phase 3 - Retention & Lifecycle Management ([#467](https://github.com/cedricziel/signaldb/issues/467)) ([28acc8d](https://github.com/cedricziel/signaldb/commit/28acc8d215f029fe0b81dcd9b916f29ccdea60d6))
* complete all-signal pipeline (traces, logs, metrics) with producer, transforms, and monolithic discovery fix ([#435](https://github.com/cedricziel/signaldb/issues/435)) ([b973458](https://github.com/cedricziel/signaldb/commit/b9734582edd68436c4ccb3891c3767726a37f433))
* **config:** per-tenant materialized-label allowlists ([#745](https://github.com/cedricziel/signaldb/issues/745)) ([41205f9](https://github.com/cedricziel/signaldb/commit/41205f95c8d039b618699d2018a29ee7a95d09aa))
* **flight:** authenticate Flight ports via internal service key ([#579](https://github.com/cedricziel/signaldb/issues/579)) ([da1b41f](https://github.com/cedricziel/signaldb/commit/da1b41f4698ce9f58348239d789a1678e23353b3)), closes [#544](https://github.com/cedricziel/signaldb/issues/544)
* **iceberg:** profiles table schema and config toggle ([#633](https://github.com/cedricziel/signaldb/issues/633)) ([9203530](https://github.com/cedricziel/signaldb/commit/920353022c1a58c5ee667954d3356bb7d481836f)), closes [#351](https://github.com/cedricziel/signaldb/issues/351)
* implement Iceberg table writer adapter to replace direct Parquet writes ([#175](https://github.com/cedricziel/signaldb/issues/175)) ([a55cc3d](https://github.com/cedricziel/signaldb/commit/a55cc3dbd06d955ee82d64e002abab588102df04))
* implement multi-tenancy with WAL isolation and authentication ([#243](https://github.com/cedricziel/signaldb/issues/243)) ([9a8945f](https://github.com/cedricziel/signaldb/commit/9a8945f06e871a96f5890e194534ae11ebb1f35b))
* implement trace querying functionality for issue [#6](https://github.com/cedricziel/signaldb/issues/6) ([#186](https://github.com/cedricziel/signaldb/issues/186)) ([ea8d9b4](https://github.com/cedricziel/signaldb/commit/ea8d9b47446cdbb89bb05b0a5c048c023d4dde49))
* integrate cargo-machete for unused dependency detection ([#130](https://github.com/cedricziel/signaldb/issues/130)) ([f305d3b](https://github.com/cedricziel/signaldb/commit/f305d3b9a6923ca2f7eca95ee83ed9002ee7cee1))
* **logs:** typed Map attribute columns — exact matching for every label ([#741](https://github.com/cedricziel/signaldb/issues/741)) ([c362536](https://github.com/cedricziel/signaldb/commit/c362536555c62de8186a2c8bd3b4f959c2c252dd))
* **metrics, profiles:** materialize configured labels ([#728](https://github.com/cedricziel/signaldb/issues/728)) ([a20caaf](https://github.com/cedricziel/signaldb/commit/a20caaf15340cecbfb9e0973bbc84d5e93329a60))
* Phase 2 Component Integration with WAL and Flight Services ([#138](https://github.com/cedricziel/signaldb/issues/138)) ([47f4174](https://github.com/cedricziel/signaldb/commit/47f417488c7b0225d031219df94a1d7eb55ff166))
* **querier,writer:** unify table reference format and shared CatalogManager ([#395](https://github.com/cedricziel/signaldb/issues/395)) ([9928f26](https://github.com/cedricziel/signaldb/commit/9928f266766d1de1d2276e5724a27ef29b1128da))
* **schema:** add materialized-labels config and column-name helper ([#723](https://github.com/cedricziel/signaldb/issues/723)) ([8c213f0](https://github.com/cedricziel/signaldb/commit/8c213f05ced5ecf9b64e7457fff06690c6156bae))
* **self-monitoring:** epic [#447](https://github.com/cedricziel/signaldb/issues/447) — SignalDB observes itself (dogfooding) ([#542](https://github.com/cedricziel/signaldb/issues/542)) ([e6d7b1f](https://github.com/cedricziel/signaldb/commit/e6d7b1fc37f370f534d8780b3a6fe5d180b1ad65))
* **traces:** materialize configured labels for trace search ([#727](https://github.com/cedricziel/signaldb/issues/727)) ([4ef9584](https://github.com/cedricziel/signaldb/commit/4ef9584f514bcb7ae77e9e95b19a1e91f6ee8073))
* **wal:** add WriteProfiles operation and per-signal profiles WAL ([#632](https://github.com/cedricziel/signaldb/issues/632)) ([9a938af](https://github.com/cedricziel/signaldb/commit/9a938af0a213ace259e2c5e6ca1d16123ecdc99e)), closes [#348](https://github.com/cedricziel/signaldb/issues/348)
* **writer:** persist profiles to the Iceberg profiles table ([#637](https://github.com/cedricziel/signaldb/issues/637)) ([5dedbdc](https://github.com/cedricziel/signaldb/commit/5dedbdcbba5080071f859c964ca88ac808685e7e)), closes [#353](https://github.com/cedricziel/signaldb/issues/353)
* **writer:** populate materialized label columns on the log write path ([#725](https://github.com/cedricziel/signaldb/issues/725)) ([8841099](https://github.com/cedricziel/signaldb/commit/88410997036445582f9bceec27c5ea21cb471bf6))


### Bug Fixes

* align Iceberg namespace paths and partition spec (Issue [#185](https://github.com/cedricziel/signaldb/issues/185)) ([#306](https://github.com/cedricziel/signaldb/issues/306)) ([cc7af60](https://github.com/cedricziel/signaldb/commit/cc7af60ad6426eefc0a0de5c628b865306227172))
* **ci:** resolve clippy 1.97 lints, security advisories, and ethnum build failure ([#516](https://github.com/cedricziel/signaldb/issues/516)) ([b21c459](https://github.com/cedricziel/signaldb/commit/b21c4596f361d14dad147447cc19da4156fb81da))
* **config:** refuse in-memory discovery/catalog in standalone services ([#599](https://github.com/cedricziel/signaldb/issues/599)) ([c8413ba](https://github.com/cedricziel/signaldb/commit/c8413babe5de5346477bf4d1ff26a7f2fef380bb))
* preserve OTLP scope/resource metadata and events/links in trace pipeline ([#183](https://github.com/cedricziel/signaldb/issues/183)) ([#307](https://github.com/cedricziel/signaldb/issues/307)) ([dfe04d7](https://github.com/cedricziel/signaldb/commit/dfe04d73d27c0e8820aa8daeed0787d048701865))
* resolve beta channel build failures and add temporary table cleanup ([#179](https://github.com/cedricziel/signaldb/issues/179)) ([d5f48dd](https://github.com/cedricziel/signaldb/commit/d5f48dd69cf1026295a825aea00f847c284ebe18))
* **self-monitoring:** box suppressed Flight handler futures ([#766](https://github.com/cedricziel/signaldb/issues/766)) ([7852012](https://github.com/cedricziel/signaldb/commit/7852012a5a78ee25892bac5fed981ccfe271cc52))
* **self-monitoring:** extend anti-loop guard to writer, querier, and router ([#765](https://github.com/cedricziel/signaldb/issues/765)) ([ec1ea04](https://github.com/cedricziel/signaldb/commit/ec1ea04be358a6b0c4f7452d758e9a6a0e7c8136))
* **wal:** honor [wal].wal_dir for acceptor and writer WAL directories ([#758](https://github.com/cedricziel/signaldb/issues/758)) ([d4bc621](https://github.com/cedricziel/signaldb/commit/d4bc621bd1725202c37369d6a373359e664a0cc7))
* **wal:** implement proper WAL segment cleanup and processed state persistence ([#252](https://github.com/cedricziel/signaldb/issues/252)) ([b3e73ff](https://github.com/cedricziel/signaldb/commit/b3e73ffe84eaa638b75b3c07c8d194801c8fcfe7))
* **writer:** harden the write path against panics and silent task death ([#605](https://github.com/cedricziel/signaldb/issues/605)) ([ca716db](https://github.com/cedricziel/signaldb/commit/ca716dbc6b2321a4eb838ff3d8031b69e1ec6075))
* **writer:** idempotent WAL-to-Iceberg commits — no duplicate rows on crash replay ([#592](https://github.com/cedricziel/signaldb/issues/592)) ([c43437b](https://github.com/cedricziel/signaldb/commit/c43437b16b4bdd575f84565fa9b0fdd40d969291))
* **writer:** remove the unverified SQL INSERT write path ([#608](https://github.com/cedricziel/signaldb/issues/608)) ([f727170](https://github.com/cedricziel/signaldb/commit/f7271705e4d967efc0796c23f15dca9da44a2f71))
* **writer:** use relative table locations to prevent path duplication in Iceberg ([#436](https://github.com/cedricziel/signaldb/issues/436)) ([3bbc0a6](https://github.com/cedricziel/signaldb/commit/3bbc0a697956067e812b08fca8e0f051667d9c7e))


### Documentation

* full staleness sweep — match all docs, skills, and READMEs to current code ([#611](https://github.com/cedricziel/signaldb/issues/611)) ([22247b0](https://github.com/cedricziel/signaldb/commit/22247b027d77820481d493c081e29f0df4efd6ed))
* refresh skills after iceberg catalog refactoring ([#460](https://github.com/cedricziel/signaldb/issues/460)) ([24bfa8c](https://github.com/cedricziel/signaldb/commit/24bfa8c8281080887cb2e3b7cdc13a357b7d4231))


### Code Refactoring

* consolidate Iceberg crate and rename schema_bridge to catalog ([#310](https://github.com/cedricziel/signaldb/issues/310)) ([571d89e](https://github.com/cedricziel/signaldb/commit/571d89ea45037a40fd701163f519afc130e58a2c))
* **iceberg:** centralize catalog management with CatalogManager ([#459](https://github.com/cedricziel/signaldb/issues/459)) ([730ceba](https://github.com/cedricziel/signaldb/commit/730cebaa994deb84478ad10f6b9a511e50201d7e))
* remove dead connection pooling abstraction from writer ([#311](https://github.com/cedricziel/signaldb/issues/311)) ([57d8e2d](https://github.com/cedricziel/signaldb/commit/57d8e2d58c61aaa7e03b4aec71d4be0640764f16))
* unify Flight data conversion and eliminate double JSON parse ([#308](https://github.com/cedricziel/signaldb/issues/308)) ([b62a081](https://github.com/cedricziel/signaldb/commit/b62a0815782f967d05f748220c51a7ba0a19cd51))


### Continuous Integration

* drop MSRV policy and fix security audit ignores ([#521](https://github.com/cedricziel/signaldb/issues/521)) ([7da71e3](https://github.com/cedricziel/signaldb/commit/7da71e3d78f593a4361f403e2d4be1e426fb8807))

## 0.1.0 (2026-03-02)


### ⚠ BREAKING CHANGES

* Minimum supported Rust version is now 1.85.0

### Features

* add Grafana datasource plugin and Docker infrastructure ([#253](https://github.com/cedricziel/signaldb/issues/253)) ([a95cdfe](https://github.com/cedricziel/signaldb/commit/a95cdfe038e0667bc9b563c3b2f7a8bd7b280069))
* add queue primitives ([#48](https://github.com/cedricziel/signaldb/issues/48)) ([caf4651](https://github.com/cedricziel/signaldb/commit/caf46518c2e7ee574d63617a9210774ed2531739))
* Add schema module with Iceberg integration and DSN-based storage ([#162](https://github.com/cedricziel/signaldb/issues/162)) ([60bbb8d](https://github.com/cedricziel/signaldb/commit/60bbb8d09a5ff63e2114c6383e7650c9dfef0d24))
* **auth:** add tenant ID validation and naming consistency ([#180](https://github.com/cedricziel/signaldb/issues/180)) ([#318](https://github.com/cedricziel/signaldb/issues/318)) ([2c2146a](https://github.com/cedricziel/signaldb/commit/2c2146a579e978842b0af48f2445485d3fb7a1e4))
* **cli:** add HTTP admin API client for TUI ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** add terminal UI with traces, logs, metrics, admin, and dashboard tabs ([#458](https://github.com/cedricziel/signaldb/issues/458)) ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** implement Admin tab with tenant/key/dataset CRUD and confirmations ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** implement Logs tab with Flight SQL query interface ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** implement Metrics tab with sparklines and Flight SQL ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** integrate TUI tabs with help overlay and error handling ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **compactor:** Phase 3 - Retention & Lifecycle Management ([#467](https://github.com/cedricziel/signaldb/issues/467)) ([28acc8d](https://github.com/cedricziel/signaldb/commit/28acc8d215f029fe0b81dcd9b916f29ccdea60d6))
* complete all-signal pipeline (traces, logs, metrics) with producer, transforms, and monolithic discovery fix ([#435](https://github.com/cedricziel/signaldb/issues/435)) ([b973458](https://github.com/cedricziel/signaldb/commit/b9734582edd68436c4ccb3891c3767726a37f433))
* implement Iceberg table writer adapter to replace direct Parquet writes ([#175](https://github.com/cedricziel/signaldb/issues/175)) ([a55cc3d](https://github.com/cedricziel/signaldb/commit/a55cc3dbd06d955ee82d64e002abab588102df04))
* implement multi-tenancy with WAL isolation and authentication ([#243](https://github.com/cedricziel/signaldb/issues/243)) ([9a8945f](https://github.com/cedricziel/signaldb/commit/9a8945f06e871a96f5890e194534ae11ebb1f35b))
* implement trace querying functionality for issue [#6](https://github.com/cedricziel/signaldb/issues/6) ([#186](https://github.com/cedricziel/signaldb/issues/186)) ([ea8d9b4](https://github.com/cedricziel/signaldb/commit/ea8d9b47446cdbb89bb05b0a5c048c023d4dde49))
* integrate cargo-machete for unused dependency detection ([#130](https://github.com/cedricziel/signaldb/issues/130)) ([f305d3b](https://github.com/cedricziel/signaldb/commit/f305d3b9a6923ca2f7eca95ee83ed9002ee7cee1))
* Phase 2 Component Integration with WAL and Flight Services ([#138](https://github.com/cedricziel/signaldb/issues/138)) ([47f4174](https://github.com/cedricziel/signaldb/commit/47f417488c7b0225d031219df94a1d7eb55ff166))
* **querier,writer:** unify table reference format and shared CatalogManager ([#395](https://github.com/cedricziel/signaldb/issues/395)) ([9928f26](https://github.com/cedricziel/signaldb/commit/9928f266766d1de1d2276e5724a27ef29b1128da))
* store instances in catalog ([#105](https://github.com/cedricziel/signaldb/issues/105)) ([6e92a90](https://github.com/cedricziel/signaldb/commit/6e92a9031a20c04658a1060fa2b7733d5e244f0e))


### Bug Fixes

* align Iceberg namespace paths and partition spec (Issue [#185](https://github.com/cedricziel/signaldb/issues/185)) ([#306](https://github.com/cedricziel/signaldb/issues/306)) ([cc7af60](https://github.com/cedricziel/signaldb/commit/cc7af60ad6426eefc0a0de5c628b865306227172))
* preserve OTLP scope/resource metadata and events/links in trace pipeline ([#183](https://github.com/cedricziel/signaldb/issues/183)) ([#307](https://github.com/cedricziel/signaldb/issues/307)) ([dfe04d7](https://github.com/cedricziel/signaldb/commit/dfe04d73d27c0e8820aa8daeed0787d048701865))
* resolve beta channel build failures and add temporary table cleanup ([#179](https://github.com/cedricziel/signaldb/issues/179)) ([d5f48dd](https://github.com/cedricziel/signaldb/commit/d5f48dd69cf1026295a825aea00f847c284ebe18))
* **wal:** implement proper WAL segment cleanup and processed state persistence ([#252](https://github.com/cedricziel/signaldb/issues/252)) ([b3e73ff](https://github.com/cedricziel/signaldb/commit/b3e73ffe84eaa638b75b3c07c8d194801c8fcfe7))
* **writer:** use relative table locations to prevent path duplication in Iceberg ([#436](https://github.com/cedricziel/signaldb/issues/436)) ([3bbc0a6](https://github.com/cedricziel/signaldb/commit/3bbc0a697956067e812b08fca8e0f051667d9c7e))


### Documentation

* refresh skills after iceberg catalog refactoring ([#460](https://github.com/cedricziel/signaldb/issues/460)) ([24bfa8c](https://github.com/cedricziel/signaldb/commit/24bfa8c8281080887cb2e3b7cdc13a357b7d4231))


### Code Refactoring

* consolidate Iceberg crate and rename schema_bridge to catalog ([#310](https://github.com/cedricziel/signaldb/issues/310)) ([571d89e](https://github.com/cedricziel/signaldb/commit/571d89ea45037a40fd701163f519afc130e58a2c))
* **iceberg:** centralize catalog management with CatalogManager ([#459](https://github.com/cedricziel/signaldb/issues/459)) ([730ceba](https://github.com/cedricziel/signaldb/commit/730cebaa994deb84478ad10f6b9a511e50201d7e))
* remove dead connection pooling abstraction from writer ([#311](https://github.com/cedricziel/signaldb/issues/311)) ([57d8e2d](https://github.com/cedricziel/signaldb/commit/57d8e2d58c61aaa7e03b4aec71d4be0640764f16))
* unify Flight data conversion and eliminate double JSON parse ([#308](https://github.com/cedricziel/signaldb/issues/308)) ([b62a081](https://github.com/cedricziel/signaldb/commit/b62a0815782f967d05f748220c51a7ba0a19cd51))
