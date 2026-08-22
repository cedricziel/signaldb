# Changelog

## [0.4.0](https://github.com/cedricziel/signaldb/compare/common-v0.3.0...common-v0.4.0) (2026-08-22)


### ⚠ BREAKING CHANGES

* **common:** signal table metadata now carries a declared sort order and a non-zero default sort order id. Binaries older than this change read the new metadata as an unfamiliar declaration rather than ignoring it, and are unsupported against tables it has touched.

### Features

* **common:** declare a canonical sort order on every signal table ([#1304](https://github.com/cedricziel/signaldb/issues/1304)) ([eedf47f](https://github.com/cedricziel/signaldb/commit/eedf47f4ba44e6782e7871c87ab591c80211434b))
* **compactor:** keep a bounded value sketch so discovery can suggest values ([#1329](https://github.com/cedricziel/signaldb/issues/1329)) ([dd64a3d](https://github.com/cedricziel/signaldb/commit/dd64a3dd8a8846499ac75bea818ba938c6ca9a87))
* **querier:** route trace search through the IR planner behind a rollout switch ([#1391](https://github.com/cedricziel/signaldb/issues/1391)) ([9e68c5d](https://github.com/cedricziel/signaldb/commit/9e68c5df947d5ef378c99297940e4dde0fea8c4d)), closes [#1382](https://github.com/cedricziel/signaldb/issues/1382)
* **query-ir:** add a describe stage and metadata envelope for discovery ([#1309](https://github.com/cedricziel/signaldb/issues/1309)) ([b1d521c](https://github.com/cedricziel/signaldb/commit/b1d521c4151efae251e208a0dc11af08f3d6332f))
* **query-ir:** register a logical timestamp field for metrics and profiles ([#1293](https://github.com/cedricziel/signaldb/issues/1293)) ([256053c](https://github.com/cedricziel/signaldb/commit/256053ce3653c43121827985e13ea6fc7672561a)), closes [#1205](https://github.com/cedricziel/signaldb/issues/1205)
* **router:** serve query discovery from the registry and statistics ([#1312](https://github.com/cedricziel/signaldb/issues/1312)) ([41d2738](https://github.com/cedricziel/signaldb/commit/41d27384df6e90bd9e9731218e084dd27581e20b))
* sort every producer's rows by the declared key and attest it per file ([#1313](https://github.com/cedricziel/signaldb/issues/1313)) ([c667eda](https://github.com/cedricziel/signaldb/commit/c667eda0c05752ff51fb1ad6ba37cf4594455c6f))
* **wal:** frame every record with a length and CRC-32 ([#1294](https://github.com/cedricziel/signaldb/issues/1294)) ([50ab64a](https://github.com/cedricziel/signaldb/commit/50ab64aefb041d471e0668f86255970ab0e12840)), closes [#946](https://github.com/cedricziel/signaldb/issues/946)


### Bug Fixes

* **acceptor:** accept gzip/zstd OTLP/HTTP bodies and cap request size ([#1383](https://github.com/cedricziel/signaldb/issues/1383)) ([f90b163](https://github.com/cedricziel/signaldb/commit/f90b163185cd903d085351359b338f35d668a423))
* **compactor:** bound the rewrite's scan batch so wide rows cannot exhaust the pool ([#1353](https://github.com/cedricziel/signaldb/issues/1353)) ([ad04904](https://github.com/cedricziel/signaldb/commit/ad04904fc264fb3642016de0b4905b727be32cb3))
* **query-ir:** stop an unknown group-by field from answering silently ([#1301](https://github.com/cedricziel/signaldb/issues/1301)) ([b4f8464](https://github.com/cedricziel/signaldb/commit/b4f8464f71192f80d407f81e8bd837efd8fafd79))
* **wal:** close and drop idle WALs instead of holding them forever ([#1341](https://github.com/cedricziel/signaldb/issues/1341)) ([dc9e635](https://github.com/cedricziel/signaldb/commit/dc9e635ba80b5bb46f426062a190844c325bbd78))
* **wal:** reclaim processed segments from the service drain loops ([#1338](https://github.com/cedricziel/signaldb/issues/1338)) ([e2b3da6](https://github.com/cedricziel/signaldb/commit/e2b3da636d773b20457e92e2a9938da68d14b712))
* **writer:** give each tenant its own WAL instead of one global WAL ([#1299](https://github.com/cedricziel/signaldb/issues/1299)) ([830900e](https://github.com/cedricziel/signaldb/commit/830900ebaddf46dff5ac9eb0748d8fb63e7b35b2))
* **writer:** retire Iceberg WAL markers left by writer ids past retention ([#1346](https://github.com/cedricziel/signaldb/issues/1346)) ([61c9f1b](https://github.com/cedricziel/signaldb/commit/61c9f1bc426edc34516922368ddcfe598d1e62f2))


### Performance Improvements

* **querier:** cache Parquet footers for repeated trace lookups ([#1310](https://github.com/cedricziel/signaldb/issues/1310)) ([cb71029](https://github.com/cedricziel/signaldb/commit/cb71029ce24cf0140b5bed920910569fcc893eff))
* **writer:** commit tenants' WAL groups concurrently, not one at a time ([#1344](https://github.com/cedricziel/signaldb/issues/1344)) ([414982c](https://github.com/cedricziel/signaldb/commit/414982cab6c0e243302ead1519ef18f9ae6685e3))


### Code Refactoring

* **common:** deduplicate repeated helper logic ([#1321](https://github.com/cedricziel/signaldb/issues/1321)) ([a12b2dd](https://github.com/cedricziel/signaldb/commit/a12b2ddea0c18a971062d5933f48fbc928525a59))
* **common:** drop the tempo-api dependency ([#1314](https://github.com/cedricziel/signaldb/issues/1314)) ([06e0f82](https://github.com/cedricziel/signaldb/commit/06e0f823a049e35ffc825cd2a433b2d1be976c2c))
* **common:** quality-only cleanup pass ([#1332](https://github.com/cedricziel/signaldb/issues/1332)) ([4fec800](https://github.com/cedricziel/signaldb/commit/4fec800414f9aae4a2b00acc2b1e4b7071482a6e))
* make query-ir and tempo-api standalone, and cover the parser crates ([#1369](https://github.com/cedricziel/signaldb/issues/1369)) ([1a4d78f](https://github.com/cedricziel/signaldb/commit/1a4d78f077616a9c4846cb6c02715b147b5ad1c2))

## [0.3.0](https://github.com/cedricziel/signaldb/compare/common-v0.2.1...common-v0.3.0) (2026-08-17)


### ⚠ BREAKING CHANGES

* **auth:** POST /api/v1/admin/tenants/{id}/api-keys requires a non-empty `scopes` array; bodies without it are rejected.
* **compactor:** [compactor.orphan_cleanup] revalidate_before_delete no longer exists. Note that a leftover key is silently ignored rather than rejected -- the design assumed unknown keys fail config parsing, but neither config struct sets serde(deny_unknown_fields), and adding it is not a safe drive-by because figment's env provider populates the same structs. Documented in the compactor configuration reference; tightening the structs deserves its own change.
* **compactor:** [compactor] min_input_file_size_kb is replaced by max_input_file_size_kb (semantics inverted) and max_files_per_job is removed. No backward-compat alias is provided.

### Features

* add an Errors & Exceptions tab ([#1167](https://github.com/cedricziel/signaldb/issues/1167)) ([79f3749](https://github.com/cedricziel/signaldb/commit/79f374916a8add7aa47abd0c8569e13c560a2d7c))
* **api:** code-first OpenAPI — generate spec + Rust/TS clients from annotations ([#856](https://github.com/cedricziel/signaldb/issues/856)) ([e34fbfb](https://github.com/cedricziel/signaldb/commit/e34fbfbd094034416f78597c59b306975dd97271))
* **auth:** schema:read/schema:write API-key scopes, scopes on every key surface ([#1217](https://github.com/cedricziel/signaldb/issues/1217)) ([34c7a28](https://github.com/cedricziel/signaldb/commit/34c7a28e4e62fad7a05089c1a3543739d6e28450))
* **auth:** tenant:manage API-key scope for the tenant management API ([#1266](https://github.com/cedricziel/signaldb/issues/1266)) ([9dfc193](https://github.com/cedricziel/signaldb/commit/9dfc193a85e813b42f8658bf97cbfd30e3b78f2e))
* **common:** bundled + custom schema registries with a precedence resolver ([#1218](https://github.com/cedricziel/signaldb/issues/1218)) ([41580e6](https://github.com/cedricziel/signaldb/commit/41580e675abba5a5c6fb1cdcbb1ec390dea3a5ac))
* **compactor:** reclaim metadata backlog and enable orphan cleanup by default ([#1008](https://github.com/cedricziel/signaldb/issues/1008)) ([908ea79](https://github.com/cedricziel/signaldb/commit/908ea798e78a6d2dd90396f56e584275e9dfc9b3))
* compute the traces group table on the server, via a scoped IR aggregate ([#1092](https://github.com/cedricziel/signaldb/issues/1092)) ([ec5c284](https://github.com/cedricziel/signaldb/commit/ec5c284cbe57c0ce34da7f295f08502de2493b82))
* DB client spans, query stage spans, compactor job spans ([#906](https://github.com/cedricziel/signaldb/issues/906)) ([04a4c4e](https://github.com/cedricziel/signaldb/commit/04a4c4e5788cf6531e0421b50b523b04ac4db38b))
* **iceberg:** tune the Parquet writer properties now that they are honored ([#1025](https://github.com/cedricziel/signaldb/issues/1025)) ([219132a](https://github.com/cedricziel/signaldb/commit/219132a3eb1bba1c15975245081ad4a2d54eb7d1))
* **mcp-admin-tool-parity:** platform-admin and tenant self-management tool/CLI parity ([#1261](https://github.com/cedricziel/signaldb/issues/1261)) ([1eadc72](https://github.com/cedricziel/signaldb/commit/1eadc728ace70aff10fa01aaa8766012ace2df4c))
* **mcp:** audit, trace, meter, and bound every tool call ([#1255](https://github.com/cedricziel/signaldb/issues/1255)) ([6627df0](https://github.com/cedricziel/signaldb/commit/6627df0f3f2fc0cff97692d3e465c23bc640e5c2))
* **mcp:** OAuth 2.1 + DCR connector support for Claude and OpenAI ([#899](https://github.com/cedricziel/signaldb/issues/899)) ([4d0104a](https://github.com/cedricziel/signaldb/commit/4d0104a608ee392e9b25acf686dcd7359fc37631))
* **mcp:** scaffold standalone signaldb-mcp server with bearer auth ([#864](https://github.com/cedricziel/signaldb/issues/864)) ([0affbf5](https://github.com/cedricziel/signaldb/commit/0affbf5e92a87dabe041b7766fb97cd1f639e73c))
* **model:** add span events to the Span model ([#847](https://github.com/cedricziel/signaldb/issues/847)) ([0dbd6e8](https://github.com/cedricziel/signaldb/commit/0dbd6e8a0701cea0ce9e46c4fc9456d1562e7d31))
* native Query IR — versioned structured query surface (query-ir-core) ([#882](https://github.com/cedricziel/signaldb/issues/882)) ([8774ac0](https://github.com/cedricziel/signaldb/commit/8774ac0fbbe4686cb7aa8b0bba73dbc25f185689))
* one signaldb binary with the services as subcommands ([#1204](https://github.com/cedricziel/signaldb/issues/1204)) ([77f3278](https://github.com/cedricziel/signaldb/commit/77f3278ca445ac9b28bf955b0e482d4366a27c07))
* **otel-native-schema:** Layer 2 logical schema foundation ([#1104](https://github.com/cedricziel/signaldb/issues/1104)) ([af66060](https://github.com/cedricziel/signaldb/commit/af6606016430645693a0d524d3f15d9db4a52ead))
* **querier,router:** surface span events on the single-trace path ([#848](https://github.com/cedricziel/signaldb/issues/848)) ([5b344e9](https://github.com/cedricziel/signaldb/commit/5b344e98b6e787aeca35d68bf18ca5ca92657454))
* **query-ir:** add v2 heatmaps ([#1102](https://github.com/cedricziel/signaldb/issues/1102)) ([96184cf](https://github.com/cedricziel/signaldb/commit/96184cf42809a4cbf0e4a15f592cb544dbb7a597))
* **query-ir:** address whole attribute containers by OTel scope ([#1283](https://github.com/cedricziel/signaldb/issues/1283)) ([ddec28e](https://github.com/cedricziel/signaldb/commit/ddec28ee4b4ba191b329765392b9942690c65d14))
* **query-ir:** flamegraph result envelope for profiles ([#1144](https://github.com/cedricziel/signaldb/issues/1144)) ([394407f](https://github.com/cedricziel/signaldb/commit/394407f72756b15c97cb6ce6efcf01ce0b61b33b))
* **query-ir:** histogram_quantile stage over metrics_histogram ([#1141](https://github.com/cedricziel/signaldb/issues/1141)) ([591efe7](https://github.com/cedricziel/signaldb/commit/591efe752bbd5bc4b3e460c950fcba287cfab5b8))
* **query-ir:** span_events field returns a span's events list ([#1281](https://github.com/cedricziel/signaldb/issues/1281)) ([dafc3e9](https://github.com/cedricziel/signaldb/commit/dafc3e932b1aa9840a3eb9d9fbec9f4f2c730da0))
* record Flight query failures as span exceptions + surface reasons ([#846](https://github.com/cedricziel/signaldb/issues/846)) ([20d89f5](https://github.com/cedricziel/signaldb/commit/20d89f51eee05ff25ddfa523053dad7ebc8ea6e2))
* return server trace context and timings on HTTP responses (Server-Timing + traceresponse) ([#918](https://github.com/cedricziel/signaldb/issues/918)) ([453dd20](https://github.com/cedricziel/signaldb/commit/453dd2050eee95f3daf1c96f77e56964e99a2bb1))
* **router:** schema registry API under /api/v1/schema ([#1219](https://github.com/cedricziel/signaldb/issues/1219)) ([71af424](https://github.com/cedricziel/signaldb/commit/71af424a0d96eb3f87198af4c4213bb89106cf28))
* **schema-model:** vendored OTel semconv + Weaver-model parser/resolver/validator ([#1215](https://github.com/cedricziel/signaldb/issues/1215)) ([e278456](https://github.com/cedricziel/signaldb/commit/e278456ea6b6f93473176ba61bddbfa2e6b3ed43))
* **schema:** fold metrics and profiles into schemas.toml ([#1237](https://github.com/cedricziel/signaldb/issues/1237)) ([b340c92](https://github.com/cedricziel/signaldb/commit/b340c92004fce17586db476d96882d04d4c707b0))
* **schema:** size and extend trace_id/span_id bloom filters ([#1045](https://github.com/cedricziel/signaldb/issues/1045)) ([2e0e352](https://github.com/cedricziel/signaldb/commit/2e0e352db80701185fe8fb4f467f2931e25ee0c8))
* **self-monitoring:** heap self-profiling as OTLP profiles ([#840](https://github.com/cedricziel/signaldb/issues/840)) ([31fb7f1](https://github.com/cedricziel/signaldb/commit/31fb7f1f12fbfb8315f76efe62215c5c1b0cc575))
* **self-monitoring:** name HTTP server spans per OTel semantic conventions ([#844](https://github.com/cedricziel/signaldb/issues/844)) ([4815f7e](https://github.com/cedricziel/signaldb/commit/4815f7ecac36a56ef1869b6fd41ad0c015331bc1))
* **self-monitoring:** runtime-configurable browser telemetry export ([#842](https://github.com/cedricziel/signaldb/issues/842)) ([343b928](https://github.com/cedricziel/signaldb/commit/343b92877d1291406de25923e671ab2a54a98028))
* **self-monitoring:** version the SignalDB schema URL with the release ([#1275](https://github.com/cedricziel/signaldb/issues/1275)) ([5fa05df](https://github.com/cedricziel/signaldb/commit/5fa05dfb6b9eedeec37aaeb866f8af223a707795))
* semconv CLIENT spans on Flight call sites ([#905](https://github.com/cedricziel/signaldb/issues/905)) ([3047cbb](https://github.com/cedricziel/signaldb/commit/3047cbbc68f03e7d586d4a2caabaa2bd7c660ca1))
* semconv registry, weaver live-check harness, ops docs ([#908](https://github.com/cedricziel/signaldb/issues/908)) ([05f4c52](https://github.com/cedricziel/signaldb/commit/05f4c52fb89d82c3c0dd0321425cad6736652f34))
* semconv self-tracing foundations (resource, span factories, acceptor boundary) ([#903](https://github.com/cedricziel/signaldb/issues/903)) ([dbe4ca2](https://github.com/cedricziel/signaldb/commit/dbe4ca2389ac8db0dba721f66d79db4d0475ed76))
* signal rate-limit throttling with Retry-After and a generous default burst ([#1256](https://github.com/cedricziel/signaldb/issues/1256)) ([5584f3f](https://github.com/cedricziel/signaldb/commit/5584f3f1ef7461401a7f1bbbf24302308192b43d))
* source-agnostic tenant registry (admin-API tenants queryable without restart) ([#853](https://github.com/cedricziel/signaldb/issues/853)) ([c685935](https://github.com/cedricziel/signaldb/commit/c6859353a739fefcdc45f56cc0c7899193a6086a))
* span.kind facet + TraceQL support ([#1125](https://github.com/cedricziel/signaldb/issues/1125)) ([35735e5](https://github.com/cedricziel/signaldb/commit/35735e5d204b4fb9f89ddce1dd15296bf9ddfe3c))
* **tenant-table-listing:** list tenant tables from the Iceberg catalog ([#1267](https://github.com/cedricziel/signaldb/issues/1267)) ([5a444c2](https://github.com/cedricziel/signaldb/commit/5a444c261eeab5643d5d2d866385c07e2772ceee))
* **tracing:** add server.address and network.peer to RPC spans ([#1111](https://github.com/cedricziel/signaldb/issues/1111)) ([4e64934](https://github.com/cedricziel/signaldb/commit/4e64934814762c25226a3a7529bc9d695035d578))
* **ui:** add user menu and management pages ([#1105](https://github.com/cedricziel/signaldb/issues/1105)) ([c49a93f](https://github.com/cedricziel/signaldb/commit/c49a93ff5d112ce36335c19b12ac3404cdb4a8ba))
* **ui:** native Profiles tab + Catalog/Traces UX improvements ([#1164](https://github.com/cedricziel/signaldb/issues/1164)) ([a9d9223](https://github.com/cedricziel/signaldb/commit/a9d9223aa67a419b924513e961cc61d9ac6c97f5))
* **writer:** ack ingest on WAL flush, commit to Iceberg asynchronously ([#893](https://github.com/cedricziel/signaldb/issues/893)) ([fffdbb1](https://github.com/cedricziel/signaldb/commit/fffdbb109c48893bb2725a8afd3e2e740968a152))
* **writer:** bound Iceberg metadata growth via delete-after-commit ([#895](https://github.com/cedricziel/signaldb/issues/895)) ([35ce5c7](https://github.com/cedricziel/signaldb/commit/35ce5c7aa18aa4f12d3e62c4f34221c849f973f3))
* **writer:** coalesce Iceberg commits with a per-table floor + force-commit primitive ([#891](https://github.com/cedricziel/signaldb/issues/891)) ([ad47bb6](https://github.com/cedricziel/signaldb/commit/ad47bb6867dd5cf622701b5778ef9f94e7b60923))
* zero-config first boot — auto-provision default tenant and print API key once ([#995](https://github.com/cedricziel/signaldb/issues/995)) ([5116c8d](https://github.com/cedricziel/signaldb/commit/5116c8d9f22950447373f74c99b17488900db00d)), closes [#796](https://github.com/cedricziel/signaldb/issues/796)


### Bug Fixes

* **acceptor:** dead-letter poison WAL entries on first failure ([#1059](https://github.com/cedricziel/signaldb/issues/1059)) ([9d43c85](https://github.com/cedricziel/signaldb/commit/9d43c85445cb8c6d1bcb19279e29015680dc3fd4))
* **acceptor:** dead-letter writer-rejected WAL entries instead of wedging the retry pass ([#1063](https://github.com/cedricziel/signaldb/issues/1063)) ([7fc6ada](https://github.com/cedricziel/signaldb/commit/7fc6ada1ea922784220789f304fb3f8448ff8ef1)), closes [#1060](https://github.com/cedricziel/signaldb/issues/1060)
* **acceptor:** reject exports on OTLP conversion failure instead of ACKing empty batches ([#926](https://github.com/cedricziel/signaldb/issues/926)) ([#981](https://github.com/cedricziel/signaldb/issues/981)) ([02c0a3b](https://github.com/cedricziel/signaldb/commit/02c0a3b99fdc1327595ad8a0bf8434de1977615d))
* address CodeRabbit review on the tenant registry ([#853](https://github.com/cedricziel/signaldb/issues/853) follow-up) ([#855](https://github.com/cedricziel/signaldb/issues/855)) ([d5011ec](https://github.com/cedricziel/signaldb/commit/d5011ecc4a6101c8a51d5944a9480dff8b19d6a8))
* **catalog:** enable WAL journaling on SQLite catalogs to stop metrics-ingest stalls ([#858](https://github.com/cedricziel/signaldb/issues/858)) ([9865762](https://github.com/cedricziel/signaldb/commit/9865762f259d4f7841e2b8f48e46355f67de5c5d))
* **common,router:** include every known dataset in the tables grouping ([#1269](https://github.com/cedricziel/signaldb/issues/1269)) ([a895618](https://github.com/cedricziel/signaldb/commit/a8956181e5fd7f4cb91432d5f9622175708d2d70))
* **common:** make `cargo test -p common` compile on its own ([#1087](https://github.com/cedricziel/signaldb/issues/1087)) ([baac410](https://github.com/cedricziel/signaldb/commit/baac410ac8e46c0d4f97e9d75e42e09a390598a6))
* **common:** resolve a tenant's default dataset even without a dataset row ([#1082](https://github.com/cedricziel/signaldb/issues/1082)) ([055733f](https://github.com/cedricziel/signaldb/commit/055733f7e2d0e016091a987836fab2e788540e82))
* **compactor:** bound the rewrite's DataFusion fan-out ([#1067](https://github.com/cedricziel/signaldb/issues/1067)) ([9fc7dde](https://github.com/cedricziel/signaldb/commit/9fc7ddeea7497ce4e63fac2f60b11d77d66c621c)), closes [#1064](https://github.com/cedricziel/signaldb/issues/1064)
* **compactor:** cover profiles in retention, snapshot expiration, and orphan cleanup ([#1021](https://github.com/cedricziel/signaldb/issues/1021)) ([3bcc644](https://github.com/cedricziel/signaldb/commit/3bcc644438874392d75e4f048fa6380614a4e935)), closes [#1014](https://github.com/cedricziel/signaldb/issues/1014)
* **compactor:** decline partitions whose inputs exceed the job budget ([#1069](https://github.com/cedricziel/signaldb/issues/1069)) ([8373ff7](https://github.com/cedricziel/signaldb/commit/8373ff71195a3dedcd11e650a39410bff4fdfe1e))
* **compactor:** derive orphan live-file set from retained snapshots, not snapshot age ([#1007](https://github.com/cedricziel/signaldb/issues/1007)) ([8835c71](https://github.com/cedricziel/signaldb/commit/8835c71335333247d7215f839f7c62d510c3453a))
* **compactor:** finish the partition-scoped lifecycle rework ([#1091](https://github.com/cedricziel/signaldb/issues/1091)) ([1f38df8](https://github.com/cedricziel/signaldb/commit/1f38df867abdac033b2504a6acea031467d7fafe))
* **compactor:** re-validate unconditionally before deleting orphans ([#1020](https://github.com/cedricziel/signaldb/issues/1020)) ([5634ab8](https://github.com/cedricziel/signaldb/commit/5634ab820f68d3ed8e24dc4e45ae120dadd15b3b))
* **compactor:** select small files for compaction via max input size ([#934](https://github.com/cedricziel/signaldb/issues/934)) ([#975](https://github.com/cedricziel/signaldb/issues/975)) ([2ea86f8](https://github.com/cedricziel/signaldb/commit/2ea86f875d87be703d552844faaa9734ee0e7b2a))
* **compactor:** use a FairSpillPool for compaction and queries ([#1068](https://github.com/cedricziel/signaldb/issues/1068)) ([6b7bd13](https://github.com/cedricziel/signaldb/commit/6b7bd1368ac4444f785be14b8c29d92629295ee2))
* **conversion:** clamp span duration to zero when end &lt; start ([#927](https://github.com/cedricziel/signaldb/issues/927)) ([#978](https://github.com/cedricziel/signaldb/issues/978)) ([71ad488](https://github.com/cedricziel/signaldb/commit/71ad488aa43fa195592d9a8c9e89f2827dfe92ca))
* **flight:** server.address double-port bug + ops do_action tracing gap ([#1116](https://github.com/cedricziel/signaldb/issues/1116)) ([73e778f](https://github.com/cedricziel/signaldb/commit/73e778f0d936931c86545bfc8722ac7a7403e0e9))
* **flight:** set explicit gRPC message-size limits and chunk oversized batches ([#990](https://github.com/cedricziel/signaldb/issues/990)) ([6499175](https://github.com/cedricziel/signaldb/commit/6499175d0e6402e1350ad28803d0b08954e43fe1))
* **flight:** stop the client timeout from masking the querier's query deadline ([#919](https://github.com/cedricziel/signaldb/issues/919)) ([46eee38](https://github.com/cedricziel/signaldb/commit/46eee382468bfd6a5f3c34f8404379e55d68a690))
* **iceberg:** backfill metadata pruning properties on pre-existing tables ([#973](https://github.com/cedricziel/signaldb/issues/973)) ([f40fce2](https://github.com/cedricziel/signaldb/commit/f40fce2db23f5e8af79b5fac03e70dd3f2a4ad7b)), closes [#959](https://github.com/cedricziel/signaldb/issues/959)
* **iceberg:** pass S3 storage config explicitly instead of mutating process env ([#948](https://github.com/cedricziel/signaldb/issues/948)) ([#988](https://github.com/cedricziel/signaldb/issues/988)) ([06af739](https://github.com/cedricziel/signaldb/commit/06af73969d302c36be46b90f521ef18688cbecf3))
* **logql:** carry log and resource attributes as structured metadata ([#1094](https://github.com/cedricziel/signaldb/issues/1094)) ([26b9d15](https://github.com/cedricziel/signaldb/commit/26b9d15457ac84c96ba2affe28d3ea520b40c664))
* **mcp:** add connect and request timeouts to router HTTP client ([#885](https://github.com/cedricziel/signaldb/issues/885)) ([#976](https://github.com/cedricziel/signaldb/issues/976)) ([f0f2182](https://github.com/cedricziel/signaldb/commit/f0f21824b654d57668e2c235f310d3a048a314f4))
* metrics without service.name land as 'unknown'; boot log flood demoted to debug ([#1227](https://github.com/cedricziel/signaldb/issues/1227)) ([7b5ea34](https://github.com/cedricziel/signaldb/commit/7b5ea343096ea8a7c0f62575029ac1e838ec514c))
* **metrics:** carry NaN/±Inf values through the wire format instead of dead-lettering ([#1239](https://github.com/cedricziel/signaldb/issues/1239)) ([9e38b3a](https://github.com/cedricziel/signaldb/commit/9e38b3a993b6d632d7c67f498f2f489ea97e6636)), closes [#1061](https://github.com/cedricziel/signaldb/issues/1061)
* **model:** stop flattening trace hierarchies to root + direct children ([#1018](https://github.com/cedricziel/signaldb/issues/1018)) ([5fee337](https://github.com/cedricziel/signaldb/commit/5fee33711628bf3f041c436c34f363f114ed93fb))
* **otlp:** preserve bytes and profiles interned values ([#1099](https://github.com/cedricziel/signaldb/issues/1099)) ([5e132ba](https://github.com/cedricziel/signaldb/commit/5e132bac9cc2335962be9418360a3c124ed0c409))
* provision signal tables for every registered dataset, and read an absent one as empty ([#1074](https://github.com/cedricziel/signaldb/issues/1074)) ([9a50ffa](https://github.com/cedricziel/signaldb/commit/9a50ffaa7e404a96cb80d7d3b0cc0850ede00f49))
* **query-ir:** correct histogram_quantile rate-mode series grouping ([#1142](https://github.com/cedricziel/signaldb/issues/1142)) ([6f66d04](https://github.com/cedricziel/signaldb/commit/6f66d0435f552a99ca5776fccbe2f7b52cf02fb6))
* restore compactor discovery and WAL pending-gauge accuracy ([#1049](https://github.com/cedricziel/signaldb/issues/1049)) ([b9254b0](https://github.com/cedricziel/signaldb/commit/b9254b065430b092978c2ba8f2e59ec1d3c1ceb8))
* **router:** materialize a tenant's default dataset as a real row ([#1085](https://github.com/cedricziel/signaldb/issues/1085)) ([9443244](https://github.com/cedricziel/signaldb/commit/94432445328a0489bfd0476aaaba12ba937a2561))
* **router:** write the tenant and its default dataset in one transaction ([#1086](https://github.com/cedricziel/signaldb/issues/1086)) ([59bdc70](https://github.com/cedricziel/signaldb/commit/59bdc705d8fddc8253d55466904f59f8f0493060))
* **self-monitoring:** default to parent-based trace sampler ([#843](https://github.com/cedricziel/signaldb/issues/843)) ([d6c12b1](https://github.com/cedricziel/signaldb/commit/d6c12b1aeb060a8438f857451bda61ee0d8828b9))
* **self-monitoring:** drop signaldb.* prefix from process/system gauges ([#1212](https://github.com/cedricziel/signaldb/issues/1212)) ([aee6733](https://github.com/cedricziel/signaldb/commit/aee6733bb4006762c740744445be7508d993a427))
* **self-monitoring:** stop emitting non-semconv bridge attributes on spans ([#967](https://github.com/cedricziel/signaldb/issues/967)) ([0b82ef4](https://github.com/cedricziel/signaldb/commit/0b82ef4256936e30462e946d62d9452ab1155e5c))
* **telemetry:** emit int-typed registry attributes as i64 ([#1013](https://github.com/cedricziel/signaldb/issues/1013)) ([be67718](https://github.com/cedricziel/signaldb/commit/be677184819e5cbe700d253a03e59cd2bffa7ba8))
* **traces:** span_kind/status_code numeric source of truth + schema evolution engine ([#1235](https://github.com/cedricziel/signaldb/issues/1235)) ([0f8603b](https://github.com/cedricziel/signaldb/commit/0f8603bdb1f39254c83af0c631653a65c8a85e3f))
* **ui:** catalog multi-source discovery, trace routing fix, span_kind fix ([#1210](https://github.com/cedricziel/signaldb/issues/1210)) ([84446f2](https://github.com/cedricziel/signaldb/commit/84446f2ef450be67fb14dbb6c4b4feb477ea0d04))
* **ui:** route Metrics builder default queries through Query IR ([#1138](https://github.com/cedricziel/signaldb/issues/1138)) ([4056261](https://github.com/cedricziel/signaldb/commit/4056261e0d406d5ae73dc2fe20bc136b8e866bb8))
* **wal:** bounds-check entry range before reading segment data ([#871](https://github.com/cedricziel/signaldb/issues/871)) ([bc36a94](https://github.com/cedricziel/signaldb/commit/bc36a9493c04ba0f285c05119a54f64ef7e82da5))
* **wal:** carry tenant/dataset/signal on WAL failure telemetry ([#866](https://github.com/cedricziel/signaldb/issues/866)) ([a023dbb](https://github.com/cedricziel/signaldb/commit/a023dbb54822964d44f7c22864391eb2af957a58))
* **wal:** offset-authoritative writes + data-size rotation ([#865](https://github.com/cedricziel/signaldb/issues/865)) ([#883](https://github.com/cedricziel/signaldb/issues/883)) ([31be2cf](https://github.com/cedricziel/signaldb/commit/31be2cfe46f67c56a479fb4b65b1dc5f4412414d))
* **wal:** skip and quarantine corrupt entries on replay instead of aborting ([#1093](https://github.com/cedricziel/signaldb/issues/1093)) ([a14db18](https://github.com/cedricziel/signaldb/commit/a14db18d0b0aee7dcb9113f5a49d0e27a8d47eef)), closes [#1033](https://github.com/cedricziel/signaldb/issues/1033)
* **writer:** derive flush scope from request metadata, not the action body ([#897](https://github.com/cedricziel/signaldb/issues/897)) ([cd94186](https://github.com/cedricziel/signaldb/commit/cd9418653c1f90812ffee4a0688dd947039dbbeb))


### Performance Improvements

* **flight,wal:** compress Flight IPC payloads and WAL entries ([#945](https://github.com/cedricziel/signaldb/issues/945)) ([#998](https://github.com/cedricziel/signaldb/issues/998)) ([efb5ef4](https://github.com/cedricziel/signaldb/commit/efb5ef4bc85e2e77483f4546255b50c564015827))
* **flight:** skip redundant discovery lookup and memoize capability discovery ([#940](https://github.com/cedricziel/signaldb/issues/940)) ([#989](https://github.com/cedricziel/signaldb/issues/989)) ([a4720ca](https://github.com/cedricziel/signaldb/commit/a4720ca5d9dc5f541c2dc814cb533934cb023c14))
* **iceberg:** stop carrying useless column bounds in every manifest entry ([#1023](https://github.com/cedricziel/signaldb/issues/1023)) ([3a77a4e](https://github.com/cedricziel/signaldb/commit/3a77a4e513808ae9299e8bf93579e2dbb26b9977))
* **querier:** enable statistics-based file grouping and Parquet filter pushdown ([#937](https://github.com/cedricziel/signaldb/issues/937)) ([#987](https://github.com/cedricziel/signaldb/issues/987)) ([7d4aefb](https://github.com/cedricziel/signaldb/commit/7d4aefb855061ea2a07c6536eee28385a49a6722))
* **wal:** batch index persistence in mark_processed_many ([#943](https://github.com/cedricziel/signaldb/issues/943)) ([#984](https://github.com/cedricziel/signaldb/issues/984)) ([41a91cd](https://github.com/cedricziel/signaldb/commit/41a91cd4938286a39c120e642f0b11261b813ab7))
* **wal:** index entries by id to kill O(n) SipHash scans in the WAL loop ([#1148](https://github.com/cedricziel/signaldb/issues/1148)) ([5d91b5d](https://github.com/cedricziel/signaldb/commit/5d91b5d21bc9d051400db5d70fa3d14edadf71b5)), closes [#1112](https://github.com/cedricziel/signaldb/issues/1112)


### Documentation

* flight-communication.md read path now describes the CLIENT hop. ([3047cbb](https://github.com/cedricziel/signaldb/commit/3047cbbc68f03e7d586d4a2caabaa2bd7c660ca1))


### Code Refactoring

* **common:** simplify pass ([#1169](https://github.com/cedricziel/signaldb/issues/1169)) ([45e455f](https://github.com/cedricziel/signaldb/commit/45e455fdfb27c65fa4f6da5f2d58c559b7feb71d))
* **compactor:** partition-scoped compaction with delta commits ([#1017](https://github.com/cedricziel/signaldb/issues/1017)) ([52dc957](https://github.com/cedricziel/signaldb/commit/52dc9572a10378d6d69f653d1a78a4cf4d2f1407))
* **flight:** decode Flight data dictionary-aware ([#1004](https://github.com/cedricziel/signaldb/issues/1004)) ([94a7a30](https://github.com/cedricziel/signaldb/commit/94a7a30edd81060f2bfc5147dbf3b53307d2de72))
* **iceberg:** configure the catalog pool instead of working around it ([#1024](https://github.com/cedricziel/signaldb/issues/1024)) ([68be19f](https://github.com/cedricziel/signaldb/commit/68be19f7327fc2660a6b45df26c29084fee6ce42))
* **logging:** forbid log:: macros in favor of tracing:: ([#1006](https://github.com/cedricziel/signaldb/issues/1006)) ([071ebb4](https://github.com/cedricziel/signaldb/commit/071ebb47d02f2d6e43ccfb60380c00e3be929248))
* simplify backend workspace (dedup, dead code, redundant clones) ([#1168](https://github.com/cedricziel/signaldb/issues/1168)) ([409b778](https://github.com/cedricziel/signaldb/commit/409b778686a1cea5c54edfba7778c3e9ed3aa29c))
* span hygiene sweep and construction guard ([#907](https://github.com/cedricziel/signaldb/issues/907)) ([c1f7b81](https://github.com/cedricziel/signaldb/commit/c1f7b81fbc00ae5fd6c9b948f9fb35c9d5a27d26))


### Tests

* add performance benchmark suite (write + read critical paths) ([#879](https://github.com/cedricziel/signaldb/issues/879)) ([149c5e1](https://github.com/cedricziel/signaldb/commit/149c5e19952c88e442d594b281ee9befbe4929d1))
* back provisioning tests with a file catalog, not a named memory one ([#1088](https://github.com/cedricziel/signaldb/issues/1088)) ([718b73d](https://github.com/cedricziel/signaldb/commit/718b73df827980e7f40856eb19addacfe4b1b4b8)), closes [#1083](https://github.com/cedricziel/signaldb/issues/1083)
* delete tautological tests and rewrite salvageable ones as contract tests ([#961](https://github.com/cedricziel/signaldb/issues/961)) ([b3e884a](https://github.com/cedricziel/signaldb/commit/b3e884ad59b4df853429133d5eef2724a8adcada))
* make tests assert what their names promise ([#966](https://github.com/cedricziel/signaldb/issues/966)) ([446ed06](https://github.com/cedricziel/signaldb/commit/446ed062a7480902ef391884b1c2e12f77ddd66f))
* pin the in-memory catalog so provisioning tests can't race ([#1083](https://github.com/cedricziel/signaldb/issues/1083)) ([218ff2a](https://github.com/cedricziel/signaldb/commit/218ff2a8db3d4bf37d9cdc53e163dee03f382fda))
* polish medium/low audit findings across the workspace ([#969](https://github.com/cedricziel/signaldb/issues/969)) ([8962f6d](https://github.com/cedricziel/signaldb/commit/8962f6d1d22c8a176d4a1d99376d61b42b1da258))
* replace sleep-based synchronization with deterministic waits ([#968](https://github.com/cedricziel/signaldb/issues/968)) ([6391326](https://github.com/cedricziel/signaldb/commit/6391326013c8620f186e4a63c2cdf3bbdf9ee963))
* **self-monitoring:** pin span-event bridge path against code.module.name ([#956](https://github.com/cedricziel/signaldb/issues/956)) ([#986](https://github.com/cedricziel/signaldb/issues/986)) ([c88e403](https://github.com/cedricziel/signaldb/commit/c88e40385868979350a29fb33a6dd2bcbf9839b5))
* **wal:** concurrency round-trip guard for entry byte-integrity ([#868](https://github.com/cedricziel/signaldb/issues/868)) ([0eab821](https://github.com/cedricziel/signaldb/commit/0eab8218443b458239a8a0fa456f68e5a67ea7dd))


### Continuous Integration

* declare the testing feature on the test that needs it ([#1090](https://github.com/cedricziel/signaldb/issues/1090)) ([96aed27](https://github.com/cedricziel/signaldb/commit/96aed27918c1c950080980416b3b30a508b010af)), closes [#1089](https://github.com/cedricziel/signaldb/issues/1089)

## [0.2.1](https://github.com/cedricziel/signaldb/compare/common-v0.2.0...common-v0.2.1) (2026-07-30)


### Features

* CPU self-profiling + Profiles tab in the Explore UI ([#835](https://github.com/cedricziel/signaldb/issues/835)) ([9434734](https://github.com/cedricziel/signaldb/commit/94347345da14db950c760db21ad8516f9fcbac92))
* **router:** join external callers' traces at the HTTP query boundary ([#838](https://github.com/cedricziel/signaldb/issues/838)) ([2818eb2](https://github.com/cedricziel/signaldb/commit/2818eb246ed7bdd5764bcf0294542b35d5473c8d))
* **writer:** propagate trace context through the WAL write path ([#836](https://github.com/cedricziel/signaldb/issues/836)) ([455c58f](https://github.com/cedricziel/signaldb/commit/455c58f79a3329449a66d9e5004f22203ad30c2c))


### Bug Fixes

* **common:** exclude pyroscope on Windows to unblock release build ([#823](https://github.com/cedricziel/signaldb/issues/823)) ([a5647de](https://github.com/cedricziel/signaldb/commit/a5647de9738160104f5efcb6213a49c8fd62c624))
* **traces:** read stored attributes and correct OTLP status mapping ([#829](https://github.com/cedricziel/signaldb/issues/829)) ([31fb70e](https://github.com/cedricziel/signaldb/commit/31fb70ed291b884ce36ae2a7b180ec6127c1bfc7))


### Performance Improvements

* **traces:** enable trace_id/span_id Parquet bloom filters ([#832](https://github.com/cedricziel/signaldb/issues/832)) ([19dc400](https://github.com/cedricziel/signaldb/commit/19dc4005215f882ac1d0fe7776cd00c2fd1a67e3))

## [0.2.0](https://github.com/cedricziel/signaldb/compare/common-v0.1.0...common-v0.2.0) (2026-07-30)


### ⚠ BREAKING CHANGES

* **compactor:** upgraded deployments running with default configuration will start deleting data older than 30 days. Operators who want infinite retention must set [compactor.retention].enabled = false (or configure longer durations).
* **heraclitus:** Minimum supported Rust version is now 1.86.0
* Minimum supported Rust version is now 1.85.0

### Features

* **acceptor:** add Prometheus remote_write handler ([d5bb53d](https://github.com/cedricziel/signaldb/commit/d5bb53d5a301d3993cb0e374af86282f807c350c))
* **acceptor:** add Prometheus remote_write ingestion endpoint ([#342](https://github.com/cedricziel/signaldb/issues/342)) ([d5bb53d](https://github.com/cedricziel/signaldb/commit/d5bb53d5a301d3993cb0e374af86282f807c350c))
* **acceptor:** OTLP profiles ingestion over gRPC and HTTP ([#636](https://github.com/cedricziel/signaldb/issues/636)) ([b0cde70](https://github.com/cedricziel/signaldb/commit/b0cde70c008dfa1c13b9a83402bb404b5d29818b)), closes [#349](https://github.com/cedricziel/signaldb/issues/349) [#350](https://github.com/cedricziel/signaldb/issues/350)
* **acceptor:** per-tenant ingest rate limits (requests/sec + bytes/sec) ([#594](https://github.com/cedricziel/signaldb/issues/594)) ([448e165](https://github.com/cedricziel/signaldb/commit/448e16562e350379221e761637e3af83b0db2330))
* add Grafana datasource plugin and Docker infrastructure ([#253](https://github.com/cedricziel/signaldb/issues/253)) ([a95cdfe](https://github.com/cedricziel/signaldb/commit/a95cdfe038e0667bc9b563c3b2f7a8bd7b280069))
* Add schema module with Iceberg integration and DSN-based storage ([#162](https://github.com/cedricziel/signaldb/issues/162)) ([60bbb8d](https://github.com/cedricziel/signaldb/commit/60bbb8d09a5ff63e2114c6383e7650c9dfef0d24))
* add tenant management admin API with OpenAPI spec, SDK, and CLI ([#313](https://github.com/cedricziel/signaldb/issues/313)) ([880c86b](https://github.com/cedricziel/signaldb/commit/880c86b6405a162c84fe88615b7d363585948abd))
* attr_tokens key=value column with bloom for arbitrary attribute equality ([#777](https://github.com/cedricziel/signaldb/issues/777)) ([b305438](https://github.com/cedricziel/signaldb/commit/b30543823d4f1d20f489c1b4c097d2fe7c448fe0))
* **auth:** add human user sessions ([629cf78](https://github.com/cedricziel/signaldb/commit/629cf78741e971bd2b3644f1d0eb08ae2f44feb3))
* **auth:** add scoped tenant self-service ([7830c3d](https://github.com/cedricziel/signaldb/commit/7830c3d706c21480f9767bca8639e5fcb82622bc))
* **auth:** add tenant ID validation and naming consistency ([#180](https://github.com/cedricziel/signaldb/issues/180)) ([#318](https://github.com/cedricziel/signaldb/issues/318)) ([2c2146a](https://github.com/cedricziel/signaldb/commit/2c2146a579e978842b0af48f2445485d3fb7a1e4))
* **auth:** per-tenant query rate limits and API key/dataset quotas ([#609](https://github.com/cedricziel/signaldb/issues/609)) ([f2ae3e9](https://github.com/cedricziel/signaldb/commit/f2ae3e955f05fde7511c344211c3d1613b6a86e9))
* **cli:** add HTTP admin API client for TUI ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** add terminal UI with traces, logs, metrics, admin, and dashboard tabs ([#458](https://github.com/cedricziel/signaldb/issues/458)) ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** implement Admin tab with tenant/key/dataset CRUD and confirmations ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** implement Logs tab with Flight SQL query interface ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** implement Metrics tab with sparklines and Flight SQL ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** integrate TUI tabs with help overlay and error handling ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **common:** add password hashing and session token utilities ([#789](https://github.com/cedricziel/signaldb/issues/789)) ([26d74f4](https://github.com/cedricziel/signaldb/commit/26d74f46b312b7e4ecb474f70b1bdb35994e4935))
* **common:** add user, tenant membership, and session catalog tables ([#790](https://github.com/cedricziel/signaldb/issues/790)) ([587cae7](https://github.com/cedricziel/signaldb/commit/587cae7ca138b47d13244bcf0eab8ce0af87f7c8))
* **common:** convert OTLP profiles to Arrow ([#623](https://github.com/cedricziel/signaldb/issues/623)) ([d8a9ad1](https://github.com/cedricziel/signaldb/commit/d8a9ad1eae55f1db34db50dc6a7821ac8200587a)), closes [#347](https://github.com/cedricziel/signaldb/issues/347)
* **common:** enable OTLP profiles proto types ([#618](https://github.com/cedricziel/signaldb/issues/618)) ([2683f39](https://github.com/cedricziel/signaldb/commit/2683f39473cffdf660d007bbdcd95e36aa315f23)), closes [#344](https://github.com/cedricziel/signaldb/issues/344)
* **common:** Iceberg schema-evolution helper for label columns ([#783](https://github.com/cedricziel/signaldb/issues/783)) ([e9a43d1](https://github.com/cedricziel/signaldb/commit/e9a43d1d5f6e5062fe2684f266f17af30d8f5012))
* **common:** implement flamegraph aggregation ([#639](https://github.com/cedricziel/signaldb/issues/639)) ([80562a8](https://github.com/cedricziel/signaldb/commit/80562a823724c9f44870ed97da2eb6304da60478)), closes [#356](https://github.com/cedricziel/signaldb/issues/356)
* **common:** Parquet bloom filters on materialized label columns ([#776](https://github.com/cedricziel/signaldb/issues/776)) ([cb8e55a](https://github.com/cedricziel/signaldb/commit/cb8e55a49c469b0086f97621fe1798d2e06a7b32))
* **common:** profile data model and Flight schema ([#619](https://github.com/cedricziel/signaldb/issues/619)) ([11b0b8a](https://github.com/cedricziel/signaldb/commit/11b0b8a7b56ce30847f3a038498b7c3baa8fd1af)), closes [#345](https://github.com/cedricziel/signaldb/issues/345)
* **compactor, querier:** persist attribute stats and query demand ([#753](https://github.com/cedricziel/signaldb/issues/753)) ([3419bd9](https://github.com/cedricziel/signaldb/commit/3419bd98505c2e61c18991fac94965ac3425422c))
* **compactor:** attribute auto-promotion decision engine (dry-run) ([#756](https://github.com/cedricziel/signaldb/issues/756)) ([51c5411](https://github.com/cedricziel/signaldb/commit/51c5411fb16e92b384a347575c2a25849b189d8b))
* **compactor:** complete epic [#432](https://github.com/cedricziel/signaldb/issues/432) — real compaction, multi-instance tests, observability ([#540](https://github.com/cedricziel/signaldb/issues/540)) ([ed95e20](https://github.com/cedricziel/signaldb/commit/ed95e2062a05b7386d05188c89a754a3606fc428))
* **compactor:** demote unqueried label columns at rewrite ([#785](https://github.com/cedricziel/signaldb/issues/785)) ([d76c5eb](https://github.com/cedricziel/signaldb/commit/d76c5ebdc26d1217b9da6f2ca281a3c8ed96bae0))
* **compactor:** enable compaction and 30d retention by default ([#767](https://github.com/cedricziel/signaldb/issues/767)) ([77e2f81](https://github.com/cedricziel/signaldb/commit/77e2f81fa8aa58d0cfda3a2c06b99fceaeeffdc6))
* **compactor:** Phase 1 - Dry-run compaction planner ([#462](https://github.com/cedricziel/signaldb/issues/462)) ([a0ad75f](https://github.com/cedricziel/signaldb/commit/a0ad75f5478be94786d77e732a1b8db319ae8650))
* **compactor:** Phase 3 - Retention & Lifecycle Management ([#467](https://github.com/cedricziel/signaldb/issues/467)) ([28acc8d](https://github.com/cedricziel/signaldb/commit/28acc8d215f029fe0b81dcd9b916f29ccdea60d6))
* **compactor:** Phase 4 — multi-instance safety (leases, round-robin, Flight endpoints) ([e9acbc2](https://github.com/cedricziel/signaldb/commit/e9acbc28ac75898fc1d9bd4fd866665b0ea076a5))
* complete OTLP trace events and links conversion implementation ([#131](https://github.com/cedricziel/signaldb/issues/131)) ([3ad7f9a](https://github.com/cedricziel/signaldb/commit/3ad7f9ab0b3288c4ce1bac288d6a4b1377e8a794)), closes [#98](https://github.com/cedricziel/signaldb/issues/98)
* **config:** per-tenant materialized-label allowlists ([#745](https://github.com/cedricziel/signaldb/issues/745)) ([41205f9](https://github.com/cedricziel/signaldb/commit/41205f95c8d039b618699d2018a29ee7a95d09aa))
* **discovery:** TTL-filter stale services, reap crashed nodes, round-robin routing ([#600](https://github.com/cedricziel/signaldb/issues/600)) ([6aad9dc](https://github.com/cedricziel/signaldb/commit/6aad9dccbb2120442da5e80cf15f113e0c3d662b))
* embedded UI session auth + tenant-scoped whoami ([#773](https://github.com/cedricziel/signaldb/issues/773)) ([f217064](https://github.com/cedricziel/signaldb/commit/f217064d3f31002132761040bc8a82fe1c5e9c59))
* end-to-end local development experience with CLI query support ([#434](https://github.com/cedricziel/signaldb/issues/434)) ([b95fb15](https://github.com/cedricziel/signaldb/commit/b95fb1595e33dd825f3c4424a88b966dded4808e))
* **flight:** authenticate Flight ports via internal service key ([#579](https://github.com/cedricziel/signaldb/issues/579)) ([da1b41f](https://github.com/cedricziel/signaldb/commit/da1b41f4698ce9f58348239d789a1678e23353b3)), closes [#544](https://github.com/cedricziel/signaldb/issues/544)
* **flight:** close out Flight port authentication ([#544](https://github.com/cedricziel/signaldb/issues/544)) ([#589](https://github.com/cedricziel/signaldb/issues/589)) ([f8a7b43](https://github.com/cedricziel/signaldb/commit/f8a7b43722fa0024e2b7c01b2243bb9329420f6c))
* **heraclitus:** add Kafka-compatible server with Apache Arrow/Parquet storage ([#212](https://github.com/cedricziel/signaldb/issues/212)) ([8ad74df](https://github.com/cedricziel/signaldb/commit/8ad74df27ab246816a7871ad55d87d32dfac954b))
* **iceberg:** profiles table schema and config toggle ([#633](https://github.com/cedricziel/signaldb/issues/633)) ([9203530](https://github.com/cedricziel/signaldb/commit/920353022c1a58c5ee667954d3356bb7d481836f)), closes [#351](https://github.com/cedricziel/signaldb/issues/351)
* implement configurable schemas and tenant management API for SignalDB ([#167](https://github.com/cedricziel/signaldb/issues/167)) ([efe6e09](https://github.com/cedricziel/signaldb/commit/efe6e0952b392ae795232bd05829fe13aaaa10cc))
* implement Iceberg table writer adapter to replace direct Parquet writes ([#175](https://github.com/cedricziel/signaldb/issues/175)) ([a55cc3d](https://github.com/cedricziel/signaldb/commit/a55cc3dbd06d955ee82d64e002abab588102df04))
* implement multi-tenancy with WAL isolation and authentication ([#243](https://github.com/cedricziel/signaldb/issues/243)) ([9a8945f](https://github.com/cedricziel/signaldb/commit/9a8945f06e871a96f5890e194534ae11ebb1f35b))
* implement service catalog-aware Flight transport ([#134](https://github.com/cedricziel/signaldb/issues/134)) ([eebe2b9](https://github.com/cedricziel/signaldb/commit/eebe2b9caa0bb833a7003f581eb9d047c0ab3533))
* implement trace querying functionality for issue [#6](https://github.com/cedricziel/signaldb/issues/6) ([#186](https://github.com/cedricziel/signaldb/issues/186)) ([ea8d9b4](https://github.com/cedricziel/signaldb/commit/ea8d9b47446cdbb89bb05b0a5c048c023d4dde49))
* integrate cargo-husky for pre-commit git hooks ([#150](https://github.com/cedricziel/signaldb/issues/150)) ([7a0d6e5](https://github.com/cedricziel/signaldb/commit/7a0d6e572f231d69a0464ca04a78cbc51c7b93ad))
* integrate cargo-machete for unused dependency detection ([#130](https://github.com/cedricziel/signaldb/issues/130)) ([f305d3b](https://github.com/cedricziel/signaldb/commit/f305d3b9a6923ca2f7eca95ee83ed9002ee7cee1))
* **logs:** typed Map attribute columns — exact matching for every label ([#741](https://github.com/cedricziel/signaldb/issues/741)) ([c362536](https://github.com/cedricziel/signaldb/commit/c362536555c62de8186a2c8bd3b4f959c2c252dd))
* **metrics, profiles:** materialize configured labels ([#728](https://github.com/cedricziel/signaldb/issues/728)) ([a20caaf](https://github.com/cedricziel/signaldb/commit/a20caaf15340cecbfb9e0973bbc84d5e93329a60))
* **metrics, profiles:** typed Map attribute columns ([#743](https://github.com/cedricziel/signaldb/issues/743)) ([f169ee6](https://github.com/cedricziel/signaldb/commit/f169ee6ae3337034dbb2222fe661a75e7da39677))
* Phase 2 Component Integration with WAL and Flight Services ([#138](https://github.com/cedricziel/signaldb/issues/138)) ([47f4174](https://github.com/cedricziel/signaldb/commit/47f417488c7b0225d031219df94a1d7eb55ff166))
* **profiles:** link profiles to traces across the query surface ([#645](https://github.com/cedricziel/signaldb/issues/645)) ([5430d27](https://github.com/cedricziel/signaldb/commit/5430d27281a66a9d88dea0e8d450f73902307137)), closes [#362](https://github.com/cedricziel/signaldb/issues/362) [#363](https://github.com/cedricziel/signaldb/issues/363)
* **querier,writer:** unify table reference format and shared CatalogManager ([#395](https://github.com/cedricziel/signaldb/issues/395)) ([9928f26](https://github.com/cedricziel/signaldb/commit/9928f266766d1de1d2276e5724a27ef29b1128da))
* **querier:** add profile diff and flamegraph queries ([#641](https://github.com/cedricziel/signaldb/issues/641)) ([a55cddf](https://github.com/cedricziel/signaldb/commit/a55cddfec4c4fb7a8952795b0bc4af0f62cb9439)), closes [#357](https://github.com/cedricziel/signaldb/issues/357)
* **querier:** enforce resource limits on query execution ([#593](https://github.com/cedricziel/signaldb/issues/593)) ([b1c6341](https://github.com/cedricziel/signaldb/commit/b1c634157d4b669df81224242c21a4e05938fca5))
* **querier:** per-tenant concurrent-query cap ([#595](https://github.com/cedricziel/signaldb/issues/595)) ([ae2c628](https://github.com/cedricziel/signaldb/commit/ae2c6289b0d30af9a636d45120bb30d7e716828f))
* **quotas:** per-tenant storage quotas backed by Iceberg usage accounting ([#634](https://github.com/cedricziel/signaldb/issues/634)) ([38a77dc](https://github.com/cedricziel/signaldb/commit/38a77dca6e3474fd148ad28eeca6f4bdfd59ae75))
* **schema:** add materialized-labels config and column-name helper ([#723](https://github.com/cedricziel/signaldb/issues/723)) ([8c213f0](https://github.com/cedricziel/signaldb/commit/8c213f05ced5ecf9b64e7457fff06690c6156bae))
* **schema:** materialize configured labels as logs table columns ([#724](https://github.com/cedricziel/signaldb/issues/724)) ([24d3d7e](https://github.com/cedricziel/signaldb/commit/24d3d7e56b302be9faabd36cff8afa87aba38843))
* **self-monitoring:** epic [#447](https://github.com/cedricziel/signaldb/issues/447) — SignalDB observes itself (dogfooding) ([#542](https://github.com/cedricziel/signaldb/issues/542)) ([e6d7b1f](https://github.com/cedricziel/signaldb/commit/e6d7b1fc37f370f534d8780b3a6fe5d180b1ad65))
* **traces:** materialize configured labels for trace search ([#727](https://github.com/cedricziel/signaldb/issues/727)) ([4ef9584](https://github.com/cedricziel/signaldb/commit/4ef9584f514bcb7ae77e9e95b19a1e91f6ee8073))
* use in-memory SQLite as default for service discovery catalog ([#136](https://github.com/cedricziel/signaldb/issues/136)) ([3aeaa22](https://github.com/cedricziel/signaldb/commit/3aeaa22ec89b21528ad311b73648a4cd840c1ced))
* **wal:** add WriteProfiles operation and per-signal profiles WAL ([#632](https://github.com/cedricziel/signaldb/issues/632)) ([9a938af](https://github.com/cedricziel/signaldb/commit/9a938af0a213ace259e2c5e6ca1d16123ecdc99e)), closes [#348](https://github.com/cedricziel/signaldb/issues/348)
* **writer:** persist profiles to the Iceberg profiles table ([#637](https://github.com/cedricziel/signaldb/issues/637)) ([5dedbdc](https://github.com/cedricziel/signaldb/commit/5dedbdcbba5080071f859c964ca88ac808685e7e)), closes [#353](https://github.com/cedricziel/signaldb/issues/353)


### Bug Fixes

* align Iceberg namespace paths and partition spec (Issue [#185](https://github.com/cedricziel/signaldb/issues/185)) ([#306](https://github.com/cedricziel/signaldb/issues/306)) ([cc7af60](https://github.com/cedricziel/signaldb/commit/cc7af60ad6426eefc0a0de5c628b865306227172))
* **auth:** return 401 for missing credentials instead of 400 ([#775](https://github.com/cedricziel/signaldb/issues/775)) ([33c768c](https://github.com/cedricziel/signaldb/commit/33c768c1082688d1ac525c19f1612b928cab5a03))
* **catalog:** replace unwrap() with proper error propagation for datetime parsing ([#315](https://github.com/cedricziel/signaldb/issues/315)) ([89725c2](https://github.com/cedricziel/signaldb/commit/89725c294ecad6e1095b26984cc036d1b8a40e2a))
* **ci:** resolve clippy 1.97 lints, security advisories, and ethnum build failure ([#516](https://github.com/cedricziel/signaldb/issues/516)) ([b21c459](https://github.com/cedricziel/signaldb/commit/b21c4596f361d14dad147447cc19da4156fb81da))
* **common:** create file:// storage directories and normalize relative DSNs ([#770](https://github.com/cedricziel/signaldb/issues/770)) ([896f7e2](https://github.com/cedricziel/signaldb/commit/896f7e23a14a841a85d4fa9ef9cd712082615f19))
* **compactor:** renew leases during long compactions and use the DB clock ([#603](https://github.com/cedricziel/signaldb/issues/603)) ([4a1ead2](https://github.com/cedricziel/signaldb/commit/4a1ead2de48102f42d98f5cec289694b61fbf69e))
* **config:** refuse in-memory discovery/catalog in standalone services ([#599](https://github.com/cedricziel/signaldb/issues/599)) ([c8413ba](https://github.com/cedricziel/signaldb/commit/c8413babe5de5346477bf4d1ff26a7f2fef380bb))
* **config:** remove the dead [auth] enabled flag ([#601](https://github.com/cedricziel/signaldb/issues/601)) ([e9d0780](https://github.com/cedricziel/signaldb/commit/e9d07805ff7d9260fadb9f57cdecd6c8d357a628))
* correct environment variable parsing using double underscore separator ([#128](https://github.com/cedricziel/signaldb/issues/128)) ([2b731a5](https://github.com/cedricziel/signaldb/commit/2b731a5bb3007d0b84fb172a2d939bbbd4fd0cb7))
* **iceberg:** load fresh table metadata in ensure_table instead of caching handles ([#606](https://github.com/cedricziel/signaldb/issues/606)) ([4539084](https://github.com/cedricziel/signaldb/commit/4539084cb5d1886edfacb000d3d93afbe584a67e)), closes [#537](https://github.com/cedricziel/signaldb/issues/537)
* integration tests ([#123](https://github.com/cedricziel/signaldb/issues/123)) ([0ab5eeb](https://github.com/cedricziel/signaldb/commit/0ab5eeb9a0637483efaae88175ab9648c9dd2fb9))
* **metrics:** ingest remote-write v2 native histograms as exponential histograms ([#764](https://github.com/cedricziel/signaldb/issues/764)) ([18562c3](https://github.com/cedricziel/signaldb/commit/18562c3f9fdc7666b38c232b816903419595eda6)), closes [#747](https://github.com/cedricziel/signaldb/issues/747)
* preserve OTLP scope/resource metadata and events/links in trace pipeline ([#183](https://github.com/cedricziel/signaldb/issues/183)) ([#307](https://github.com/cedricziel/signaldb/issues/307)) ([dfe04d7](https://github.com/cedricziel/signaldb/commit/dfe04d73d27c0e8820aa8daeed0787d048701865))
* **prometheus:** improve target_info generation logic ([d5bb53d](https://github.com/cedricziel/signaldb/commit/d5bb53d5a301d3993cb0e374af86282f807c350c))
* propagate attributes through query path and implement trace search ([#309](https://github.com/cedricziel/signaldb/issues/309)) ([98244d8](https://github.com/cedricziel/signaldb/commit/98244d8f9ebfe623ea07ab240356511acbde5d07))
* resolve beta channel build failures and add temporary table cleanup ([#179](https://github.com/cedricziel/signaldb/issues/179)) ([d5f48dd](https://github.com/cedricziel/signaldb/commit/d5f48dd69cf1026295a825aea00f847c284ebe18))
* **self-monitoring:** extend anti-loop guard to writer, querier, and router ([#765](https://github.com/cedricziel/signaldb/issues/765)) ([ec1ea04](https://github.com/cedricziel/signaldb/commit/ec1ea04be358a6b0c4f7452d758e9a6a0e7c8136))
* **self-monitoring:** report process-scoped memory/cpu, add system-level gauges ([#762](https://github.com/cedricziel/signaldb/issues/762)) ([1ca447e](https://github.com/cedricziel/signaldb/commit/1ca447eb9f98994da2dcb18c9fda76cf0678e040)), closes [#759](https://github.com/cedricziel/signaldb/issues/759)
* test_iceberg_sql_catalog_basic_operations by removing unimplemented namespace operations ([#251](https://github.com/cedricziel/signaldb/issues/251)) ([98de7ed](https://github.com/cedricziel/signaldb/commit/98de7ed43a3663ef6670cc8bb3c25e37b11832f6))
* **wal:** fsync segments on flush, close, and index save ([#576](https://github.com/cedricziel/signaldb/issues/576)) ([da69589](https://github.com/cedricziel/signaldb/commit/da695899d9d4f057682da9983390268edb279cc4)), closes [#545](https://github.com/cedricziel/signaldb/issues/545)
* **wal:** honor [wal].wal_dir for acceptor and writer WAL directories ([#758](https://github.com/cedricziel/signaldb/issues/758)) ([d4bc621](https://github.com/cedricziel/signaldb/commit/d4bc621bd1725202c37369d6a373359e664a0cc7))
* **wal:** implement proper WAL segment cleanup and processed state persistence ([#252](https://github.com/cedricziel/signaldb/issues/252)) ([b3e73ff](https://github.com/cedricziel/signaldb/commit/b3e73ffe84eaa638b75b3c07c8d194801c8fcfe7))
* **wal:** preserve sealed segments on rotation and read across all segments ([#573](https://github.com/cedricziel/signaldb/issues/573)) ([b2749c2](https://github.com/cedricziel/signaldb/commit/b2749c2714bdbe8afded582cfa091cfea8804550)), closes [#547](https://github.com/cedricziel/signaldb/issues/547) [#548](https://github.com/cedricziel/signaldb/issues/548)
* **writer:** harden the write path against panics and silent task death ([#605](https://github.com/cedricziel/signaldb/issues/605)) ([ca716db](https://github.com/cedricziel/signaldb/commit/ca716dbc6b2321a4eb838ff3d8031b69e1ec6075))
* **writer:** idempotent WAL-to-Iceberg commits — no duplicate rows on crash replay ([#592](https://github.com/cedricziel/signaldb/issues/592)) ([c43437b](https://github.com/cedricziel/signaldb/commit/c43437b16b4bdd575f84565fa9b0fdd40d969291))


### Documentation

* add comprehensive per-component README documentation ([#151](https://github.com/cedricziel/signaldb/issues/151)) ([5ea02a2](https://github.com/cedricziel/signaldb/commit/5ea02a2dd660bc03639a3a82c0146f18c147ecfb)), closes [#139](https://github.com/cedricziel/signaldb/issues/139)
* **common:** correct database env var to SIGNALDB_DATABASE_DSN ([#541](https://github.com/cedricziel/signaldb/issues/541)) ([0246ff7](https://github.com/cedricziel/signaldb/commit/0246ff74ddf544cd6967c9cb0430c60e5a5c6374)), closes [#125](https://github.com/cedricziel/signaldb/issues/125)
* full staleness sweep — match all docs, skills, and READMEs to current code ([#611](https://github.com/cedricziel/signaldb/issues/611)) ([22247b0](https://github.com/cedricziel/signaldb/commit/22247b027d77820481d493c081e29f0df4efd6ed))
* refresh skills after iceberg catalog refactoring ([#460](https://github.com/cedricziel/signaldb/issues/460)) ([24bfa8c](https://github.com/cedricziel/signaldb/commit/24bfa8c8281080887cb2e3b7cdc13a357b7d4231))


### Code Refactoring

* consolidate Iceberg crate and rename schema_bridge to catalog ([#310](https://github.com/cedricziel/signaldb/issues/310)) ([571d89e](https://github.com/cedricziel/signaldb/commit/571d89ea45037a40fd701163f519afc130e58a2c))
* extract Heraclitus to separate repository ([#240](https://github.com/cedricziel/signaldb/issues/240)) ([f0bfcec](https://github.com/cedricziel/signaldb/commit/f0bfcec7e26fbda82270b6ead696ec84ebde41e1))
* **iceberg:** centralize catalog management with CatalogManager ([#459](https://github.com/cedricziel/signaldb/issues/459)) ([730ceba](https://github.com/cedricziel/signaldb/commit/730cebaa994deb84478ad10f6b9a511e50201d7e))
* remove obsolete NATS-based discovery infrastructure ([#132](https://github.com/cedricziel/signaldb/issues/132)) ([1e0fc55](https://github.com/cedricziel/signaldb/commit/1e0fc55b8b7657fd14163dc45caa508ecb0af355))


### Continuous Integration

* drop MSRV policy and fix security audit ignores ([#521](https://github.com/cedricziel/signaldb/issues/521)) ([7da71e3](https://github.com/cedricziel/signaldb/commit/7da71e3d78f593a4361f403e2d4be1e426fb8807))

## 0.1.0 (2026-03-02)


### ⚠ BREAKING CHANGES

* **heraclitus:** Minimum supported Rust version is now 1.86.0
* Minimum supported Rust version is now 1.85.0

### Features

* **acceptor:** add Prometheus remote_write handler ([d5bb53d](https://github.com/cedricziel/signaldb/commit/d5bb53d5a301d3993cb0e374af86282f807c350c))
* **acceptor:** add Prometheus remote_write ingestion endpoint ([#342](https://github.com/cedricziel/signaldb/issues/342)) ([d5bb53d](https://github.com/cedricziel/signaldb/commit/d5bb53d5a301d3993cb0e374af86282f807c350c))
* add global config ([#32](https://github.com/cedricziel/signaldb/issues/32)) ([fbb9a40](https://github.com/cedricziel/signaldb/commit/fbb9a407d45ae8f606334fc4154caee7ae4a12d9))
* add Grafana datasource plugin and Docker infrastructure ([#253](https://github.com/cedricziel/signaldb/issues/253)) ([a95cdfe](https://github.com/cedricziel/signaldb/commit/a95cdfe038e0667bc9b563c3b2f7a8bd7b280069))
* add infrastructure for querying traces ([#7](https://github.com/cedricziel/signaldb/issues/7)) ([0417c9f](https://github.com/cedricziel/signaldb/commit/0417c9ffea7187fb40160217ae8b8ab78c43d12c))
* add querier ([0417c9f](https://github.com/cedricziel/signaldb/commit/0417c9ffea7187fb40160217ae8b8ab78c43d12c))
* add queue primitives ([#48](https://github.com/cedricziel/signaldb/issues/48)) ([caf4651](https://github.com/cedricziel/signaldb/commit/caf46518c2e7ee574d63617a9210774ed2531739))
* Add schema module with Iceberg integration and DSN-based storage ([#162](https://github.com/cedricziel/signaldb/issues/162)) ([60bbb8d](https://github.com/cedricziel/signaldb/commit/60bbb8d09a5ff63e2114c6383e7650c9dfef0d24))
* add schemas for metrics, logs, traces ([#97](https://github.com/cedricziel/signaldb/issues/97)) ([1569d73](https://github.com/cedricziel/signaldb/commit/1569d73ec09cf68ca8745a5ba107b15d763c970b))
* add single trace api endpoint ([0417c9f](https://github.com/cedricziel/signaldb/commit/0417c9ffea7187fb40160217ae8b8ab78c43d12c))
* add tenant management admin API with OpenAPI spec, SDK, and CLI ([#313](https://github.com/cedricziel/signaldb/issues/313)) ([880c86b](https://github.com/cedricziel/signaldb/commit/880c86b6405a162c84fe88615b7d363585948abd))
* **auth:** add tenant ID validation and naming consistency ([#180](https://github.com/cedricziel/signaldb/issues/180)) ([#318](https://github.com/cedricziel/signaldb/issues/318)) ([2c2146a](https://github.com/cedricziel/signaldb/commit/2c2146a579e978842b0af48f2445485d3fb7a1e4))
* **cli:** add HTTP admin API client for TUI ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** add terminal UI with traces, logs, metrics, admin, and dashboard tabs ([#458](https://github.com/cedricziel/signaldb/issues/458)) ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** implement Admin tab with tenant/key/dataset CRUD and confirmations ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** implement Logs tab with Flight SQL query interface ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** implement Metrics tab with sparklines and Flight SQL ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** integrate TUI tabs with help overlay and error handling ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **compactor:** Phase 1 - Dry-run compaction planner ([#462](https://github.com/cedricziel/signaldb/issues/462)) ([a0ad75f](https://github.com/cedricziel/signaldb/commit/a0ad75f5478be94786d77e732a1b8db319ae8650))
* **compactor:** Phase 3 - Retention & Lifecycle Management ([#467](https://github.com/cedricziel/signaldb/issues/467)) ([28acc8d](https://github.com/cedricziel/signaldb/commit/28acc8d215f029fe0b81dcd9b916f29ccdea60d6))
* **compactor:** Phase 4 — multi-instance safety (leases, round-robin, Flight endpoints) ([e9acbc2](https://github.com/cedricziel/signaldb/commit/e9acbc28ac75898fc1d9bd4fd866665b0ea076a5))
* compile tempopb ([0417c9f](https://github.com/cedricziel/signaldb/commit/0417c9ffea7187fb40160217ae8b8ab78c43d12c))
* complete OTLP trace events and links conversion implementation ([#131](https://github.com/cedricziel/signaldb/issues/131)) ([3ad7f9a](https://github.com/cedricziel/signaldb/commit/3ad7f9ab0b3288c4ce1bac288d6a4b1377e8a794)), closes [#98](https://github.com/cedricziel/signaldb/issues/98)
* convert arrow &lt;&gt; otlp ([#99](https://github.com/cedricziel/signaldb/issues/99)) ([ba65d14](https://github.com/cedricziel/signaldb/commit/ba65d144173d2dbeee22011ded650e834df4f5c9))
* create simple write path ([#4](https://github.com/cedricziel/signaldb/issues/4)) ([8ce08ba](https://github.com/cedricziel/signaldb/commit/8ce08ba53b8499c90bba270b2f9cd8e6c5e18c3f))
* end-to-end local development experience with CLI query support ([#434](https://github.com/cedricziel/signaldb/issues/434)) ([b95fb15](https://github.com/cedricziel/signaldb/commit/b95fb1595e33dd825f3c4424a88b966dded4808e))
* generate client + server stubs ([0417c9f](https://github.com/cedricziel/signaldb/commit/0417c9ffea7187fb40160217ae8b8ab78c43d12c))
* **heraclitus:** add Kafka-compatible server with Apache Arrow/Parquet storage ([#212](https://github.com/cedricziel/signaldb/issues/212)) ([8ad74df](https://github.com/cedricziel/signaldb/commit/8ad74df27ab246816a7871ad55d87d32dfac954b))
* implement configurable schemas and tenant management API for SignalDB ([#167](https://github.com/cedricziel/signaldb/issues/167)) ([efe6e09](https://github.com/cedricziel/signaldb/commit/efe6e0952b392ae795232bd05829fe13aaaa10cc))
* implement Iceberg table writer adapter to replace direct Parquet writes ([#175](https://github.com/cedricziel/signaldb/issues/175)) ([a55cc3d](https://github.com/cedricziel/signaldb/commit/a55cc3dbd06d955ee82d64e002abab588102df04))
* implement multi-tenancy with WAL isolation and authentication ([#243](https://github.com/cedricziel/signaldb/issues/243)) ([9a8945f](https://github.com/cedricziel/signaldb/commit/9a8945f06e871a96f5890e194534ae11ebb1f35b))
* implement service catalog-aware Flight transport ([#134](https://github.com/cedricziel/signaldb/issues/134)) ([eebe2b9](https://github.com/cedricziel/signaldb/commit/eebe2b9caa0bb833a7003f581eb9d047c0ab3533))
* implement trace querying functionality for issue [#6](https://github.com/cedricziel/signaldb/issues/6) ([#186](https://github.com/cedricziel/signaldb/issues/186)) ([ea8d9b4](https://github.com/cedricziel/signaldb/commit/ea8d9b47446cdbb89bb05b0a5c048c023d4dde49))
* integrate cargo-husky for pre-commit git hooks ([#150](https://github.com/cedricziel/signaldb/issues/150)) ([7a0d6e5](https://github.com/cedricziel/signaldb/commit/7a0d6e572f231d69a0464ca04a78cbc51c7b93ad))
* integrate cargo-machete for unused dependency detection ([#130](https://github.com/cedricziel/signaldb/issues/130)) ([f305d3b](https://github.com/cedricziel/signaldb/commit/f305d3b9a6923ca2f7eca95ee83ed9002ee7cee1))
* Phase 2 Component Integration with WAL and Flight Services ([#138](https://github.com/cedricziel/signaldb/issues/138)) ([47f4174](https://github.com/cedricziel/signaldb/commit/47f417488c7b0225d031219df94a1d7eb55ff166))
* **querier,writer:** unify table reference format and shared CatalogManager ([#395](https://github.com/cedricziel/signaldb/issues/395)) ([9928f26](https://github.com/cedricziel/signaldb/commit/9928f266766d1de1d2276e5724a27ef29b1128da))
* separate tempo-api into client and server ([0417c9f](https://github.com/cedricziel/signaldb/commit/0417c9ffea7187fb40160217ae8b8ab78c43d12c))
* store instances in catalog ([#105](https://github.com/cedricziel/signaldb/issues/105)) ([6e92a90](https://github.com/cedricziel/signaldb/commit/6e92a9031a20c04658a1060fa2b7733d5e244f0e))
* use in-memory SQLite as default for service discovery catalog ([#136](https://github.com/cedricziel/signaldb/issues/136)) ([3aeaa22](https://github.com/cedricziel/signaldb/commit/3aeaa22ec89b21528ad311b73648a4cd840c1ced))


### Bug Fixes

* adapt exports to tempopb ([0417c9f](https://github.com/cedricziel/signaldb/commit/0417c9ffea7187fb40160217ae8b8ab78c43d12c))
* align Iceberg namespace paths and partition spec (Issue [#185](https://github.com/cedricziel/signaldb/issues/185)) ([#306](https://github.com/cedricziel/signaldb/issues/306)) ([cc7af60](https://github.com/cedricziel/signaldb/commit/cc7af60ad6426eefc0a0de5c628b865306227172))
* **catalog:** replace unwrap() with proper error propagation for datetime parsing ([#315](https://github.com/cedricziel/signaldb/issues/315)) ([89725c2](https://github.com/cedricziel/signaldb/commit/89725c294ecad6e1095b26984cc036d1b8a40e2a))
* correct environment variable parsing using double underscore separator ([#128](https://github.com/cedricziel/signaldb/issues/128)) ([2b731a5](https://github.com/cedricziel/signaldb/commit/2b731a5bb3007d0b84fb172a2d939bbbd4fd0cb7))
* correctly build tempopb ([0417c9f](https://github.com/cedricziel/signaldb/commit/0417c9ffea7187fb40160217ae8b8ab78c43d12c))
* integration tests ([#123](https://github.com/cedricziel/signaldb/issues/123)) ([0ab5eeb](https://github.com/cedricziel/signaldb/commit/0ab5eeb9a0637483efaae88175ab9648c9dd2fb9))
* preserve OTLP scope/resource metadata and events/links in trace pipeline ([#183](https://github.com/cedricziel/signaldb/issues/183)) ([#307](https://github.com/cedricziel/signaldb/issues/307)) ([dfe04d7](https://github.com/cedricziel/signaldb/commit/dfe04d73d27c0e8820aa8daeed0787d048701865))
* **prometheus:** improve target_info generation logic ([d5bb53d](https://github.com/cedricziel/signaldb/commit/d5bb53d5a301d3993cb0e374af86282f807c350c))
* propagate attributes through query path and implement trace search ([#309](https://github.com/cedricziel/signaldb/issues/309)) ([98244d8](https://github.com/cedricziel/signaldb/commit/98244d8f9ebfe623ea07ab240356511acbde5d07))
* resolve beta channel build failures and add temporary table cleanup ([#179](https://github.com/cedricziel/signaldb/issues/179)) ([d5f48dd](https://github.com/cedricziel/signaldb/commit/d5f48dd69cf1026295a825aea00f847c284ebe18))
* set crates versions ([#21](https://github.com/cedricziel/signaldb/issues/21)) ([2113160](https://github.com/cedricziel/signaldb/commit/21131604ba4d7277f2b9e7f26b103ac03ef1bcaf))
* test_iceberg_sql_catalog_basic_operations by removing unimplemented namespace operations ([#251](https://github.com/cedricziel/signaldb/issues/251)) ([98de7ed](https://github.com/cedricziel/signaldb/commit/98de7ed43a3663ef6670cc8bb3c25e37b11832f6))
* **wal:** implement proper WAL segment cleanup and processed state persistence ([#252](https://github.com/cedricziel/signaldb/issues/252)) ([b3e73ff](https://github.com/cedricziel/signaldb/commit/b3e73ffe84eaa638b75b3c07c8d194801c8fcfe7))


### Documentation

* add comprehensive per-component README documentation ([#151](https://github.com/cedricziel/signaldb/issues/151)) ([5ea02a2](https://github.com/cedricziel/signaldb/commit/5ea02a2dd660bc03639a3a82c0146f18c147ecfb)), closes [#139](https://github.com/cedricziel/signaldb/issues/139)
* refresh skills after iceberg catalog refactoring ([#460](https://github.com/cedricziel/signaldb/issues/460)) ([24bfa8c](https://github.com/cedricziel/signaldb/commit/24bfa8c8281080887cb2e3b7cdc13a357b7d4231))


### Code Refactoring

* consolidate Iceberg crate and rename schema_bridge to catalog ([#310](https://github.com/cedricziel/signaldb/issues/310)) ([571d89e](https://github.com/cedricziel/signaldb/commit/571d89ea45037a40fd701163f519afc130e58a2c))
* extract Heraclitus to separate repository ([#240](https://github.com/cedricziel/signaldb/issues/240)) ([f0bfcec](https://github.com/cedricziel/signaldb/commit/f0bfcec7e26fbda82270b6ead696ec84ebde41e1))
* **iceberg:** centralize catalog management with CatalogManager ([#459](https://github.com/cedricziel/signaldb/issues/459)) ([730ceba](https://github.com/cedricziel/signaldb/commit/730cebaa994deb84478ad10f6b9a511e50201d7e))
* remove obsolete NATS-based discovery infrastructure ([#132](https://github.com/cedricziel/signaldb/issues/132)) ([1e0fc55](https://github.com/cedricziel/signaldb/commit/1e0fc55b8b7657fd14163dc45caa508ecb0af355))


### Tests

* add unit test for search_result serialization and deserialization ([0417c9f](https://github.com/cedricziel/signaldb/commit/0417c9ffea7187fb40160217ae8b8ab78c43d12c))
