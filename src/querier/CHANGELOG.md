# Changelog

## [0.2.1](https://github.com/cedricziel/signaldb/compare/querier-v0.2.0...querier-v0.2.1) (2026-07-30)


### Bug Fixes

* **traces:** read stored attributes and correct OTLP status mapping ([#829](https://github.com/cedricziel/signaldb/issues/829)) ([31fb70e](https://github.com/cedricziel/signaldb/commit/31fb70ed291b884ce36ae2a7b180ec6127c1bfc7))


### Performance Improvements

* **querier:** prune partitions and project columns on trace-by-id lookup ([#827](https://github.com/cedricziel/signaldb/issues/827)) ([cbe3a3e](https://github.com/cedricziel/signaldb/commit/cbe3a3e1e44c94f479bfcf95d92369103780ac0d))

## [0.2.0](https://github.com/cedricziel/signaldb/compare/querier-v0.1.0...querier-v0.2.0) (2026-07-30)


### ⚠ BREAKING CHANGES

* Minimum supported Rust version is now 1.85.0

### Features

* Add schema module with Iceberg integration and DSN-based storage ([#162](https://github.com/cedricziel/signaldb/issues/162)) ([60bbb8d](https://github.com/cedricziel/signaldb/commit/60bbb8d09a5ff63e2114c6383e7650c9dfef0d24))
* attr_tokens key=value column with bloom for arbitrary attribute equality ([#777](https://github.com/cedricziel/signaldb/issues/777)) ([b305438](https://github.com/cedricziel/signaldb/commit/b30543823d4f1d20f489c1b4c097d2fe7c448fe0))
* **cli:** add HTTP admin API client for TUI ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** add terminal UI with traces, logs, metrics, admin, and dashboard tabs ([#458](https://github.com/cedricziel/signaldb/issues/458)) ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** implement Admin tab with tenant/key/dataset CRUD and confirmations ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** implement Logs tab with Flight SQL query interface ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** implement Metrics tab with sparklines and Flight SQL ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** integrate TUI tabs with help overlay and error handling ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **compactor, querier:** persist attribute stats and query demand ([#753](https://github.com/cedricziel/signaldb/issues/753)) ([3419bd9](https://github.com/cedricziel/signaldb/commit/3419bd98505c2e61c18991fac94965ac3425422c))
* end-to-end local development experience with CLI query support ([#434](https://github.com/cedricziel/signaldb/issues/434)) ([b95fb15](https://github.com/cedricziel/signaldb/commit/b95fb1595e33dd825f3c4424a88b966dded4808e))
* **flight:** authenticate Flight ports via internal service key ([#579](https://github.com/cedricziel/signaldb/issues/579)) ([da1b41f](https://github.com/cedricziel/signaldb/commit/da1b41f4698ce9f58348239d789a1678e23353b3)), closes [#544](https://github.com/cedricziel/signaldb/issues/544)
* implement Iceberg table writer adapter to replace direct Parquet writes ([#175](https://github.com/cedricziel/signaldb/issues/175)) ([a55cc3d](https://github.com/cedricziel/signaldb/commit/a55cc3dbd06d955ee82d64e002abab588102df04))
* implement multi-tenancy with WAL isolation and authentication ([#243](https://github.com/cedricziel/signaldb/issues/243)) ([9a8945f](https://github.com/cedricziel/signaldb/commit/9a8945f06e871a96f5890e194534ae11ebb1f35b))
* implement trace querying functionality for issue [#6](https://github.com/cedricziel/signaldb/issues/6) ([#186](https://github.com/cedricziel/signaldb/issues/186)) ([ea8d9b4](https://github.com/cedricziel/signaldb/commit/ea8d9b47446cdbb89bb05b0a5c048c023d4dde49))
* integrate cargo-machete for unused dependency detection ([#130](https://github.com/cedricziel/signaldb/issues/130)) ([f305d3b](https://github.com/cedricziel/signaldb/commit/f305d3b9a6923ca2f7eca95ee83ed9002ee7cee1))
* **logql:** execute LogQL metric queries end-to-end ([#667](https://github.com/cedricziel/signaldb/issues/667)) ([2fc630d](https://github.com/cedricziel/signaldb/commit/2fc630d34d596d4003b6d148d4ce6b38495dc86b))
* **logql:** group log metric queries by materialized labels ([#740](https://github.com/cedricziel/signaldb/issues/740)) ([139c2e8](https://github.com/cedricziel/signaldb/commit/139c2e85518dc5879e7540b79fae9ea61d6e47d9))
* **logql:** support and/or/unless logical operators ([#718](https://github.com/cedricziel/signaldb/issues/718)) ([fe1e3af](https://github.com/cedricziel/signaldb/commit/fe1e3afbd27438352c67d505e4c34ee21088cc7a))
* **logql:** support avg/min/max/count outer aggregations ([#709](https://github.com/cedricziel/signaldb/issues/709)) ([47c85c0](https://github.com/cedricziel/signaldb/commit/47c85c00fd634d47456a232aca4ba47cd3e44a77))
* **logql:** support first_over_time and last_over_time ([#713](https://github.com/cedricziel/signaldb/issues/713)) ([0f2d500](https://github.com/cedricziel/signaldb/commit/0f2d500e2f3c988a1926d56138fd61b5ca9d20ca))
* **logql:** support label_replace ([#719](https://github.com/cedricziel/signaldb/issues/719)) ([87f9f2d](https://github.com/cedricziel/signaldb/commit/87f9f2def18586c43b25dcd60a29663e20545efc))
* **logql:** support on/ignoring vector matching ([#722](https://github.com/cedricziel/signaldb/issues/722)) ([33492f4](https://github.com/cedricziel/signaldb/commit/33492f46d6f4c7ad8f587c9d4b1f4edd5441a674))
* **logql:** support quantile_over_time ([#711](https://github.com/cedricziel/signaldb/issues/711)) ([02782dc](https://github.com/cedricziel/signaldb/commit/02782dc070fcd9363efdc92d09244dc0973652f1))
* **logql:** support sort and sort_desc ([#720](https://github.com/cedricziel/signaldb/issues/720)) ([7e49148](https://github.com/cedricziel/signaldb/commit/7e491485d802203d3b20a71a4cca3cd1833cfbf6))
* **logql:** support stddev_over_time and stdvar_over_time ([#712](https://github.com/cedricziel/signaldb/issues/712)) ([ff55253](https://github.com/cedricziel/signaldb/commit/ff552534ec53c3177b6f51f65d1b5fb7540858be))
* **logql:** support stddev/stdvar outer aggregations ([#710](https://github.com/cedricziel/signaldb/issues/710)) ([4adc30d](https://github.com/cedricziel/signaldb/commit/4adc30de1c168fc96da37311e804d982d1aa3661))
* **logql:** support topk and bottomk ([#714](https://github.com/cedricziel/signaldb/issues/714)) ([6dda034](https://github.com/cedricziel/signaldb/commit/6dda0341b3b53f7534ae753e372814818c56b9da))
* **logql:** support vector-scalar arithmetic ([#715](https://github.com/cedricziel/signaldb/issues/715)) ([568b2d1](https://github.com/cedricziel/signaldb/commit/568b2d17886f185f4260ff01a35edb81fc7a9191))
* **logql:** support vector-to-vector arithmetic ([#716](https://github.com/cedricziel/signaldb/issues/716)) ([def8a04](https://github.com/cedricziel/signaldb/commit/def8a047b161c8b892fc8f6caea2093e34f90fed))
* **logql:** support vector-to-vector comparisons ([#717](https://github.com/cedricziel/signaldb/issues/717)) ([2d714c0](https://github.com/cedricziel/signaldb/commit/2d714c087f25350859094fc66b08b2ce8ba69b9f))
* **logql:** support vector(N) ([#721](https://github.com/cedricziel/signaldb/issues/721)) ([05969fd](https://github.com/cedricziel/signaldb/commit/05969fda09f7e17202b72c59787680a8341409c3))
* **logs:** end-to-end LogQL log queries (querier service + router) ([#665](https://github.com/cedricziel/signaldb/issues/665)) ([7e77dcf](https://github.com/cedricziel/signaldb/commit/7e77dcff12f7d9a49afe2c40a4104cbe302f1a48))
* **logs:** typed Map attribute columns — exact matching for every label ([#741](https://github.com/cedricziel/signaldb/issues/741)) ([c362536](https://github.com/cedricziel/signaldb/commit/c362536555c62de8186a2c8bd3b4f959c2c252dd))
* **metrics, profiles:** materialize configured labels ([#728](https://github.com/cedricziel/signaldb/issues/728)) ([a20caaf](https://github.com/cedricziel/signaldb/commit/a20caaf15340cecbfb9e0973bbc84d5e93329a60))
* **metrics, profiles:** typed Map attribute columns ([#743](https://github.com/cedricziel/signaldb/issues/743)) ([f169ee6](https://github.com/cedricziel/signaldb/commit/f169ee6ae3337034dbb2222fe661a75e7da39677))
* Phase 2 Component Integration with WAL and Flight Services ([#138](https://github.com/cedricziel/signaldb/issues/138)) ([47f4174](https://github.com/cedricziel/signaldb/commit/47f417488c7b0225d031219df94a1d7eb55ff166))
* **profiles:** link profiles to traces across the query surface ([#645](https://github.com/cedricziel/signaldb/issues/645)) ([5430d27](https://github.com/cedricziel/signaldb/commit/5430d27281a66a9d88dea0e8d450f73902307137)), closes [#362](https://github.com/cedricziel/signaldb/issues/362) [#363](https://github.com/cedricziel/signaldb/issues/363)
* Prometheus remote_write ingestion and PromQL parser ([#382](https://github.com/cedricziel/signaldb/issues/382)) ([d548eb6](https://github.com/cedricziel/signaldb/commit/d548eb65388f254faec795bc44fa17d77d95659b))
* PromQL query support — /prometheus API (epic [#328](https://github.com/cedricziel/signaldb/issues/328)) ([#671](https://github.com/cedricziel/signaldb/issues/671)) ([9fe8264](https://github.com/cedricziel/signaldb/commit/9fe8264b0d2fbb3f785779034a6388da5c0cdd95))
* **promql:** group by materialized labels ([#749](https://github.com/cedricziel/signaldb/issues/749)) ([a5171d1](https://github.com/cedricziel/signaldb/commit/a5171d19bd5d22537eba994865ede407705a3a2a))
* **querier, router:** detected_fields discovery endpoint ([#738](https://github.com/cedricziel/signaldb/issues/738)) ([01bb757](https://github.com/cedricziel/signaldb/commit/01bb757889d4e178cad57a8696c6767a50b26384))
* **querier,writer:** unify table reference format and shared CatalogManager ([#395](https://github.com/cedricziel/signaldb/issues/395)) ([9928f26](https://github.com/cedricziel/signaldb/commit/9928f266766d1de1d2276e5724a27ef29b1128da))
* **querier:** add profile diff and flamegraph queries ([#641](https://github.com/cedricziel/signaldb/issues/641)) ([a55cddf](https://github.com/cedricziel/signaldb/commit/a55cddfec4c4fb7a8952795b0bc4af0f62cb9439)), closes [#357](https://github.com/cedricziel/signaldb/issues/357)
* **querier:** add PromQL to DataFusion query translator ([#383](https://github.com/cedricziel/signaldb/issues/383)) ([9630947](https://github.com/cedricziel/signaldb/commit/9630947c14a1908521326ebc8b9767701c4d9c2a))
* **querier:** add sql_profiles Flight ticket ([#642](https://github.com/cedricziel/signaldb/issues/642)) ([c0685e2](https://github.com/cedricziel/signaldb/commit/c0685e2dd1c06af51ba50ab4fdb471ec4e079747)), closes [#358](https://github.com/cedricziel/signaldb/issues/358)
* **querier:** apply TraceQL and tag filters on trace search — no more silently unfiltered results ([#596](https://github.com/cedricziel/signaldb/issues/596)) ([fb8f0ba](https://github.com/cedricziel/signaldb/commit/fb8f0ba081aee3dcf5f524deec12851d38a2acf5)), closes [#551](https://github.com/cedricziel/signaldb/issues/551)
* **querier:** enforce resource limits on query execution ([#593](https://github.com/cedricziel/signaldb/issues/593)) ([b1c6341](https://github.com/cedricziel/signaldb/commit/b1c634157d4b669df81224242c21a4e05938fca5))
* **querier:** implement the Tempo gRPC querier protocol ([#617](https://github.com/cedricziel/signaldb/issues/617)) ([5774227](https://github.com/cedricziel/signaldb/commit/5774227f2e401cb5eb5be89b2c87ba2c878011d5))
* **querier:** per-tenant concurrent-query cap ([#595](https://github.com/cedricziel/signaldb/issues/595)) ([ae2c628](https://github.com/cedricziel/signaldb/commit/ae2c6289b0d30af9a636d45120bb30d7e716828f))
* **querier:** profile query service and Flight tickets ([#638](https://github.com/cedricziel/signaldb/issues/638)) ([898e882](https://github.com/cedricziel/signaldb/commit/898e882ff441d86bbb39e5dead99d1d0ee3e93f6)), closes [#355](https://github.com/cedricziel/signaldb/issues/355)
* **querier:** PromQL &lt;agg&gt;_over_time functions + function inventory ([#676](https://github.com/cedricziel/signaldb/issues/676)) ([f43c510](https://github.com/cedricziel/signaldb/commit/f43c5104345f4840892240c8295b41ba819e37cb))
* **querier:** PromQL &lt;agg&gt;_over_time under an outer aggregation ([#702](https://github.com/cedricziel/signaldb/issues/702)) ([a9adc9d](https://github.com/cedricziel/signaldb/commit/a9adc9d14912323ac7e2103064bdc0202f11b732)), closes [#336](https://github.com/cedricziel/signaldb/issues/336)
* **querier:** PromQL absent() and absent_over_time() ([#703](https://github.com/cedricziel/signaldb/issues/703)) ([4a91ecf](https://github.com/cedricziel/signaldb/commit/4a91ecfde827e1074581ba09d7eb28ef86bd792f)), closes [#336](https://github.com/cedricziel/signaldb/issues/336)
* **querier:** PromQL delta() range function ([#688](https://github.com/cedricziel/signaldb/issues/688)) ([3d85c3f](https://github.com/cedricziel/signaldb/commit/3d85c3ffe7829dffaf30d7bea0eea629cf28bdb3)), closes [#336](https://github.com/cedricziel/signaldb/issues/336)
* **querier:** PromQL deriv() range function ([#689](https://github.com/cedricziel/signaldb/issues/689)) ([9da3d26](https://github.com/cedricziel/signaldb/commit/9da3d261902cef14714425ea197003850242c129)), closes [#336](https://github.com/cedricziel/signaldb/issues/336)
* **querier:** PromQL histogram_count() and histogram_sum() ([#697](https://github.com/cedricziel/signaldb/issues/697)) ([b6d2eca](https://github.com/cedricziel/signaldb/commit/b6d2eca7b097732028b19b884125c55698ea82f7)), closes [#336](https://github.com/cedricziel/signaldb/issues/336)
* **querier:** PromQL histogram_fraction(lower, upper, v) ([#706](https://github.com/cedricziel/signaldb/issues/706)) ([c5167f3](https://github.com/cedricziel/signaldb/commit/c5167f38773e815918828e6aa726c54fca2a5031)), closes [#336](https://github.com/cedricziel/signaldb/issues/336)
* **querier:** PromQL histogram_quantile ([#335](https://github.com/cedricziel/signaldb/issues/335)) ([#674](https://github.com/cedricziel/signaldb/issues/674)) ([53c3289](https://github.com/cedricziel/signaldb/commit/53c32897d36ae125dc134e54cef7884ab9c4e926))
* **querier:** PromQL histogram_quantile(phi, rate(metric[5m])) ([#704](https://github.com/cedricziel/signaldb/issues/704)) ([1d3224a](https://github.com/cedricziel/signaldb/commit/1d3224aaa393cbf9492f8b775de99e6893fff84d)), closes [#336](https://github.com/cedricziel/signaldb/issues/336)
* **querier:** PromQL irate() and idelta() range functions ([#692](https://github.com/cedricziel/signaldb/issues/692)) ([938d730](https://github.com/cedricziel/signaldb/commit/938d7307d6e4d065cf6315f2868d0a856edf108f)), closes [#336](https://github.com/cedricziel/signaldb/issues/336)
* **querier:** PromQL logical set operators (and/or/unless) ([#701](https://github.com/cedricziel/signaldb/issues/701)) ([9c666e8](https://github.com/cedricziel/signaldb/commit/9c666e88827a50cad3b2def66b21c655b71458d4))
* **querier:** PromQL math functions and scalar arithmetic ([#678](https://github.com/cedricziel/signaldb/issues/678)) ([92a3cf7](https://github.com/cedricziel/signaldb/commit/92a3cf7f5ae5efeda36347f03f574b83256164a4))
* **querier:** PromQL offset modifier ([#694](https://github.com/cedricziel/signaldb/issues/694)) ([15a2314](https://github.com/cedricziel/signaldb/commit/15a231440c89971af261c3606e780186d67ad20c)), closes [#336](https://github.com/cedricziel/signaldb/issues/336)
* **querier:** PromQL present_over_time() ([#690](https://github.com/cedricziel/signaldb/issues/690)) ([1876d57](https://github.com/cedricziel/signaldb/commit/1876d57c8b19434776e87454622afb6720006067)), closes [#336](https://github.com/cedricziel/signaldb/issues/336)
* **querier:** PromQL quantile_over_time(phi, …) ([#695](https://github.com/cedricziel/signaldb/issues/695)) ([44c336d](https://github.com/cedricziel/signaldb/commit/44c336d58a9b4ccd0a8c37bab1ac4aa7410cb3ef)), closes [#336](https://github.com/cedricziel/signaldb/issues/336)
* **querier:** PromQL quantile(phi, …) aggregation ([#693](https://github.com/cedricziel/signaldb/issues/693)) ([18ee8d4](https://github.com/cedricziel/signaldb/commit/18ee8d4ce3707ac06d384c8f9dc1033e5a4dc804)), closes [#336](https://github.com/cedricziel/signaldb/issues/336)
* **querier:** PromQL resets() and changes() range functions ([#698](https://github.com/cedricziel/signaldb/issues/698)) ([5e85147](https://github.com/cedricziel/signaldb/commit/5e8514709b6312398f8f7de997af79f7a34b5e09)), closes [#336](https://github.com/cedricziel/signaldb/issues/336)
* **querier:** PromQL scalar comparison operators ([#691](https://github.com/cedricziel/signaldb/issues/691)) ([9256315](https://github.com/cedricziel/signaldb/commit/92563156c23b62cfdbc1e4dcb4d9293aadd5c83f)), closes [#336](https://github.com/cedricziel/signaldb/issues/336)
* **querier:** PromQL stddev, stdvar, and group aggregations ([#686](https://github.com/cedricziel/signaldb/issues/686)) ([531c767](https://github.com/cedricziel/signaldb/commit/531c7675e50c3a9b7d8d4f6afdcf8a13f95a1819)), closes [#336](https://github.com/cedricziel/signaldb/issues/336)
* **querier:** PromQL timestamp() ([#705](https://github.com/cedricziel/signaldb/issues/705)) ([efce2d8](https://github.com/cedricziel/signaldb/commit/efce2d8be53f81d4e4855e2156f4c2fb15590b15)), closes [#336](https://github.com/cedricziel/signaldb/issues/336)
* **querier:** PromQL topk/bottomk selection ([#679](https://github.com/cedricziel/signaldb/issues/679)) ([172da37](https://github.com/cedricziel/signaldb/commit/172da37bcd10f1f9f855126cbaf7b852ee4384c9))
* **querier:** PromQL vector-to-vector arithmetic (a OP b) ([#699](https://github.com/cedricziel/signaldb/issues/699)) ([31b82b7](https://github.com/cedricziel/signaldb/commit/31b82b7bd165276a3a21659205e284b56450ef16)), closes [#336](https://github.com/cedricziel/signaldb/issues/336)
* **querier:** PromQL vector-to-vector comparison (a CMP b) ([#700](https://github.com/cedricziel/signaldb/issues/700)) ([fa392ff](https://github.com/cedricziel/signaldb/commit/fa392ff887f52a9d584da85bee2c957d591bd4aa))
* **querier:** PromQL without(...) grouping ([#687](https://github.com/cedricziel/signaldb/issues/687)) ([8461dcc](https://github.com/cedricziel/signaldb/commit/8461dcc594006fce599e30bc4dfceefb60ed99d7)), closes [#336](https://github.com/cedricziel/signaldb/issues/336)
* **querier:** reject the PromQL @ modifier instead of ignoring it ([#696](https://github.com/cedricziel/signaldb/issues/696)) ([f9f5585](https://github.com/cedricziel/signaldb/commit/f9f55858f273d77c9fafa45bc9d27ac3eee313a1)), closes [#336](https://github.com/cedricziel/signaldb/issues/336)
* **querier:** route materialized labels to their columns ([#726](https://github.com/cedricziel/signaldb/issues/726)) ([3bbafb4](https://github.com/cedricziel/signaldb/commit/3bbafb49ff50ac45fde2c851b5161d82ca94f12c))
* **querier:** surface trace not-found as an explicit Flight status ([#616](https://github.com/cedricziel/signaldb/issues/616)) ([d6daeb6](https://github.com/cedricziel/signaldb/commit/d6daeb6fc63e6a1c49fefdcfb2391750f01dbcc8))
* **querier:** transpile LogQL log queries to DataFusion filters ([#664](https://github.com/cedricziel/signaldb/issues/664)) ([c537320](https://github.com/cedricziel/signaldb/commit/c53732042a26ff2a391e3ae930bbcbaac838eaed)), closes [#373](https://github.com/cedricziel/signaldb/issues/373)
* **self-monitoring:** epic [#447](https://github.com/cedricziel/signaldb/issues/447) — SignalDB observes itself (dogfooding) ([#542](https://github.com/cedricziel/signaldb/issues/542)) ([e6d7b1f](https://github.com/cedricziel/signaldb/commit/e6d7b1fc37f370f534d8780b3a6fe5d180b1ad65))
* **tempo:** honor spss span cap when shaping search results ([#615](https://github.com/cedricziel/signaldb/issues/615)) ([6a1d04b](https://github.com/cedricziel/signaldb/commit/6a1d04bef21c6b4ce85c2547f24cb884b40d8da3))
* **tempo:** honor start/end time hints in single-trace lookup ([#614](https://github.com/cedricziel/signaldb/issues/614)) ([ddb81fb](https://github.com/cedricziel/signaldb/commit/ddb81fbc2803ab5cb87e92b6b36773b3752009b6))
* **traces:** materialize configured labels for trace search ([#727](https://github.com/cedricziel/signaldb/issues/727)) ([4ef9584](https://github.com/cedricziel/signaldb/commit/4ef9584f514bcb7ae77e9e95b19a1e91f6ee8073))
* **traces:** typed Map attribute columns for trace search ([#742](https://github.com/cedricziel/signaldb/issues/742)) ([c4035a9](https://github.com/cedricziel/signaldb/commit/c4035a96fb0397c09f917156e632077e33c450a7))


### Bug Fixes

* align Iceberg namespace paths and partition spec (Issue [#185](https://github.com/cedricziel/signaldb/issues/185)) ([#306](https://github.com/cedricziel/signaldb/issues/306)) ([cc7af60](https://github.com/cedricziel/signaldb/commit/cc7af60ad6426eefc0a0de5c628b865306227172))
* **config:** refuse in-memory discovery/catalog in standalone services ([#599](https://github.com/cedricziel/signaldb/issues/599)) ([c8413ba](https://github.com/cedricziel/signaldb/commit/c8413babe5de5346477bf4d1ff26a7f2fef380bb))
* integration tests ([#123](https://github.com/cedricziel/signaldb/issues/123)) ([0ab5eeb](https://github.com/cedricziel/signaldb/commit/0ab5eeb9a0637483efaae88175ab9648c9dd2fb9))
* preserve OTLP scope/resource metadata and events/links in trace pipeline ([#183](https://github.com/cedricziel/signaldb/issues/183)) ([#307](https://github.com/cedricziel/signaldb/issues/307)) ([dfe04d7](https://github.com/cedricziel/signaldb/commit/dfe04d73d27c0e8820aa8daeed0787d048701865))
* propagate attributes through query path and implement trace search ([#309](https://github.com/cedricziel/signaldb/issues/309)) ([98244d8](https://github.com/cedricziel/signaldb/commit/98244d8f9ebfe623ea07ab240356511acbde5d07))
* **querier:** per-request SessionContext instead of mutating shared session defaults ([#577](https://github.com/cedricziel/signaldb/issues/577)) ([af4218f](https://github.com/cedricziel/signaldb/commit/af4218ff68d0e004ddad477d348e75fa005c4d3c)), closes [#564](https://github.com/cedricziel/signaldb/issues/564)
* resolve beta channel build failures and add temporary table cleanup ([#179](https://github.com/cedricziel/signaldb/issues/179)) ([d5f48dd](https://github.com/cedricziel/signaldb/commit/d5f48dd69cf1026295a825aea00f847c284ebe18))
* **self-monitoring:** box suppressed Flight handler futures ([#766](https://github.com/cedricziel/signaldb/issues/766)) ([7852012](https://github.com/cedricziel/signaldb/commit/7852012a5a78ee25892bac5fed981ccfe271cc52))
* **self-monitoring:** extend anti-loop guard to writer, querier, and router ([#765](https://github.com/cedricziel/signaldb/issues/765)) ([ec1ea04](https://github.com/cedricziel/signaldb/commit/ec1ea04be358a6b0c4f7452d758e9a6a0e7c8136))


### Documentation

* add comprehensive per-component README documentation ([#151](https://github.com/cedricziel/signaldb/issues/151)) ([5ea02a2](https://github.com/cedricziel/signaldb/commit/5ea02a2dd660bc03639a3a82c0146f18c147ecfb)), closes [#139](https://github.com/cedricziel/signaldb/issues/139)
* full staleness sweep — match all docs, skills, and READMEs to current code ([#611](https://github.com/cedricziel/signaldb/issues/611)) ([22247b0](https://github.com/cedricziel/signaldb/commit/22247b027d77820481d493c081e29f0df4efd6ed))


### Code Refactoring

* **querier:** remove superseded dead query code ([#613](https://github.com/cedricziel/signaldb/issues/613)) ([3520777](https://github.com/cedricziel/signaldb/commit/3520777c338a6a0aab10dcae45953325ae1c66c5))


### Tests

* **logql:** end-to-end integration test; fix timestamp unit in conversion ([#669](https://github.com/cedricziel/signaldb/issues/669)) ([7e38037](https://github.com/cedricziel/signaldb/commit/7e3803779f79edf2fd6e21ecb24dfc1db4a85e81)), closes [#378](https://github.com/cedricziel/signaldb/issues/378)


### Continuous Integration

* drop MSRV policy and fix security audit ignores ([#521](https://github.com/cedricziel/signaldb/issues/521)) ([7da71e3](https://github.com/cedricziel/signaldb/commit/7da71e3d78f593a4361f403e2d4be1e426fb8807))

## 0.1.0 (2026-03-02)


### ⚠ BREAKING CHANGES

* Minimum supported Rust version is now 1.85.0

### Features

* add global config ([#32](https://github.com/cedricziel/signaldb/issues/32)) ([fbb9a40](https://github.com/cedricziel/signaldb/commit/fbb9a407d45ae8f606334fc4154caee7ae4a12d9))
* add infrastructure for querying traces ([#7](https://github.com/cedricziel/signaldb/issues/7)) ([0417c9f](https://github.com/cedricziel/signaldb/commit/0417c9ffea7187fb40160217ae8b8ab78c43d12c))
* add querier ([0417c9f](https://github.com/cedricziel/signaldb/commit/0417c9ffea7187fb40160217ae8b8ab78c43d12c))
* add queue primitives ([#48](https://github.com/cedricziel/signaldb/issues/48)) ([caf4651](https://github.com/cedricziel/signaldb/commit/caf46518c2e7ee574d63617a9210774ed2531739))
* Add schema module with Iceberg integration and DSN-based storage ([#162](https://github.com/cedricziel/signaldb/issues/162)) ([60bbb8d](https://github.com/cedricziel/signaldb/commit/60bbb8d09a5ff63e2114c6383e7650c9dfef0d24))
* add single trace api endpoint ([0417c9f](https://github.com/cedricziel/signaldb/commit/0417c9ffea7187fb40160217ae8b8ab78c43d12c))
* **cli:** add HTTP admin API client for TUI ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** add terminal UI with traces, logs, metrics, admin, and dashboard tabs ([#458](https://github.com/cedricziel/signaldb/issues/458)) ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** implement Admin tab with tenant/key/dataset CRUD and confirmations ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** implement Logs tab with Flight SQL query interface ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** implement Metrics tab with sparklines and Flight SQL ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* **cli:** integrate TUI tabs with help overlay and error handling ([cbb967f](https://github.com/cedricziel/signaldb/commit/cbb967fe98eee9b461908ae946d3d3b2bbe8c703))
* compile tempopb ([0417c9f](https://github.com/cedricziel/signaldb/commit/0417c9ffea7187fb40160217ae8b8ab78c43d12c))
* end-to-end local development experience with CLI query support ([#434](https://github.com/cedricziel/signaldb/issues/434)) ([b95fb15](https://github.com/cedricziel/signaldb/commit/b95fb1595e33dd825f3c4424a88b966dded4808e))
* generate client + server stubs ([0417c9f](https://github.com/cedricziel/signaldb/commit/0417c9ffea7187fb40160217ae8b8ab78c43d12c))
* implement Iceberg table writer adapter to replace direct Parquet writes ([#175](https://github.com/cedricziel/signaldb/issues/175)) ([a55cc3d](https://github.com/cedricziel/signaldb/commit/a55cc3dbd06d955ee82d64e002abab588102df04))
* implement multi-tenancy with WAL isolation and authentication ([#243](https://github.com/cedricziel/signaldb/issues/243)) ([9a8945f](https://github.com/cedricziel/signaldb/commit/9a8945f06e871a96f5890e194534ae11ebb1f35b))
* implement trace querying functionality for issue [#6](https://github.com/cedricziel/signaldb/issues/6) ([#186](https://github.com/cedricziel/signaldb/issues/186)) ([ea8d9b4](https://github.com/cedricziel/signaldb/commit/ea8d9b47446cdbb89bb05b0a5c048c023d4dde49))
* integrate cargo-machete for unused dependency detection ([#130](https://github.com/cedricziel/signaldb/issues/130)) ([f305d3b](https://github.com/cedricziel/signaldb/commit/f305d3b9a6923ca2f7eca95ee83ed9002ee7cee1))
* Phase 2 Component Integration with WAL and Flight Services ([#138](https://github.com/cedricziel/signaldb/issues/138)) ([47f4174](https://github.com/cedricziel/signaldb/commit/47f417488c7b0225d031219df94a1d7eb55ff166))
* Prometheus remote_write ingestion and PromQL parser ([#382](https://github.com/cedricziel/signaldb/issues/382)) ([d548eb6](https://github.com/cedricziel/signaldb/commit/d548eb65388f254faec795bc44fa17d77d95659b))
* **querier,writer:** unify table reference format and shared CatalogManager ([#395](https://github.com/cedricziel/signaldb/issues/395)) ([9928f26](https://github.com/cedricziel/signaldb/commit/9928f266766d1de1d2276e5724a27ef29b1128da))
* **querier:** add PromQL to DataFusion query translator ([#383](https://github.com/cedricziel/signaldb/issues/383)) ([9630947](https://github.com/cedricziel/signaldb/commit/9630947c14a1908521326ebc8b9767701c4d9c2a))
* separate tempo-api into client and server ([0417c9f](https://github.com/cedricziel/signaldb/commit/0417c9ffea7187fb40160217ae8b8ab78c43d12c))
* store instances in catalog ([#105](https://github.com/cedricziel/signaldb/issues/105)) ([6e92a90](https://github.com/cedricziel/signaldb/commit/6e92a9031a20c04658a1060fa2b7733d5e244f0e))


### Bug Fixes

* adapt exports to tempopb ([0417c9f](https://github.com/cedricziel/signaldb/commit/0417c9ffea7187fb40160217ae8b8ab78c43d12c))
* align Iceberg namespace paths and partition spec (Issue [#185](https://github.com/cedricziel/signaldb/issues/185)) ([#306](https://github.com/cedricziel/signaldb/issues/306)) ([cc7af60](https://github.com/cedricziel/signaldb/commit/cc7af60ad6426eefc0a0de5c628b865306227172))
* correctly build tempopb ([0417c9f](https://github.com/cedricziel/signaldb/commit/0417c9ffea7187fb40160217ae8b8ab78c43d12c))
* integration tests ([#123](https://github.com/cedricziel/signaldb/issues/123)) ([0ab5eeb](https://github.com/cedricziel/signaldb/commit/0ab5eeb9a0637483efaae88175ab9648c9dd2fb9))
* preserve OTLP scope/resource metadata and events/links in trace pipeline ([#183](https://github.com/cedricziel/signaldb/issues/183)) ([#307](https://github.com/cedricziel/signaldb/issues/307)) ([dfe04d7](https://github.com/cedricziel/signaldb/commit/dfe04d73d27c0e8820aa8daeed0787d048701865))
* propagate attributes through query path and implement trace search ([#309](https://github.com/cedricziel/signaldb/issues/309)) ([98244d8](https://github.com/cedricziel/signaldb/commit/98244d8f9ebfe623ea07ab240356511acbde5d07))
* resolve beta channel build failures and add temporary table cleanup ([#179](https://github.com/cedricziel/signaldb/issues/179)) ([d5f48dd](https://github.com/cedricziel/signaldb/commit/d5f48dd69cf1026295a825aea00f847c284ebe18))
* set crates versions ([#21](https://github.com/cedricziel/signaldb/issues/21)) ([2113160](https://github.com/cedricziel/signaldb/commit/21131604ba4d7277f2b9e7f26b103ac03ef1bcaf))


### Documentation

* add comprehensive per-component README documentation ([#151](https://github.com/cedricziel/signaldb/issues/151)) ([5ea02a2](https://github.com/cedricziel/signaldb/commit/5ea02a2dd660bc03639a3a82c0146f18c147ecfb)), closes [#139](https://github.com/cedricziel/signaldb/issues/139)


### Tests

* add unit test for search_result serialization and deserialization ([0417c9f](https://github.com/cedricziel/signaldb/commit/0417c9ffea7187fb40160217ae8b8ab78c43d12c))
