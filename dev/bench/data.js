window.BENCHMARK_DATA = {
  "lastUpdate": 1787900147276,
  "repoUrl": "https://github.com/cedricziel/signaldb",
  "entries": {
    "Criterion": [
      {
        "commit": {
          "author": {
            "name": "Cedric Ziel",
            "username": "cedricziel",
            "email": "mail@cedric-ziel.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "c2559b804faf786d14357ca25b2f1b26c4272d31",
          "message": "docs(architecture): document trace_tags/trace_tag_values, drop stale tag claims (#1259)\n\nAdd the two new Flight tickets to the ticket-grammar table\n(flight-communication.md), document the tempo-api v2 schema-name\ndisambiguation technique (openapi-codegen.md), and fix the Tempo tags rows\nin overview.md and the traces-facets note in explore-ui.md that still\ndescribed the old hardcoded/501 behavior. The explore UI's \"Group by\nattribute\" input now suggests real observed keys via the same discovery\nAPI, which explore-ui.md now describes instead of pointing at #1073 as\nstill-pending.\n\nPart of tempo-tag-discovery (#1073).",
          "timestamp": "2026-08-16T12:24:13Z",
          "url": "https://github.com/cedricziel/signaldb/commit/c2559b804faf786d14357ca25b2f1b26c4272d31"
        },
        "date": 1786883694481,
        "tool": "cargo",
        "benches": [
          {
            "name": "acceptor_ingest/otlp_decode_and_convert",
            "value": 1898479,
            "range": "± 20362",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest/otlp_convert_only",
            "value": 1167175,
            "range": "± 18611",
            "unit": "ns/iter"
          },
          {
            "name": "wal/record_batch_roundtrip",
            "value": 727406,
            "range": "± 3305",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest_logs/otlp_decode_and_convert",
            "value": 1625347,
            "range": "± 7708",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest_logs/otlp_convert_only",
            "value": 776936,
            "range": "± 2303",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest_metrics/otlp_decode_and_convert",
            "value": 1891423,
            "range": "± 20202",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest_metrics/otlp_convert_only",
            "value": 1189855,
            "range": "± 14091",
            "unit": "ns/iter"
          },
          {
            "name": "single_batch_writes/100_rows_0.0MB",
            "value": 1286668,
            "range": "± 2969",
            "unit": "ns/iter"
          },
          {
            "name": "single_batch_writes/1000_rows_0.4MB",
            "value": 2339539,
            "range": "± 105702",
            "unit": "ns/iter"
          },
          {
            "name": "single_batch_writes/10000_rows_2.9MB",
            "value": 10480822,
            "range": "± 45812",
            "unit": "ns/iter"
          },
          {
            "name": "single_batch_writes/100000_rows_33.0MB",
            "value": 88176174,
            "range": "± 133305",
            "unit": "ns/iter"
          },
          {
            "name": "multi_batch_writes/2_batches_2000_rows",
            "value": 3431780,
            "range": "± 6228",
            "unit": "ns/iter"
          },
          {
            "name": "multi_batch_writes/5_batches_5000_rows",
            "value": 6724280,
            "range": "± 42382",
            "unit": "ns/iter"
          },
          {
            "name": "multi_batch_writes/10_batches_10000_rows",
            "value": 12215879,
            "range": "± 100596",
            "unit": "ns/iter"
          },
          {
            "name": "multi_batch_writes/20_batches_20000_rows",
            "value": 23124180,
            "range": "± 315862",
            "unit": "ns/iter"
          },
          {
            "name": "writer/creation",
            "value": 933075,
            "range": "± 4111",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_writes/2_writers",
            "value": 2089898,
            "range": "± 32833",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_writes/4_writers",
            "value": 3072129,
            "range": "± 67165",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_writes/8_writers",
            "value": 5813825,
            "range": "± 138180",
            "unit": "ns/iter"
          },
          {
            "name": "schema_transform/transform_trace_v1_to_v2",
            "value": 602931,
            "range": "± 24462",
            "unit": "ns/iter"
          },
          {
            "name": "compactor/rewrite_6_files",
            "value": 21020370,
            "range": "± 555437",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_unbounded",
            "value": 27656587,
            "range": "± 602786",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_windowed",
            "value": 6607758,
            "range": "± 402287",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_via_index",
            "value": 15217780,
            "range": "± 213361",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_search_groups",
            "value": 33413457,
            "range": "± 788609",
            "unit": "ns/iter"
          },
          {
            "name": "signal_read/logs_filter_line_proxy",
            "value": 103650772,
            "range": "± 12196120",
            "unit": "ns/iter"
          },
          {
            "name": "signal_read/metrics_range_aggregation_proxy",
            "value": 75707756,
            "range": "± 1843113",
            "unit": "ns/iter"
          },
          {
            "name": "trace_index_scaling/10000",
            "value": 1037994,
            "range": "± 37466",
            "unit": "ns/iter"
          },
          {
            "name": "trace_index_scaling/100000",
            "value": 1033678,
            "range": "± 22408",
            "unit": "ns/iter"
          },
          {
            "name": "trace_index_scaling/1000000",
            "value": 1095245,
            "range": "± 15947",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Cedric Ziel",
            "username": "cedricziel",
            "email": "mail@cedric-ziel.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "e3a03759ea46da69a9aa9e9ccc062f1486d38e5a",
          "message": "chore(openspec): archive tenant-table-listing, management-api-key-scope, pyroscope-openapi-parity (#1271)\n\nSync the deltas into the main specs after #1267/#1269, #1266, and #1268\nmerged: catalog-backed table listing (every known dataset listed, empty ones\nincluded), the tenant:manage API-key scope with the full CLI tenant group and\nthe parity exclusion list reduced to the three human/browser-only operations,\nand the Pyroscope compat endpoints in the OpenAPI contract with CLI/MCP/UI\nsurfaces.",
          "timestamp": "2026-08-16T20:38:21Z",
          "url": "https://github.com/cedricziel/signaldb/commit/e3a03759ea46da69a9aa9e9ccc062f1486d38e5a"
        },
        "date": 1786941282064,
        "tool": "cargo",
        "benches": [
          {
            "name": "acceptor_ingest/otlp_decode_and_convert",
            "value": 1994063,
            "range": "± 17275",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest/otlp_convert_only",
            "value": 1231917,
            "range": "± 25600",
            "unit": "ns/iter"
          },
          {
            "name": "wal/record_batch_roundtrip",
            "value": 700708,
            "range": "± 29366",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest_logs/otlp_decode_and_convert",
            "value": 1660383,
            "range": "± 16772",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest_logs/otlp_convert_only",
            "value": 813623,
            "range": "± 4702",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest_metrics/otlp_decode_and_convert",
            "value": 1993212,
            "range": "± 40721",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest_metrics/otlp_convert_only",
            "value": 1262787,
            "range": "± 22314",
            "unit": "ns/iter"
          },
          {
            "name": "single_batch_writes/100_rows_0.0MB",
            "value": 1376630,
            "range": "± 47881",
            "unit": "ns/iter"
          },
          {
            "name": "single_batch_writes/1000_rows_0.4MB",
            "value": 2471212,
            "range": "± 11750",
            "unit": "ns/iter"
          },
          {
            "name": "single_batch_writes/10000_rows_2.9MB",
            "value": 11472965,
            "range": "± 78106",
            "unit": "ns/iter"
          },
          {
            "name": "single_batch_writes/100000_rows_33.0MB",
            "value": 93607644,
            "range": "± 591221",
            "unit": "ns/iter"
          },
          {
            "name": "multi_batch_writes/2_batches_2000_rows",
            "value": 3656528,
            "range": "± 14057",
            "unit": "ns/iter"
          },
          {
            "name": "multi_batch_writes/5_batches_5000_rows",
            "value": 7211357,
            "range": "± 144312",
            "unit": "ns/iter"
          },
          {
            "name": "multi_batch_writes/10_batches_10000_rows",
            "value": 13034421,
            "range": "± 124450",
            "unit": "ns/iter"
          },
          {
            "name": "multi_batch_writes/20_batches_20000_rows",
            "value": 24801877,
            "range": "± 210908",
            "unit": "ns/iter"
          },
          {
            "name": "writer/creation",
            "value": 924329,
            "range": "± 5782",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_writes/2_writers",
            "value": 2229274,
            "range": "± 28031",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_writes/4_writers",
            "value": 3255853,
            "range": "± 71653",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_writes/8_writers",
            "value": 6276181,
            "range": "± 202406",
            "unit": "ns/iter"
          },
          {
            "name": "schema_transform/transform_trace_v1_to_v2",
            "value": 604651,
            "range": "± 2906",
            "unit": "ns/iter"
          },
          {
            "name": "compactor/rewrite_6_files",
            "value": 22883214,
            "range": "± 375682",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_unbounded",
            "value": 31661397,
            "range": "± 979697",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_windowed",
            "value": 8072726,
            "range": "± 691880",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_via_index",
            "value": 18014518,
            "range": "± 221819",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_search_groups",
            "value": 37824777,
            "range": "± 693912",
            "unit": "ns/iter"
          },
          {
            "name": "querier_service/find_trace_by_id",
            "value": 32284735,
            "range": "± 1085594",
            "unit": "ns/iter"
          },
          {
            "name": "querier_service/find_trace_by_id_hinted",
            "value": 8234693,
            "range": "± 323990",
            "unit": "ns/iter"
          },
          {
            "name": "querier_service/search_traces_recent",
            "value": 139967793,
            "range": "± 3008355",
            "unit": "ns/iter"
          },
          {
            "name": "querier_service/promql_range_avg_by_service",
            "value": 137495851,
            "range": "± 4689198",
            "unit": "ns/iter"
          },
          {
            "name": "querier_service/logql_line_filter",
            "value": 146687861,
            "range": "± 2553057",
            "unit": "ns/iter"
          },
          {
            "name": "trace_index_scaling/10000",
            "value": 1032758,
            "range": "± 24276",
            "unit": "ns/iter"
          },
          {
            "name": "trace_index_scaling/100000",
            "value": 1044733,
            "range": "± 12530",
            "unit": "ns/iter"
          },
          {
            "name": "trace_index_scaling/1000000",
            "value": 1106059,
            "range": "± 16444",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Cedric Ziel",
            "username": "cedricziel",
            "email": "mail@cedric-ziel.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "54cf2a8b3bfc5f8c7211818c449f184e4e1ba9a3",
          "message": "refactor(querier): dedupe query-path helpers and cut avoidable allocations (#1325)\n\n* refactor(querier): dedupe query-path helpers and cut avoidable allocations\n\nApplies a simplify pass over the querier and logql crates' query-execution\npaths: no intended behavior change, only reuse/simplification/efficiency\ncleanups.\n\nReuse:\n- Consolidate three near-identical QuerierError-to-Status mappers (flight.rs\n  x2, services/tempo.rs) around one shared common_error_status helper.\n- Replace duplicated column-downcast/dedup helpers (ir_planner::downcast_string,\n  trace::required_string_column, profile::string_column, profile's inline\n  distinct-service scan, trace's inline is_map_column/label_ column scan)\n  with the existing table_lookup/logs helpers they duplicated.\n- Consolidate four independently-declared LABEL_SCAN_LIMIT/TAG_SCAN_LIMIT\n  constants (logs, metrics, profile, trace) into one table_lookup constant.\n- Share a single token_to_match_op mapping between LogQL's two matcher-token\n  parsing sites instead of two copies (one panicking, one erroring).\n\nSimplification:\n- IrService/ProfileService: derive(Clone) instead of a hand-written impl.\n- cli.rs: drop needless `let mut builder = builder` rebindings.\n- ir_planner: fold Predicate::And/Or via Iterator::reduce instead of an\n  index-tracked accumulator loop.\n\nEfficiency:\n- flight.rs trace_to_record_batches: walk spans by reference instead of\n  deep-cloning each one, and pre-size the per-column output Vecs.\n- trace.rs: avoid a per-row String allocation for the trace_id once it's\n  already been captured; stop cloning parent_span_id where it's used once.\n- metrics.rs: precompile each label_replace regex once per call instead of\n  once per RecordBatch; move rather than clone HistogramAcc bounds/apply_topk\n  row fields where they're read once; pre-size output Vecs where the size is\n  known or well bounded.\n- table_lookup::distinct_non_empty: read each array value once per row\n  instead of twice.\n\nVerification: cargo fmt clean; logql (cargo clippy + cargo test, 84 tests)\nclean. Local querier clippy/test could not complete — the shared build\nenvironment repeatedly reclaimed this worktree's target/ mid-compile under\ndisk pressure (unrelated to this diff, confirmed by identical failures deep\nin third-party dependency compilation before reaching querier's own code).\nRelying on CI to verify querier/tempo/loki/prometheus/pyroscope-api.\n\n* docs(querier): trim a noisy parenthetical in the string_column comments",
          "timestamp": "2026-08-18T03:03:03Z",
          "url": "https://github.com/cedricziel/signaldb/commit/54cf2a8b3bfc5f8c7211818c449f184e4e1ba9a3"
        },
        "date": 1787026741172,
        "tool": "cargo",
        "benches": [
          {
            "name": "acceptor_ingest/otlp_decode_and_convert",
            "value": 1275513,
            "range": "± 36655",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest/otlp_convert_only",
            "value": 838750,
            "range": "± 23884",
            "unit": "ns/iter"
          },
          {
            "name": "wal/record_batch_roundtrip",
            "value": 509776,
            "range": "± 19494",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest_logs/otlp_decode_and_convert",
            "value": 1028873,
            "range": "± 56926",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest_logs/otlp_convert_only",
            "value": 559146,
            "range": "± 15534",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest_metrics/otlp_decode_and_convert",
            "value": 1376524,
            "range": "± 53754",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest_metrics/otlp_convert_only",
            "value": 896117,
            "range": "± 46446",
            "unit": "ns/iter"
          },
          {
            "name": "single_batch_writes/100_rows_0.0MB",
            "value": 1305627,
            "range": "± 15077",
            "unit": "ns/iter"
          },
          {
            "name": "single_batch_writes/1000_rows_0.4MB",
            "value": 2035986,
            "range": "± 11974",
            "unit": "ns/iter"
          },
          {
            "name": "single_batch_writes/10000_rows_2.9MB",
            "value": 7793264,
            "range": "± 60045",
            "unit": "ns/iter"
          },
          {
            "name": "single_batch_writes/100000_rows_33.0MB",
            "value": 77007959,
            "range": "± 1908907",
            "unit": "ns/iter"
          },
          {
            "name": "multi_batch_writes/2_batches_2000_rows",
            "value": 2949354,
            "range": "± 14613",
            "unit": "ns/iter"
          },
          {
            "name": "multi_batch_writes/5_batches_5000_rows",
            "value": 5485251,
            "range": "± 24247",
            "unit": "ns/iter"
          },
          {
            "name": "multi_batch_writes/10_batches_10000_rows",
            "value": 9651320,
            "range": "± 31027",
            "unit": "ns/iter"
          },
          {
            "name": "multi_batch_writes/20_batches_20000_rows",
            "value": 18975082,
            "range": "± 145448",
            "unit": "ns/iter"
          },
          {
            "name": "writer/creation",
            "value": 1052739,
            "range": "± 61400",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_writes/2_writers",
            "value": 2035523,
            "range": "± 72539",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_writes/4_writers",
            "value": 2869921,
            "range": "± 109967",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_writes/8_writers",
            "value": 4921950,
            "range": "± 109584",
            "unit": "ns/iter"
          },
          {
            "name": "schema_transform/transform_trace_v1_to_v2",
            "value": 601723,
            "range": "± 17657",
            "unit": "ns/iter"
          },
          {
            "name": "compactor/rewrite_6_files",
            "value": 16917678,
            "range": "± 374844",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_unbounded",
            "value": 21141573,
            "range": "± 1124606",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_cold_without_cache",
            "value": 20095475,
            "range": "± 1078129",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_cold_with_cache",
            "value": 20432951,
            "range": "± 913814",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_warm_with_cache",
            "value": 19996288,
            "range": "± 667951",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_windowed",
            "value": 5544358,
            "range": "± 261572",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_via_index",
            "value": 12262042,
            "range": "± 351696",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_search_groups",
            "value": 26358705,
            "range": "± 982509",
            "unit": "ns/iter"
          },
          {
            "name": "querier_service/find_trace_by_id",
            "value": 21824118,
            "range": "± 1088803",
            "unit": "ns/iter"
          },
          {
            "name": "querier_service/find_trace_by_id_hinted",
            "value": 5731316,
            "range": "± 249605",
            "unit": "ns/iter"
          },
          {
            "name": "querier_service/search_traces_recent",
            "value": 55448519,
            "range": "± 1941481",
            "unit": "ns/iter"
          },
          {
            "name": "querier_service/promql_range_avg_by_service",
            "value": 103604086,
            "range": "± 2902892",
            "unit": "ns/iter"
          },
          {
            "name": "querier_service/logql_line_filter",
            "value": 109901159,
            "range": "± 3163857",
            "unit": "ns/iter"
          },
          {
            "name": "trace_index_scaling/10000",
            "value": 1053452,
            "range": "± 47427",
            "unit": "ns/iter"
          },
          {
            "name": "trace_index_scaling/100000",
            "value": 1069011,
            "range": "± 44376",
            "unit": "ns/iter"
          },
          {
            "name": "trace_index_scaling/1000000",
            "value": 1136620,
            "range": "± 27495",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Cedric Ziel",
            "username": "cedricziel",
            "email": "mail@cedric-ziel.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "61c9f1bc426edc34516922368ddcfe598d1e62f2",
          "message": "fix(writer): retire Iceberg WAL markers left by writer ids past retention (#1346)\n\n* fix(writer): retire Iceberg WAL markers left by writer ids past retention\n\nThe WAL to Iceberg idempotency marker is a table property keyed by the\nwriter's id. Nothing ever removed one, so every writer id that has ever\ncommitted to a table left a permanent property there. A writer id is stable per\nWAL directory but a new one is generated whenever that directory is created or\nwiped: a redeploy on ephemeral storage, a WAL quarantined and recreated after\ncorruption (#883), an operator clearing `.data/wal`. The per-tenant WAL fanout\nmultiplies the number of WAL directories, and therefore of writer ids, by\ntenant x dataset x signal.\n\nEvery property lands in `metadata.json`, which #959 fought down from 11.9 MB to\n28.5 KB, and its size is paid on every read and every commit.\n\nMarker values now lead with the commit time, so a marker can be dated and\ntherefore retired. Values written before this have no such field and still\ndecode — they are live idempotency evidence for whichever writer wrote them.\n\nA marker is evidence that its writer committed rows it may not have marked\nprocessed yet, so deleting one that is still needed makes that writer\nre-insert those rows as duplicates on its next replay. Three rules keep that\nfrom happening:\n\n- Never one of ours. Every WAL this process holds has its own writer id and\n  its own marker, and all of them are excluded, not just the one being\n  committed through.\n- Only past `[writer].wal_marker_retention` (default 30 days). A writer that\n  committed inside the window may still be alive and mid-recovery.\n- An undated marker only once this process has itself been up longer than the\n  window. Such a marker could belong to a writer that is healthy but has not\n  committed since the deploy; outliving the window proves otherwise, because a\n  live writer would have rewritten it with a dated one by then.\n\nThe delete is a metadata-only commit asserting the branch's current snapshot,\nso a marker written between the read and the delete fails this commit rather\nthan discarding fresh evidence. Failure is never fatal: the markers stay until\nthe next pass, which runs hourly over the tables this writer commits to.\n\nCloses #1307\n\n* docs(configuration): document the writer wal_marker_retention setting",
          "timestamp": "2026-08-18T10:56:31Z",
          "url": "https://github.com/cedricziel/signaldb/commit/61c9f1bc426edc34516922368ddcfe598d1e62f2"
        },
        "date": 1787113960987,
        "tool": "cargo",
        "benches": [
          {
            "name": "acceptor_ingest/otlp_decode_and_convert",
            "value": 1925178,
            "range": "± 20860",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest/otlp_convert_only",
            "value": 1168811,
            "range": "± 17807",
            "unit": "ns/iter"
          },
          {
            "name": "wal/record_batch_roundtrip",
            "value": 752275,
            "range": "± 24684",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest_logs/otlp_decode_and_convert",
            "value": 1597751,
            "range": "± 60523",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest_logs/otlp_convert_only",
            "value": 795102,
            "range": "± 4700",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest_metrics/otlp_decode_and_convert",
            "value": 1925762,
            "range": "± 14668",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest_metrics/otlp_convert_only",
            "value": 1226764,
            "range": "± 10627",
            "unit": "ns/iter"
          },
          {
            "name": "single_batch_writes/100_rows_0.0MB",
            "value": 1383010,
            "range": "± 4936",
            "unit": "ns/iter"
          },
          {
            "name": "single_batch_writes/1000_rows_0.4MB",
            "value": 2574880,
            "range": "± 198870",
            "unit": "ns/iter"
          },
          {
            "name": "single_batch_writes/10000_rows_2.9MB",
            "value": 11730597,
            "range": "± 89457",
            "unit": "ns/iter"
          },
          {
            "name": "single_batch_writes/100000_rows_33.0MB",
            "value": 105451278,
            "range": "± 5142893",
            "unit": "ns/iter"
          },
          {
            "name": "multi_batch_writes/2_batches_2000_rows",
            "value": 3983356,
            "range": "± 58182",
            "unit": "ns/iter"
          },
          {
            "name": "multi_batch_writes/5_batches_5000_rows",
            "value": 8022951,
            "range": "± 70237",
            "unit": "ns/iter"
          },
          {
            "name": "multi_batch_writes/10_batches_10000_rows",
            "value": 14598847,
            "range": "± 111970",
            "unit": "ns/iter"
          },
          {
            "name": "multi_batch_writes/20_batches_20000_rows",
            "value": 28231310,
            "range": "± 116975",
            "unit": "ns/iter"
          },
          {
            "name": "writer/creation",
            "value": 958554,
            "range": "± 4987",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_writes/2_writers",
            "value": 2265193,
            "range": "± 55893",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_writes/4_writers",
            "value": 3516527,
            "range": "± 168507",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_writes/8_writers",
            "value": 6408146,
            "range": "± 163632",
            "unit": "ns/iter"
          },
          {
            "name": "schema_transform/transform_trace_v1_to_v2",
            "value": 643031,
            "range": "± 22233",
            "unit": "ns/iter"
          },
          {
            "name": "compactor/rewrite_6_files",
            "value": 20717004,
            "range": "± 233835",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_unbounded",
            "value": 26961360,
            "range": "± 687390",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_cold_without_cache",
            "value": 25890907,
            "range": "± 1520819",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_cold_with_cache",
            "value": 25858620,
            "range": "± 173475",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_warm_with_cache",
            "value": 25692213,
            "range": "± 969637",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_windowed",
            "value": 6419381,
            "range": "± 51943",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_via_index",
            "value": 15199660,
            "range": "± 101959",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_search_groups",
            "value": 32171329,
            "range": "± 212083",
            "unit": "ns/iter"
          },
          {
            "name": "querier_service/find_trace_by_id",
            "value": 28431371,
            "range": "± 1442289",
            "unit": "ns/iter"
          },
          {
            "name": "querier_service/find_trace_by_id_hinted",
            "value": 6522785,
            "range": "± 139494",
            "unit": "ns/iter"
          },
          {
            "name": "querier_service/search_traces_recent",
            "value": 68350266,
            "range": "± 1408346",
            "unit": "ns/iter"
          },
          {
            "name": "querier_service/promql_range_avg_by_service",
            "value": 125417091,
            "range": "± 1638931",
            "unit": "ns/iter"
          },
          {
            "name": "querier_service/logql_line_filter",
            "value": 133986791,
            "range": "± 1656273",
            "unit": "ns/iter"
          },
          {
            "name": "trace_index_scaling/10000",
            "value": 1110226,
            "range": "± 43381",
            "unit": "ns/iter"
          },
          {
            "name": "trace_index_scaling/100000",
            "value": 1057680,
            "range": "± 9404",
            "unit": "ns/iter"
          },
          {
            "name": "trace_index_scaling/1000000",
            "value": 1105505,
            "range": "± 10086",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Cedric Ziel",
            "username": "cedricziel",
            "email": "mail@cedric-ziel.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "29994a1d88bface511f7480dfb1b3232c087984f",
          "message": "fix(querier): union metric tables on an identical scan schema (#1351)\n\n* fix(querier): union metric tables on an identical scan schema\n\nFiltering the metrics source by a resource attribute served from the\nattribute map failed to plan:\n\n  Optimizer rule 'optimize_projections' failed\n  UNION field 0 have different type in inputs: left has Utf8 whereas\n  right has Date32\n\nGrouping by the same attribute worked, which is why #1206's fix did not\ncover this. That change reconciled column *types* across the union\nbranches, and types are enough until something is pushed between the\nprojection and the scan. A predicate is exactly that.\n\nThe branches disagreed on more than type. metrics_sum carries\naggregation_temporality and is_monotonic in the middle of its schema, so\nevery later column sits at a different index than in metrics_gauge, and\nboth end with the date_day/hour partition helpers. Each branch's scan\nexposed its own full schema and relied on a projection above it to pick\nthe common columns by name. push_down_filter moves the predicate below\nthe union into each branch; optimize_projections then rebuilds each\nbranch's scan projection from sorted column indices, and the branches are\nrebuilt against different index spaces — lining Utf8 up against\ndate_day's Date32.\n\nSo the provider now presents the union's common columns directly: the\nrow_defaults list, in that order, already coerced. Every branch's\nTableScan has an identical schema and there is no index space left for\nthe optimizer to disagree about. scan() translates back to the inner\ntable's indices, so filter and partition pushdown are unaffected.\n\nwrap() is fallible now. It used to skip a column missing from a table;\nsince the provider defines the branch's width, that would silently yield\na branch of the wrong shape, so it names the missing column instead.\n\nCovered for the class rather than the reported operator: eq, ne,\ncontains, regex and exists all plan over the union. `in` and `between`\nare untested here — the former takes a different operand shape, the\nlatter is meaningless on a string attribute.\n\nCloses #1348\n\n* test(querier): cover in/between and exclusion on the metrics union\n\nThe operator coverage added with the #1348 fix stopped at eq, ne,\ncontains, regex and exists, and justified the omission of `between` by\ncalling it meaningless on a string attribute. That was wrong: it lowers\nto `>= lo AND <= hi`, a lexicographic range, and it is a different plan\nshape from every single-comparison leaf — two comparisons over one field\nexpression. `in` lowers to an in-list, a third shape. Both are exactly\nthe kind of node arrangement that exposed the union misalignment, so\nneither belonged outside the net.\n\nThe existing cases also only asserted that a predicate matches all three\nfixture rows. A predicate that plans but is dropped during the rewrite\nmatches all three too, so those cases could not tell a working filter\nfrom a vanished one. The added test requires each operator to exclude\nevery row, pinning the other direction.\n\ngt/gte/lt/lte still have no leaf of their own, deliberately: `between`\nexercises that lowering already, and the test says so.\n\n* docs(querier): correct what the identical-schema guard actually spares\n\nThe guard's doc claimed it spares \"a single-table source\", which is not a\npath that exists: scan_source_tables returns at providers.len() == 1 and\nnever reaches wrap(). It also implied the check pays off in practice,\nwhen row_defaults is a strict subset of any real physical schema — which\nalso carries date_day/hour — so no real table matches and the guard is an\nidentity check, not an optimization. A reader would have gone looking for\na caller that isn't there.\n\nAlso marks the per-branch select() as the identity projection it became:\nthe provider now presents row_defaults directly, so the select reorders\nnothing and only normalizes the column names the two branches carry into\nthe union. As written it read as load-bearing.\n\nComments only — verified no non-comment line in the diff.",
          "timestamp": "2026-08-19T11:10:29Z",
          "url": "https://github.com/cedricziel/signaldb/commit/29994a1d88bface511f7480dfb1b3232c087984f"
        },
        "date": 1787200436632,
        "tool": "cargo",
        "benches": [
          {
            "name": "acceptor_ingest/otlp_decode_and_convert",
            "value": 1975783,
            "range": "± 22111",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest/otlp_convert_only",
            "value": 1180683,
            "range": "± 28859",
            "unit": "ns/iter"
          },
          {
            "name": "wal/record_batch_roundtrip",
            "value": 725275,
            "range": "± 8868",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest_logs/otlp_decode_and_convert",
            "value": 1611724,
            "range": "± 21177",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest_logs/otlp_convert_only",
            "value": 786912,
            "range": "± 4318",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest_metrics/otlp_decode_and_convert",
            "value": 1948314,
            "range": "± 87841",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest_metrics/otlp_convert_only",
            "value": 1234791,
            "range": "± 15157",
            "unit": "ns/iter"
          },
          {
            "name": "single_batch_writes/100_rows_0.0MB",
            "value": 1424887,
            "range": "± 43024",
            "unit": "ns/iter"
          },
          {
            "name": "single_batch_writes/1000_rows_0.4MB",
            "value": 2600543,
            "range": "± 40283",
            "unit": "ns/iter"
          },
          {
            "name": "single_batch_writes/10000_rows_2.9MB",
            "value": 12252498,
            "range": "± 54779",
            "unit": "ns/iter"
          },
          {
            "name": "single_batch_writes/100000_rows_33.0MB",
            "value": 111820354,
            "range": "± 1829929",
            "unit": "ns/iter"
          },
          {
            "name": "multi_batch_writes/2_batches_2000_rows",
            "value": 4096534,
            "range": "± 23386",
            "unit": "ns/iter"
          },
          {
            "name": "multi_batch_writes/5_batches_5000_rows",
            "value": 8396374,
            "range": "± 54167",
            "unit": "ns/iter"
          },
          {
            "name": "multi_batch_writes/10_batches_10000_rows",
            "value": 15075293,
            "range": "± 270280",
            "unit": "ns/iter"
          },
          {
            "name": "multi_batch_writes/20_batches_20000_rows",
            "value": 29567466,
            "range": "± 238459",
            "unit": "ns/iter"
          },
          {
            "name": "writer/creation",
            "value": 979757,
            "range": "± 4870",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_writes/2_writers",
            "value": 2307647,
            "range": "± 63981",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_writes/4_writers",
            "value": 3382623,
            "range": "± 102483",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_writes/8_writers",
            "value": 6509958,
            "range": "± 79491",
            "unit": "ns/iter"
          },
          {
            "name": "schema_transform/transform_trace_v1_to_v2",
            "value": 611829,
            "range": "± 5338",
            "unit": "ns/iter"
          },
          {
            "name": "compactor/rewrite_6_files",
            "value": 21122101,
            "range": "± 607450",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_unbounded",
            "value": 28415179,
            "range": "± 1200338",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_cold_without_cache",
            "value": 26944174,
            "range": "± 312261",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_cold_with_cache",
            "value": 27031542,
            "range": "± 168012",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_warm_with_cache",
            "value": 26989318,
            "range": "± 787693",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_windowed",
            "value": 6738955,
            "range": "± 173583",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_via_index",
            "value": 15880425,
            "range": "± 237397",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_search_groups",
            "value": 34457422,
            "range": "± 676134",
            "unit": "ns/iter"
          },
          {
            "name": "querier_service/find_trace_by_id",
            "value": 30828917,
            "range": "± 2243486",
            "unit": "ns/iter"
          },
          {
            "name": "querier_service/find_trace_by_id_hinted",
            "value": 6604540,
            "range": "± 50483",
            "unit": "ns/iter"
          },
          {
            "name": "querier_service/search_traces_recent",
            "value": 73004187,
            "range": "± 1372907",
            "unit": "ns/iter"
          },
          {
            "name": "querier_service/promql_range_avg_by_service",
            "value": 132772249,
            "range": "± 1483866",
            "unit": "ns/iter"
          },
          {
            "name": "querier_service/logql_line_filter",
            "value": 143559221,
            "range": "± 3199838",
            "unit": "ns/iter"
          },
          {
            "name": "trace_index_scaling/10000",
            "value": 1125650,
            "range": "± 36909",
            "unit": "ns/iter"
          },
          {
            "name": "trace_index_scaling/100000",
            "value": 1066291,
            "range": "± 23496",
            "unit": "ns/iter"
          },
          {
            "name": "trace_index_scaling/1000000",
            "value": 1116152,
            "range": "± 24353",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "dependabot[bot]",
            "username": "dependabot[bot]",
            "email": "49699333+dependabot[bot]@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "a4897e85bbe372ac8f60a315f5bc0f70ffaf27ff",
          "message": "chore(deps-dev): bump @swc/core in the build-tools group (#1370)\n\nBumps the build-tools group with 1 update: [@swc/core](https://github.com/swc-project/swc/tree/HEAD/packages/core).\n\n\nUpdates `@swc/core` from 1.15.47 to 1.16.0\n- [Release notes](https://github.com/swc-project/swc/releases)\n- [Changelog](https://github.com/swc-project/swc/blob/main/CHANGELOG.md)\n- [Commits](https://github.com/swc-project/swc/commits/v1.16.0/packages/core)\n\n---\nupdated-dependencies:\n- dependency-name: \"@swc/core\"\n  dependency-version: 1.16.0\n  dependency-type: direct:development\n  update-type: version-update:semver-minor\n  dependency-group: build-tools\n...\n\nSigned-off-by: dependabot[bot] <support@github.com>\nCo-authored-by: dependabot[bot] <49699333+dependabot[bot]@users.noreply.github.com>",
          "timestamp": "2026-08-20T23:32:28Z",
          "url": "https://github.com/cedricziel/signaldb/commit/a4897e85bbe372ac8f60a315f5bc0f70ffaf27ff"
        },
        "date": 1787286270350,
        "tool": "cargo",
        "benches": [
          {
            "name": "acceptor_ingest/otlp_decode_and_convert",
            "value": 1538657,
            "range": "± 24247",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest/otlp_convert_only",
            "value": 921666,
            "range": "± 3066",
            "unit": "ns/iter"
          },
          {
            "name": "wal/record_batch_roundtrip",
            "value": 559762,
            "range": "± 20981",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest_logs/otlp_decode_and_convert",
            "value": 1257032,
            "range": "± 14557",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest_logs/otlp_convert_only",
            "value": 606394,
            "range": "± 2786",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest_metrics/otlp_decode_and_convert",
            "value": 1446178,
            "range": "± 17444",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest_metrics/otlp_convert_only",
            "value": 889335,
            "range": "± 10349",
            "unit": "ns/iter"
          },
          {
            "name": "single_batch_writes/100_rows_0.0MB",
            "value": 1110978,
            "range": "± 5265",
            "unit": "ns/iter"
          },
          {
            "name": "single_batch_writes/1000_rows_0.4MB",
            "value": 2039732,
            "range": "± 5363",
            "unit": "ns/iter"
          },
          {
            "name": "single_batch_writes/10000_rows_2.9MB",
            "value": 9892249,
            "range": "± 69053",
            "unit": "ns/iter"
          },
          {
            "name": "single_batch_writes/100000_rows_33.0MB",
            "value": 94193831,
            "range": "± 3890604",
            "unit": "ns/iter"
          },
          {
            "name": "multi_batch_writes/2_batches_2000_rows",
            "value": 3185540,
            "range": "± 9741",
            "unit": "ns/iter"
          },
          {
            "name": "multi_batch_writes/5_batches_5000_rows",
            "value": 6573272,
            "range": "± 55868",
            "unit": "ns/iter"
          },
          {
            "name": "multi_batch_writes/10_batches_10000_rows",
            "value": 12167610,
            "range": "± 233384",
            "unit": "ns/iter"
          },
          {
            "name": "multi_batch_writes/20_batches_20000_rows",
            "value": 24181029,
            "range": "± 335055",
            "unit": "ns/iter"
          },
          {
            "name": "writer/creation",
            "value": 712830,
            "range": "± 4681",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_writes/2_writers",
            "value": 1899763,
            "range": "± 32050",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_writes/4_writers",
            "value": 2765432,
            "range": "± 105586",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_writes/8_writers",
            "value": 5472152,
            "range": "± 122050",
            "unit": "ns/iter"
          },
          {
            "name": "schema_transform/transform_trace_v1_to_v2",
            "value": 469188,
            "range": "± 7861",
            "unit": "ns/iter"
          },
          {
            "name": "compactor/rewrite_6_files",
            "value": 18438186,
            "range": "± 488506",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_unbounded",
            "value": 24990273,
            "range": "± 692100",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_cold_without_cache",
            "value": 23700559,
            "range": "± 1398942",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_cold_with_cache",
            "value": 23401518,
            "range": "± 359715",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_warm_with_cache",
            "value": 23472917,
            "range": "± 219900",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_windowed",
            "value": 5918508,
            "range": "± 182192",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_via_index",
            "value": 14073306,
            "range": "± 361266",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_search_groups",
            "value": 29418973,
            "range": "± 386249",
            "unit": "ns/iter"
          },
          {
            "name": "querier_service/find_trace_by_id",
            "value": 25206216,
            "range": "± 1143306",
            "unit": "ns/iter"
          },
          {
            "name": "querier_service/find_trace_by_id_hinted",
            "value": 5783681,
            "range": "± 303024",
            "unit": "ns/iter"
          },
          {
            "name": "querier_service/search_traces_recent",
            "value": 61816493,
            "range": "± 1301475",
            "unit": "ns/iter"
          },
          {
            "name": "querier_service/promql_range_avg_by_service",
            "value": 109215646,
            "range": "± 2466149",
            "unit": "ns/iter"
          },
          {
            "name": "querier_service/logql_line_filter",
            "value": 116440338,
            "range": "± 2049961",
            "unit": "ns/iter"
          },
          {
            "name": "trace_index_scaling/10000",
            "value": 803947,
            "range": "± 27770",
            "unit": "ns/iter"
          },
          {
            "name": "trace_index_scaling/100000",
            "value": 813136,
            "range": "± 8569",
            "unit": "ns/iter"
          },
          {
            "name": "trace_index_scaling/1000000",
            "value": 858731,
            "range": "± 11896",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Cedric Ziel",
            "username": "cedricziel",
            "email": "mail@cedric-ziel.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "92d165fb14494ed97bc49bc90b19af1d1341a596",
          "message": "test(querier): pin ql-ir's field mapping against the real logical schema (#1379)\n\n* feat(ql-ir): lower TraceQL onto the query IR\n\nStarts design D6 as option (c): a bridge crate between the parsers and the\nIR, rather than putting the lowering in either.\n\nNot in query-ir, which would make SignalDB's own query surface import every\ncompat grammar. Not in the parser crates either, for two reasons: cargo\npublish rejects a path-only dependency, so logql-parser depending on the\nunpublished query-ir would break publishing outright — and more importantly,\nhow TraceQL maps onto *our* IR is not part of TraceQL. Tempo has no such IR.\nThat mapping is a SignalDB decision, and belongs in a SignalDB crate, the\nsame argument that kept unscoped_selector private.\n\nTraceQL first because it is the provable case: its equality subset is\nliterally a where stage of conjoined equality leaves, so the lowering is a\nrename of vocabulary rather than a translation of structure. The scoping\ncarries over intact — the IR's container qualifiers mean what TraceQL's\nscopes mean, so span.http.method stays scoped and a bare key stays\ncoalescing.\n\nField names come from LogicalSchema::core() rather than TraceQL's spelling:\nname becomes span.name, status becomes status.code, kind becomes span_kind.\nVerified against the schema instead of guessed, and mutation-tested — the\nsuite catches span.name silently becoming name.\n\nql-ir is publish = false and is deliberately absent from the leaf-purity\nlist: depending on all three crates is its entire job.\n\n* docs: add ql-ir to the crate tables and the FDAP rationale\n\nThe new crate owes a row in both workspace tables. fdap.md gains the\nreason it belongs in that section: all three of ql-ir's dependencies are\nFDAP-free, so it is too — which is what separates client-side query\nconstruction from client-side syntax checking.\n\nVerified rather than asserted: cargo tree -p ql-ir shows zero datafusion,\narrow, parquet or iceberg.\n\nDeliberately not added to the architecture skill's query path. ql-ir\ncompiles but nothing calls it — no service, endpoint, or Flight ticket —\nand describing it there would document an aspiration as architecture. The\nsection that genuinely changes is the one covering the two parallel\nlowerings, and it changes when ql-ir is wired in.\n\n* docs(ql-ir): record why LogQL cannot lower onto the IR yet\n\nProbed the IR's stage vocabulary against LogQL's surface before writing more\nlowering, since the answer decides whether this crate can finish.\n\nOf LogQL's 15 range functions, 7 map to existing IR aggregates; of its 11\nvector aggregations, 9 do. The unmapped eight split in two, and the split is\nthe actual finding:\n\nAdditive — stddev/stdvar/first/last/absent need new AggFn variants and\nnothing else.\n\nStructural — rate, bytes_rate and rate_counter are a per-bucket count\ndivided by the window, and the IR has no arithmetic stage at all. Confirmed\nby grep: nothing computes over another stage's output. The same absence\nblocks binary operations between series and label_replace.\n\nrate is the most-used LogQL metric function, so partial coverage is not a\nuseful state to ship. Closing the gap is a design change to SignalDB's own\nquery surface, not a lowering detail — which makes it a proposal, not a\ncontinuation of this branch.\n\n* feat(query-ir)!: aggregate divisor and four functions at irVersion 5\n\nI called rate structurally inexpressible. The querier disproves it:\n\n    RangeFunction::Rate => (Aggregate::Count, Some(range_seconds))\n\nA rate is an aggregate plus a scalar divisor, and MetricPlan already carries\nrate_divisor_seconds through vector aggregation. That makes it additive, not\nstructural — my earlier reading generalised from 'no arithmetic stage exists'\nto 'rate needs one'.\n\nSo v5 adds two things to aggregate, neither changing document shape:\n\n- stddev, stdvar, first, last as AggFn variants. first/last order by the\n  source's own time column via SourcePlan.time_col, so 'first' means earliest\n  rather than whatever order the scan produced.\n- an optional divisor, reporting an aggregate per unit instead of absolute.\n  Named for the operation rather than per_seconds: dividing by a scalar is\n  not inherently temporal and this IR is signal-agnostic.\n\nA divided aggregate is Float64 whatever it divided — a count per 300 seconds\nis not an Int64. Divisors must be finite and positive; JSON cannot carry NaN\nor infinity (serde_json renders both as null, i.e. absent), so that guard\nexists for Rust callers constructing documents directly, which is exactly\nwhat ql-ir does. A test pins both paths.\n\nAggFn is deliberately not non_exhaustive: the compiler found the single\nplanner match that needed the new arms, which is the outcome we want when a\nnew aggregate appears.\n\nBREAKING CHANGE: irVersion 5 is the new maximum. Every v1-v4 document keeps\nits exact meaning; documents using stddev/stdvar/first/last or a divisor\nbelow v5 are rejected naming the version they need, never silently coerced.\n\n* docs: document irVersion 5's aggregate additions\n\nThe user-facing IR reference gains an aggregate-function table with the\nversion each was introduced in, and a section on divisor showing the rate\nshape it exists for.\n\nCorrects the architecture skill, which asserted that rate has no IR\nequivalent. That was true when written and my change makes it false. What\nremains genuinely inexpressible is narrower and now stated as such: irate,\nhistogram_fraction, and cross-series formulas like a / b and label_replace,\nall of which need computation across series rather than within one\naggregate.\n\nNo client regeneration: the OpenAPI models pipeline as opaque objects and\nirVersion as a plain integer, so the stage grammar is deliberately outside\nthe schema. cargo xtask check confirms.\n\n* feat(ql-ir): lower LogQL onto the query IR\n\nLog queries become a where stage; metric queries a where plus a stepped\naggregate. rate lowers as count carrying a divisor — the case irVersion 5\nwas added for, and the one I had wrongly called structurally impossible.\n\nWhat the IR still cannot say is refused by name rather than approximated:\ncross-series arithmetic (a / b, label_replace), `without` grouping, topk\nand bottomk as vector aggregations, ip(), unwrap, irate. A partially\nlowered query returns more rows than asked for while looking successful,\nwhich is the failure this crate exists to prevent.\n\nReview findings applied:\n\n- Document::minimum_ir_version() moves the version rule into query-ir,\n  where it belongs. ql-ir was asserting 'a divisor means 5' itself, making\n  the same fact true in three places with nothing keeping them in step. The\n  lowering now builds the document and asks it what version it needs, which\n  also covers the v2/v3/v4 gates it never knew about.\n- One shared ir_range helper instead of the same Range literal in both\n  lowerings.\n- label_field now lists trace_id and span_id explicitly, matching the set\n  querier::query::logql::column_for_label special-cases. They pass through\n  unchanged either way, but the two lists had already drifted, and a future\n  alias added to one is now visibly absent from the other.\n\nDocuments claim the lowest version that carries them, so a query needing\nnothing from v5 declares v1 and stays executable on an older server.\n\n* test(querier): pin ql-ir's field mapping against the real logical schema\n\nql-ir is a leaf crate by design, so it cannot see LogicalSchema — its field\nnames (span.name, status.code, span_kind, service.name, severity_text) were\nonly as correct as my reading of logical.rs, with nothing checking them. A\nwrong name does not fail loudly: it resolves through the attribute-container\nfallback as a key nothing ever has, and the query silently returns no rows.\n\nThis test is the join, and it takes ql-ir as the querier's first dependency\non it — the first step of routing the compat endpoints through the IR.\n\nThe first version of it did not work. It asked \"does the emitted name\nresolve?\", which cannot catch a typo: an unrecognised name is\nindistinguishable from an ordinary attribute key. Mutating status.code to\nstatus_code — the physical column, exactly the mistake worth catching — left\nit green. It now pins the expected logical field per query, so a wrong name\nfails on the expectation rather than needing to be recognised, and the same\nmutation reports:\n\n    { status = error }: expected a predicate on 'status.code', got [\"status_code\"]\n\nNot the full swap: routing Tempo search through ir_planner needs a\nSchemaResolver, which is private and wants a live DFSchema, and that is too\nlarge to do safely against a live API in one change. This is its\nprerequisite — the names have to line up before anything is rerouted.",
          "timestamp": "2026-08-21T22:55:13Z",
          "url": "https://github.com/cedricziel/signaldb/commit/92d165fb14494ed97bc49bc90b19af1d1341a596"
        },
        "date": 1787373082518,
        "tool": "cargo",
        "benches": [
          {
            "name": "acceptor_ingest/otlp_decode_and_convert",
            "value": 1929110,
            "range": "± 19175",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest/otlp_convert_only",
            "value": 1138438,
            "range": "± 31864",
            "unit": "ns/iter"
          },
          {
            "name": "wal/record_batch_roundtrip",
            "value": 742942,
            "range": "± 24965",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest_logs/otlp_decode_and_convert",
            "value": 1594900,
            "range": "± 22007",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest_logs/otlp_convert_only",
            "value": 763401,
            "range": "± 18002",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest_metrics/otlp_decode_and_convert",
            "value": 1872005,
            "range": "± 44156",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest_metrics/otlp_convert_only",
            "value": 1168523,
            "range": "± 14375",
            "unit": "ns/iter"
          },
          {
            "name": "single_batch_writes/100_rows_0.0MB",
            "value": 1445740,
            "range": "± 8237",
            "unit": "ns/iter"
          },
          {
            "name": "single_batch_writes/1000_rows_0.4MB",
            "value": 2712506,
            "range": "± 30777",
            "unit": "ns/iter"
          },
          {
            "name": "single_batch_writes/10000_rows_2.9MB",
            "value": 12710333,
            "range": "± 251101",
            "unit": "ns/iter"
          },
          {
            "name": "single_batch_writes/100000_rows_33.0MB",
            "value": 117761665,
            "range": "± 4411419",
            "unit": "ns/iter"
          },
          {
            "name": "multi_batch_writes/2_batches_2000_rows",
            "value": 4239678,
            "range": "± 64647",
            "unit": "ns/iter"
          },
          {
            "name": "multi_batch_writes/5_batches_5000_rows",
            "value": 8711453,
            "range": "± 134818",
            "unit": "ns/iter"
          },
          {
            "name": "multi_batch_writes/10_batches_10000_rows",
            "value": 15801315,
            "range": "± 360957",
            "unit": "ns/iter"
          },
          {
            "name": "multi_batch_writes/20_batches_20000_rows",
            "value": 31525515,
            "range": "± 863155",
            "unit": "ns/iter"
          },
          {
            "name": "writer/creation",
            "value": 985908,
            "range": "± 7882",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_writes/2_writers",
            "value": 2374733,
            "range": "± 49501",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_writes/4_writers",
            "value": 3389569,
            "range": "± 168834",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_writes/8_writers",
            "value": 6572395,
            "range": "± 164286",
            "unit": "ns/iter"
          },
          {
            "name": "schema_transform/transform_trace_v1_to_v2",
            "value": 616726,
            "range": "± 7874",
            "unit": "ns/iter"
          },
          {
            "name": "compactor/rewrite_6_files",
            "value": 21050471,
            "range": "± 263433",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_unbounded",
            "value": 28265944,
            "range": "± 1503623",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_cold_without_cache",
            "value": 26961816,
            "range": "± 305870",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_cold_with_cache",
            "value": 27217773,
            "range": "± 301713",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_warm_with_cache",
            "value": 27010413,
            "range": "± 200566",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_windowed",
            "value": 6606280,
            "range": "± 58977",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_via_index",
            "value": 15835027,
            "range": "± 182415",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_search_groups",
            "value": 34047761,
            "range": "± 217925",
            "unit": "ns/iter"
          },
          {
            "name": "querier_service/find_trace_by_id",
            "value": 28552526,
            "range": "± 2067719",
            "unit": "ns/iter"
          },
          {
            "name": "querier_service/find_trace_by_id_hinted",
            "value": 6608692,
            "range": "± 55325",
            "unit": "ns/iter"
          },
          {
            "name": "querier_service/search_traces_recent",
            "value": 70815841,
            "range": "± 1281487",
            "unit": "ns/iter"
          },
          {
            "name": "querier_service/promql_range_avg_by_service",
            "value": 130983950,
            "range": "± 2299939",
            "unit": "ns/iter"
          },
          {
            "name": "querier_service/logql_line_filter",
            "value": 141377259,
            "range": "± 1573306",
            "unit": "ns/iter"
          },
          {
            "name": "trace_index_scaling/10000",
            "value": 1093914,
            "range": "± 32570",
            "unit": "ns/iter"
          },
          {
            "name": "trace_index_scaling/100000",
            "value": 1069299,
            "range": "± 9066",
            "unit": "ns/iter"
          },
          {
            "name": "trace_index_scaling/1000000",
            "value": 1119429,
            "range": "± 27131",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Cedric Ziel",
            "username": "cedricziel",
            "email": "mail@cedric-ziel.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "ab5eef6fed3924dfa78903fdbb7d13446b241620",
          "message": "docs(openspec): archive ir-single-lowering (#1407)\n\n* docs: fix crate-map compactor invocation, table pipe, and querier paths\n\nAddresses CodeRabbit's three documentation nits on #1406: the compactor\nruns as `signaldb compactor`, the inline pipe in the ql-ir row broke the\ntable, and compat-crates.md referenced querier paths without the leading\nsrc/.\n\n* docs(openspec): archive ir-single-lowering\n\nSyncs the two query-ir-core requirements the change introduced into the\nmain spec and moves the completed change to the archive. Closes the loop\non #1382 (all five PRs merged).",
          "timestamp": "2026-08-23T02:52:07Z",
          "url": "https://github.com/cedricziel/signaldb/commit/ab5eef6fed3924dfa78903fdbb7d13446b241620"
        },
        "date": 1787458988034,
        "tool": "cargo",
        "benches": [
          {
            "name": "acceptor_ingest/otlp_decode_and_convert",
            "value": 1501392,
            "range": "± 21875",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest/otlp_convert_only",
            "value": 887408,
            "range": "± 3802",
            "unit": "ns/iter"
          },
          {
            "name": "wal/record_batch_roundtrip",
            "value": 533674,
            "range": "± 5835",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest_logs/otlp_decode_and_convert",
            "value": 1259417,
            "range": "± 15507",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest_logs/otlp_convert_only",
            "value": 600536,
            "range": "± 3896",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest_metrics/otlp_decode_and_convert",
            "value": 1432752,
            "range": "± 14783",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest_metrics/otlp_convert_only",
            "value": 874999,
            "range": "± 2849",
            "unit": "ns/iter"
          },
          {
            "name": "single_batch_writes/100_rows_0.0MB",
            "value": 1116491,
            "range": "± 8737",
            "unit": "ns/iter"
          },
          {
            "name": "single_batch_writes/1000_rows_0.4MB",
            "value": 2042123,
            "range": "± 4009",
            "unit": "ns/iter"
          },
          {
            "name": "single_batch_writes/10000_rows_2.9MB",
            "value": 9940005,
            "range": "± 142717",
            "unit": "ns/iter"
          },
          {
            "name": "single_batch_writes/100000_rows_33.0MB",
            "value": 93070522,
            "range": "± 1400626",
            "unit": "ns/iter"
          },
          {
            "name": "multi_batch_writes/2_batches_2000_rows",
            "value": 3192016,
            "range": "± 14678",
            "unit": "ns/iter"
          },
          {
            "name": "multi_batch_writes/5_batches_5000_rows",
            "value": 6466487,
            "range": "± 194747",
            "unit": "ns/iter"
          },
          {
            "name": "multi_batch_writes/10_batches_10000_rows",
            "value": 11813587,
            "range": "± 53426",
            "unit": "ns/iter"
          },
          {
            "name": "multi_batch_writes/20_batches_20000_rows",
            "value": 23107876,
            "range": "± 277510",
            "unit": "ns/iter"
          },
          {
            "name": "writer/creation",
            "value": 717576,
            "range": "± 3424",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_writes/2_writers",
            "value": 1841912,
            "range": "± 34413",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_writes/4_writers",
            "value": 2724598,
            "range": "± 71689",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_writes/8_writers",
            "value": 5159493,
            "range": "± 62854",
            "unit": "ns/iter"
          },
          {
            "name": "schema_transform/transform_trace_v1_to_v2",
            "value": 470701,
            "range": "± 938",
            "unit": "ns/iter"
          },
          {
            "name": "compactor/rewrite_6_files",
            "value": 17747940,
            "range": "± 318319",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_unbounded",
            "value": 23837326,
            "range": "± 998131",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_cold_without_cache",
            "value": 22329168,
            "range": "± 546940",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_cold_with_cache",
            "value": 22115545,
            "range": "± 440525",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_warm_with_cache",
            "value": 22089501,
            "range": "± 567452",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_windowed",
            "value": 5289290,
            "range": "± 82819",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_via_index",
            "value": 12799392,
            "range": "± 181040",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_search_groups",
            "value": 27862038,
            "range": "± 570982",
            "unit": "ns/iter"
          },
          {
            "name": "querier_service/find_trace_by_id",
            "value": 24916572,
            "range": "± 950884",
            "unit": "ns/iter"
          },
          {
            "name": "querier_service/find_trace_by_id_hinted",
            "value": 5565747,
            "range": "± 87541",
            "unit": "ns/iter"
          },
          {
            "name": "querier_service/search_traces_recent",
            "value": 62854564,
            "range": "± 1523739",
            "unit": "ns/iter"
          },
          {
            "name": "querier_service/promql_range_avg_by_service",
            "value": 107529469,
            "range": "± 1967730",
            "unit": "ns/iter"
          },
          {
            "name": "querier_service/logql_line_filter",
            "value": 116423961,
            "range": "± 2161707",
            "unit": "ns/iter"
          },
          {
            "name": "trace_index_scaling/10000",
            "value": 806206,
            "range": "± 25552",
            "unit": "ns/iter"
          },
          {
            "name": "trace_index_scaling/100000",
            "value": 814526,
            "range": "± 7138",
            "unit": "ns/iter"
          },
          {
            "name": "trace_index_scaling/1000000",
            "value": 857746,
            "range": "± 8362",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Cedric Ziel",
            "username": "cedricziel",
            "email": "mail@cedric-ziel.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "43422e6f4ac08af152109e8160e0c3dfce40e703",
          "message": "fix(writer): hygiene follow-ups from writer review (W8, W10, W11) (#1409)\n\n* fix(writer): reject unrepresentable casts when coercing to table schema\n\ncoerce_batch_to_schema used arrow::compute::cast's default safe:true\noptions, so a batch column that drifted from the table's Iceberg\nschema (e.g. Utf8 landing on an Int64 column) silently nulled the\nrow instead of failing. Switch to cast_with_options with safe:false\nso an unrepresentable value errors loudly.\n\nCloses #1404 (W8)\n\n* refactor(writer): remove unused RetryConfig setter/getter\n\nset_retry_config/retry_config on IcebergTableWriter had no callers\noutside the crate (grep across the workspace confirms it, including\ntests-integration) -- every writer runs the Default retry policy.\nDrop the dead API and the README section that documented it.\n\nRefs #1404 (W10)\n\n* test(writer): delete vacuous RetryConfig default-value test\n\ntest_retry_logic.rs only pinned RetryConfig::default()'s literal\nfield values -- it exercised no writer behavior and would still pass\nafter deleting the setter this stack just removed. Delete it and\ndrop the module's now-stale \"remain in writer crate\" comment.\n\nRefs #1404 (W10)\n\n* test(writer): dedupe repeated WalConfig test literals\n\nSix processor.rs tests each hand-typed a full WalConfig struct\nliteral differing only in wal_dir/max_segment_size/flush_interval_secs\n/tenant_id/dataset_id. Build from WalConfig::with_defaults(dir) with\njust the differing fields overridden, matching the pattern\ncoalescing_wal_config already used.\n\nRefs #1404 (W10)\n\n* chore(writer): fix Cargo.toml dependency misdeclarations\n\nasync-trait was machete-ignored with a stale justification (\"used by\nthe trait implementations\") -- the crate only uses tonic's re-exported\n#[tonic::async_trait], never `async_trait` directly, so the direct\ndependency was dead weight; drop it and its ignore entry. tempfile is\nonly used under #[cfg(test)]; move it from [dependencies] to\n[dev-dependencies].\n\nRefs #1404 (W10)\n\n* test(writer): derive metrics schema-consistency touched sets from Arrow schema\n\nThe five metrics schema_consistency tests compared schemas.toml\nagainst hand-typed field lists that could drift from the transform's\nactual create_metrics_*_arrow_schema() output without either list\nbeing wrong on its own. Derive the touched set from the Arrow schema\ninstead (minus the computed date_day/hour columns), matching how the\ntraces/logs/profiles tests already self-check at runtime. Add a\nnegative test proving a field present in the Arrow schema but absent\nfrom schemas.toml fails the check.\n\nCloses #1404 (W11)",
          "timestamp": "2026-08-23T08:38:01Z",
          "url": "https://github.com/cedricziel/signaldb/commit/43422e6f4ac08af152109e8160e0c3dfce40e703"
        },
        "date": 1787546292941,
        "tool": "cargo",
        "benches": [
          {
            "name": "acceptor_ingest/otlp_decode_and_convert",
            "value": 1972848,
            "range": "± 21091",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest/otlp_convert_only",
            "value": 1157202,
            "range": "± 13397",
            "unit": "ns/iter"
          },
          {
            "name": "wal/record_batch_roundtrip",
            "value": 735565,
            "range": "± 3966",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest_logs/otlp_decode_and_convert",
            "value": 1575890,
            "range": "± 54862",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest_logs/otlp_convert_only",
            "value": 774407,
            "range": "± 6240",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest_metrics/otlp_decode_and_convert",
            "value": 1836880,
            "range": "± 18985",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest_metrics/otlp_convert_only",
            "value": 1129311,
            "range": "± 11919",
            "unit": "ns/iter"
          },
          {
            "name": "single_batch_writes/100_rows_0.0MB",
            "value": 1421210,
            "range": "± 6560",
            "unit": "ns/iter"
          },
          {
            "name": "single_batch_writes/1000_rows_0.4MB",
            "value": 2667666,
            "range": "± 124528",
            "unit": "ns/iter"
          },
          {
            "name": "single_batch_writes/10000_rows_2.9MB",
            "value": 12351234,
            "range": "± 81306",
            "unit": "ns/iter"
          },
          {
            "name": "single_batch_writes/100000_rows_33.0MB",
            "value": 112081840,
            "range": "± 3022930",
            "unit": "ns/iter"
          },
          {
            "name": "multi_batch_writes/2_batches_2000_rows",
            "value": 4118328,
            "range": "± 19573",
            "unit": "ns/iter"
          },
          {
            "name": "multi_batch_writes/5_batches_5000_rows",
            "value": 8434748,
            "range": "± 82324",
            "unit": "ns/iter"
          },
          {
            "name": "multi_batch_writes/10_batches_10000_rows",
            "value": 15568761,
            "range": "± 141243",
            "unit": "ns/iter"
          },
          {
            "name": "multi_batch_writes/20_batches_20000_rows",
            "value": 30659051,
            "range": "± 107422",
            "unit": "ns/iter"
          },
          {
            "name": "writer/creation",
            "value": 985163,
            "range": "± 4576",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_writes/2_writers",
            "value": 2353867,
            "range": "± 45182",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_writes/4_writers",
            "value": 3501903,
            "range": "± 75763",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_writes/8_writers",
            "value": 6670341,
            "range": "± 106278",
            "unit": "ns/iter"
          },
          {
            "name": "schema_transform/transform_trace_v1_to_v2",
            "value": 557167,
            "range": "± 5716",
            "unit": "ns/iter"
          },
          {
            "name": "compactor/rewrite_6_files",
            "value": 21973818,
            "range": "± 512769",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_unbounded",
            "value": 29860231,
            "range": "± 582173",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_cold_without_cache",
            "value": 28537795,
            "range": "± 438603",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_cold_with_cache",
            "value": 28495372,
            "range": "± 222088",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_warm_with_cache",
            "value": 28220854,
            "range": "± 220565",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_windowed",
            "value": 7031681,
            "range": "± 114050",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_via_index",
            "value": 16987131,
            "range": "± 439603",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_search_groups",
            "value": 35674759,
            "range": "± 297279",
            "unit": "ns/iter"
          },
          {
            "name": "querier_service/find_trace_by_id",
            "value": 31144124,
            "range": "± 2461542",
            "unit": "ns/iter"
          },
          {
            "name": "querier_service/find_trace_by_id_hinted",
            "value": 7141700,
            "range": "± 110682",
            "unit": "ns/iter"
          },
          {
            "name": "querier_service/search_traces_recent",
            "value": 79802171,
            "range": "± 1566657",
            "unit": "ns/iter"
          },
          {
            "name": "querier_service/promql_range_avg_by_service",
            "value": 142614758,
            "range": "± 2187064",
            "unit": "ns/iter"
          },
          {
            "name": "querier_service/logql_line_filter",
            "value": 151712338,
            "range": "± 2163552",
            "unit": "ns/iter"
          },
          {
            "name": "trace_index_scaling/10000",
            "value": 1131795,
            "range": "± 56603",
            "unit": "ns/iter"
          },
          {
            "name": "trace_index_scaling/100000",
            "value": 1081292,
            "range": "± 22810",
            "unit": "ns/iter"
          },
          {
            "name": "trace_index_scaling/1000000",
            "value": 1130009,
            "range": "± 26367",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Cedric Ziel",
            "username": "cedricziel",
            "email": "mail@cedric-ziel.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "6a683e0a3403a6975997fb29415b0a3d28c433f9",
          "message": "fix(ci): keep Criterion data out of the cached target dir (#1411)\n\nrust-cache prunes every file under unknown target/ subdirectories before\nsaving but keeps the directory tree. On the first cache hit after a save,\ntarget/criterion/<bench>/base/ came back as an empty directory, which\nCriterion treats as a saved baseline: it tried to load base/sample.json,\nfailed, and printed the error to stdout in the middle of each bencher\nline. github-action-benchmark then found no parseable result and the\nnightly run failed (run 32805503550).\n\nPoint CRITERION_HOME at the runner temp directory so Criterion's data\nnever enters the cache, and note the trap in the benchmarking doc.",
          "timestamp": "2026-08-25T06:14:14Z",
          "url": "https://github.com/cedricziel/signaldb/commit/6a683e0a3403a6975997fb29415b0a3d28c433f9"
        },
        "date": 1787640893085,
        "tool": "cargo",
        "benches": [
          {
            "name": "acceptor_ingest/otlp_decode_and_convert",
            "value": 1450559,
            "range": "± 15915",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest/otlp_convert_only",
            "value": 959114,
            "range": "± 31681",
            "unit": "ns/iter"
          },
          {
            "name": "wal/record_batch_roundtrip",
            "value": 579791,
            "range": "± 6574",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest_logs/otlp_decode_and_convert",
            "value": 1159842,
            "range": "± 30087",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest_logs/otlp_convert_only",
            "value": 638773,
            "range": "± 7291",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest_metrics/otlp_decode_and_convert",
            "value": 1535878,
            "range": "± 23975",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest_metrics/otlp_convert_only",
            "value": 968967,
            "range": "± 16158",
            "unit": "ns/iter"
          },
          {
            "name": "single_batch_writes/100_rows_0.0MB",
            "value": 1392193,
            "range": "± 19940",
            "unit": "ns/iter"
          },
          {
            "name": "single_batch_writes/1000_rows_0.4MB",
            "value": 2223639,
            "range": "± 87067",
            "unit": "ns/iter"
          },
          {
            "name": "single_batch_writes/10000_rows_2.9MB",
            "value": 8884447,
            "range": "± 78264",
            "unit": "ns/iter"
          },
          {
            "name": "single_batch_writes/100000_rows_33.0MB",
            "value": 79588280,
            "range": "± 2235686",
            "unit": "ns/iter"
          },
          {
            "name": "multi_batch_writes/2_batches_2000_rows",
            "value": 3300551,
            "range": "± 22155",
            "unit": "ns/iter"
          },
          {
            "name": "multi_batch_writes/5_batches_5000_rows",
            "value": 6255865,
            "range": "± 35011",
            "unit": "ns/iter"
          },
          {
            "name": "multi_batch_writes/10_batches_10000_rows",
            "value": 11001764,
            "range": "± 117880",
            "unit": "ns/iter"
          },
          {
            "name": "multi_batch_writes/20_batches_20000_rows",
            "value": 20865824,
            "range": "± 163953",
            "unit": "ns/iter"
          },
          {
            "name": "writer/creation",
            "value": 1035194,
            "range": "± 7253",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_writes/2_writers",
            "value": 2151890,
            "range": "± 57809",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_writes/4_writers",
            "value": 2937324,
            "range": "± 97763",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_writes/8_writers",
            "value": 5435191,
            "range": "± 56991",
            "unit": "ns/iter"
          },
          {
            "name": "schema_transform/transform_trace_v1_to_v2",
            "value": 720200,
            "range": "± 7853",
            "unit": "ns/iter"
          },
          {
            "name": "compactor/rewrite_6_files",
            "value": 17946360,
            "range": "± 543219",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_unbounded",
            "value": 24196153,
            "range": "± 850445",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_cold_without_cache",
            "value": 23122223,
            "range": "± 483358",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_cold_with_cache",
            "value": 23118361,
            "range": "± 421152",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_warm_with_cache",
            "value": 22934032,
            "range": "± 288870",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_windowed",
            "value": 5981165,
            "range": "± 223022",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_via_index",
            "value": 13939790,
            "range": "± 165096",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_search_groups",
            "value": 29510467,
            "range": "± 481293",
            "unit": "ns/iter"
          },
          {
            "name": "querier_service/find_trace_by_id",
            "value": 25534761,
            "range": "± 1570859",
            "unit": "ns/iter"
          },
          {
            "name": "querier_service/find_trace_by_id_hinted",
            "value": 6258265,
            "range": "± 84661",
            "unit": "ns/iter"
          },
          {
            "name": "querier_service/search_traces_recent",
            "value": 64248256,
            "range": "± 1315428",
            "unit": "ns/iter"
          },
          {
            "name": "querier_service/promql_range_avg_by_service",
            "value": 112156633,
            "range": "± 2090622",
            "unit": "ns/iter"
          },
          {
            "name": "querier_service/logql_line_filter",
            "value": 121632131,
            "range": "± 1828487",
            "unit": "ns/iter"
          },
          {
            "name": "trace_index_scaling/10000",
            "value": 1129421,
            "range": "± 29720",
            "unit": "ns/iter"
          },
          {
            "name": "trace_index_scaling/100000",
            "value": 1135210,
            "range": "± 22946",
            "unit": "ns/iter"
          },
          {
            "name": "trace_index_scaling/1000000",
            "value": 1203873,
            "range": "± 20504",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Cedric Ziel",
            "username": "cedricziel",
            "email": "mail@cedric-ziel.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "6a683e0a3403a6975997fb29415b0a3d28c433f9",
          "message": "fix(ci): keep Criterion data out of the cached target dir (#1411)\n\nrust-cache prunes every file under unknown target/ subdirectories before\nsaving but keeps the directory tree. On the first cache hit after a save,\ntarget/criterion/<bench>/base/ came back as an empty directory, which\nCriterion treats as a saved baseline: it tried to load base/sample.json,\nfailed, and printed the error to stdout in the middle of each bencher\nline. github-action-benchmark then found no parseable result and the\nnightly run failed (run 32805503550).\n\nPoint CRITERION_HOME at the runner temp directory so Criterion's data\nnever enters the cache, and note the trap in the benchmarking doc.",
          "timestamp": "2026-08-25T06:14:14Z",
          "url": "https://github.com/cedricziel/signaldb/commit/6a683e0a3403a6975997fb29415b0a3d28c433f9"
        },
        "date": 1787718074534,
        "tool": "cargo",
        "benches": [
          {
            "name": "acceptor_ingest/otlp_decode_and_convert",
            "value": 1980457,
            "range": "± 13123",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest/otlp_convert_only",
            "value": 1141121,
            "range": "± 18087",
            "unit": "ns/iter"
          },
          {
            "name": "wal/record_batch_roundtrip",
            "value": 723026,
            "range": "± 33691",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest_logs/otlp_decode_and_convert",
            "value": 1565364,
            "range": "± 20981",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest_logs/otlp_convert_only",
            "value": 770204,
            "range": "± 4536",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest_metrics/otlp_decode_and_convert",
            "value": 1851070,
            "range": "± 19691",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest_metrics/otlp_convert_only",
            "value": 1146481,
            "range": "± 36843",
            "unit": "ns/iter"
          },
          {
            "name": "single_batch_writes/100_rows_0.0MB",
            "value": 1400075,
            "range": "± 6368",
            "unit": "ns/iter"
          },
          {
            "name": "single_batch_writes/1000_rows_0.4MB",
            "value": 2686131,
            "range": "± 126959",
            "unit": "ns/iter"
          },
          {
            "name": "single_batch_writes/10000_rows_2.9MB",
            "value": 12131031,
            "range": "± 120335",
            "unit": "ns/iter"
          },
          {
            "name": "single_batch_writes/100000_rows_33.0MB",
            "value": 112180285,
            "range": "± 2231359",
            "unit": "ns/iter"
          },
          {
            "name": "multi_batch_writes/2_batches_2000_rows",
            "value": 4025242,
            "range": "± 27354",
            "unit": "ns/iter"
          },
          {
            "name": "multi_batch_writes/5_batches_5000_rows",
            "value": 8193643,
            "range": "± 54730",
            "unit": "ns/iter"
          },
          {
            "name": "multi_batch_writes/10_batches_10000_rows",
            "value": 15424777,
            "range": "± 229677",
            "unit": "ns/iter"
          },
          {
            "name": "multi_batch_writes/20_batches_20000_rows",
            "value": 29728953,
            "range": "± 185949",
            "unit": "ns/iter"
          },
          {
            "name": "writer/creation",
            "value": 961459,
            "range": "± 4108",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_writes/2_writers",
            "value": 2346356,
            "range": "± 33488",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_writes/4_writers",
            "value": 3378018,
            "range": "± 79144",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_writes/8_writers",
            "value": 6345931,
            "range": "± 126534",
            "unit": "ns/iter"
          },
          {
            "name": "schema_transform/transform_trace_v1_to_v2",
            "value": 570558,
            "range": "± 4660",
            "unit": "ns/iter"
          },
          {
            "name": "compactor/rewrite_6_files",
            "value": 22220798,
            "range": "± 436992",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_unbounded",
            "value": 30303565,
            "range": "± 1269721",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_cold_without_cache",
            "value": 28594458,
            "range": "± 733315",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_cold_with_cache",
            "value": 27941225,
            "range": "± 520887",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_warm_with_cache",
            "value": 28183598,
            "range": "± 587366",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_windowed",
            "value": 6670851,
            "range": "± 123963",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_via_index",
            "value": 16066468,
            "range": "± 351638",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_search_groups",
            "value": 35178956,
            "range": "± 589176",
            "unit": "ns/iter"
          },
          {
            "name": "querier_service/find_trace_by_id",
            "value": 29289809,
            "range": "± 1483172",
            "unit": "ns/iter"
          },
          {
            "name": "querier_service/find_trace_by_id_hinted",
            "value": 6836774,
            "range": "± 150887",
            "unit": "ns/iter"
          },
          {
            "name": "querier_service/search_traces_recent",
            "value": 74065733,
            "range": "± 2087718",
            "unit": "ns/iter"
          },
          {
            "name": "querier_service/promql_range_avg_by_service",
            "value": 138459683,
            "range": "± 3436648",
            "unit": "ns/iter"
          },
          {
            "name": "querier_service/logql_line_filter",
            "value": 148118634,
            "range": "± 2169941",
            "unit": "ns/iter"
          },
          {
            "name": "trace_index_scaling/10000",
            "value": 1055521,
            "range": "± 33039",
            "unit": "ns/iter"
          },
          {
            "name": "trace_index_scaling/100000",
            "value": 1059058,
            "range": "± 11408",
            "unit": "ns/iter"
          },
          {
            "name": "trace_index_scaling/1000000",
            "value": 1120301,
            "range": "± 19542",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "dependabot[bot]",
            "username": "dependabot[bot]",
            "email": "49699333+dependabot[bot]@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "8d7e1fa48b9629e0be17e8d1886b117ed872e82d",
          "message": "chore(deps): bump crc32fast from 1.5.0 to 1.5.1 (#1420)\n\nBumps [crc32fast](https://github.com/srijs/rust-crc32fast) from 1.5.0 to 1.5.1.\n- [Commits](https://github.com/srijs/rust-crc32fast/compare/v1.5.0...v1.5.1)\n\n---\nupdated-dependencies:\n- dependency-name: crc32fast\n  dependency-version: 1.5.1\n  dependency-type: direct:production\n  update-type: version-update:semver-patch\n...\n\nSigned-off-by: dependabot[bot] <support@github.com>\nCo-authored-by: dependabot[bot] <49699333+dependabot[bot]@users.noreply.github.com>",
          "timestamp": "2026-08-26T21:16:29Z",
          "url": "https://github.com/cedricziel/signaldb/commit/8d7e1fa48b9629e0be17e8d1886b117ed872e82d"
        },
        "date": 1787810479611,
        "tool": "cargo",
        "benches": [
          {
            "name": "acceptor_ingest/otlp_decode_and_convert",
            "value": 1964902,
            "range": "± 21266",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest/otlp_convert_only",
            "value": 1159086,
            "range": "± 27133",
            "unit": "ns/iter"
          },
          {
            "name": "wal/record_batch_roundtrip",
            "value": 712242,
            "range": "± 3299",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest_logs/otlp_decode_and_convert",
            "value": 1590607,
            "range": "± 16262",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest_logs/otlp_convert_only",
            "value": 777867,
            "range": "± 5961",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest_metrics/otlp_decode_and_convert",
            "value": 1857644,
            "range": "± 15945",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest_metrics/otlp_convert_only",
            "value": 1125527,
            "range": "± 14975",
            "unit": "ns/iter"
          },
          {
            "name": "single_batch_writes/100_rows_0.0MB",
            "value": 1425202,
            "range": "± 5673",
            "unit": "ns/iter"
          },
          {
            "name": "single_batch_writes/1000_rows_0.4MB",
            "value": 2648554,
            "range": "± 199561",
            "unit": "ns/iter"
          },
          {
            "name": "single_batch_writes/10000_rows_2.9MB",
            "value": 12450596,
            "range": "± 93262",
            "unit": "ns/iter"
          },
          {
            "name": "single_batch_writes/100000_rows_33.0MB",
            "value": 112697047,
            "range": "± 4959508",
            "unit": "ns/iter"
          },
          {
            "name": "multi_batch_writes/2_batches_2000_rows",
            "value": 4132457,
            "range": "± 9705",
            "unit": "ns/iter"
          },
          {
            "name": "multi_batch_writes/5_batches_5000_rows",
            "value": 8419947,
            "range": "± 25599",
            "unit": "ns/iter"
          },
          {
            "name": "multi_batch_writes/10_batches_10000_rows",
            "value": 15429322,
            "range": "± 53834",
            "unit": "ns/iter"
          },
          {
            "name": "multi_batch_writes/20_batches_20000_rows",
            "value": 29727590,
            "range": "± 103686",
            "unit": "ns/iter"
          },
          {
            "name": "writer/creation",
            "value": 891093,
            "range": "± 4281",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_writes/2_writers",
            "value": 2377622,
            "range": "± 71152",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_writes/4_writers",
            "value": 3404674,
            "range": "± 131577",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_writes/8_writers",
            "value": 6400601,
            "range": "± 142893",
            "unit": "ns/iter"
          },
          {
            "name": "schema_transform/transform_trace_v1_to_v2",
            "value": 594594,
            "range": "± 1859",
            "unit": "ns/iter"
          },
          {
            "name": "compactor/rewrite_6_files",
            "value": 20217453,
            "range": "± 412943",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_unbounded",
            "value": 28274095,
            "range": "± 317606",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_cold_without_cache",
            "value": 27016402,
            "range": "± 806640",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_cold_with_cache",
            "value": 26985006,
            "range": "± 622926",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_warm_with_cache",
            "value": 26744898,
            "range": "± 178065",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_windowed",
            "value": 6521780,
            "range": "± 33415",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_via_index",
            "value": 15826186,
            "range": "± 77466",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_search_groups",
            "value": 33461974,
            "range": "± 148064",
            "unit": "ns/iter"
          },
          {
            "name": "querier_service/find_trace_by_id",
            "value": 29985951,
            "range": "± 1142591",
            "unit": "ns/iter"
          },
          {
            "name": "querier_service/find_trace_by_id_hinted",
            "value": 6720386,
            "range": "± 52062",
            "unit": "ns/iter"
          },
          {
            "name": "querier_service/search_traces_recent",
            "value": 70832279,
            "range": "± 1005587",
            "unit": "ns/iter"
          },
          {
            "name": "querier_service/promql_range_avg_by_service",
            "value": 123888975,
            "range": "± 1274782",
            "unit": "ns/iter"
          },
          {
            "name": "querier_service/logql_line_filter",
            "value": 133723857,
            "range": "± 1473419",
            "unit": "ns/iter"
          },
          {
            "name": "trace_index_scaling/10000",
            "value": 1078944,
            "range": "± 45223",
            "unit": "ns/iter"
          },
          {
            "name": "trace_index_scaling/100000",
            "value": 1019607,
            "range": "± 11423",
            "unit": "ns/iter"
          },
          {
            "name": "trace_index_scaling/1000000",
            "value": 1079520,
            "range": "± 26586",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Cedric Ziel",
            "username": "cedricziel",
            "email": "mail@cedric-ziel.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "efe84a70a3fd6365e4345c1f838efad4e777c926",
          "message": "fix(acceptor): capture trailer-carried grpc-status for streaming calls (#1435)\n\n* fix(acceptor): capture trailer-carried grpc-status for streaming gRPC calls\n\nGrpcTraceLayer only read the outcome from the response's grpc-status\nheader, which tonic sets on immediately-failed unary calls. A response\nwhose status instead arrives in HTTP/2 trailers (the streaming path)\nwas mis-recorded as OK.\n\nWrap the response body in TrailerStatusBody, which inspects each frame\nas it is drained: a trailers frame carrying grpc-status is recorded on\nthe SERVER span (taking precedence), otherwise the pre-parsed header\nstatus is recorded once the body is exhausted, defaulting to OK. This\nalso keeps the span open across the whole response stream instead of\nclosing it as soon as headers are sent.\n\nCloses #915\n\n* fix(acceptor): record grpc-status via Drop when a response body is never polled\n\nhyper's HTTP/2 server skips poll_frame entirely for a body whose\nis_end_stream() is true, which is exactly the shape of tonic's\nimmediately-failed unary responses (tonic::body::Body::empty()). That\nleft TrailerStatusBody's poll_frame-only recording unreachable on the\nhigher-volume error path (auth rejections, failed OTLP exports) that\nthe header-only implementation used to capture correctly, trading one\nbug for a worse one.\n\nAdd a Drop impl as the general backstop: it records the pre-parsed\nheader status if there is one, else Cancelled (a dropped, never-polled\nbody has no way to know it finished successfully, so the previous OK\ndefault would misreport a cut-off call). record_and_close's existing\nspan.take() guard keeps this idempotent with the trailers and\nend-of-stream paths, so nothing double-records.\n\nFour new tests: the Trailers-Only dispatch shape that reproduces the\nregression (proved red before this fix), a dropped-without-draining\ncase, a body-error-frame case, and a header+trailer precedence case.",
          "timestamp": "2026-08-27T23:03:27Z",
          "url": "https://github.com/cedricziel/signaldb/commit/efe84a70a3fd6365e4345c1f838efad4e777c926"
        },
        "date": 1787900146390,
        "tool": "cargo",
        "benches": [
          {
            "name": "acceptor_ingest/otlp_decode_and_convert",
            "value": 1948151,
            "range": "± 20056",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest/otlp_convert_only",
            "value": 1138596,
            "range": "± 14880",
            "unit": "ns/iter"
          },
          {
            "name": "wal/record_batch_roundtrip",
            "value": 691140,
            "range": "± 5357",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest_logs/otlp_decode_and_convert",
            "value": 1613936,
            "range": "± 58302",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest_logs/otlp_convert_only",
            "value": 765392,
            "range": "± 6640",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest_metrics/otlp_decode_and_convert",
            "value": 1897517,
            "range": "± 15074",
            "unit": "ns/iter"
          },
          {
            "name": "acceptor_ingest_metrics/otlp_convert_only",
            "value": 1161521,
            "range": "± 10282",
            "unit": "ns/iter"
          },
          {
            "name": "single_batch_writes/100_rows_0.0MB",
            "value": 1442338,
            "range": "± 6113",
            "unit": "ns/iter"
          },
          {
            "name": "single_batch_writes/1000_rows_0.4MB",
            "value": 2711630,
            "range": "± 192736",
            "unit": "ns/iter"
          },
          {
            "name": "single_batch_writes/10000_rows_2.9MB",
            "value": 12894564,
            "range": "± 112531",
            "unit": "ns/iter"
          },
          {
            "name": "single_batch_writes/100000_rows_33.0MB",
            "value": 120896385,
            "range": "± 2969946",
            "unit": "ns/iter"
          },
          {
            "name": "multi_batch_writes/2_batches_2000_rows",
            "value": 4235319,
            "range": "± 14193",
            "unit": "ns/iter"
          },
          {
            "name": "multi_batch_writes/5_batches_5000_rows",
            "value": 8680299,
            "range": "± 33546",
            "unit": "ns/iter"
          },
          {
            "name": "multi_batch_writes/10_batches_10000_rows",
            "value": 16106456,
            "range": "± 118223",
            "unit": "ns/iter"
          },
          {
            "name": "multi_batch_writes/20_batches_20000_rows",
            "value": 31402087,
            "range": "± 245709",
            "unit": "ns/iter"
          },
          {
            "name": "writer/creation",
            "value": 902956,
            "range": "± 133513",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_writes/2_writers",
            "value": 2373903,
            "range": "± 45843",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_writes/4_writers",
            "value": 3441156,
            "range": "± 80583",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent_writes/8_writers",
            "value": 6576949,
            "range": "± 112358",
            "unit": "ns/iter"
          },
          {
            "name": "schema_transform/transform_trace_v1_to_v2",
            "value": 599731,
            "range": "± 4739",
            "unit": "ns/iter"
          },
          {
            "name": "compactor/rewrite_6_files",
            "value": 20770147,
            "range": "± 244849",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_unbounded",
            "value": 30150762,
            "range": "± 1572183",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_cold_without_cache",
            "value": 28429781,
            "range": "± 457278",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_cold_with_cache",
            "value": 28503880,
            "range": "± 723813",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_warm_with_cache",
            "value": 28258691,
            "range": "± 173170",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_windowed",
            "value": 6734229,
            "range": "± 48915",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_lookup_by_id_via_index",
            "value": 16634406,
            "range": "± 236937",
            "unit": "ns/iter"
          },
          {
            "name": "querier_read/trace_search_groups",
            "value": 35489558,
            "range": "± 591965",
            "unit": "ns/iter"
          },
          {
            "name": "querier_service/find_trace_by_id",
            "value": 30117589,
            "range": "± 1591237",
            "unit": "ns/iter"
          },
          {
            "name": "querier_service/find_trace_by_id_hinted",
            "value": 6765658,
            "range": "± 75531",
            "unit": "ns/iter"
          },
          {
            "name": "querier_service/search_traces_recent",
            "value": 74101581,
            "range": "± 1923861",
            "unit": "ns/iter"
          },
          {
            "name": "querier_service/promql_range_avg_by_service",
            "value": 131004404,
            "range": "± 3387162",
            "unit": "ns/iter"
          },
          {
            "name": "querier_service/logql_line_filter",
            "value": 142717110,
            "range": "± 2783716",
            "unit": "ns/iter"
          },
          {
            "name": "trace_index_scaling/10000",
            "value": 1017812,
            "range": "± 28607",
            "unit": "ns/iter"
          },
          {
            "name": "trace_index_scaling/100000",
            "value": 1037738,
            "range": "± 15557",
            "unit": "ns/iter"
          },
          {
            "name": "trace_index_scaling/1000000",
            "value": 1097143,
            "range": "± 11168",
            "unit": "ns/iter"
          }
        ]
      }
    ]
  }
}