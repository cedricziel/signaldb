window.BENCHMARK_DATA = {
  "lastUpdate": 1787286271796,
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
      }
    ]
  }
}