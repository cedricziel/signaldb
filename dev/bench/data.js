window.BENCHMARK_DATA = {
  "lastUpdate": 1787026747085,
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
      }
    ]
  }
}