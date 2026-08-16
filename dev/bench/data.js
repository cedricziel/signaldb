window.BENCHMARK_DATA = {
  "lastUpdate": 1786883695493,
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
      }
    ]
  }
}