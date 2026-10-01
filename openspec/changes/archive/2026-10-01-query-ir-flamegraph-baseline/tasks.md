## 1. IR

- [x] 1.1 `baseline` on the document, gated at `irVersion` 13, flamegraph-only, bounds coercible to timestamps
- [x] 1.2 Querier: read both windows and encode a `DiffFlamegraph`
- [x] 1.3 Router: `baseline` on the request; `baseline_total`/`comparison_total` on the flamegraph result
- [x] 1.4 Regenerate the OpenAPI document, SDK and UI types
- [x] 1.5 Document `baseline` in `docs/users/querying-ir.md`
- [x] 1.6 Reject an inverted baseline; read the two windows sequentially
