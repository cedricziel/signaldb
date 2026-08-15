## Why

Catalog entity details are addressed by query parameters
(`/catalog?entity=service&primary=signaldb%1Fsignaldb&secondary=…`), unlike
every other drill-down in the UI, which is a route (`/traces/:traceId`,
`/schema/conventions/:ns/:version/...`). That makes entity pages unbookmarkable
in any readable way, hides the entity type, and leaks the internal
unit-separator composite key into URLs. Entity type and identity belong in the
path.

## What Changes

- **BREAKING (UI URLs)**: catalog selection moves from query parameters to the
  path: `/catalog/:entity` (list for an entity type), `/catalog/:entity/:primary`
  (entity detail), `/catalog/:entity/:primary/:secondary` (breakdown row
  drill). `:entity` is the entity type id (`service`, `database`, `host`,
  `k8s_pod`, …); `:primary`/`:secondary` encode the identity values as
  comma-separated percent-encoded segments (`/catalog/service/signaldb,signaldb`).
  The `entity`, `primary`, `secondary` query params are removed (no
  compatibility redirect, per project policy).
- `/catalog` alone keeps meaning "default entity type, list view".
- Tenant/dataset/range stay in the query string as for every other view.

## Capabilities

### New Capabilities

_None._

### Modified Capabilities

- `explore-ui-navigation`: catalog entity type and drilled-into entity are
  URL path segments.

## Impact

- **ui**: `lib/urlState.ts` (path build/parse for catalog), `routes.tsx`,
  `features/catalog/*` (no behaviour change beyond URLs), tests,
  `docs/users/explore-ui.md`.
