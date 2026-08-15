## 1. URL state

- [x] 1.1 Failing tests in `lib/urlState.test.ts`: `buildPath` for catalog
      (`/catalog`, `/catalog/service`, `/catalog/service/checkout,shop`,
      `/catalog/service/checkout,shop/GET%20%2Fhealth`), reserved-character
      and not-set round trips, query params `entity/primary/secondary` no
      longer emitted or parsed
- [x] 1.2 Implement `encodeCatalogSegment`/`decodeCatalogSegment` and the
      catalog branch of `buildPath`; parse from the pathname in
      `useExploreState`; drop the query params

## 2. Routes and views

- [x] 2.1 Failing App/route tests: `/catalog/host/db-01` renders the host
      detail; drilling from the list pushes `/catalog/service/<id>` and back
      returns to the list; `/catalog?entity=service&primary=x` shows the
      default list
- [x] 2.2 Add `catalog/:entity`, `catalog/:entity/:primary`,
      `catalog/:entity/:primary/:secondary` routes; catalog views unchanged
      otherwise

## 3. Docs

- [x] 3.1 `docs/users/explore-ui.md`: catalog URLs; doc-freshness gate clean
- [x] 3.2 UI suite, tsc, eslint clean
