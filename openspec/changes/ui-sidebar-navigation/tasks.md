## 1. Navigation model

- [x] 1.1 Tests for page grouping, current-page resolution and cross-page links (`navModel.test.ts`).
- [x] 1.2 `features/shell/navModel.ts` and the nav icon set.

## 2. Shell

- [x] 2.1 Update `App.test.tsx` and `e2e/navigation.spec.ts` to navigate through the sidebar instead of tabs.
- [x] 2.2 Sidebar with tenant/dataset switcher, grouped pages, Manage, account and collapse toggle; replace `TopBar` in `App.tsx`.
- [x] 2.3 Collapse persistence and the `[` shortcut (`AppNav.test.tsx`).
- [x] 2.4 Page header with breadcrumb and palette trigger.
- [x] 2.5 Mobile top bar and drawer below 720px.
- [x] 2.6 Remove the signal tab strip from `ExploreView`.

## 3. Command palette

- [x] 3.1 Tests for grouping, caps and trace-id detection (`paletteModel.test.ts`), and for recent-query storage (`recentQueries.test.ts`).
- [x] 3.2 `CommandPalette` with pages, catalog services, recent queries, actions and trace-id jump; keyboard navigation.
- [x] 3.3 Record logs/traces queries as recent queries.

## 4. Docs

- [x] 4.1 `docs/users/explore-ui.md`: Navigation and Command palette sections; drop tab-strip and top-bar references.
