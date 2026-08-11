## 1. UserMenu Component

- [x] 1.1 Write UserMenu.test.tsx with failing tests (unauthenticated renders nothing, avatar initials, display name, popover opens, user info, theme toggle, nav links, sign out, Escape closes, backdrop closes)
- [x] 1.2 Write UserMenu.css with design token styling (trigger, avatar, popover, info, items, actions, backdrop)
- [x] 1.3 Write UserMenu.tsx component (useQuery whoami, initials helper, popover with backdrop/Escape, theme toggle, nav links, sign out)
- [x] 1.4 Fix corrupted z-index value in UserMenu.css (replace `数和41` with `41`)
- [x] 1.5 Integrate UserMenu into TopBar.tsx (import and render after Manage link)
- [x] 1.6 Adjust TopBar.css layout for UserMenu placement (ensure flex spacer pushes UserMenu right)
- [x] 1.7 Run `pnpm ui:test` to verify all UserMenu tests pass

## 2. SelectTenant Page

- [x] 2.1 Write SelectTenant.test.tsx with failing tests (shows tenant list, current tenant expanded, datasets visible, click dataset navigates, lazy-fetch on expand, redirect when unauthenticated)
- [x] 2.2 Write SelectTenant.tsx component (whoami query, collapsible tenant rows, dataset selection, lazy whoami(tenant_id) on expand, navigate on dataset click)
- [x] 2.3 Write SelectTenant.css (panel layout, tenant rows, expand/collapse, dataset rows, active highlight)
- [x] 2.4 Run `pnpm ui:test` to verify SelectTenant tests pass

## 3. ApiKeys Page

- [x] 3.1 Write ApiKeys.test.tsx with failing tests (redirect non-admin, list keys, create key shows secret modal, revoke key, revoked key styling, secret modal dismiss)
- [x] 3.2 Write ApiKeys.tsx component (admin auth guard, create form with scopes, key list sorted by created_at, revoke action, secret modal with copy)
- [x] 3.3 Write ApiKeys.css (form, key list, revoked styling, secret modal, code block)
- [x] 3.4 Run `pnpm ui:test` to verify ApiKeys tests pass

## 4. Instrumentation Page

- [x] 4.1 Write Instrumentation.test.tsx with failing tests (shows 6 sources, OTel SDK default, source switch updates content, snippets include tenant data, copy button, verification section)
- [x] 4.2 Write Instrumentation.tsx component (source selector sidebar, content area with per-source instructions + snippets, verification status, copy-to-clipboard)
- [x] 4.3 Write Instrumentation.css (two-column layout, sidebar, content panel, code blocks, verification section)
- [x] 4.4 Run `pnpm ui:test` to verify Instrumentation tests pass

## 5. Route Updates

- [x] 5.1 Add SelectTenantRoute import and route to routes.tsx (`/select-tenant`)
- [x] 5.2 Add ApiKeysRoute import and route to routes.tsx (`/api-keys`) with admin guard
- [x] 5.3 Add InstrumentationRoute import and route to routes.tsx (`/instrumentation`)
- [x] 5.4 Run `pnpm ui:build` to verify compilation succeeds
- [x] 5.5 Run `pnpm ui:test` to verify all tests pass

## 6. Documentation

- [x] 6.1 Update `docs/users/explore-ui.md` to document UserMenu, SelectTenant, ApiKeys, and Instrumentation pages
- [x] 6.2 Verify doc freshness with doc-freshness gate
