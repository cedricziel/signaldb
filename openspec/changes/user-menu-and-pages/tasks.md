## 1. UserMenu Component

- [ ] 1.1 Write UserMenu.test.tsx with failing tests (unauthenticated renders nothing, avatar initials, display name, popover opens, user info, theme toggle, nav links, sign out, Escape closes, backdrop closes)
- [ ] 1.2 Write UserMenu.css with design token styling (trigger, avatar, popover, info, items, actions, backdrop)
- [ ] 1.3 Write UserMenu.tsx component (useQuery whoami, initials helper, popover with backdrop/Escape, theme toggle, nav links, sign out)
- [ ] 1.4 Fix corrupted z-index value in UserMenu.css (replace `数和41` with `41`)
- [ ] 1.5 Integrate UserMenu into TopBar.tsx (import and render after Manage link)
- [ ] 1.6 Adjust TopBar.css layout for UserMenu placement (ensure flex spacer pushes UserMenu right)
- [ ] 1.7 Run `pnpm ui:test` to verify all UserMenu tests pass

## 2. SelectTenant Page

- [ ] 2.1 Write SelectTenant.test.tsx with failing tests (shows tenant list, current tenant expanded, datasets visible, click dataset navigates, lazy-fetch on expand, redirect when unauthenticated)
- [ ] 2.2 Write SelectTenant.tsx component (whoami query, collapsible tenant rows, dataset selection, lazy whoami(tenant_id) on expand, navigate on dataset click)
- [ ] 2.3 Write SelectTenant.css (panel layout, tenant rows, expand/collapse, dataset rows, active highlight)
- [ ] 2.4 Run `pnpm ui:test` to verify SelectTenant tests pass

## 3. ApiKeys Page

- [ ] 3.1 Write ApiKeys.test.tsx with failing tests (redirect non-admin, list keys, create key shows secret modal, revoke key, revoked key styling, secret modal dismiss)
- [ ] 3.2 Write ApiKeys.tsx component (admin auth guard, create form with scopes, key list sorted by created_at, revoke action, secret modal with copy)
- [ ] 3.3 Write ApiKeys.css (form, key list, revoked styling, secret modal, code block)
- [ ] 3.4 Run `pnpm ui:test` to verify ApiKeys tests pass

## 4. Instrumentation Page

- [ ] 4.1 Write Instrumentation.test.tsx with failing tests (shows 6 sources, OTel SDK default, source switch updates content, snippets include tenant data, copy button, verification section)
- [ ] 4.2 Write Instrumentation.tsx component (source selector sidebar, content area with per-source instructions + snippets, verification status, copy-to-clipboard)
- [ ] 4.3 Write Instrumentation.css (two-column layout, sidebar, content panel, code blocks, verification section)
- [ ] 4.4 Run `pnpm ui:test` to verify Instrumentation tests pass

## 5. Route Updates

- [ ] 5.1 Add SelectTenantRoute import and route to routes.tsx (`/select-tenant`)
- [ ] 5.2 Add ApiKeysRoute import and route to routes.tsx (`/api-keys`) with admin guard
- [ ] 5.3 Add InstrumentationRoute import and route to routes.tsx (`/instrumentation`)
- [ ] 5.4 Run `pnpm ui:build` to verify compilation succeeds
- [ ] 5.5 Run `pnpm ui:test` to verify all tests pass

## 6. Documentation

- [ ] 6.1 Update `docs/users/explore-ui.md` to document UserMenu, SelectTenant, ApiKeys, and Instrumentation pages
- [ ] 6.2 Verify doc freshness with doc-freshness gate
