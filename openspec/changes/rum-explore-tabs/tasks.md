Each numbered group is one PR in a stack (under ~500 changed lines each).
Builds on the shipped `2026-09-28-real-user-monitoring` change (see
proposal — Why).

## 1. Network + frontend → backend

- [x] 1.1 Failing tests for the `correlate`-based client/backend split and traced-share decoders.
- [x] 1.2 Network tab (requests table, untraced-origin callout, resources by initiator type) and the Overview "Frontend → backend" panel.
- [x] 1.3 Overview: users and traced-request KPIs; Setup: "Requests joined to traces" checklist step.

## 2. Pages + Interactions

- [x] 2.1 Failing tests for per-route vitals and navigation-timing breakdown.
- [x] 2.2 Pages tab (route list sorted by poor share, route detail, load breakdown, backend calls) with `?route=`; missing-route callout; Overview "Slowest pages" linking into it.
- [x] 2.3 Failing tests for click-target aggregation; Interactions tab (clicks by target, INP p75 of the page).

## 3. Sessions + session detail

- [x] 3.1 Failing tests for session list aggregation/filters and for merging a session's spans and logs into lanes and events.
- [x] 3.2 Sessions tab with quick filters and attribute filter; `?session=` detail with lane timeline, event list, inline trace waterfall (reusing `lib/waterfall.ts`), exception panel with preceding failed request, attributes.
- [x] 3.3 Palette: jump to a pasted session id.

## 4. Errors

- [x] 4.1 Failing tests for app-scoped error groups, "new in release" and the preceding-failed-request lookup (one batched read for the list, not per row).
- [ ] 4.2 Errors tab reusing `api/errors.ts` scoped by `service.name`, stack frames, by-browser breakdown, backend cause, latest session link; Overview "Top errors" rows link into it.

## 5. Platform labels + docs

- [ ] 5.1 Failing tests for platform detection from `telemetry.sdk.language`; relabel tabs and hide browser-only panels for mobile.
- [ ] 5.2 Switching apps clears the selected route, error group and session.
- [ ] 5.3 Docs: extend the "Real users" section in `docs/users/explore-ui.md` per tab; a user guide for instrumenting a browser app (routed via the docs skill), linked from the Setup tab.
