## Context

The design decisions carry over from
`archive/2026-09-28-real-user-monitoring/design.md`: frontend → backend
split and traced share via `correlate` (decision 3), one read per KPI over
both windows (3a), sessions from both signals, bounded (4), reuse of the
existing trace waterfall, errors grouping and stack-frame source context
(5), and fixtures only in stories (6). Its risks section (vitals without a
route, session detail cost, PR size) applies unchanged.

## Goals / Non-Goals

**Goals:** the Network, Pages, Interactions, Sessions and Errors tabs on
real data, and the Overview/Setup/palette pieces that depend on them.

**Non-Goals:** anything new in the IR or ingest path; the scoped-out items
listed in the proposal.

## Decisions

No new decisions. If the cross-signal `correlate`
(`query-cross-signal-correlate`) lands first, session detail and the Errors
tab's preceding-request lookup use it instead of two bounded reads merged
client-side.
