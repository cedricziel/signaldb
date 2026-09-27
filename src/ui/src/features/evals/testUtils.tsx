// Shared scaffolding for the Evaluate page tests. Each test file still
// declares its own `vi.mock("../../api/queryIr", …)` (mocks are hoisted per
// file); this module drives that mock.
import { QueryClientProvider } from "@tanstack/react-query";
import { render } from "@testing-library/react";
import type { ComponentType } from "react";
import { MemoryRouter } from "react-router";
import { vi } from "vitest";
import * as queryIrApi from "../../api/queryIr";
import { testQueryClient } from "../../lib/queryClient";
import type { ShellContext } from "../../lib/outletState";
import { DEFAULT_STATE, type ExploreState } from "../../lib/urlState";
import { withColumns, type IrDoc } from "./evalFixtures";

export type { IrDoc };

/** A `table` response; its columns are filled in from the request by
 * {@link mockIr}. */
export function table(rows: unknown[][]) {
  return { result: "table", window: { start_ns: 0, end_ns: 0 }, rows };
}

/** A `rows` response, columns likewise filled in by {@link mockIr}. */
export function rows(cells: unknown[][]) {
  return { result: "rows", window: { start_ns: 0, end_ns: 0 }, rows: cells };
}

export function aggBy(doc: IrDoc): string[] {
  return doc.pipeline?.find((s) => s.aggregate)?.aggregate?.by ?? [];
}

export function wherePred(
  doc: IrDoc,
  field: string,
): { op?: string; value?: unknown } | undefined {
  return doc.pipeline?.find((s) => s.where?.field === field)?.where;
}

/** The run ids a request is scoped to (`resultsWhere`'s `runIds`). */
export function runIdsOf(doc: IrDoc): string[] {
  return (wherePred(doc, "signaldb.eval.run_id")?.value as string[]) ?? [];
}

/** Answers every IR request with `handler`, adding the columns the
 * request's shape implies. */
export function mockIr(handler: (doc: IrDoc) => unknown) {
  vi.mocked(queryIrApi.runIrQuery).mockImplementation(
    async (raw) =>
      withColumns(raw, handler(raw as IrDoc)) as Awaited<
        ReturnType<typeof queryIrApi.runIrQuery>
      >,
  );
}

/** Renders an Evaluate page with `state` over the defaults; returns its
 * `update` mock. */
export function renderEvalView(
  View: ComponentType<ShellContext>,
  state: Partial<ExploreState> = {},
) {
  const update = vi.fn();
  render(
    <QueryClientProvider client={testQueryClient()}>
      <MemoryRouter>
        <View state={{ ...DEFAULT_STATE, ...state }} update={update} />
      </MemoryRouter>
    </QueryClientProvider>,
  );
  return update;
}

/** One run as `buildRunsDoc` groups it: a passing group plus, when it has
 * errors, an errored one. Columns: run identity, label, error, then
 * n/high/low/score_sum/scored, first, last, unlinked. */
export function runRows(r: {
  id: string;
  set: string;
  version: string;
  firstMs: number;
  lastMs: number;
  n: number;
  errors?: number;
  unlinked?: number;
}): unknown[][] {
  const id = [r.id, r.set, "support-triage", null, r.version, null];
  const span = [r.firstMs * 1e6, r.lastMs * 1e6];
  const errors = r.errors ?? 0;
  const out = [
    [...id, "pass", null, r.n - errors, 0, 0, 0, 0, ...span, r.unlinked ?? 0],
  ];
  if (errors)
    out.push([...id, null, "timeout", errors, 0, 0, 0, 0, ...span, 0]);
  return out;
}
