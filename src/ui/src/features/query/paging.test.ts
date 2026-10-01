import { describe, expect, it } from "vitest";

import type { QueryIrRequest, QueryIrResponse } from "../../api/gen";
import { ROWS_PAGE_SIZE, mergePages, pagedDocument } from "./paging";

const doc: QueryIrRequest = {
  irVersion: 1,
  from: "logs",
  range: { from: "now-1h", to: "now" },
  result: "rows",
  pipeline: [],
};

function page(rows: unknown[][], next?: string): QueryIrResponse {
  return {
    result: "rows",
    window: { start_ns: 0, end_ns: 1 },
    columns: [{ name: "body", type: "string" }],
    rows,
    page: next ? { next_cursor: next } : {},
  };
}

describe("pagedDocument", () => {
  it("adds a page and raises the version to the one that carries it", () => {
    expect(pagedDocument(doc)).toEqual({
      ...doc,
      irVersion: 14,
      page: { size: ROWS_PAGE_SIZE },
    });
    expect(pagedDocument(doc, "c1").page).toEqual({
      size: ROWS_PAGE_SIZE,
      cursor: "c1",
    });
  });
});

describe("mergePages", () => {
  it("concatenates rows and keeps the last page's cursor", () => {
    const merged = mergePages([page([["a"]], "c1"), page([["b"]])]);
    expect(merged?.rows).toEqual([["a"], ["b"]]);
    expect(merged?.page?.next_cursor).toBeUndefined();
    expect(mergePages([])).toBeUndefined();
  });
});
