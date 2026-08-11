import { describe, expect, it } from "vitest";
import { compareValues, sortRows } from "./sortTable";

describe("compareValues", () => {
  it("compares numbers numerically", () => {
    expect(compareValues(1, 2)).toBeLessThan(0);
    expect(compareValues(2, 1)).toBeGreaterThan(0);
    expect(compareValues(1, 1)).toBe(0);
  });

  it("compares strings lexicographically when either side is a string", () => {
    expect(compareValues("a", "b")).toBeLessThan(0);
    expect(compareValues("10", "9")).toBeLessThan(0);
  });

  it("falls back to string comparison for mixed bigint/number pairs", () => {
    expect(compareValues(10n, 9)).toBeGreaterThan(0);
  });
});

describe("sortRows", () => {
  const rows = [
    { k: "b", n: 2 },
    { k: "a", n: 3 },
    { k: "c", n: 1 },
  ];
  const value = (row: (typeof rows)[number], key: string) =>
    key === "k" ? row.k : row.n;

  it("sorts ascending by the given key without mutating the input", () => {
    const sorted = sortRows(rows, { key: "n", dir: "asc" }, value);
    expect(sorted.map((r) => r.n)).toEqual([1, 2, 3]);
    expect(rows.map((r) => r.n)).toEqual([2, 3, 1]);
  });

  it("sorts descending by the given key", () => {
    const sorted = sortRows(rows, { key: "k", dir: "desc" }, value);
    expect(sorted.map((r) => r.k)).toEqual(["c", "b", "a"]);
  });
});
