import { describe, expect, it } from "vitest";
import { diffLines } from "./lineDiff";

describe("diffLines", () => {
  it("diffs small payloads via LCS", () => {
    expect(diffLines("a\nb\nc", "a\nx\nc")).toEqual([
      { kind: "same", text: "a" },
      { kind: "removed", text: "b" },
      { kind: "added", text: "x" },
      { kind: "same", text: "c" },
    ]);
  });

  it("falls back to a linear diff for very large inputs instead of hanging", () => {
    const a = Array.from({ length: 2000 }, (_, i) => `a${i}`).join("\n");
    const b = Array.from({ length: 2000 }, (_, i) => `b${i}`).join("\n");

    const start = Date.now();
    const result = diffLines(a, b);
    expect(Date.now() - start).toBeLessThan(2000);

    expect(result).toHaveLength(4000);
    expect(result.slice(0, 2000).every((line) => line.kind === "removed")).toBe(
      true,
    );
    expect(result.slice(2000).every((line) => line.kind === "added")).toBe(
      true,
    );
  }, 10_000);

  it("runs LCS only over the changed middle, so a long payload with one edit stays exact", () => {
    const lines = Array.from({ length: 3000 }, (_, i) => `line${i}`);
    const edited = [...lines];
    edited[1500] = "changed";

    const changed = diffLines(lines.join("\n"), edited.join("\n")).filter(
      (line) => line.kind !== "same",
    );

    expect(changed).toEqual([
      { kind: "removed", text: "line1500" },
      { kind: "added", text: "changed" },
    ]);
  });
});
