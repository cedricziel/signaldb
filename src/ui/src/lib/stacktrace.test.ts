import { describe, expect, it } from "vitest";
import { parseStacktraceLines } from "./stacktrace";

describe("parseStacktraceLines", () => {
  it("marks the first line as the header, regardless of leading whitespace", () => {
    const lines = parseStacktraceLines(
      "ReferenceError: useFlamegraphRender is not defined\n" +
        "    at CompareView (http://localhost/ProfilesView.tsx:375:24)",
    );
    expect(lines[0]).toEqual({
      kind: "header",
      text: "ReferenceError: useFlamegraphRender is not defined",
    });
  });

  it("strips leading whitespace from frame lines (the renderer supplies its own hanging indent)", () => {
    const lines = parseStacktraceLines(
      "TypeError: boom\n    at foo (a.js:1:1)\n\tat bar (b.js:2:2)",
    );
    expect(lines[1]).toEqual({ kind: "frame", text: "at foo (a.js:1:1)" });
    expect(lines[2]).toEqual({ kind: "frame", text: "at bar (b.js:2:2)" });
  });

  it("classifies a frame whose location is inside a dependency as vendor", () => {
    const lines = parseStacktraceLines(
      "Error: boom\n" +
        "    at foo (http://localhost/src/app.ts:1:1)\n" +
        "    at bar (http://localhost/node_modules/react-dom/index.js:2:2)",
    );
    expect(lines[1]).toEqual({
      kind: "frame",
      text: "at foo (http://localhost/src/app.ts:1:1)",
    });
    expect(lines[2]).toEqual({
      kind: "vendor",
      text: "at bar (http://localhost/node_modules/react-dom/index.js:2:2)",
    });
  });

  it("returns a single header line for a one-line stacktrace", () => {
    expect(parseStacktraceLines("boom")).toEqual([
      { kind: "header", text: "boom" },
    ]);
  });

  it("drops trailing blank lines", () => {
    const lines = parseStacktraceLines("Error: boom\n  at foo (a.js:1:1)\n\n");
    expect(lines).toHaveLength(2);
  });
});
