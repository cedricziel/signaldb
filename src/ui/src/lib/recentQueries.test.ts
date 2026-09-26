import { afterEach, describe, expect, it } from "vitest";
import {
  loadRecentQueries,
  recentQueryText,
  recordRecentQuery,
} from "./recentQueries";
import { DEFAULT_STATE } from "./urlState";

afterEach(() => localStorage.clear());

describe("recent queries", () => {
  it("puts the newest first and de-duplicates by text and signal", () => {
    recordRecentQuery({ text: "a", signal: "logs", href: "/logs?q=a" });
    recordRecentQuery({ text: "b", signal: "logs", href: "/logs?q=b" });
    recordRecentQuery({ text: "a", signal: "traces", href: "/traces" });
    recordRecentQuery({ text: " a ", signal: "logs", href: "/logs?q=a2" });
    expect(loadRecentQueries()).toEqual([
      { text: "a", signal: "logs", href: "/logs?q=a2" },
      { text: "a", signal: "traces", href: "/traces" },
      { text: "b", signal: "logs", href: "/logs?q=b" },
    ]);
  });

  it("keeps ten at most and ignores blank text", () => {
    for (let i = 0; i < 12; i++) {
      recordRecentQuery({ text: `q${i}`, signal: "logs", href: "/logs" });
    }
    recordRecentQuery({ text: "  ", signal: "logs", href: "/logs" });
    const stored = loadRecentQueries();
    expect(stored).toHaveLength(10);
    expect(stored[0]!.text).toBe("q11");
  });

  it("drops malformed or off-site entries from storage", () => {
    localStorage.setItem(
      "sdb.recentQueries",
      JSON.stringify([
        { text: "ok", signal: "logs", href: "/logs" },
        { text: "evil", signal: "logs", href: "https://example.com" },
        { text: 1, signal: "logs", href: "/logs" },
      ]),
    );
    expect(loadRecentQueries().map((q) => q.text)).toEqual(["ok"]);
    localStorage.setItem("sdb.recentQueries", "{not json");
    expect(loadRecentQueries()).toEqual([]);
  });
});

describe("recentQueryText", () => {
  it("joins logs filters and search", () => {
    expect(
      recentQueryText({
        ...DEFAULT_STATE,
        signal: "logs",
        filters: [{ label: "level", op: "=", value: "error" }],
        search: " timeout ",
      }),
    ).toBe("level=error timeout");
  });

  it("describes traces by their facet filters", () => {
    expect(
      recentQueryText({
        ...DEFAULT_STATE,
        signal: "traces",
        traceFilters: [
          { field: "service.name", value: "checkout" },
          { field: "status", value: "", op: "absent" },
        ],
      }),
    ).toBe("service.name=checkout !status");
  });

  it("is empty for other views", () => {
    expect(recentQueryText({ ...DEFAULT_STATE, signal: "metrics" })).toBe("");
  });
});
