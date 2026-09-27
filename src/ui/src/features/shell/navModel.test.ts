import { describe, expect, it } from "vitest";
import { DEFAULT_STATE } from "../../lib/urlState";
import {
  currentPageFor,
  NAV_GROUPS,
  pageHref,
  visibleNavGroups,
} from "./navModel";

describe("currentPageFor", () => {
  it("maps a page's own path and its deep links to the page", () => {
    expect(currentPageFor("/logs")).toEqual({
      id: "logs",
      group: "Investigate",
      label: "Logs",
    });
    expect(currentPageFor("/traces/abc123").id).toBe("traces");
    expect(currentPageFor("/catalog/service/checkout").id).toBe("catalog");
    expect(currentPageFor("/schema/conventions/new")).toMatchObject({
      id: "schema",
      group: "Configure",
    });
  });

  it("matches the longest path prefix, so nested Evaluate pages keep their own item", () => {
    expect(currentPageFor("/evals")).toEqual({
      id: "evals",
      group: "Evaluate",
      label: "Agents & scores",
    });
    expect(currentPageFor("/evals/runs").id).toBe("runs");
    expect(currentPageFor("/evals/compare/case").id).toBe("compare");
    expect(currentPageFor("/evalsx").id).toBeNull();
  });

  it("files the admin pages under Settings, each with its own item", () => {
    expect(currentPageFor("/manage")).toEqual({
      id: "manage",
      group: "Settings",
      label: "Manage",
    });
    expect(currentPageFor("/api-keys")).toEqual({
      id: "api-keys",
      group: "Settings",
      label: "API keys",
    });
    expect(currentPageFor("/integrations/github")).toEqual({
      id: "integrations",
      group: "Settings",
      label: "Integrations",
    });
  });

  it("names the send-data page Send data", () => {
    expect(currentPageFor("/instrumentation")).toEqual({
      id: "instrumentation",
      group: "Configure",
      label: "Send data",
    });
  });

  it("gives tenant selection a crumb with no group", () => {
    expect(currentPageFor("/select-tenant")).toEqual({
      id: null,
      group: "",
      label: "Switch tenant",
    });
  });

  it("has nothing to highlight for an unknown path", () => {
    expect(currentPageFor("/nope")).toEqual({ id: null, group: "", label: "" });
  });
});

describe("visibleNavGroups", () => {
  const titles = (opts: { canManage: boolean; isDemo: boolean }) =>
    visibleNavGroups(opts).map((g) => g.title);
  const ids = (opts: { canManage: boolean; isDemo: boolean }) =>
    visibleNavGroups(opts).flatMap((g) => g.pages.map((p) => p.id));

  it("shows Settings, with Manage, API keys and Integrations, only to managers", () => {
    expect(titles({ canManage: true, isDemo: false })).toEqual([
      "Monitor",
      "Investigate",
      "Evaluate",
      "Configure",
      "Settings",
    ]);
    expect(
      visibleNavGroups({ canManage: true, isDemo: false }).at(-1)!.pages.map(
        (p) => [p.label, p.path],
      ),
    ).toEqual([
      ["Manage", "/manage"],
      ["API keys", "/api-keys"],
      ["Integrations", "/integrations/github"],
    ]);
    expect(titles({ canManage: false, isDemo: false })).not.toContain(
      "Settings",
    );
  });

  it("hides the mutating Schema and Processors pages in demo mode", () => {
    expect(ids({ canManage: false, isDemo: false })).toEqual(
      expect.arrayContaining(["schema", "processors", "instrumentation"]),
    );
    const demo = ids({ canManage: false, isDemo: true });
    expect(demo).not.toContain("schema");
    expect(demo).not.toContain("processors");
    expect(demo).toContain("instrumentation");
  });
});

describe("pageHref", () => {
  const state = {
    ...DEFAULT_STATE,
    search: "boom",
    tenant: "acme",
    dataset: "prod",
  };
  const page = (id: string) =>
    NAV_GROUPS.flatMap((g) => g.pages).find((p) => p.id === id)!;

  it("carries the window and tenant context, not view state, to explore pages", () => {
    expect(pageHref(page("traces"), state)).toBe(
      "/traces?tenant=acme&dataset=prod",
    );
  });

  it("carries the window and tenant context to Evaluate pages", () => {
    expect(pageHref(page("runs"), state)).toBe(
      "/evals/runs?tenant=acme&dataset=prod",
    );
  });

  it("links configure and settings pages bare", () => {
    expect(pageHref(page("schema"), state)).toBe("/schema");
    expect(pageHref(page("api-keys"), state)).toBe("/api-keys");
  });
});
