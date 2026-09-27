import { describe, expect, it } from "vitest";
import { DEFAULT_STATE } from "../../lib/urlState";
import { currentPageFor, NAV_GROUPS, pageHref } from "./navModel";

describe("currentPageFor", () => {
  it("maps a page's own path and its deep links to the page", () => {
    expect(currentPageFor("/logs")).toEqual({
      id: "logs",
      group: "Investigate",
      label: "Logs",
    });
    expect(currentPageFor("/traces/abc123").id).toBe("traces");
    expect(currentPageFor("/catalog/service/checkout").id).toBe("catalog");
    expect(currentPageFor("/rum/overview")).toEqual({
      id: "rum",
      group: "Monitor",
      label: "Real users",
    });
    expect(currentPageFor("/rum/setup").id).toBe("rum");
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

  it("labels admin routes under Admin, highlighting only Manage", () => {
    expect(currentPageFor("/manage")).toEqual({
      id: "manage",
      group: "Admin",
      label: "Manage",
    });
    expect(currentPageFor("/api-keys")).toEqual({
      id: null,
      group: "Admin",
      label: "API keys",
    });
  });

  it("has nothing to highlight for an unknown path", () => {
    expect(currentPageFor("/nope")).toEqual({ id: null, group: "", label: "" });
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

  it("links configure pages bare", () => {
    expect(pageHref(page("schema"), state)).toBe("/schema");
  });
});
