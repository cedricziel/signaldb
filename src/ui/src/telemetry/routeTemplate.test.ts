import { describe, expect, it } from "vitest";
import { buildRouteTemplate } from "./routeTemplate";

describe("buildRouteTemplate", () => {
  it("templates a high-cardinality param (trace id)", () => {
    expect(
      buildRouteTemplate("/traces/7c1e9f2a3b4c5d6e", {
        traceId: "7c1e9f2a3b4c5d6e",
      }),
    ).toBe("/traces/:traceId");
  });

  it("templates catalog routes, keeping the low-cardinality entity literal", () => {
    expect(
      buildRouteTemplate("/catalog/service/checkout/us-east", {
        entity: "service",
        primary: "checkout",
        secondary: "us-east",
      }),
    ).toBe("/catalog/service/:primary/:secondary");
  });

  it("templates a param whose value is URL-encoded in the pathname", () => {
    expect(
      buildRouteTemplate("/catalog/database/jobradar%2Cpostgresql", {
        entity: "database",
        primary: "jobradar,postgresql",
      }),
    ).toBe("/catalog/database/:primary");
  });

  it("templates catalog routes with only entity and primary", () => {
    expect(
      buildRouteTemplate("/catalog/service/checkout", {
        entity: "service",
        primary: "checkout",
      }),
    ).toBe("/catalog/service/:primary");
  });

  it("templates schema convention routes, keeping the low-cardinality kind literal", () => {
    expect(
      buildRouteTemplate(
        "/schema/conventions/io.signaldb/1.2.0/attributes/http.method",
        {
          ns: "io.signaldb",
          version: "1.2.0",
          kind: "attributes",
          name: "http.method",
        },
      ),
    ).toBe("/schema/conventions/:ns/:version/attributes/:name");
  });

  it("keeps a low-cardinality signal route literal", () => {
    expect(buildRouteTemplate("/logs", { signal: "logs" })).toBe("/logs");
  });

  it("keeps another low-cardinality signal route literal", () => {
    expect(buildRouteTemplate("/traces", { signal: "traces" })).toBe("/traces");
  });

  it("templates an unknown param the same as any other high-cardinality one", () => {
    expect(
      buildRouteTemplate("/processors/my-processor/edit", {
        name: "my-processor",
      }),
    ).toBe("/processors/:name/edit");
  });

  it("collapses a splat into a single wildcard segment", () => {
    expect(
      buildRouteTemplate("/unknown/path/here", { "*": "unknown/path/here" }),
    ).toBe("/*");
  });

  it("returns the pathname unchanged when there are no params", () => {
    expect(buildRouteTemplate("/overview", {})).toBe("/overview");
  });

  it("ignores undefined and empty param values", () => {
    expect(
      buildRouteTemplate("/catalog/service", {
        entity: "service",
        primary: undefined,
      }),
    ).toBe("/catalog/service");
  });
});
