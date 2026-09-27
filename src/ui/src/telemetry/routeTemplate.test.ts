import { describe, expect, it } from "vitest";
import { routeTemplateFor } from "../routes";
import { buildRouteTemplate } from "./routeTemplate";

describe("buildRouteTemplate", () => {
  it("joins the matched route paths", () => {
    expect(buildRouteTemplate(["/", "traces/:traceId"], {})).toBe(
      "/traces/:traceId",
    );
  });

  it("fills in low-cardinality params and keeps the rest as placeholders", () => {
    expect(
      buildRouteTemplate(["/", "catalog/:entity/:primary"], {
        entity: "service",
        primary: "checkout",
      }),
    ).toBe("/catalog/service/:primary");
  });

  it("skips pathless layout routes", () => {
    expect(
      buildRouteTemplate([undefined, "/", undefined, ":signal"], {
        signal: "logs",
      }),
    ).toBe("/logs");
  });
});

describe("routeTemplateFor", () => {
  it.each([
    ["/logs", "/logs"],
    ["/traces", "/traces"],
    ["/traces/7c1e9f2a3b4c5d6e", "/traces/:traceId"],
    ["/catalog/database/jobradar%2Cpostgresql", "/catalog/database/:primary"],
    [
      "/schema/conventions/io.signaldb/1.2.0/attributes/http.method",
      "/schema/conventions/:ns/:version/attributes/:name",
    ],
    ["/processors/my-processor/edit", "/processors/:name/edit"],
    ["/overview", "/overview"],
    ["/nope/deeper", "/*"],
  ])("%s → %s", (pathname, template) => {
    expect(routeTemplateFor(pathname)).toBe(template);
  });
});
