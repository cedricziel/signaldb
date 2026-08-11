import { describe, expect, it } from "vitest";
import { describeService, groupSpanAttributes } from "./spanAttributes";

describe("groupSpanAttributes", () => {
  it("splits resource.-prefixed keys into a Resource group, stripping the prefix", () => {
    const groups = groupSpanAttributes({
      target_element: "BUTTON",
      "resource.service.name": "signaldb-ui",
    });
    expect(groups).toEqual([
      { label: "Span", entries: [["target_element", "BUTTON"]] },
      { label: "Resource", entries: [["service.name", "signaldb-ui"]] },
    ]);
  });

  it("sorts entries alphabetically by key within each group", () => {
    const groups = groupSpanAttributes({
      "url.full": "https://example.com",
      "session.id": "abc",
      "resource.telemetry.sdk.version": "2.1.0",
      "resource.service.name": "signaldb-ui",
    });
    expect(
      groups.find((g) => g.label === "Span")?.entries.map(([k]) => k),
    ).toEqual(["session.id", "url.full"]);
    expect(
      groups.find((g) => g.label === "Resource")?.entries.map(([k]) => k),
    ).toEqual(["service.name", "telemetry.sdk.version"]);
  });

  it("omits a group with no entries instead of rendering it empty", () => {
    expect(
      groupSpanAttributes({ "resource.service.name": "signaldb-ui" }),
    ).toEqual([
      { label: "Resource", entries: [["service.name", "signaldb-ui"]] },
    ]);
    expect(groupSpanAttributes({ target_element: "BUTTON" })).toEqual([
      { label: "Span", entries: [["target_element", "BUTTON"]] },
    ]);
    expect(groupSpanAttributes({})).toEqual([]);
  });
});

describe("describeService", () => {
  it("compiles namespace, environment, and version onto the service name", () => {
    expect(
      describeService("checkout", {
        "resource.service.namespace": "payments-team",
        "resource.service.version": "1.4.0",
        "resource.deployment.environment.name": "production",
      }),
    ).toBe("checkout (payments-team) · production · v1.4.0");
  });

  it("falls back to the bare service name when no resource attributes are present", () => {
    expect(describeService("checkout", {})).toBe("checkout");
  });

  it("omits pieces that are individually absent", () => {
    expect(
      describeService("checkout", {
        "resource.deployment.environment.name": "staging",
      }),
    ).toBe("checkout · staging");
  });
});
