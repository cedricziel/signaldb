import { describe, expect, it } from "vitest";
import { summarizeAttributes, type SummaryField } from "./attrSummary";

const FIELDS: SummaryField[] = [
  { keys: ["service_name", "service.name"] },
  { keys: ["level"] },
  {
    keys: ["telemetry.sdk.language", "telemetry.sdk.version"],
    render: (found) => ({
      key: "sdk",
      value: [
        found.get("telemetry.sdk.language"),
        found.get("telemetry.sdk.version"),
      ]
        .filter(Boolean)
        .join(" "),
    }),
  },
];

describe("summarizeAttributes", () => {
  it("returns one part per curated field found, in field order", () => {
    const entries: [string, string][] = [
      ["service_name", "checkout"],
      ["level", "info"],
    ];
    expect(summarizeAttributes(entries, FIELDS)).toEqual({
      parts: [
        { key: "service_name", value: "checkout" },
        { key: "level", value: "info" },
      ],
      more: 0,
    });
  });

  it("prefers the first present spelling in a field's key list", () => {
    const entries: [string, string][] = [["service.name", "checkout"]];
    expect(summarizeAttributes(entries, FIELDS)).toEqual({
      parts: [{ key: "service.name", value: "checkout" }],
      more: 0,
    });
  });

  it("counts an alternate spelling the default render hides toward more", () => {
    const entries: [string, string][] = [
      ["service_name", "checkout"],
      ["service.name", "checkout"],
    ];
    expect(summarizeAttributes(entries, FIELDS)).toEqual({
      parts: [{ key: "service_name", value: "checkout" }],
      more: 1,
    });
  });

  it("combines a multi-key field into one part via its render function", () => {
    const entries: [string, string][] = [
      ["telemetry.sdk.language", "go"],
      ["telemetry.sdk.version", "1.28.0"],
    ];
    expect(summarizeAttributes(entries, FIELDS)).toEqual({
      parts: [{ key: "sdk", value: "go 1.28.0" }],
      more: 0,
    });
  });

  it("counts attributes not surfaced as a part in more", () => {
    const entries: [string, string][] = [
      ["service_name", "checkout"],
      ["k8s.pod.name", "checkout-7"],
      ["cloud.region", "us-east-1"],
    ];
    expect(summarizeAttributes(entries, FIELDS)).toEqual({
      parts: [{ key: "service_name", value: "checkout" }],
      more: 2,
    });
  });

  it("returns no parts and zero more for an empty input", () => {
    expect(summarizeAttributes([], FIELDS)).toEqual({ parts: [], more: 0 });
  });

  it("does not double-count a multi-key field's keys toward more", () => {
    const entries: [string, string][] = [
      ["telemetry.sdk.language", "go"],
      ["telemetry.sdk.version", "1.28.0"],
    ];
    expect(summarizeAttributes(entries, FIELDS).more).toBe(0);
  });
});
