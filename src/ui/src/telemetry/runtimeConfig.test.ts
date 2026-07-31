import { describe, expect, it } from "vitest";
import { resolveExportConfig, resolveServiceName } from "./runtimeConfig";

describe("resolveExportConfig", () => {
  it("returns null when there is no runtime config and no build-time endpoint", () => {
    expect(resolveExportConfig(undefined, "")).toBeNull();
  });

  it("falls back to the build-time endpoint with no auth headers", () => {
    expect(resolveExportConfig(undefined, "http://collector:4318")).toEqual({
      endpoint: "http://collector:4318",
      headers: {},
    });
  });

  it("uses the runtime endpoint and attaches auth + tenant headers when enabled", () => {
    const resolved = resolveExportConfig(
      {
        telemetry: {
          enabled: true,
          endpoint: "http://signaldb.example:4318",
          apiKey: "sk-ingest",
          tenantId: "_system",
          datasetId: "_monitoring",
        },
      },
      "http://build-time:4318",
    );
    expect(resolved).toEqual({
      endpoint: "http://signaldb.example:4318",
      headers: {
        Authorization: "Bearer sk-ingest",
        "X-Tenant-ID": "_system",
        "X-Dataset-ID": "_monitoring",
      },
    });
  });

  it("omits the Authorization header when no key is provided", () => {
    const resolved = resolveExportConfig(
      {
        telemetry: { enabled: true, endpoint: "http://c:4318", tenantId: "t" },
      },
      "",
    );
    expect(resolved?.headers).toEqual({ "X-Tenant-ID": "t" });
  });

  it("treats enabled-without-endpoint as disabled and falls back", () => {
    expect(
      resolveExportConfig(
        { telemetry: { enabled: true, apiKey: "sk-ingest" } },
        "",
      ),
    ).toBeNull();
  });

  it("does not export when runtime config explicitly disables it and no build-time endpoint", () => {
    expect(
      resolveExportConfig({ telemetry: { enabled: false } }, ""),
    ).toBeNull();
  });
});

describe("resolveServiceName", () => {
  it("prefers the runtime service name", () => {
    expect(
      resolveServiceName(
        { telemetry: { serviceName: "signaldb-ui-prod" } },
        "signaldb-ui",
      ),
    ).toBe("signaldb-ui-prod");
  });

  it("falls back to the build-time default when unset", () => {
    expect(resolveServiceName(undefined, "signaldb-ui")).toBe("signaldb-ui");
    expect(resolveServiceName({ telemetry: {} }, "signaldb-ui")).toBe(
      "signaldb-ui",
    );
  });
});
