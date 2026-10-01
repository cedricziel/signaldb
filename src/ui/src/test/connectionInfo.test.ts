import { describe, expect, it } from "vitest";
import { connectionInfoBody } from "./connectionInfo";

describe("connectionInfoBody", () => {
  it("derives OTEL_EXPORTER_OTLP_HEADERS from the default headers", () => {
    const body = connectionInfoBody();
    expect(body.otel_env.OTEL_EXPORTER_OTLP_HEADERS).toBe(
      "authorization=Bearer <api-key>,x-tenant-id=acme,x-dataset-id=production",
    );
  });

  it("stays internally consistent when headers are overridden without an otel_env override", () => {
    const body = connectionInfoBody({
      headers: {
        authorization: "Bearer <api-key>",
        "x-tenant-id": "acme",
        "x-dataset-id": "staging",
      },
    });
    expect(body.otel_env.OTEL_EXPORTER_OTLP_HEADERS).toBe(
      "authorization=Bearer <api-key>,x-tenant-id=acme,x-dataset-id=staging",
    );
  });

  it("still allows an explicit otel_env override to win", () => {
    const body = connectionInfoBody({
      headers: {
        authorization: "Bearer <api-key>",
        "x-tenant-id": "acme",
        "x-dataset-id": "staging",
      },
      otel_env: {
        OTEL_EXPORTER_OTLP_ENDPOINT: "https://custom.example.com:4317",
        OTEL_EXPORTER_OTLP_PROTOCOL: "grpc",
        OTEL_EXPORTER_OTLP_HEADERS: "authorization=custom",
      },
    });
    expect(body.otel_env.OTEL_EXPORTER_OTLP_HEADERS).toBe(
      "authorization=custom",
    );
  });
});
