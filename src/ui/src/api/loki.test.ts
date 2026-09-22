import { afterEach, describe, expect, it } from "vitest";
import { setTenantContext } from "./http";
import { lokiLabels, lokiLabelValues } from "./loki";
import { resetApiClient, stubApiFetch } from "../test/apiClient";

const RANGE = { fromMs: 1_000_000, toMs: 2_000_000 };

afterEach(resetApiClient);

describe("label endpoints", () => {
  it("fetches labels within the range", async () => {
    const calls = stubApiFetch({
      status: "success",
      data: ["level", "service_name"],
    });
    const labels = await lokiLabels(RANGE);
    expect(labels).toEqual(["level", "service_name"]);
    expect(calls[0]!.url).toContain("/loki/api/v1/labels?");
  });

  it("URL-encodes label names for the values endpoint", async () => {
    const calls = stubApiFetch({ status: "success", data: ["a"] });
    await lokiLabelValues("weird/label", RANGE);
    expect(calls[0]!.url).toContain("/loki/api/v1/label/weird%2Flabel/values?");
  });

  it("tolerates a missing data field", async () => {
    stubApiFetch({ status: "success" });
    expect(await lokiLabels(RANGE)).toEqual([]);
  });

  it("attaches tenant headers from the current context", async () => {
    const calls = stubApiFetch({ status: "success", data: [] });
    setTenantContext({ tenant: "acme", dataset: "prod" });
    try {
      await lokiLabels(RANGE);
    } finally {
      setTenantContext({ tenant: "", dataset: "" });
    }
    expect(calls[0]!.headers).toMatchObject({
      "x-tenant-id": "acme",
      "x-dataset-id": "prod",
    });
  });

  it("throws a readable error on HTTP failure", async () => {
    stubApiFetch({ error: "boom" }, 500);
    await expect(lokiLabels(RANGE)).rejects.toThrow(
      /Loki labels failed \(500\)/,
    );
  });
});
