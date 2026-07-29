import { afterEach, describe, expect, it } from "vitest";
import { setTenantContext, tenantHeaders } from "./http";

afterEach(() => {
  setTenantContext({ tenant: "", dataset: "" });
});

describe("tenantHeaders", () => {
  it("sends no tenant headers for the ambient default context", () => {
    expect(tenantHeaders()).toEqual({ Accept: "application/json" });
  });

  it("sends tenant and dataset headers when set", () => {
    setTenantContext({ tenant: "acme", dataset: "production" });
    expect(tenantHeaders()).toEqual({
      Accept: "application/json",
      "X-Tenant-ID": "acme",
      "X-Dataset-ID": "production",
    });
  });

  it("omits the dataset header when only the tenant is set", () => {
    setTenantContext({ tenant: "acme", dataset: "" });
    expect(tenantHeaders()).toEqual({
      Accept: "application/json",
      "X-Tenant-ID": "acme",
    });
  });
});
