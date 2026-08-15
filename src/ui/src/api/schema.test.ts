import { afterEach, describe, expect, it } from "vitest";
import { resetApiClient, stubApiFetch } from "../test/apiClient";
import { ApiError } from "./http";
import { resolveAttributes, searchAttributes } from "./schema";

afterEach(() => resetApiClient());

describe("schema API", () => {
  it("resolves a batch of keys with the comma-joined keys= form", async () => {
    const calls = stubApiFetch({
      hits: [],
      resolutions: [{ key: "k8s.pod.uid", hits: [], primary: null }],
    });
    const res = await resolveAttributes(["k8s.pod.uid", "service.name"]);
    expect(res).toHaveLength(1);
    const url = new URL(calls[0]!.url);
    expect(url.pathname).toBe("/api/v1/schema/attributes");
    expect(url.searchParams.get("keys")).toBe("k8s.pod.uid,service.name");
  });

  it("makes no request for an empty batch", async () => {
    const calls = stubApiFetch({ hits: [] });
    expect(await resolveAttributes([])).toEqual([]);
    expect(calls).toHaveLength(0);
  });

  it("searches by prefix", async () => {
    const calls = stubApiFetch({ hits: [] });
    await searchAttributes("http.re", 10);
    const url = new URL(calls[0]!.url);
    expect(url.searchParams.get("prefix")).toBe("http.re");
    expect(url.searchParams.get("limit")).toBe("10");
  });

  it("surfaces failures as ApiError with the status", async () => {
    stubApiFetch({ error: "forbidden" }, 403);
    await expect(searchAttributes("x")).rejects.toBeInstanceOf(ApiError);
  });
});
