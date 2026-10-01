import { describe, expect, it } from "vitest";
import { ApiError } from "../api/http";
import { queryRetry } from "./queryRetry";

describe("queryRetry", () => {
  it("does not retry a 401 ApiError", () => {
    const error = new ApiError("Unauthorized", 401);
    expect(queryRetry(0, error)).toBe(false);
  });

  it("does not retry a 403 ApiError", () => {
    const error = new ApiError("Forbidden", 403);
    expect(queryRetry(0, error)).toBe(false);
  });

  it("retries once for a non-auth ApiError", () => {
    const error = new ApiError("Internal Server Error", 500);
    expect(queryRetry(0, error)).toBe(true);
    expect(queryRetry(1, error)).toBe(false);
  });

  it("retries once for a non-ApiError failure", () => {
    const error = new Error("network down");
    expect(queryRetry(0, error)).toBe(true);
    expect(queryRetry(1, error)).toBe(false);
  });
});
