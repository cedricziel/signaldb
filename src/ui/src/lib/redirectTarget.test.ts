import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_TARGET, safeRedirectTarget } from "./redirectTarget";

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("safeRedirectTarget", () => {
  it("returns the default for a missing value", () => {
    expect(safeRedirectTarget(null)).toBe(DEFAULT_TARGET);
    expect(safeRedirectTarget(undefined)).toBe(DEFAULT_TARGET);
    expect(safeRedirectTarget("")).toBe(DEFAULT_TARGET);
  });

  it("accepts a same-app relative path, preserving query and hash", () => {
    expect(safeRedirectTarget("/traces?range=15m")).toBe("/traces?range=15m");
    expect(safeRedirectTarget("/logs#section")).toBe("/logs#section");
  });

  it("preserves a full oauth consent url intact, matching the backend parity test", () => {
    const consent =
      "/oauth/consent?client_id=abc&redirect_uri=https%3A%2F%2Fclient.example%2Fcb&state=xyz";
    expect(safeRedirectTarget(consent)).toBe(consent);
  });

  it.each([
    ["//evil.com"],
    ["https://evil.com"],
    ["/\\evil.com"],
    // Dot-segment host escapes: `new URL` normalizes these to a pathname
    // starting with `//`, a protocol-relative URL, even though the
    // same-origin check on `url.origin` passes.
    ["/.//evil.example"],
    ["/../\\evil.example"],
    // Control characters (CR, LF, tab) must be rejected on the raw input,
    // mirroring the backend's `safe_redirect_target` header-injection guard.
    ["/foo\r\n/evil.example"],
    ["/foo\t/evil"],
  ])("falls back to the default for the unsafe target %s", (unsafe) => {
    expect(safeRedirectTarget(unsafe)).toBe(DEFAULT_TARGET);
  });

  it("falls back to the default for a path not starting with /", () => {
    expect(safeRedirectTarget("evil.com")).toBe(DEFAULT_TARGET);
  });

  it("falls back to the default for /login, to avoid looping the credential step", () => {
    expect(safeRedirectTarget("/login")).toBe(DEFAULT_TARGET);
    expect(safeRedirectTarget("/login?redirect=%2Flogs")).toBe(DEFAULT_TARGET);
  });
});
