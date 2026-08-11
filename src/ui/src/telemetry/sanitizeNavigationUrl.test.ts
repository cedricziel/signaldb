import { describe, expect, it } from "vitest";
import { sanitizeNavigationUrl } from "./sanitizeNavigationUrl";

describe("sanitizeNavigationUrl", () => {
  it("passes through a URL with no credentials or sensitive params", () => {
    expect(sanitizeNavigationUrl("https://example.com/logs?tenant=acme")).toBe(
      "https://example.com/logs?tenant=acme",
    );
  });

  it("strips embedded userinfo credentials", () => {
    expect(sanitizeNavigationUrl("https://user:hunter2@example.com/logs")).toBe(
      "https://example.com/logs",
    );
  });

  it("redacts known sensitive query parameter values, case-insensitively", () => {
    const url = sanitizeNavigationUrl(
      "https://example.com/logs?api_key=sk-live-123&Token=abc&tenant=acme",
    );
    const parsed = new URL(url);
    expect(parsed.searchParams.get("api_key")).toBe("REDACTED");
    expect(parsed.searchParams.get("Token")).toBe("REDACTED");
    expect(parsed.searchParams.get("tenant")).toBe("acme");
  });

  it("resolves a relative URL against the current origin", () => {
    const url = sanitizeNavigationUrl("/logs?tenant=acme");
    expect(url).toBe(`${location.origin}/logs?tenant=acme`);
  });

  it("returns the input unchanged when it cannot be parsed as a URL", () => {
    // A relative-looking value resolves fine against location.origin (the
    // WHATWG parser treats almost anything as a path in that case); a
    // malformed *absolute*-looking value (recognized scheme, invalid
    // authority) is what actually throws.
    expect(sanitizeNavigationUrl("http://[not-valid")).toBe(
      "http://[not-valid",
    );
  });
});
