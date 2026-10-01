import { afterEach, describe, expect, it, vi } from "vitest";
import { NAVIGATION_TIMEOUT_MS, networkFirstNavigation } from "./navigation";

const request = new Request("https://example.test/traces");

describe("networkFirstNavigation", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it("returns the network response when the network answers", async () => {
    const response = new Response("<html></html>");
    const shell = vi.fn();
    const result = await networkFirstNavigation(request, {
      fetch: vi.fn().mockResolvedValue(response),
      shell,
    });
    expect(result).toBe(response);
    expect(shell).not.toHaveBeenCalled();
  });

  it("passes a redirect-type response through unchanged", async () => {
    // A reverse proxy answering an expired login with a redirect must reach
    // the browser as-is so it can follow it to the login page.
    const redirect = { type: "opaqueredirect" } as Response;
    const result = await networkFirstNavigation(request, {
      fetch: vi.fn().mockResolvedValue(redirect),
      shell: vi.fn(),
    });
    expect(result).toBe(redirect);
  });

  it("falls back to the shell when the fetch rejects", async () => {
    const shellResponse = new Response("<html>shell</html>");
    const result = await networkFirstNavigation(request, {
      fetch: vi.fn().mockRejectedValue(new TypeError("network down")),
      shell: vi.fn().mockResolvedValue(shellResponse),
    });
    expect(result).toBe(shellResponse);
  });

  it("falls back to the shell when the network is slower than the timeout", async () => {
    vi.useFakeTimers();
    const shellResponse = new Response("<html>shell</html>");
    const neverResolves = new Promise<Response>(() => {});
    const promise = networkFirstNavigation(request, {
      fetch: vi.fn().mockReturnValue(neverResolves),
      shell: vi.fn().mockResolvedValue(shellResponse),
    });
    await vi.advanceTimersByTimeAsync(NAVIGATION_TIMEOUT_MS);
    await expect(promise).resolves.toBe(shellResponse);
  });
});
