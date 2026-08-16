import { act, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { resetThrottleState, retryingFetch } from "../../api/http";
import { ThrottleBanner } from "./ThrottleBanner";

describe("ThrottleBanner", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    resetThrottleState();
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.unstubAllGlobals();
    resetThrottleState();
  });

  it("appears while a retry is pending and disappears after", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(
        new Response("{}", { status: 429, headers: { "retry-after": "1" } }),
      )
      .mockResolvedValueOnce(new Response("{}", { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);

    render(<ThrottleBanner />);
    expect(screen.queryByRole("status")).toBeNull();

    let promise: Promise<Response> | undefined;
    await act(async () => {
      promise = retryingFetch("/api/x");
      await vi.advanceTimersByTimeAsync(10);
    });
    expect(screen.getByRole("status")).toHaveTextContent(
      "Some requests are being retried after throttling… (waiting 1 s)",
    );

    await act(async () => {
      await vi.runAllTimersAsync();
      await promise;
    });
    expect(screen.queryByRole("status")).toBeNull();
  });
});
