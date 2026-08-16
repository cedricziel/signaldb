// Retry-on-throttle contract of the UI's fetch (openspec change
// `sdk-retry-on-throttle`, spec `client-retry-on-throttle`): the shared
// `api/retry-cases.json` fixture replayed against `decideRetry`, and
// `retryingFetch` driven with fake timers and a stub `fetch`.
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  ApiError,
  DEFAULT_RETRY_POLICY,
  NO_RETRY_POLICY,
  decideRetry,
  getThrottleState,
  parseRetryAfterMs,
  resetThrottleState,
  retryingFetch,
  subscribeThrottleState,
  throttlingMessage,
  type RetryFailure,
  type RetryPolicy,
} from "./http";

interface FixtureCase {
  name: string;
  failure: string;
  method: string;
  retry_after: string | null;
  attempt: number;
  expect: {
    retry: boolean;
    fail_fast?: boolean;
    wait_ms_min?: number;
    wait_ms_max?: number;
  };
}

interface Fixture {
  policy: {
    max_attempts: number;
    base_ms: number;
    per_attempt_cap_ms: number;
    total_cap_ms: number;
  };
  cases: FixtureCase[];
}

const fixture: Fixture = JSON.parse(
  // vitest runs from the package root (src/ui); the fixture lives at the
  // repository's api/.
  readFileSync(resolve(process.cwd(), "../../api/retry-cases.json"), "utf8"),
) as Fixture;

function failureOf(spec: string): RetryFailure {
  if (spec === "connect") return { kind: "connect" };
  if (spec === "timeout") return { kind: "timeout" };
  return { kind: "status", status: Number(spec.replace("status:", "")) };
}

describe("shared retry fixture", () => {
  const policy: RetryPolicy = {
    maxAttempts: fixture.policy.max_attempts,
    baseMs: fixture.policy.base_ms,
    perAttemptCapMs: fixture.policy.per_attempt_cap_ms,
    totalCapMs: fixture.policy.total_cap_ms,
    retryTransientIdempotent: true,
  };

  it("matches the default policy", () => {
    expect(policy).toEqual(DEFAULT_RETRY_POLICY);
  });

  for (const c of fixture.cases) {
    it(c.name, () => {
      const retryAfterMs = parseRetryAfterMs(c.retry_after);
      // Both jitter extremes: random() → 0 and → ~1.
      for (const random of [() => 0, () => 0.999999, Math.random]) {
        const decision = decideRetry(
          policy,
          failureOf(c.failure),
          c.method,
          retryAfterMs,
          c.attempt,
          0,
          random,
        );
        if (c.expect.retry) {
          expect(decision.action).toBe("retry");
          if (decision.action === "retry") {
            expect(decision.waitMs).toBeGreaterThanOrEqual(
              c.expect.wait_ms_min ?? 0,
            );
            expect(decision.waitMs).toBeLessThanOrEqual(
              c.expect.wait_ms_max ?? Infinity,
            );
          }
        } else if (c.expect.fail_fast) {
          expect(decision.action).toBe("fail-fast");
        } else {
          expect(decision.action).toBe("return");
        }
      }
    });
  }
});

describe("parseRetryAfterMs", () => {
  it("parses delay-seconds and HTTP-dates", () => {
    expect(parseRetryAfterMs("5")).toBe(5000);
    expect(parseRetryAfterMs(" 0 ")).toBe(0);
    expect(parseRetryAfterMs("soon")).toBeNull();
    expect(parseRetryAfterMs(null)).toBeNull();
    const now = Date.parse("Wed, 21 Oct 2015 07:28:00 GMT");
    expect(parseRetryAfterMs("Wed, 21 Oct 2015 07:28:30 GMT", now)).toBe(
      30_000,
    );
    expect(parseRetryAfterMs("Wed, 21 Oct 2015 07:27:00 GMT", now)).toBe(0);
  });
});

describe("ApiError", () => {
  it("renders a throttling message naming the wait for a 429", () => {
    const err = new ApiError("IR query failed (429)", 429, 5000);
    expect(err.message).toBe("Rate limited — server asked to retry in 5 s");
    expect(err.retryAfterMs).toBe(5000);
    expect(new ApiError("x", 429).message).toBe(throttlingMessage(null));
    expect(new ApiError("boom", 500).message).toBe("boom");
    expect(new ApiError("boom", 500).retryAfterMs).toBeNull();
  });
});

function response(status: number, headers: Record<string, string> = {}) {
  return new Response(status === 204 ? null : "{}", { status, headers });
}

describe("retryingFetch", () => {
  const fast: RetryPolicy = { ...DEFAULT_RETRY_POLICY, baseMs: 10 };

  beforeEach(() => {
    vi.useFakeTimers();
    resetThrottleState();
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.unstubAllGlobals();
    resetThrottleState();
  });

  it("retries a throttled GET after the server-stated wait", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(response(429, { "retry-after": "2" }))
      .mockResolvedValueOnce(response(200));
    vi.stubGlobal("fetch", fetchMock);

    const promise = retryingFetch("/api/x", undefined, fast);
    await vi.advanceTimersByTimeAsync(1_999);
    expect(fetchMock).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(1);
    const res = await promise;
    expect(res.status).toBe(200);
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it("retries a throttled POST too", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(response(429, { "retry-after": "0" }))
      .mockResolvedValueOnce(response(200));
    vi.stubGlobal("fetch", fetchMock);
    const promise = retryingFetch(
      "/api/x",
      { method: "POST", body: "{}" },
      fast,
    );
    await vi.runAllTimersAsync();
    expect((await promise).status).toBe(200);
    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(fetchMock.mock.calls[1]?.[1]).toMatchObject({ method: "POST" });
  });

  it("does not retry a transient failure on POST", async () => {
    const fetchMock = vi.fn().mockResolvedValue(response(503));
    vi.stubGlobal("fetch", fetchMock);
    const res = await retryingFetch("/api/x", { method: "POST" }, fast);
    expect(res.status).toBe(503);
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("retries a transient failure on GET with jittered backoff", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(response(503))
      .mockResolvedValueOnce(response(200));
    vi.stubGlobal("fetch", fetchMock);
    const promise = retryingFetch("/api/x", undefined, fast);
    await vi.runAllTimersAsync();
    expect((await promise).status).toBe(200);
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it("does not retry client errors", async () => {
    const fetchMock = vi.fn().mockResolvedValue(response(404));
    vi.stubGlobal("fetch", fetchMock);
    const res = await retryingFetch("/api/x", undefined, fast);
    expect(res.status).toBe(404);
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("fails fast when Retry-After exceeds the per-attempt cap", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValue(response(429, { "retry-after": "3600" }));
    vi.stubGlobal("fetch", fetchMock);
    const res = await retryingFetch("/api/x", undefined, fast);
    expect(res.status).toBe(429);
    expect(res.headers.get("retry-after")).toBe("3600");
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("stops after max attempts and returns the last 429", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValue(response(429, { "retry-after": "0" }));
    vi.stubGlobal("fetch", fetchMock);
    const promise = retryingFetch("/api/x", undefined, fast);
    await vi.runAllTimersAsync();
    const res = await promise;
    expect(res.status).toBe(429);
    expect(fetchMock).toHaveBeenCalledTimes(DEFAULT_RETRY_POLICY.maxAttempts);
  });

  it("never retries with the disabled policy", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValue(response(429, { "retry-after": "0" }));
    vi.stubGlobal("fetch", fetchMock);
    const res = await retryingFetch("/api/x", undefined, NO_RETRY_POLICY);
    expect(res.status).toBe(429);
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("clones a Request per attempt (the generated client's shape)", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(response(429, { "retry-after": "0" }))
      .mockResolvedValueOnce(response(200));
    vi.stubGlobal("fetch", fetchMock);
    const request = new Request("http://localhost/api/v1/query", {
      method: "POST",
      body: JSON.stringify({ irVersion: 1 }),
      headers: { "content-type": "application/json" },
    });
    const promise = retryingFetch(request, undefined, fast);
    await vi.runAllTimersAsync();
    expect((await promise).status).toBe(200);
    const sent = fetchMock.mock.calls.map((c) => c[0] as Request);
    expect(sent).toHaveLength(2);
    expect(await sent[0]!.text()).toBe('{"irVersion":1}');
    expect(await sent[1]!.text()).toBe('{"irVersion":1}');
    expect(sent[1]!.method).toBe("POST");
  });

  it("an aborted signal cancels a pending wait and surfaces the abort", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValue(response(429, { "retry-after": "5" }));
    vi.stubGlobal("fetch", fetchMock);
    const controller = new AbortController();
    const promise = retryingFetch(
      "/api/x",
      { signal: controller.signal },
      fast,
    );
    const outcome = promise.then(
      () => "resolved",
      (e: unknown) => e,
    );
    await vi.advanceTimersByTimeAsync(100);
    expect(getThrottleState().pending).toBe(1);
    controller.abort();
    const err = await outcome;
    expect((err as { name?: string }).name).toBe("AbortError");
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(getThrottleState().pending).toBe(0);
  });

  it("retries a network failure on GET but not on POST", async () => {
    const failing = vi
      .fn()
      .mockRejectedValueOnce(new TypeError("Failed to fetch"))
      .mockResolvedValueOnce(response(200));
    vi.stubGlobal("fetch", failing);
    const promise = retryingFetch("/api/x", undefined, fast);
    await vi.runAllTimersAsync();
    expect((await promise).status).toBe(200);
    expect(failing).toHaveBeenCalledTimes(2);

    const failingPost = vi
      .fn()
      .mockRejectedValue(new TypeError("Failed to fetch"));
    vi.stubGlobal("fetch", failingPost);
    await expect(
      retryingFetch("/api/x", { method: "POST" }, fast),
    ).rejects.toBeInstanceOf(TypeError);
    expect(failingPost).toHaveBeenCalledTimes(1);
  });

  it("publishes throttle state while a retry is pending", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(response(429, { "retry-after": "1" }))
      .mockResolvedValueOnce(response(200));
    vi.stubGlobal("fetch", fetchMock);
    const seen: number[] = [];
    const unsubscribe = subscribeThrottleState(() => {
      seen.push(getThrottleState().pending);
    });
    const promise = retryingFetch("/api/x", undefined, fast);
    await vi.advanceTimersByTimeAsync(10);
    expect(getThrottleState()).toEqual({ pending: 1, lastWaitMs: 1000 });
    await vi.runAllTimersAsync();
    await promise;
    expect(getThrottleState().pending).toBe(0);
    expect(seen).toEqual([1, 0]);
    unsubscribe();
  });
});
