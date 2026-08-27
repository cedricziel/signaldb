// Request-scoped tenant context. The explore state (URL) is the source of
// truth; views sync it here so the API clients can attach the headers. In
// dev, the Vite proxy fills in env defaults for requests without them.

export interface TenantContext {
  tenant: string;
  dataset: string;
}

/** HTTP error with the response status attached, so callers can react to
 * specific codes (401 → show the login form). A `429` (throttled past the
 * retry budget, see `retryingFetch`) carries the server-stated wait as
 * `retryAfterMs` and renders a throttling message instead of the generic
 * one, so per-panel `error.message` renderers need no special casing. */
export class ApiError extends Error {
  readonly status: number;
  /** Server-stated wait (`Retry-After`) on a throttled response, else null. */
  readonly retryAfterMs: number | null;

  constructor(message: string, status: number, retryAfterMs?: number | null) {
    const wait = retryAfterMs ?? null;
    super(status === 429 ? throttlingMessage(wait) : message);
    this.name = "ApiError";
    this.status = status;
    this.retryAfterMs = wait;
  }
}

/** True when the error is a throttling failure (retries exhausted). */
export function isThrottledError(err: unknown): err is ApiError {
  return err instanceof ApiError && err.status === 429;
}

/** The user-facing throttling message, naming the wait when known. */
export function throttlingMessage(retryAfterMs: number | null): string {
  if (retryAfterMs == null) return "Rate limited — please retry shortly";
  return `Rate limited — server asked to retry in ${formatWait(retryAfterMs)}`;
}

function formatWait(ms: number): string {
  if (ms < 1000) return `${Math.max(0, Math.round(ms))} ms`;
  const s = ms / 1000;
  return Number.isInteger(s) ? `${s} s` : `${s.toFixed(1)} s`;
}

/** Parse a `Retry-After` header value (delay-seconds or HTTP-date) to a
 * wait in milliseconds; a past date is 0; unparseable is null. */
export function parseRetryAfterMs(
  value: string | null | undefined,
  now: number = Date.now(),
): number | null {
  if (value == null) return null;
  const trimmed = value.trim();
  if (/^\d+$/.test(trimmed)) return Number(trimmed) * 1000;
  const at = Date.parse(trimmed);
  if (Number.isNaN(at)) return null;
  return Math.max(0, at - now);
}

/** The server-stated wait carried by a response, if any. */
export function retryAfterMsFrom(
  response: Response | null | undefined,
): number | null {
  if (!response) return null;
  return parseRetryAfterMs(response.headers.get("retry-after"));
}

// ---------------------------------------------------------------------------
// Retry on throttle — the TypeScript half of the contract shared with the
// Rust SDK (`signaldb_sdk::retry`); `api/retry-cases.json` is the executable
// fixture both test suites replay.
//
// - 429 is retried on any method (a throttled request was not processed).
// - 502/503/504, network failures, and timeouts are retried only for
//   idempotent methods (GET/HEAD/PUT/DELETE/OPTIONS/TRACE).
// - Wait = Retry-After (seconds or HTTP-date) when present, else a fully
//   jittered exponential backoff U(0, min(cap, base·2^n)).
// - A Retry-After beyond the per-attempt cap, or a total wait beyond the
//   total cap, fails fast: the throttling response is returned at once.
// - Attempts are bounded; the caller's AbortSignal cancels a pending wait.
// ---------------------------------------------------------------------------

export interface RetryPolicy {
  /** Total attempts including the first request; 1 disables retry. */
  maxAttempts: number;
  /** Base of the jittered backoff (ms) used without Retry-After. */
  baseMs: number;
  /** Ceiling on any single wait (ms); a Retry-After above it fails fast. */
  perAttemptCapMs: number;
  /** Ceiling on the sum of all waits for one call (ms). */
  totalCapMs: number;
  /** Whether 502/503/504/network failures retry for idempotent methods. */
  retryTransientIdempotent: boolean;
}

export const DEFAULT_RETRY_POLICY: RetryPolicy = {
  maxAttempts: 4,
  baseMs: 250,
  perAttemptCapMs: 10_000,
  totalCapMs: 30_000,
  retryTransientIdempotent: true,
};

export const NO_RETRY_POLICY: RetryPolicy = {
  ...DEFAULT_RETRY_POLICY,
  maxAttempts: 1,
};

/** One attempt's failure shape, transport-independent. */
export type RetryFailure =
  | { kind: "status"; status: number }
  | { kind: "connect" }
  | { kind: "timeout" };

export type RetryDecision =
  | { action: "return" }
  | { action: "retry"; waitMs: number }
  | { action: "fail-fast" };

const IDEMPOTENT = new Set(["GET", "HEAD", "PUT", "DELETE", "OPTIONS", "TRACE"]);

export function isIdempotentMethod(method: string): boolean {
  return IDEMPOTENT.has(method.toUpperCase());
}

function isRetryable(
  policy: RetryPolicy,
  failure: RetryFailure,
  method: string,
): boolean {
  if (failure.kind === "status" && failure.status === 429) return true;
  const transient =
    failure.kind === "connect" ||
    failure.kind === "timeout" ||
    (failure.kind === "status" &&
      failure.status >= 502 &&
      failure.status <= 504);
  return (
    transient && policy.retryTransientIdempotent && isIdempotentMethod(method)
  );
}

/** Upper bound of the jittered wait after `attempt` (1-based). */
export function backoffCeilingMs(policy: RetryPolicy, attempt: number): number {
  const exp = Math.min(31, Math.max(0, attempt - 1));
  return Math.min(policy.perAttemptCapMs, policy.baseMs * 2 ** exp);
}

/** Decide what to do after `attempt` (1-based) ended in `failure`.
 * `retryAfterMs` is the server-stated wait; `waitedMs` the sum of waits
 * already spent on this call. `random` is injectable for tests. */
export function decideRetry(
  policy: RetryPolicy,
  failure: RetryFailure,
  method: string,
  retryAfterMs: number | null,
  attempt: number,
  waitedMs: number,
  random: () => number = Math.random,
): RetryDecision {
  if (!isRetryable(policy, failure, method)) return { action: "return" };
  if (attempt >= policy.maxAttempts) return { action: "return" };
  let waitMs: number;
  if (retryAfterMs != null) {
    if (retryAfterMs > policy.perAttemptCapMs) return { action: "fail-fast" };
    waitMs = retryAfterMs;
  } else {
    waitMs = backoffCeilingMs(policy, attempt) * random();
  }
  if (waitedMs + waitMs > policy.totalCapMs) return { action: "fail-fast" };
  return { action: "retry", waitMs };
}

// --- "retrying" visibility ---------------------------------------------------

export interface ThrottleState {
  /** Requests currently waiting to be retried. */
  pending: number;
  /** The wait (ms) of the most recent retry, for the banner copy. */
  lastWaitMs: number | null;
}

let throttleState: ThrottleState = { pending: 0, lastWaitMs: null };
const throttleListeners = new Set<() => void>();

/** Current snapshot (stable identity until it changes — safe for
 * `useSyncExternalStore`). */
export function getThrottleState(): ThrottleState {
  return throttleState;
}

export function subscribeThrottleState(listener: () => void): () => void {
  throttleListeners.add(listener);
  return () => {
    throttleListeners.delete(listener);
  };
}

function setThrottleState(next: ThrottleState): void {
  throttleState = next;
  for (const listener of throttleListeners) listener();
}

/** Test hook: forget any pending retries. */
export function resetThrottleState(): void {
  setThrottleState({ pending: 0, lastWaitMs: null });
}

// --- the fetch --------------------------------------------------------------

function abortError(signal: AbortSignal): unknown {
  const reason: unknown = signal.reason;
  if (reason !== undefined) return reason;
  return new DOMException("The operation was aborted.", "AbortError");
}

/** Sleep for `ms`, rejecting with the signal's reason if aborted first. */
function abortableSleep(ms: number, signal: AbortSignal | null): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) {
      reject(abortError(signal));
      return;
    }
    const onAbort = () => {
      clearTimeout(timer);
      reject(abortError(signal!));
    };
    const timer = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}

function classifyFetchError(err: unknown): RetryFailure | null {
  // Name-based: DOMException identity differs across realms (jsdom/Node).
  const name = (err as { name?: unknown } | null)?.name;
  if (name === "AbortError") return null;
  if (name === "TimeoutError") return { kind: "timeout" };
  // A failed `fetch` (network down, connection refused/reset, DNS) rejects
  // with a TypeError; anything else is a programming error, not transient.
  if (err instanceof TypeError) return { kind: "connect" };
  return null;
}

/**
 * `fetch` with the shared retry-on-throttle policy. Installed on the
 * generated client (`client.setConfig({ fetch: retryingFetch })`) and used
 * by the remaining raw-fetch callers, so every UI request backs off the same
 * way. Respects the request's `AbortSignal` during waits; the final response
 * (or error) is returned unchanged so callers keep their own status handling.
 */
export async function retryingFetch(
  input: RequestInfo | URL,
  init?: RequestInit,
  policy: RetryPolicy = DEFAULT_RETRY_POLICY,
): Promise<Response> {
  // The generated client hands over one `Request`; raw callers pass
  // `(url, init)`. A `Request` body is single-use, so a fresh clone is sent
  // per attempt (the original is never sent itself and stays cloneable);
  // `(url, init)` is re-sent as-is, which is fine for the string/URLSearchParams
  // bodies the raw callers use.
  const request = input instanceof Request ? input : null;
  const signal = init?.signal ?? request?.signal ?? null;
  const method = (init?.method ?? request?.method ?? "GET").toUpperCase();
  const send = () =>
    request ? fetch(request.clone()) : fetch(input as string | URL, init);
  let attempt = 1;
  let waitedMs = 0;
  for (;;) {
    let response: Response | null = null;
    let failure: RetryFailure | null = null;
    let thrown: unknown;
    try {
      response = await send();
      if (response.status >= 400) {
        failure = { kind: "status", status: response.status };
      }
    } catch (err) {
      thrown = err;
      failure = classifyFetchError(err);
      if (failure === null) throw err;
    }
    if (failure === null) return response as Response;
    const retryAfterMs = retryAfterMsFrom(response);
    const decision = decideRetry(
      policy,
      failure,
      method,
      retryAfterMs,
      attempt,
      waitedMs,
    );
    if (decision.action !== "retry") {
      if (response) return response;
      throw thrown;
    }
    setThrottleState({
      pending: throttleState.pending + 1,
      lastWaitMs: decision.waitMs,
    });
    try {
      await abortableSleep(decision.waitMs, signal);
    } finally {
      setThrottleState({
        pending: Math.max(0, throttleState.pending - 1),
        lastWaitMs: throttleState.lastWaitMs,
      });
    }
    waitedMs += decision.waitMs;
    attempt += 1;
  }
}

/** True when the error is an authentication failure (missing/invalid
 * credentials) that a login can fix. */
export function isAuthError(err: unknown): boolean {
  return err instanceof ApiError && err.status === 401;
}

/** Result envelope produced by the generated SDK (`RequestResult` with the
 * default `fields` response style). */
export interface SdkResult<T> {
  data?: T;
  error?: unknown;
  response?: Response;
}

/** Unwrap a generated SDK result, preserving the error contract callers rely
 * on. The SDK does not throw by default: it returns `error` set (and
 * `response` unset on a network/URL error) instead. Re-throw as `ApiError`
 * with the HTTP status so `isAuthError(401)` keeps working. `label` names
 * the request family in the generic fallback message (e.g. "Management"). */
export function unwrapSdkResult<T>(result: SdkResult<T>, label: string): T {
  const { error, response } = result;
  if (error !== undefined || !response?.ok) {
    const status = response?.status ?? 0;
    const message =
      (error as { error?: string } | undefined)?.error ??
      `${label} request failed (${status})`;
    throw new ApiError(message, status, retryAfterMsFrom(response));
  }
  return result.data as T;
}

/** Render a caught value as a display string, whether or not it's an Error. */
export function toErrorMessage(value: unknown): string {
  return value instanceof Error ? value.message : String(value);
}

let current: TenantContext = { tenant: "", dataset: "" };

/**
 * localStorage key under which the last non-empty tenant/dataset context is
 * kept, so a new tab or a bookmark that opens a bare route (no `?tenant=`)
 * resumes where the user was instead of sending tenant-less requests.
 */
export const TENANT_CONTEXT_STORAGE_KEY = "signaldb.tenantContext";

/** The persisted context, if any and well-formed. */
export function loadPersistedTenantContext(): TenantContext | null {
  try {
    const raw = localStorage.getItem(TENANT_CONTEXT_STORAGE_KEY);
    if (!raw) return null;
    const parsed: unknown = JSON.parse(raw);
    if (
      parsed &&
      typeof parsed === "object" &&
      typeof (parsed as TenantContext).tenant === "string" &&
      (parsed as TenantContext).tenant !== ""
    ) {
      const { tenant, dataset } = parsed as TenantContext;
      return { tenant, dataset: typeof dataset === "string" ? dataset : "" };
    }
  } catch {
    // Storage unavailable or corrupt: behave as if nothing was persisted.
  }
  return null;
}

/** Remember a non-empty context for later tabs; empty tenant is ignored. */
export function persistTenantContext(ctx: TenantContext): void {
  if (!ctx.tenant) return;
  try {
    localStorage.setItem(TENANT_CONTEXT_STORAGE_KEY, JSON.stringify(ctx));
  } catch {
    // Storage unavailable (private mode, quota): the URL still carries it.
  }
}

/** Forget the persisted context (sign-out). */
export function clearPersistedTenantContext(): void {
  try {
    localStorage.removeItem(TENANT_CONTEXT_STORAGE_KEY);
  } catch {
    // ignore
  }
}

export function setTenantContext(ctx: TenantContext): void {
  current = ctx;
}

export function getTenantContext(): TenantContext {
  return current;
}

export function tenantHeaders(): Record<string, string> {
  const headers: Record<string, string> = { Accept: "application/json" };
  if (current.tenant) headers["X-Tenant-ID"] = current.tenant;
  if (current.dataset) headers["X-Dataset-ID"] = current.dataset;
  return headers;
}

/** Build-time defaults injected by Vite from SIGNALDB_TENANT/_DATASET. */
export const DEFAULT_TENANT: string =
  typeof __SIGNALDB_DEFAULT_TENANT__ !== "undefined"
    ? __SIGNALDB_DEFAULT_TENANT__
    : "";
export const DEFAULT_DATASET: string =
  typeof __SIGNALDB_DEFAULT_DATASET__ !== "undefined"
    ? __SIGNALDB_DEFAULT_DATASET__
    : "";
