// Request-scoped tenant context. The explore state (URL) is the source of
// truth; views sync it here so the API clients can attach the headers. In
// dev, the Vite proxy fills in env defaults for requests without them.

export interface TenantContext {
  tenant: string;
  dataset: string;
}

/** HTTP error with the response status attached, so callers can react to
 * specific codes (401 → show the login form). */
export class ApiError extends Error {
  readonly status: number;

  constructor(message: string, status: number) {
    super(message);
    this.name = "ApiError";
    this.status = status;
  }
}

/** True when the error is an authentication failure (missing/invalid
 * credentials) that a login can fix. */
export function isAuthError(err: unknown): boolean {
  return err instanceof ApiError && err.status === 401;
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
