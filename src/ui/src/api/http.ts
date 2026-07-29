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

let current: TenantContext = { tenant: "", dataset: "" };

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
