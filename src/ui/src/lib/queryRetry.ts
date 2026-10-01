import { isAuthError, isForbiddenError } from "../api/http";

/** React Query's `retry` predicate (`main.tsx`'s `QueryClient` default): a
 * 401/403 is an auth failure, not a transient one, and every panel query
 * (whoami, tags, labels, query, schema attributes) fails at once when a
 * session lapses — retrying each would turn one auth failure into a burst of
 * doubled requests. Any other error keeps the previous behavior of one
 * retry. */
export function queryRetry(failureCount: number, error: unknown): boolean {
  if (isAuthError(error) || isForbiddenError(error)) {
    return false;
  }
  return failureCount < 1;
}
