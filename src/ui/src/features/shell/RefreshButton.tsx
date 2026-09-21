import {
  useIsFetching,
  useQueryClient,
  type Query,
} from "@tanstack/react-query";

// Session, config and registry lookups don't depend on the time range, so a
// refresh neither re-issues them nor spins for them.
const NON_RANGE_QUERIES = new Set([
  "whoami",
  "current-session",
  "login-config",
  "catalog-registry-entities",
  "source-context-availability",
  "source-context",
  "pyro-byid",
]);

const isRangeQuery = (query: Query) =>
  !NON_RANGE_QUERIES.has(String(query.queryKey[0]));

export function RefreshButton() {
  const queryClient = useQueryClient();
  const loading = useIsFetching({ predicate: isRangeQuery }) > 0;
  return (
    <button
      type="button"
      className="btn refresh-btn"
      aria-label="Refresh"
      title="Refresh"
      aria-busy={loading || undefined}
      onClick={() =>
        void queryClient.refetchQueries(
          { type: "active", predicate: isRangeQuery },
          { cancelRefetch: false },
        )
      }
    >
      <svg viewBox="0 0 16 16" aria-hidden="true" className="refresh-icon">
        <path d="M13.5 8a5.5 5.5 0 1 1-1.6-3.9" />
        <path d="M13.5 2.5v3h-3" />
      </svg>
    </button>
  );
}
