import { QueryClient } from "@tanstack/react-query";

/** A `QueryClient` for contexts with no real backend to answer queries —
 * tests and Storybook — so an unmocked query fails once instead of retrying
 * with backoff. */
export function testQueryClient(): QueryClient {
  return new QueryClient({
    defaultOptions: {
      queries: { retry: false, refetchOnWindowFocus: false },
    },
  });
}
