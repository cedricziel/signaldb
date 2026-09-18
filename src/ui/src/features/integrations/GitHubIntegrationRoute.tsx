import { GitHubIntegration } from "./GitHubIntegration";

/**
 * `/integrations/github` — route for the GitHub App integration.
 * GitHubIntegration handles its own authentication and admin check,
 * redirecting non-admins to /logs.
 */
export function GitHubIntegrationRoute() {
  // GitHubIntegration fetches whoami internally and performs its own
  // redirect.
  return <GitHubIntegration />;
}
