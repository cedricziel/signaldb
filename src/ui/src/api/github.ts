// GitHub App integration API, layered over the generated OpenAPI SDK — the
// tenant management endpoints behind the Explore UI's Integrations → GitHub
// page (see docs/operations/github-app.md). Same shape as management.ts:
// each export delegates to a generated operation and unwraps the result into
// the historical contract (data on success, `ApiError` on failure).
import "./client";

import {
  attachGithubInstallation as generatedAttachGithubInstallation,
  listGithubInstallations as generatedListGithubInstallations,
  removeGithubInstallation as generatedRemoveGithubInstallation,
  startGithubLink as generatedStartGithubLink,
  type GitHubInstallationResponse,
  type GitHubInstallationsResponse,
  type StartGithubLinkResponse,
} from "./gen";
import { type SdkResult, unwrapErrorEnvelope } from "./http";

/** The tenant's configured-ness and linked installations, as returned by
 * `list_github_installations`. */
export type GithubInstallations = GitHubInstallationsResponse;

/** One linked GitHub App installation. */
export type GithubInstallation = GitHubInstallationResponse;

const unwrap = <T>(result: SdkResult<T>): T =>
  unwrapErrorEnvelope(result, "GitHub");

/** `GET /api/v1/tenants/{id}/github-installations`: whether
 * `[github]` is configured on this deployment and, if so, every installation
 * linked to the tenant. Always 200 — an unconfigured deployment answers with
 * `configured: false` rather than a 404. */
export const listGithubInstallations = async (
  tenant: string,
): Promise<GithubInstallations> =>
  unwrap(
    await generatedListGithubInstallations({ path: { tenant_id: tenant } }),
  );

/** `POST /api/v1/tenants/{id}/github-installations/link`: mints a
 * single-use install link for this tenant/admin. Follow `install_url` to
 * hand the browser to GitHub's install flow. */
export const startGithubLink = async (
  tenant: string,
): Promise<StartGithubLinkResponse> =>
  unwrap(await generatedStartGithubLink({ path: { tenant_id: tenant } }));

/** `POST /api/v1/tenants/{id}/github-installations/attach`: attaches
 * an installation that already exists for this GitHub App — e.g. one
 * already linked to another tenant on the same account — directly, with no
 * OAuth install flow. */
export const attachGithubInstallation = async (
  tenant: string,
  installationId: number,
): Promise<GithubInstallation> =>
  unwrap(
    await generatedAttachGithubInstallation({
      path: { tenant_id: tenant },
      body: { installation_id: installationId },
    }),
  );

/** `DELETE /api/v1/tenants/{id}/github-installations/{id}`: removes
 * the link immediately (SignalDB stops using it); the App stays installed on
 * GitHub until uninstalled there. */
export const removeGithubInstallation = async (
  tenant: string,
  installationId: number,
): Promise<void> => {
  unwrap(
    await generatedRemoveGithubInstallation({
      path: { tenant_id: tenant, installation_id: installationId },
    }),
  );
};
