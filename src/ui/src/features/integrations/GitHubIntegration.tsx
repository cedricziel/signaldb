// `/integrations/github` — connect SignalDB's GitHub App to a tenant so it
// can read the repositories behind this tenant's telemetry. See
// docs/operations/github-app.md ("Connect a tenant", "How linking is
// secured") for the flow this page drives.

import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useEffect, useState } from "react";
import { Navigate, useSearchParams } from "react-router";
import {
  listGithubInstallations,
  removeGithubInstallation,
  startGithubLink,
  type GithubInstallation,
} from "../../api/github";
import { toErrorMessage } from "../../api/http";
import type { WhoamiResponse } from "../../api/session";
import { ConfirmButton } from "../../components/ConfirmButton";
import { EmptyState } from "../../components/EmptyState";
import { QueryError, whoamiQueryError } from "../../components/QueryError";
import { navigateExternal } from "../../lib/navigateExternal";
import { useOutletState } from "../../lib/outletState";
import { useWhoami } from "../../lib/useWhoami";
import "./GitHubIntegration.css";

const GITHUB_DOCS_URL = "https://signaldb.dev/docs/operations/github-app/";

/** One human sentence per `?github=error&reason=…` value the router can
 * send back (see docs/operations/github-app.md's "How linking is secured")
 * — deliberately generic, matching the router's own refusal to disclose
 * which check failed. */
const ERROR_REASON_MESSAGES: Record<string, string> = {
  state: "The link request expired or was already used. Start again.",
  session:
    "You need to be signed in to SignalDB in this browser to finish connecting.",
  forbidden:
    "This link was started by someone else, or the installation isn't yours to link.",
  github: "GitHub rejected the request. Try again.",
  permissions:
    "That installation grants write permissions; SignalDB only accepts read-only installations.",
  internal: "Something went wrong on the server. Try again.",
};

/** The callback banner's content, captured once from the URL on mount (see
 * `GitHubIntegrationBody`) — not re-derived from `searchParams` on every
 * render, since those are stripped right after. */
type CallbackBanner =
  | { kind: "linked"; installationId: string | null }
  | { kind: "error"; reason: string | null };

export function GitHubIntegration() {
  const { state } = useOutletState();
  const {
    data: who,
    isLoading,
    isError: whoamiIsError,
    error: whoamiError,
    canManage,
  } = useWhoami(state);

  if (isLoading) return null;
  if (whoamiIsError) return whoamiQueryError("your account", whoamiError);

  if (!who || !canManage) {
    return <Navigate to="/logs" replace />;
  }

  // Keyed on the outlet's own tenant, same reasoning as ApiKeys: every piece
  // of local state (the callback banner, the error message) means nothing
  // once the user has switched tenants.
  return <GitHubIntegrationBody key={state.tenant} who={who} />;
}

function GitHubIntegrationBody({ who }: { who: WhoamiResponse }) {
  const tenant = who.tenant.id;
  const queryClient = useQueryClient();
  const [searchParams, setSearchParams] = useSearchParams();
  const [error, setError] = useState<string | null>(null);

  const installationsQueryKey = ["github-installations", tenant];
  const invalidateInstallations = () =>
    queryClient.invalidateQueries({ queryKey: installationsQueryKey });

  const installations = useQuery({
    queryKey: installationsQueryKey,
    queryFn: () => listGithubInstallations(tenant),
  });

  // Read once, before the URL is scrubbed below.
  const [banner] = useState<CallbackBanner | null>(() => {
    const github = searchParams.get("github");
    if (github === "linked") {
      return { kind: "linked", installationId: searchParams.get("installation_id") };
    }
    if (github === "error") {
      return { kind: "error", reason: searchParams.get("reason") };
    }
    return null;
  });

  useEffect(() => {
    if (!banner) return;
    // The mount fetch above is already fresh (it runs after the callback
    // redirect), so a `linked` banner needs no extra invalidation.
    // Strip the callback params so a reload doesn't re-show the banner.
    const next = new URLSearchParams(searchParams);
    next.delete("github");
    next.delete("installation_id");
    next.delete("reason");
    setSearchParams(next, { replace: true });
    // Runs once, against the params this component mounted with.
  }, []);

  const startMutation = useMutation({
    mutationFn: () => startGithubLink(tenant),
    onSuccess: (result) => {
      setError(null);
      navigateExternal(result.install_url);
    },
    onError: (value) => setError(toErrorMessage(value)),
  });

  const removeMutation = useMutation({
    mutationFn: (installationId: number) =>
      removeGithubInstallation(tenant, installationId),
    onSuccess: () => {
      setError(null);
      void invalidateInstallations();
    },
    onError: (value) => setError(toErrorMessage(value)),
  });

  return (
    <div className="github-page">
      <h1 className="github-title">GitHub</h1>
      <p className="github-subtitle">
        Connecting lets SignalDB read the repositories that produce{" "}
        <strong>{tenant}</strong>'s telemetry — read-only, and no GitHub
        token is ever stored.
      </p>

      {error && (
        <p className="error-text" role="alert">
          {error}
        </p>
      )}

      {banner?.kind === "linked" && (
        <p className="github-banner github-banner-success" role="status">
          GitHub installation {banner.installationId} linked.
        </p>
      )}
      {banner?.kind === "error" && (
        <p className="github-banner error-text" role="alert">
          {ERROR_REASON_MESSAGES[banner.reason ?? ""] ??
            ERROR_REASON_MESSAGES.internal}
        </p>
      )}

      {installations.isError && (
        <QueryError what="GitHub installations" error={installations.error} />
      )}

      {installations.data && !installations.data.configured && (
        <EmptyState title="GitHub is not configured on this server">
          Set the <code>[github]</code> section in the server config to
          enable this integration — see{" "}
          <a href={GITHUB_DOCS_URL} target="_blank" rel="noopener noreferrer">
            the GitHub App docs
          </a>
          .
        </EmptyState>
      )}

      {installations.data?.configured && (
        <>
          <section className="github-connect">
            <button
              type="button"
              className="btn btn-primary"
              disabled={startMutation.isPending}
              onClick={() => startMutation.mutate()}
            >
              Connect GitHub
            </button>
            <p className="github-connect-note">
              You&apos;ll be sent to GitHub to pick the organization and
              repositories; GitHub brings you back here.
            </p>
          </section>

          <section className="github-installations">
            <h2>Linked installations</h2>
            {installations.data.installations.length === 0 && (
              <EmptyState title="No GitHub installations linked yet." />
            )}
            <ul>
              {installations.data.installations.map((installation) => (
                <GitHubInstallationRow
                  key={installation.installation_id}
                  installation={installation}
                  removing={removeMutation.isPending}
                  onRemove={() =>
                    removeMutation.mutate(installation.installation_id)
                  }
                />
              ))}
            </ul>
          </section>
        </>
      )}
    </div>
  );
}

function GitHubInstallationRow({
  installation,
  removing,
  onRemove,
}: {
  installation: GithubInstallation;
  removing: boolean;
  onRemove: () => void;
}) {
  return (
    <li className="github-installation-row">
      <div className="github-installation-main">
        <div className="github-installation-title">
          <span className="github-installation-login">
            {installation.account_login}
          </span>
          <span className="chip github-account-type">
            {installation.account_type}
          </span>
          {installation.stale && (
            <span className="github-stale-marker">
              (stale — GitHub could not be reached)
            </span>
          )}
        </div>
        <div className="github-installation-meta">
          Installation {installation.installation_id}
          {installation.linked_by_github_login && (
            <> · Linked by @{installation.linked_by_github_login}</>
          )}
          {" · "}
          <a
            href={installation.manage_url}
            target="_blank"
            rel="noopener noreferrer"
          >
            Manage on GitHub ↗
          </a>
        </div>
        <div className="github-installation-repos">
          {installation.repositories.map((repo) => (
            <code key={repo}>{repo}</code>
          ))}
        </div>
        <div className="github-installation-synced">
          Repositories synced{" "}
          {new Date(installation.repositories_synced_at).toLocaleString()}
        </div>
      </div>
      <ConfirmButton
        label="Remove"
        prompt={`Remove the link to ${installation.account_login}? SignalDB stops using this installation immediately; the App stays installed on GitHub until you uninstall it there.`}
        disabled={removing}
        onConfirm={onRemove}
      />
    </li>
  );
}
