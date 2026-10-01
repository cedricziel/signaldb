// The shell's "Connect" dialog: how to reach this deployment from outside the
// browser — the MCP endpoint and how to add it to an agent, the CLI, and the
// HTTP API. Every URL comes from `GET /api/v1/connection`, which honors
// `[public]` in `signaldb.toml` rather than the browser's own hostname.

import { useQuery, type UseQueryResult } from "@tanstack/react-query";
import { useId, type ReactNode } from "react";
import { Link } from "react-router";
import {
  bearerCredential,
  connectionQuery,
  type ConnectionHeaders,
  type ConnectionInfoResponse,
} from "../../api/connection";
import { ApiError, toErrorMessage } from "../../api/http";
import { CopyValueButton } from "../../components/CopyValueButton";
import { Dialog } from "../../components/Dialog";
import type { ExploreState } from "../../lib/urlState";
import { SkeletonLines } from "../explore/Skeleton";
import "./ConnectPanel.css";

const SAMPLE_IR =
  '{"irVersion":1,"from":"logs","range":{"from":"now-15m","to":"now"},"result":"rows"}';

function claudeCodeCommand(headers: ConnectionHeaders, mcpUrl: string) {
  return [
    `claude mcp add --transport http signaldb ${mcpUrl}`,
    `  --header "Authorization: ${headers.authorization}"`,
    `  --header "X-Tenant-ID: ${headers["x-tenant-id"]}"`,
  ].join(" \\\n");
}

function cliSnippet(info: ConnectionInfoResponse) {
  return [
    `export SIGNALDB_URL=${info.query.api_url}`,
    `export SIGNALDB_API_KEY=${bearerCredential(info.headers.authorization)}`,
    `export SIGNALDB_TENANT_ID=${info.headers["x-tenant-id"]}`,
    `export SIGNALDB_DATASET_ID=${info.headers["x-dataset-id"]}`,
    "",
    "signaldb-cli whoami",
    `signaldb-cli query --ir '${SAMPLE_IR}'`,
  ].join("\n");
}

function curlSnippet(headers: ConnectionHeaders, queryUrl: string) {
  return [
    `curl -X POST ${queryUrl}`,
    `  -H "Authorization: ${headers.authorization}"`,
    `  -H "X-Tenant-ID: ${headers["x-tenant-id"]}"`,
    `  -H "X-Dataset-ID: ${headers["x-dataset-id"]}"`,
    `  -H "Content-Type: application/json"`,
    `  -d '${SAMPLE_IR}'`,
  ].join(" \\\n");
}

export function ConnectPanel({
  state,
  canManage,
  onClose,
}: {
  state: Pick<ExploreState, "tenant" | "dataset">;
  canManage: boolean;
  onClose: () => void;
}) {
  const connection = useQuery(connectionQuery(state));
  return (
    <Dialog label="Connect" onClose={onClose} className="connect-panel">
      <div className="connect-head">
        <h2 className="connect-title">Connect</h2>
        <p className="connect-copy">
          Reach{" "}
          <strong>
            {state.tenant} / {state.dataset}
          </strong>{" "}
          from an agent, the CLI or your own code. Replace{" "}
          <code>&lt;api-key&gt;</code> with a key that has the read scopes.
          {canManage && (
            <>
              {" "}
              <Link to="/api-keys" onClick={onClose}>
                Create an API key
              </Link>
            </>
          )}
        </p>
      </div>
      <ConnectBody connection={connection} />
    </Dialog>
  );
}

function ConnectBody({
  connection,
}: {
  connection: UseQueryResult<ConnectionInfoResponse>;
}) {
  if (connection.isError) {
    return (
      <div className="error-text" role="alert">
        {connection.error instanceof ApiError &&
        connection.error.status === 403 ? (
          <p>The current tenant does not grant access to connection details.</p>
        ) : (
          <>
            <p>
              Could not load connection details:{" "}
              {toErrorMessage(connection.error)}
            </p>
            <button
              type="button"
              className="btn"
              onClick={() => void connection.refetch()}
            >
              Retry
            </button>
          </>
        )}
      </div>
    );
  }
  const info = connection.data;
  if (!info) return <SkeletonLines lines={10} />;

  const { headers, mcp, query } = info;
  const queryUrl = `${query.api_url}${query.query_ir}`;
  return (
    <>
      {info.notes.length > 0 && (
        <div className="warn-callout connect-notes" role="note">
          {info.notes.map((n) => (
            <p key={n}>{n}</p>
          ))}
        </div>
      )}
      <Section title="MCP">
        {mcp ? (
          <>
            <Value label="Endpoint" value={mcp.url} />
            <Snippet
              title="Claude Code"
              value={claudeCodeCommand(headers, mcp.url)}
            />
            <p className="connect-hint">
              Claude.ai and ChatGPT: open Settings → Connectors → Add custom
              connector and paste the endpoint. You sign in and pick tenants on
              the consent screen; no API key needed.
            </p>
          </>
        ) : (
          <p className="connect-hint">
            No MCP endpoint is configured for this deployment.
          </p>
        )}
      </Section>
      <Section title="CLI">
        <Snippet title="signaldb-cli" value={cliSnippet(info)} />
      </Section>
      <Section title="HTTP API">
        <Value label="Base URL" value={query.api_url} />
        <Value label="Query IR" value={queryUrl} />
        <Value label="OpenAPI" value={`${query.api_url}${query.openapi}`} />
        <Snippet title="curl" value={curlSnippet(headers, queryUrl)} />
      </Section>
    </>
  );
}

function Section({ title, children }: { title: string; children: ReactNode }) {
  const id = useId();
  return (
    <section className="connect-section" aria-labelledby={id}>
      <h3 id={id} className="connect-section-title">
        {title}
      </h3>
      {children}
    </section>
  );
}

function Value({ label, value }: { label: string; value: string }) {
  return (
    <div className="connect-value">
      <span className="connect-value-label">{label}</span>
      <code className="connect-value-text">{value}</code>
      <CopyValueButton value={value} label={label} />
    </div>
  );
}

function Snippet({ title, value }: { title: string; value: string }) {
  return (
    <div className="connect-snippet">
      <div className="connect-snippet-head">
        <span>{title}</span>
        <CopyValueButton value={value} label={`${title} snippet`} />
      </div>
      <pre>
        <code>{value}</code>
      </pre>
    </div>
  );
}
