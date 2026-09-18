// The "View source" affordance behind a stack frame — see
// docs/users/explore-ui.md's "View source (GitHub)". A compact trigger that,
// on click, fetches the bounded snippet around one file/line from the
// tenant's linked GitHub repositories and expands inline; nothing is
// fetched until then, and an unavailable/error result renders one short
// sentence rather than failing the surrounding view.
import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import {
  fetchSourceContext,
  type SourceSnippet as SourceSnippetResult,
  type UnavailableReason,
} from "../api/sourceContext";
import { toErrorMessage } from "../api/http";
import { useSourceContextEnabled } from "../lib/useSourceContextEnabled";
import "./SourceSnippet.css";

interface Props {
  tenant: string;
  /** `owner/name` (or a GitHub URL); omitted to probe every repository the
   * tenant's installations cover. */
  repository?: string;
  /** Commit/branch/tag; omitted to read the repository's default branch. */
  gitRef?: string;
  path: string;
  line: number;
  /** Trigger button text; defaults to "View source". */
  label?: string;
}

const REASON_MESSAGES: Record<UnavailableReason, string> = {
  not_configured: "GitHub is not configured on this server.",
  no_installation: "No linked GitHub repository covers this file.",
  not_found: "File not found in the linked repositories at that ref.",
  not_a_file: "This path can't be shown (not a text file, or too large).",
  too_large: "This path can't be shown (not a text file, or too large).",
  undecodable: "This path can't be shown (not a text file, or too large).",
  line_out_of_range: "The file is shorter than that line at that ref.",
  github_error: "Source is temporarily unavailable.",
  internal: "Source is temporarily unavailable.",
};

/** `@<short sha>` for a commit-shaped ref, else `@<ref>` as given (a branch
 * or tag name is already short). */
function refBadge(ref: string | null | undefined): string {
  if (!ref) return "default branch (unpinned)";
  const short = /^[0-9a-f]{7,40}$/i.test(ref) ? ref.slice(0, 7) : ref;
  return `@${short}`;
}

/** The trigger button plus its expandable panel. Gates itself on
 * `useSourceContextEnabled` — a caller never needs to check the tenant's
 * GitHub-linked state before rendering this — and renders nothing at all
 * when it isn't. */
export function SourceSnippet({
  tenant,
  repository,
  gitRef,
  path,
  line,
  label,
}: Props) {
  const [open, setOpen] = useState(false);
  const enabled = useSourceContextEnabled(tenant);
  if (!enabled) return null;

  return (
    <span className="source-snippet-wrap">
      <button
        type="button"
        className="btn btn-ghost source-snippet-trigger"
        title={`${path}:${line}`}
        aria-expanded={open}
        onClick={() => setOpen((v) => !v)}
      >
        {label ?? "View source"}
      </button>
      {open && (
        <SourceSnippetPanel
          tenant={tenant}
          repository={repository}
          gitRef={gitRef}
          path={path}
          line={line}
        />
      )}
    </span>
  );
}

/** The fetched snippet (or its loading/error/unavailable state) — mounted,
 * and its `useQuery` only running, while the trigger is open, so a
 * stacktrace with many frames doesn't register a query per frame up front. */
function SourceSnippetPanel({
  tenant,
  repository,
  gitRef,
  path,
  line,
}: Omit<Props, "label">) {
  const query = useQuery({
    queryKey: [
      "source-context",
      tenant,
      repository ?? "",
      gitRef ?? "",
      path,
      line,
    ],
    queryFn: () =>
      fetchSourceContext(tenant, {
        repository,
        ref: gitRef,
        path,
        line,
      }),
    staleTime: 5 * 60_000,
    retry: false,
  });

  return (
    <span className="source-snippet-panel">
      {query.isPending && <span className="view-note">Loading source…</span>}
      {query.isError && (
        <span className="error-text">{toErrorMessage(query.error)}</span>
      )}
      {query.data?.status === "unavailable" && (
        <span className="view-note">
          {query.data.reason
            ? REASON_MESSAGES[query.data.reason]
            : "Source is temporarily unavailable."}
        </span>
      )}
      {query.data?.status === "available" && query.data.snippet && (
        <SnippetBody snippet={query.data.snippet} />
      )}
    </span>
  );
}

function SnippetBody({ snippet }: { snippet: SourceSnippetResult }) {
  return (
    <span className="source-snippet-body">
      <span className="source-snippet-head">
        <a
          href={snippet.html_url}
          target="_blank"
          rel="noopener noreferrer"
        >
          {snippet.repository} · {snippet.path}
        </a>
        <span className="source-snippet-ref">{refBadge(snippet.ref)}</span>
      </span>
      <pre className="source-snippet">
        {snippet.lines.map((text, i) => {
          const lineNo = snippet.start_line + i;
          const current = lineNo === snippet.line;
          return (
            <div
              key={lineNo}
              className={`source-snippet-line${current ? " source-snippet-line-current" : ""}`}
              aria-current={current ? "true" : undefined}
            >
              <span className="source-snippet-lineno">{lineNo}</span>
              <span className="source-snippet-text">{text}</span>
            </div>
          );
        })}
      </pre>
    </span>
  );
}
