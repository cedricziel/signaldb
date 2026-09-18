// The stacktrace, one line per row, shared by the trace-detail exception
// panel (TracesView) and the errors occurrence detail (ErrorsView) — a
// `<SourceSnippet>` trigger after each line whose `extractFrameLocation`
// hits. `SourceSnippet` gates itself on `useSourceContextEnabled`, so this
// only needs a `tenant` to try; it renders nothing extra when the tenant
// isn't set or isn't gated in.
import { useMemo } from "react";
import { SourceSnippet } from "./SourceSnippet";
import { extractFrameLocation } from "../lib/sourceLocation";
import { parseStacktraceLines } from "../lib/stacktrace";

export type StacktraceVariant = "trace" | "error";

interface Props {
  text: string;
  tenant?: string;
  /** Repository/ref hints for the source lookup (see `repositoryHints`);
   * omitted to probe every repository the tenant's installations cover, at
   * each repository's default branch. */
  hints?: { repository?: string; ref?: string };
  /** Wrapper element's class; defaults to the variant's own wrapper class. */
  className?: string;
  /** Selects which view's existing CSS this renders under — `trace` for
   * `.span-event-trace-*` (TracesView), `error` for `.errors-stacktrace-*`
   * (ErrorsView). Defaults to `trace`. */
  variant?: StacktraceVariant;
}

const VARIANT = {
  trace: {
    Wrapper: "div",
    wrapperClass: "span-event-trace-lines",
    prefix: "span-event-trace",
    textClass: "span-event-trace-text",
  },
  error: {
    Wrapper: "pre",
    wrapperClass: "errors-stacktrace",
    prefix: "errors-stacktrace",
    textClass: undefined,
  },
} as const;

export function StacktraceLines({
  text,
  tenant,
  hints,
  className,
  variant = "trace",
}: Props) {
  const lines = useMemo(
    () =>
      parseStacktraceLines(text).map((line) => ({
        ...line,
        location:
          line.kind === "header" ? null : extractFrameLocation(line.text),
      })),
    [text],
  );
  const { Wrapper, wrapperClass, prefix, textClass } = VARIANT[variant];

  return (
    <Wrapper className={className ?? wrapperClass}>
      {lines.map((line, i) => (
        <div key={i} className={`${prefix}-line ${prefix}-${line.kind}`}>
          <span className={textClass}>{line.text}</span>
          {tenant && line.location && (
            <SourceSnippet
              tenant={tenant}
              repository={hints?.repository}
              gitRef={hints?.ref}
              path={line.location.path}
              line={line.location.line}
            />
          )}
        </div>
      ))}
    </Wrapper>
  );
}
