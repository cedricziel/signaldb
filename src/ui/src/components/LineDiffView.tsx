import type { DiffLine } from "../features/processors/lineDiff";
import "./LineDiffView.css";

/** Renders a {@link DiffLine} sequence (`features/processors/lineDiff.ts`'s
 * before/after diff) as a `-`/`+`/space-prefixed line list — the processors
 * test panel's own rendering, pulled out so any other before/after text
 * comparison (a future dry-run view) can reuse it without re-deriving the
 * markup. */
export function LineDiffView({ diff }: { diff: DiffLine[] }) {
  return (
    <pre className="line-diff-view" aria-label="diff">
      {diff.map((line, index) => (
        <div
          key={index}
          className={`line-diff-view-line line-diff-view-${line.kind}`}
        >
          {line.kind === "removed" ? "- " : line.kind === "added" ? "+ " : "  "}
          {line.text}
        </div>
      ))}
    </pre>
  );
}
