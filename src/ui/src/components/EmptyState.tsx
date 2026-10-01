import type { ReactNode } from "react";

interface Props {
  /** The headline, in one of two shapes: "No <things> in this range" when
   * the absence is scoped to the query/time window, or "No <things> yet"
   * when nothing has ever existed. */
  title: string;
  /** An existing actionable hint (a link, a suggestion), rendered smaller
   * and dimmer below the title. Omit when there's nothing more to say. */
  children?: ReactNode;
}

/**
 * The shared "nothing to show" render for every feature view — a list, a
 * table, a chart, a search result. `role="status"` so assistive tech
 * announces the empty result the same way a screen reader would notice new
 * rows appearing, without the alarm of `role="alert"`.
 */
export function EmptyState({ title, children }: Props) {
  return (
    <div className="empty-state" role="status">
      <p className="empty-state-title">{title}</p>
      {children && <p className="empty-state-detail">{children}</p>}
    </div>
  );
}
