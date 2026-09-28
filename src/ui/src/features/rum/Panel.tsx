// The Real users page's shared card chrome — title, optional meta text,
// padded body. Mirrors the design prototype's `Panel` helper
// (rum/overview.jsx), shared by the Overview and Setup tabs.
import type { ReactNode } from "react";

export function Panel({
  title,
  meta,
  actions,
  children,
}: {
  title: string;
  meta?: string;
  /** A drill-in control (e.g. "View all") at the header's trailing edge. */
  actions?: ReactNode;
  children: ReactNode;
}) {
  return (
    <section className="rum-panel">
      <header className="rum-panel-h">
        <h3>{title}</h3>
        {meta && <span className="rum-meta">{meta}</span>}
        {actions && <span className="rum-panel-actions">{actions}</span>}
      </header>
      <div className="rum-panel-body">{children}</div>
    </section>
  );
}
