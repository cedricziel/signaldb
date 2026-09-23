import type { ReactNode } from "react";

/** Wraps a page story in a `data-theme="dark"` scope (see `global.css`'s
 * scoped `[data-theme]` overrides) so a `Dark` page story renders correctly
 * regardless of the Storybook toolbar's own theme selection — used for the
 * `Pages/*` "Dark" variants that feed Claude Design's dark-mode cards. */
export function DarkScope({ children }: { children: ReactNode }) {
  return (
    <div data-theme="dark" style={{ width: "100%", minHeight: "100%" }}>
      {children}
    </div>
  );
}
