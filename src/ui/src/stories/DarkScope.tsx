import { useLayoutEffect, type ReactNode } from "react";

/** Wraps a page story in a `data-theme="dark"` scope (see `global.css`'s
 * scoped `[data-theme]` overrides) so a `Dark` page story renders correctly
 * regardless of the Storybook toolbar's own theme selection — used for the
 * `Pages/*` "Dark" variants that feed Claude Design's dark-mode cards.
 *
 * The scope takes the whole {@link PageFrame} (`height: 100%`, at least one
 * viewport), and while mounted the document root is dark too, so `body`
 * paints `--bg` behind anything that overflows the frame: no light band
 * below a short page or under a phone-width page that runs past the fold. */
export function DarkScope({ children }: { children: ReactNode }) {
  useLayoutEffect(() => {
    const root = document.documentElement;
    const previous = root.getAttribute("data-theme");
    root.setAttribute("data-theme", "dark");
    return () => {
      if (previous === null) root.removeAttribute("data-theme");
      else root.setAttribute("data-theme", previous);
    };
  }, []);
  return (
    <div
      data-theme="dark"
      style={{ width: "100%", height: "100%", minHeight: "100vh" }}
    >
      {children}
    </div>
  );
}
