import type { ComponentType, ReactNode } from "react";

/**
 * The frame every page story (`Pages/*`, `Shell/*`) renders in: as wide as
 * the viewport, so phone and tablet widths reach the page's own responsive
 * CSS, and exactly one viewport tall (`fill`), so the app's `height: 100%`
 * flex layouts resolve the way they do under `#root`. At the design-sync
 * capture viewport (1280x800) that is the same box the old fixed
 * `1280x800` wrapper drew.
 *
 * `grow` is for pages that scroll the document instead of an inner pane
 * (Overview, Catalog, Evals): at least one viewport tall, taller when the
 * content is.
 */
export function PageFrame({
  fit = "fill",
  children,
}: {
  fit?: "fill" | "grow";
  children: ReactNode;
}) {
  return (
    <div
      data-page-frame=""
      style={
        fit === "fill"
          ? { width: "100%", height: "100vh" }
          : { width: "100%", minHeight: "100vh" }
      }
    >
      {children}
    </div>
  );
}

/** `decorators: [pageFrame]` for pages that fill one viewport. Typed on the
 * story component alone so it fits any story's args. */
export const pageFrame = (Story: ComponentType) => (
  <PageFrame>
    <Story />
  </PageFrame>
);

/** `decorators: [growingPageFrame]` for pages taller than the viewport. */
export const growingPageFrame = (Story: ComponentType) => (
  <PageFrame fit="grow">
    <Story />
  </PageFrame>
);
