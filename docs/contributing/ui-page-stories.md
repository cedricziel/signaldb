---
audience: contributor
type: how-to
status: living
sources:
  - src/ui/e2e-stories/**
  - src/ui/playwright.stories.config.ts
  - src/ui/src/stories/PageFrame.tsx
  - src/ui/src/stories/DarkScope.tsx
  - src/ui/src/features/shell/AppNav.css
---

# Check the UI page stories at every screen size

Every routed page has a Storybook page story (`Pages/<Name>`, plus the
`Shell/App Shell` stories) with a light `Default` and a `Dark` variant. The
stories render in `PageFrame`, which is as wide as the viewport and one
viewport tall, so the same story shows the phone, tablet and desktop
layouts. `DarkScope` paints the whole frame and the document behind it dark.

`src/ui/e2e-stories/pageStories.spec.ts` renders every story whose id starts
with `pages-` or `shell-` from a static Storybook build, at 390, 768 and
1280 pixels wide, and fails when a render:

- throws (an uncaught page error, or Storybook's error display);
- scrolls the document sideways (`scrollWidth > clientWidth + 1`);
- has an axe `color-contrast` violation.

Dark mode is covered by the `Dark` stories themselves, so a page without one
is only checked in light.

## Prerequisites

- Node 24 and pnpm, dependencies installed (`pnpm install`).
- A Playwright Chromium (`pnpm --filter signaldb-ui exec playwright install
chromium`), or any local Chromium passed as `CHROMIUM_PATH`.

## Run the check

```bash
pnpm --filter signaldb-ui test:stories
```

The config builds Storybook into `src/ui/storybook-static/` (about 15
seconds), serves it with `vite preview` on port 6007, and runs the spec.
To reuse a build you already have, or a system Chromium:

```bash
cd src/ui
pnpm exec storybook build -c .storybook -o /tmp/sb
STORYBOOK_STATIC_DIR=/tmp/sb CHROMIUM_PATH=/path/to/chrome pnpm test:stories
```

A failure lists each story id, width and finding. Open the story at that
width in Storybook (`pnpm --filter signaldb-ui storybook`, then resize the
browser or use the viewport toolbar) to see it.

## Known violations

`src/ui/e2e-stories/allowlist.ts` lists the failures the check tolerates
today, each with a reason. A `color-contrast` entry caps the number of
failing elements, so a new violation on an allowlisted page still fails.
An entry that names no story fails the run. An entry that no longer fails is
reported as a `stale-allowlist` annotation: delete it once the page is
fixed.

Add an entry only for a violation someone is already fixing elsewhere, and
say so in its `reason`.

## Writing a page story

- Use `decorators: [pageFrame]` for a page that fills the viewport and
  scrolls inside its own panes, or `[growingPageFrame]` for one that scrolls
  the document (Overview, Catalog, Evals). Don't give the story a fixed
  width or height: the check and the design-sync capture set the viewport.
- Wrap each `Dark` story's page in `<DarkScope>`.
- Register the page for design-sync as `src/ui/CLAUDE.md` describes.

## Breakpoints

A page does not know how wide it is from the viewport: the app sidebar next
to it is 234px, 58px or gone depending on the width and on whether the user
collapsed it. `.app-main` (the column every page renders in) is therefore a
size container named `app-main`, and `PageFrame` is one too, so page stories
match the app.

- **Space for the page** (stack a grid, fold a side pane into a drawer,
  tighten padding, drop table columns): query the column, with two widths.

  ```css
  @container app-main (max-width: 720px) {
    /* side panes become drawers */
  }
  @container app-main (max-width: 600px) {
    /* phone-sized content */
  }
  ```

- **The device** (the shell itself, a viewport-fixed popover, the login
  page, which renders outside the shell): use a media query on the shell's
  own breakpoints from `NARROW_QUERY` and `TABLET_QUERY` in `AppNav.tsx`:
  `(max-width: 719px)` for the mobile top bar, `(max-width: 1023px)` where
  the sidebar starts collapsed.
- **Touch**: use `(hover: none)` to show controls that otherwise appear on
  hover, and `(pointer: coarse)` to enlarge hit areas to at least 32px
  (the block at the end of `styles/global.css`).

A resizable side pane caps its saved width to a share of the space it has,
so a width dragged on a wide screen never squeezes the list next to it on a
laptop. The facet sidebar uses `min(var(--sidebar-w), 32cqi)`, the
span-detail pane `min(var(--span-detail-w), 40%)`.

## In CI

The `UI` job in `.github/workflows/ci.yml` runs `test:stories` after the
mocked e2e suite, reusing the Chromium it installs. On failure the HTML
report is uploaded with the other Playwright reports.
