---
audience: contributor
type: how-to
status: living
sources:
  - src/ui/e2e-stories/**
  - src/ui/playwright.stories.config.ts
  - src/ui/src/stories/PageFrame.tsx
  - src/ui/src/stories/DarkScope.tsx
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

## In CI

The `UI` job in `.github/workflows/ci.yml` runs `test:stories` after the
mocked e2e suite, reusing the Chromium it installs. On failure the HTML
report is uploaded with the other Playwright reports.
