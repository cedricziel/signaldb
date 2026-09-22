# design-sync notes (SignalDB UI)

- [GENERAL] src/ui is an app, not a published package. `.design-sync/pkg/build.sh` (cfg.buildCmd) builds a stand-in library: tsc emits .d.ts for `src/ui/src/components/*.tsx`, and esbuild bundles an entry that re-exports the storied components + global.css into `.design-sync/pkg/dist/`. Add new storied components to COMPONENTS in build.sh.
- [GENERAL] build.sh mirrors vite `define` globals (`__SIGNALDB_*__`); a new define in vite.config.ts must be added there too, or esbuild leaves it as an undefined reference.
- [GENERAL] React Query context: the storybook decorator's QueryClientProvider (and story-local `@tanstack/react-query` imports) otherwise bundle a separate copy, so components see "No QueryClient set". Fix: the bundle exports QueryClient/QueryClientProvider/previewQueryClient, `cfg.provider` wraps with them, and `cfg.storyImports.shim: ["@tanstack/react-query"]` routes story imports to the bundle.
- SemanticKey story title → export `SemanticKeyLabel` (titleMap).
- Skipped (render nothing in storybook either): MobileSidebarDrawer OpenByDefault/RightSide (drawer is mobile-only CSS), SemanticKey Unresolved (renders null without semantics), SourceSnippet Unavailable (gated off with no seeded probe).
- ConfirmButton/SourceSnippet: owned previews replay the story `play` click after mount (storybook shows post-play state).
- BrandMark: an unsized svg that fills its container; the owned preview puts it in a 240px box.
- [FONT_MISSING] JetBrains Mono / Cascadia Code are fallback entries in the `--mono` system stack; the app ships no webfonts, so the system fonts are the real design (accepted).
- Framing: the storybook canvas paints `--bg` behind stories; the preview pages are white. That's a harness difference, not a component one.

## Re-sync risks
- Owned previews (ConfirmButton, SourceSnippet, BrandMark) mirror story exports by name. Renaming or adding stories needs them updated.
- build.sh's COMPONENTS list and define globals are hand-maintained.
- AttributeTable has 7 stories; the capture cap is 6, so the 7th was never graded.
- AttributeKeyInput Prefilled graded close (the capture auto-focuses the input and opens suggestions); SourceSnippet Available close (the focus-ring color differs between userEvent and programmatic focus).
