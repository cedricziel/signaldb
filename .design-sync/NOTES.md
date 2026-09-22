# design-sync notes (SignalDB UI)

- [GENERAL] src/ui is an app, not a published package. `.design-sync/pkg/build.sh` (cfg.buildCmd) builds a stand-in library: tsc emits .d.ts for `src/ui/src/components/*.tsx`, and esbuild bundles an entry that re-exports the storied components + global.css into `.design-sync/pkg/dist/`. Add new storied components to COMPONENTS in build.sh.
- [GENERAL] build.sh mirrors vite `define` globals (`__SIGNALDB_*__`); a new define in vite.config.ts must be added there too, or esbuild leaves it as an undefined reference.
- [GENERAL] React Query context: the storybook decorator's QueryClientProvider (and story-local `@tanstack/react-query` imports) otherwise bundle a separate copy, so components see "No QueryClient set". Fix: the bundle exports QueryClient/QueryClientProvider/previewQueryClient, `cfg.provider` wraps with them, and `cfg.storyImports.shim: ["@tanstack/react-query"]` routes story imports to the bundle.
- [GENERAL] Pages (LogsView, TracesView) are exported via PAGES in build.sh and mapped by titleMap (Pages/Logs, Pages/Traces). Their stories import `MemoryRouter` and stub the generated API client; `react-router` and `api/gen/client.gen` are shimmed to the bundle (storyImports.shim) so router context and the fetch stub reach the components. Without it: "useNavigate() may be used only in the context of a <Router>" and silently unstubbed fetches.
- [GENERAL] build.sh imports global.css FIRST in the entry, matching main.tsx. Loaded last, its `.btn` rules override feature CSS of equal specificity (e.g. `.mobile-filters-toggle {display:none}`), so the mobile Filters button showed at desktop width.
- Pages use cardMode single + viewport 1280x800; at the default capture width they fall under the 900px breakpoint and collapse the sidebar.
- MemberTable (Populated) and StacktraceLines (Trace Variant) rely on traces.css, which only TracesView imports. The storybook reference renders them without those styles; the preview (bundle carries all CSS) matches the real app. Graded match against the app. Fixing it in the repo means importing traces.css from those components.
- SemanticKey story title → export `SemanticKeyLabel` (titleMap).
- Skipped (render nothing in storybook either): MobileSidebarDrawer OpenByDefault/RightSide (drawer is mobile-only CSS), SemanticKey Unresolved (renders null without semantics), SourceSnippet Unavailable (gated off with no seeded probe).
- ConfirmButton/SourceSnippet: owned previews replay the story `play` click after mount (storybook shows post-play state).
- BrandMark: an unsized svg that fills its container; the owned preview puts it in a 240px box.
- [FONT_MISSING] JetBrains Mono / Cascadia Code are fallback entries in the `--mono` system stack; the app ships no webfonts, so the system fonts are the real design (accepted).
- Framing: the storybook canvas paints `--bg` behind stories; the preview pages are white. That's a harness difference, not a component one.

## Re-sync risks
- Owned previews (ConfirmButton, SourceSnippet, BrandMark) mirror story exports by name. Renaming or adding stories needs them updated.
- build.sh's COMPONENTS/PAGES lists and define globals are hand-maintained.
- TracesView Trace Detail graded close: the preview shows a Details header button the storybook render omits at this width.
- AttributeTable has 7 stories; the capture cap is 6, so the 7th was never graded.
- AttributeKeyInput Prefilled graded close (the capture auto-focuses the input and opens suggestions); SourceSnippet Available close (the focus-ring color differs between userEvent and programmatic focus).
