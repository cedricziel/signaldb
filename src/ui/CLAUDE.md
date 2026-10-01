# UI guidelines

- Every new page (a routed view) ships with a Storybook page story — `Pages/<Name>`, with at least a light `Default` and a `Dark` story, fixtures derived from each request's own range — and is registered for design-sync: a `PAGES` line in `.design-sync/pkg/build.sh` plus `titleMap`/`overrides` entries in `.design-sync/config.json` (see `.design-sync/NOTES.md`)
