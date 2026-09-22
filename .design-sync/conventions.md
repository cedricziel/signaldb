# SignalDB UI conventions

These components come from the SignalDB observability console (logs, traces, metrics, profiles). It's a dense, data-heavy tool UI: small type, monospace for data, flat surfaces, one orange accent.

## Setup

Wrap the app in `QueryClientProvider` with the bundled `previewQueryClient`. `SourceSnippet` and a few others call React Query hooks; without the provider they throw "No QueryClient set".

```jsx
const { QueryClientProvider, previewQueryClient } = window.SignalDBUI;
<QueryClientProvider client={previewQueryClient}>{app}</QueryClientProvider>
```

The theme comes from `prefers-color-scheme`. To force one, put `data-theme="light"` or `data-theme="dark"` on `<html>`. The page background is `var(--bg)`, and panels and cards sit on `var(--surface)`.

## Styling: CSS custom properties plus a few global classes

There's no utility-class system. Style your own layout with plain CSS that uses these tokens (they're defined in `styles.css` → `_ds_bundle.css`):

- Surfaces: `--bg`, `--surface`, `--surface2`, `--surface3`, `--border`, `--bg-inset`
- Text: `--text`, `--dim` (secondary), `--faint` (tertiary)
- Accent: `--accent`, `--accent-soft` (tint), `--on-accent` (text on solid accent)
- Status: `--err`, `--warn`, `--info`, `--ok`, `--debug`, plus `--err-bar`, `--warn-bar`, `--info-bar`, `--debug-bar` for chart and level bars, and `--ok-text` / `--warn-banner-text` for AA text on tints
- Series colors: `--svc-a` … `--svc-l`
- Fonts: `--ui` (system sans) for chrome, `--mono` for every data value (attribute keys, IDs, durations, code)
- Spacing and type: `--gutter` (16px pane inset), `--gutter-sm` (12px), `--text-title` (18px page and dialog titles), `--text-section` (14px section headings)

Global classes you can use on your own elements: `btn`, `btn-primary`, `btn-danger`, `btn-ghost` for buttons; `chip` for filter chips; `error-text` for inline errors. Don't make up other class names. Use tokens in inline or local CSS instead.

Look/feel rules: 1px `var(--border)` hairlines, small radii (4–6px), no heavy shadows (only Dialog and floating tooltips have one), and `--accent` only for the primary action or selection.

## Where the truth lives

Read `styles.css` and its import `_ds_bundle.css` for every token and class. Each component folder has `<Name>.d.ts` (props) and `<Name>.prompt.md` (usage).

## Example

```jsx
const { EmptyState, ConfirmButton, AttributeTable } = window.SignalDBUI;
<section style={{ background: "var(--surface)", border: "1px solid var(--border)",
  borderRadius: 6, padding: "var(--gutter)" }}>
  <h2 style={{ fontSize: "var(--text-section)", margin: 0, color: "var(--text)" }}>Datasets</h2>
  <EmptyState title="No datasets yet">Create one to start ingesting.</EmptyState>
  <ConfirmButton label="Delete" prompt="Delete dataset staging?" onConfirm={() => {}} />
</section>
```
