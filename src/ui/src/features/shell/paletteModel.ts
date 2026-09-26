// What the ⌘K command palette lists for a given query — pure so the grouping,
// caps and trace-ID detection are testable without rendering the dialog.

export interface PaletteItem {
  label: string;
  /** Right-aligned hint: the page's group, "service", the signal, … */
  meta: string;
  /** In-app path (with search) the row navigates to. */
  href: string;
}

export interface PaletteGroup {
  title: string;
  items: PaletteItem[];
}

export interface PaletteSources {
  pages: PaletteItem[];
  services: PaletteItem[];
  recent: PaletteItem[];
  actions: PaletteItem[];
}

const TRACE_OR_SPAN_ID = /^(?:[0-9a-f]{16}|[0-9a-f]{32})$/i;

export function isTraceOrSpanId(query: string): boolean {
  return TRACE_OR_SPAN_ID.test(query.trim());
}

export function buildPaletteGroups(
  query: string,
  sources: PaletteSources,
): PaletteGroup[] {
  const q = query.trim();
  if (q === "") {
    return nonEmpty([
      { title: "Recent queries", items: sources.recent.slice(0, 3) },
      { title: "Pages", items: sources.pages.slice(0, 6) },
      { title: "Actions", items: sources.actions.slice(0, 3) },
    ]);
  }
  if (isTraceOrSpanId(q)) {
    // 16 hex digits is also a legacy 64-bit trace id. There's no span-id
    // lookup in the traces view yet, so both lengths open the trace view.
    const id = q.toLowerCase();
    return [
      {
        title: "Jump to ID",
        items: [
          {
            label: `Open trace ${id}`,
            meta: "trace",
            href: `/traces/${encodeURIComponent(id)}`,
          },
        ],
      },
    ];
  }
  const needle = q.toLowerCase();
  const match = (items: PaletteItem[], cap: number) =>
    items.filter((i) => i.label.toLowerCase().includes(needle)).slice(0, cap);
  return nonEmpty([
    { title: "Pages", items: match(sources.pages, 6) },
    { title: "Services", items: match(sources.services, 5) },
    { title: "Recent queries", items: match(sources.recent, 3) },
    { title: "Actions", items: match(sources.actions, 4) },
  ]);
}

function nonEmpty(groups: PaletteGroup[]): PaletteGroup[] {
  return groups.filter((g) => g.items.length > 0);
}
