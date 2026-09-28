// What the ⌘K command palette lists for a given query — pure so the grouping,
// caps and trace-ID detection are testable without rendering the dialog.

export interface PaletteItem {
  label: string;
  /** Right-aligned hint: the page's group, "service", the signal, … */
  meta: string;
  /** In-app path (with search) the row navigates to. */
  href: string;
  /** Pages only: the nav group it sits under in the sidebar. */
  group?: string;
}

export interface PaletteGroup {
  title: string;
  items: PaletteItem[];
}

export interface PaletteSources {
  pages: PaletteItem[];
  services: PaletteItem[];
  /** Frontend apps with RUM data (`explore-ui-rum`'s "Real users command
   * palette entries" requirement) — matched like `services`, not shown on
   * an empty query. */
  rumApps: PaletteItem[];
  recent: PaletteItem[];
  actions: PaletteItem[];
  /** The "open session" row for a pasted session id, once the caller's
   * bounded lookup (`fetchSessionLookup`) has resolved it — `null` while
   * unresolved or when the id doesn't match anything, in which case
   * `isSessionIdLike`'s "Jump to ID" group shows no items rather than a
   * spinner. */
  sessionLookup: PaletteItem | null;
}

const TRACE_OR_SPAN_ID = /^(?:[0-9a-f]{16}|[0-9a-f]{32})$/i;

export function isTraceOrSpanId(query: string): boolean {
  return TRACE_OR_SPAN_ID.test(query.trim());
}

const SESSION_ID_UUID =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const SESSION_ID_HEX = /^[0-9a-f]{12,}$/i;

/** A UUID, or a bare hex string of at least 12 characters — the spec's own
 * "session id (UUID or ≥12 hex chars)" — excluding trace/span ids, which
 * take priority. */
export function isSessionIdLike(query: string): boolean {
  const q = query.trim();
  if (isTraceOrSpanId(q)) return false;
  return SESSION_ID_UUID.test(q) || SESSION_ID_HEX.test(q);
}

export function buildPaletteGroups(
  query: string,
  sources: PaletteSources,
): PaletteGroup[] {
  const q = query.trim();
  if (q === "") {
    return nonEmpty([
      ...groupPages(sources.pages),
      { title: "Recent queries", items: sources.recent.slice(0, 3) },
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
  if (isSessionIdLike(q)) {
    // The caller's bounded lookup resolves asynchronously — no group (not
    // an empty "Jump to ID" group) while it's pending or came back empty,
    // so a wrong guess doesn't flash a heading with nothing under it.
    return sources.sessionLookup
      ? [{ title: "Jump to ID", items: [sources.sessionLookup] }]
      : [];
  }
  const needle = q.toLowerCase();
  const match = (items: PaletteItem[], cap: number) =>
    items.filter((i) => i.label.toLowerCase().includes(needle)).slice(0, cap);
  return nonEmpty([
    { title: "Pages", items: match(sources.pages, 6) },
    { title: "Services", items: match(sources.services, 5) },
    { title: "Real users apps", items: match(sources.rumApps, 5) },
    { title: "Recent queries", items: match(sources.recent, 3) },
    { title: "Actions", items: match(sources.actions, 4) },
  ]);
}

/** Pages split by their nav group, in first-seen order — the sidebar's. */
function groupPages(pages: PaletteItem[]): PaletteGroup[] {
  const groups = new Map<string, PaletteItem[]>();
  for (const p of pages) {
    const title = p.group ?? "Pages";
    groups.set(title, [...(groups.get(title) ?? []), p]);
  }
  return [...groups].map(([title, items]) => ({ title, items }));
}

function nonEmpty(groups: PaletteGroup[]): PaletteGroup[] {
  return groups.filter((g) => g.items.length > 0);
}
