/**
 * Known failures of `pageStories.spec.ts`, each with a reason. An entry
 * matches a story id exactly, or every id under a prefix ending in `*`;
 * `widths` narrows it to some viewport widths. A `color-contrast` entry
 * allows up to `nodes` failing elements per render (so a new violation on
 * the same page still fails); an `overflow` entry allows horizontal
 * document scroll. The spec fails on an entry that names no story, and
 * reports one that no longer fails as a `stale-allowlist` warning: fix the
 * page, then delete its line.
 */
export interface AllowEntry {
  story: string;
  check: "color-contrast" | "overflow";
  widths?: number[];
  nodes?: number;
  reason: string;
}

const PROCESSORS =
  "Processors list styling is owned by a separate processors-styling task: status pills and disabled rows (opacity) fall under 4.5:1.";
const REVOKED_KEYS =
  "Revoked API-key rows are dimmed with opacity, taking the name and meta line under 4.5:1.";

export const ALLOWLIST: AllowEntry[] = [
  {
    story: "pages-processors--list",
    check: "color-contrast",
    nodes: 10,
    reason: PROCESSORS,
  },
  {
    story: "pages-api-keys--default",
    check: "color-contrast",
    nodes: 2,
    reason: REVOKED_KEYS,
  },
  {
    story: "pages-api-keys--dark",
    check: "color-contrast",
    nodes: 2,
    reason: REVOKED_KEYS,
  },
];
