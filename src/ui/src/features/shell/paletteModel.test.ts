import { describe, expect, it } from "vitest";
import { buildPaletteGroups, type PaletteSources } from "./paletteModel";

const item = (label: string, meta = "x") => ({
  label,
  meta,
  href: `/${label}`,
});

const SOURCES: PaletteSources = {
  pages: [
    "Errors",
    "Catalog",
    "Logs",
    "Traces",
    "Metrics",
    "Profiles",
    "Query",
  ].map((l) => item(l, "page")),
  services: ["checkout", "cart", "payments"].map((l) => item(l, "service")),
  recent: ["level=error", "service.name=checkout", "a", "b"].map((l) =>
    item(l, "logs"),
  ),
  actions: [
    "Invite members",
    "Create API key",
    "Instrument a service",
    "Switch tenant",
  ].map((l) => item(l, "action")),
};

const titles = (q: string) =>
  buildPaletteGroups(q, SOURCES).map((g) => g.title);

describe("buildPaletteGroups", () => {
  it("lists recent queries (3), pages (6) and actions (3) for an empty query", () => {
    const groups = buildPaletteGroups("  ", SOURCES);
    expect(groups.map((g) => [g.title, g.items.length])).toEqual([
      ["Recent queries", 3],
      ["Pages", 6],
      ["Actions", 3],
    ]);
  });

  it("substring-matches every source, hiding empty groups", () => {
    expect(titles("check")).toEqual(["Services", "Recent queries"]);
    const [pages] = buildPaletteGroups("TRA", SOURCES);
    expect(pages).toEqual({ title: "Pages", items: [item("Traces", "page")] });
  });

  it("returns no groups when nothing matches", () => {
    expect(buildPaletteGroups("zzz", SOURCES)).toEqual([]);
  });

  it("offers only the trace jump for a pasted 32- or 16-digit hex id", () => {
    const id = "4BF92F3577B34DA6A3CE929D0E0E4736";
    expect(buildPaletteGroups(id, SOURCES)).toEqual([
      {
        title: "Jump to ID",
        items: [
          {
            label: `Open trace ${id.toLowerCase()}`,
            meta: "trace",
            href: `/traces/${id.toLowerCase()}`,
          },
        ],
      },
    ]);
    expect(titles("00f067aa0ba902b7")).toEqual(["Jump to ID"]);
    // Not an id: wrong length, or not hex.
    expect(titles("00f067aa0ba902b")).not.toContain("Jump to ID");
  });
});
