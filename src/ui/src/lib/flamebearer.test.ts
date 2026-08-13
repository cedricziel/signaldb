import { describe, expect, it } from "vitest";
import type { Flamebearer } from "../api/pyroscope";
import {
  ancestorPath,
  collapseSmallFrames,
  colorBucket,
  decodeFlamebearer,
  type FlameFrame,
  formatPct,
  formatTicks,
  frameView,
  OTHER_FRAME_NAME,
  placeFrames,
  rootView,
  topFunctionsBySelf,
} from "./flamebearer";

// root(100) -> [a(60 self 20), b(40 self 40)]; a -> [c(40 self 40)]
// Encoding (delta offset from previous frame's end at that level):
//   L0: total spans 0..100
//   L1: a at x=0 (offset 0), b at x=60 (offset 0 after a ends at 60)
//   L2: c at x=0 (offset 0)
const FB: Flamebearer = {
  names: ["total", "a", "b", "c"],
  levels: [
    [0, 100, 0, 0],
    [0, 60, 20, 1, 0, 40, 40, 2],
    [0, 40, 40, 3],
  ],
  numTicks: 100,
  maxSelf: 40,
};

describe("decodeFlamebearer", () => {
  it("expands delta offsets into absolute frames", () => {
    const levels = decodeFlamebearer(FB);
    expect(levels[0]).toEqual([
      { level: 0, x: 0, total: 100, self: 0, name: "total" },
    ]);
    expect(levels[1]).toEqual([
      { level: 1, x: 0, total: 60, self: 20, name: "a" },
      { level: 1, x: 60, total: 40, self: 40, name: "b" },
    ]);
    expect(levels[2]).toEqual([
      { level: 2, x: 0, total: 40, self: 40, name: "c" },
    ]);
  });

  it("places a sibling after the previous frame using its delta", () => {
    // Two siblings with a gap: second starts at 70 (delta 10 after first
    // ends at 60).
    const fb: Flamebearer = {
      names: ["total", "x", "y"],
      levels: [
        [0, 100, 0, 0],
        [0, 60, 60, 1, 10, 30, 30, 2],
      ],
      numTicks: 100,
      maxSelf: 60,
    };
    const [, level1] = decodeFlamebearer(fb);
    expect(level1?.map((f) => f.x)).toEqual([0, 70]);
  });
});

describe("placeFrames", () => {
  it("scales frames to viewport percentages at the root", () => {
    const levels = decodeFlamebearer(FB);
    const placed = placeFrames(levels, rootView(FB));
    expect(placed[1]?.[0]).toMatchObject({ leftPct: 0, widthPct: 60 });
    expect(placed[1]?.[1]).toMatchObject({ leftPct: 60, widthPct: 40 });
  });

  it("zooms so the focused frame fills the width, ancestors span above it", () => {
    const levels = decodeFlamebearer(FB);
    const a = levels[1]![0]!; // frame "a": x 0..60
    const placed = placeFrames(levels, frameView(a));
    // Ancestor "total" (level 0) is clamped to full width above the focus.
    expect(placed[0]).toEqual([
      {
        frame: expect.objectContaining({ name: "total" }),
        leftPct: 0,
        widthPct: 100,
      },
    ]);
    // The focused frame fills its own level.
    expect(placed[1]?.[0]).toMatchObject({
      leftPct: 0,
      widthPct: 100,
      frame: { name: "a" },
    });
    // "c" (child of a) scales within the window; "b" (x 60..100) drops out.
    const names = placed.flat().map((p) => p.frame.name);
    expect(names).toContain("c");
    expect(names).not.toContain("b");
    expect(placed[2]?.[0]?.frame.name).toBe("c");
  });
});

describe("collapseSmallFrames", () => {
  // total(100) -> [big(70, self 0), s1(10,self10), s2(10,self10), s3(10,self10)]
  // big -> gc(70, self 70); s2 -> leaf(10, self 10)
  const levels: FlameFrame[][] = [
    [{ level: 0, x: 0, total: 100, self: 0, name: "total" }],
    [
      { level: 1, x: 0, total: 70, self: 0, name: "big" },
      { level: 1, x: 70, total: 10, self: 10, name: "s1" },
      { level: 1, x: 80, total: 10, self: 10, name: "s2" },
      { level: 1, x: 90, total: 10, self: 10, name: "s3" },
    ],
    [
      { level: 2, x: 0, total: 70, self: 70, name: "gc" },
      { level: 2, x: 80, total: 10, self: 10, name: "leaf-under-s2" },
    ],
  ];

  it("returns the levels unchanged when the threshold is off", () => {
    expect(collapseSmallFrames(levels, 100, 0)).toBe(levels);
  });

  it("merges a contiguous run of below-threshold siblings into one (other), dropping their descendants", () => {
    const collapsed = collapseSmallFrames(levels, 100, 0.15);

    expect(collapsed[1]).toEqual([
      { level: 1, x: 0, total: 70, self: 0, name: "big" },
      { level: 1, x: 70, total: 30, self: 30, name: OTHER_FRAME_NAME },
    ]);
    // "gc" survives (child of the kept "big"); "leaf-under-s2" is dropped
    // (its parent "s2" was folded into "(other)").
    expect(collapsed[2]).toEqual([
      { level: 2, x: 0, total: 70, self: 70, name: "gc" },
    ]);
  });

  it("collapses even a single isolated small frame, uniformly", () => {
    const oneSmall: FlameFrame[][] = [
      [{ level: 0, x: 0, total: 100, self: 0, name: "total" }],
      [
        { level: 1, x: 0, total: 95, self: 95, name: "big" },
        { level: 1, x: 95, total: 5, self: 5, name: "tiny" },
      ],
    ];
    const collapsed = collapseSmallFrames(oneSmall, 100, 0.1);
    expect(collapsed[1]).toEqual([
      { level: 1, x: 0, total: 95, self: 95, name: "big" },
      { level: 1, x: 95, total: 5, self: 5, name: OTHER_FRAME_NAME },
    ]);
  });
});

describe("topFunctionsBySelf", () => {
  it("aggregates self and total time by name across repeated (recursive) occurrences, sorted by self descending", () => {
    const levels: FlameFrame[][] = [
      [{ level: 0, x: 0, total: 100, self: 0, name: "total" }],
      [
        { level: 1, x: 0, total: 60, self: 20, name: "a" },
        { level: 1, x: 60, total: 40, self: 40, name: "b" },
      ],
      [
        { level: 2, x: 0, total: 40, self: 40, name: "c" },
        { level: 2, x: 60, total: 10, self: 5, name: "a" }, // recursive
      ],
    ];

    const top = topFunctionsBySelf(levels);
    expect(top).toEqual([
      { name: "b", self: 40, total: 40, count: 1 },
      { name: "c", self: 40, total: 40, count: 1 },
      { name: "a", self: 25, total: 70, count: 2 },
      { name: "total", self: 0, total: 100, count: 1 },
    ]);
  });
});

describe("ancestorPath", () => {
  it("returns just the root when targeting the root frame", () => {
    const levels = decodeFlamebearer(FB);
    const root = levels[0]![0]!;
    expect(ancestorPath(levels, root)).toEqual([root]);
  });

  it("walks from the root down to a deep frame, skipping unrelated siblings", () => {
    const levels = decodeFlamebearer(FB);
    const c = levels[2]![0]!; // "c", nested under "a", sibling of "b"
    const path = ancestorPath(levels, c);
    expect(path.map((f) => f.name)).toEqual(["total", "a", "c"]);
  });

  it("resolves a shallower frame reached directly, without its descendants", () => {
    const levels = decodeFlamebearer(FB);
    const b = levels[1]![1]!; // "b", a leaf sibling of "a"
    expect(ancestorPath(levels, b).map((f) => f.name)).toEqual(["total", "b"]);
  });
});

describe("formatTicks", () => {
  it("renders time units as durations", () => {
    expect(formatTicks(1_500_000_000, "nanoseconds")).toBe("1.50s");
    expect(formatTicks(2_000_000, "nanoseconds")).toBe("2.0ms");
    expect(formatTicks(3_000, "nanoseconds")).toBe("3.0µs");
    expect(formatTicks(500, "nanoseconds")).toBe("500ns");
  });

  it("renders non-time units as counts", () => {
    expect(formatTicks(1234, "samples")).toBe("1,234 samples");
    expect(formatTicks(5, "")).toBe("5");
  });
});

describe("formatPct", () => {
  it("is a share of the total", () => {
    expect(formatPct(40, 100)).toBe("40.0%");
    expect(formatPct(1, 0)).toBe("0%");
  });
});

describe("colorBucket", () => {
  it("is stable and within range", () => {
    expect(colorBucket("datafusion::exec", 5)).toBe(
      colorBucket("datafusion::exec", 5),
    );
    expect(colorBucket("anything", 5)).toBeLessThan(5);
    expect(colorBucket("anything", 5)).toBeGreaterThanOrEqual(0);
  });
});
