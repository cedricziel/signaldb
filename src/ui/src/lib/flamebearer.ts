// Decode the delta-encoded flamebearer (see api/pyroscope.ts) into absolute
// frames, and derive the geometry a flame graph renders. Pure + tested.

import type { Flamebearer } from "../api/pyroscope";

export interface FlameFrame {
  level: number;
  /** Absolute start offset in ticks. */
  x: number;
  /** Width in ticks (samples-in-subtree). */
  total: number;
  /** Ticks spent in this frame itself, not its children. */
  self: number;
  name: string;
}

/**
 * Expand each level's `[offsetDelta, total, self, nameIndex]` quadruples
 * into frames with absolute `x`. Within a level the cursor advances by
 * `offsetDelta` (the gap after the previous frame's end) then by the
 * frame's `total`, exactly mirroring the encoder.
 */
export function decodeFlamebearer(fb: Flamebearer): FlameFrame[][] {
  return fb.levels.map((level, depth) => {
    const frames: FlameFrame[] = [];
    let cursor = 0;
    for (let i = 0; i + 3 < level.length; i += 4) {
      const x = cursor + (level[i] ?? 0);
      const total = level[i + 1] ?? 0;
      const self = level[i + 2] ?? 0;
      const name = fb.names[level[i + 3] ?? 0] ?? "";
      frames.push({ level: depth, x, total, self, name });
      cursor = x + total;
    }
    return frames;
  });
}

/**
 * The viewport a flame graph draws through: a horizontal tick window and the
 * top level. Unzoomed it is the whole root; zooming to a frame narrows the
 * window to that frame's extent and drops the levels above it.
 */
export interface FlameView {
  x: number;
  total: number;
  level: number;
}

export function rootView(fb: Flamebearer): FlameView {
  return { x: 0, total: Math.max(fb.numTicks, 1), level: 0 };
}

export function frameView(frame: FlameFrame): FlameView {
  return { x: frame.x, total: Math.max(frame.total, 1), level: frame.level };
}

/** Placement of a frame within the current viewport, in percentages. */
export interface FramePlacement {
  frame: FlameFrame;
  leftPct: number;
  widthPct: number;
}

/**
 * Frames visible in `view`, as left/width percentages of the viewport, one
 * array per level (aligned to absolute depth). A frame shows when it
 * overlaps the tick window; its placement is clamped to [0, 100]. When
 * zoomed, each ancestor level therefore contributes exactly one bar spanning
 * the full width (the frame containing the focus) — the conventional "path
 * to the focused frame stacked above the scaled subtree" — while sibling
 * subtrees fall outside the window and drop out. Empty levels are kept so
 * callers can align colors/keys by depth; skip them when rendering.
 */
export function placeFrames(
  levels: FlameFrame[][],
  view: FlameView,
): FramePlacement[][] {
  const viewEnd = view.x + view.total;
  return levels.map((level) => {
    const placements: FramePlacement[] = [];
    for (const frame of level) {
      if (frame.x + frame.total <= view.x || frame.x >= viewEnd) continue;
      const rawLeft = ((frame.x - view.x) / view.total) * 100;
      const rawRight = ((frame.x + frame.total - view.x) / view.total) * 100;
      const leftPct = Math.max(0, rawLeft);
      const widthPct = Math.min(100, rawRight) - leftPct;
      if (widthPct > 0) placements.push({ frame, leftPct, widthPct });
    }
    return placements;
  });
}

/**
 * The chain of frames from the root down to (and including) `target`, one
 * per level. Each ancestor is the unique frame at that level whose interval
 * contains `target`'s — proper flamegraph nesting guarantees exactly one.
 * Used to build a breadcrumb trail and to support zooming to a frame the
 * user never explicitly stepped through (e.g. clicking straight into a deep
 * frame from the root view).
 */
export function ancestorPath(
  levels: FlameFrame[][],
  target: FlameFrame,
): FlameFrame[] {
  const path: FlameFrame[] = [];
  const end = target.x + target.total;
  for (let level = 0; level <= target.level; level++) {
    const found = levels[level]?.find(
      (f) => f.x <= target.x && f.x + f.total >= end,
    );
    if (found) path.push(found);
  }
  return path;
}

/** Synthetic bucket a run of below-threshold frames collapses into. */
export const OTHER_FRAME_NAME = "(other)";

/**
 * Fold every frame narrower than `minFraction` of `totalTicks` — and its
 * whole subtree, since a child's total can never exceed its parent's —
 * into a single `OTHER_FRAME_NAME` sibling per contiguous run. Computed
 * against the root total (not the current zoom view) so it stays stable as
 * the user zooms and matches the %-of-root figures shown everywhere else.
 * `minFraction <= 0` returns `levels` unchanged.
 */
export function collapseSmallFrames(
  levels: FlameFrame[][],
  totalTicks: number,
  minFraction: number,
): FlameFrame[][] {
  if (minFraction <= 0 || levels.length === 0) return levels;
  const minTicks = totalTicks * minFraction;
  const result: FlameFrame[][] = [];
  // Ranges (in root-tick space) still expanded at the previous level — a
  // frame only survives to the next level if its parent wasn't collapsed.
  let keptRanges: Array<[number, number]> = [
    [-Infinity, Infinity], // level 0 has no parent to have collapsed
  ];

  for (const source of levels) {
    const candidates = source.filter((f) =>
      keptRanges.some(([lo, hi]) => f.x >= lo && f.x + f.total <= hi),
    );
    const out: FlameFrame[] = [];
    const nextKept: Array<[number, number]> = [];
    let run: FlameFrame[] = [];
    const flushRun = () => {
      if (run.length === 0) return;
      const total = run.reduce((sum, f) => sum + f.total, 0);
      const self = run.reduce((sum, f) => sum + f.self, 0);
      const first = run[0]!; // guarded by the run.length === 0 check above
      out.push({
        level: first.level,
        x: first.x,
        total,
        self,
        name: OTHER_FRAME_NAME,
      });
      run = [];
    };
    for (const frame of candidates) {
      if (frame.total < minTicks) {
        run.push(frame);
      } else {
        flushRun();
        out.push(frame);
        nextKept.push([frame.x, frame.x + frame.total]);
      }
    }
    flushRun();
    result.push(out);
    keptRanges = nextKept;
  }
  return result;
}

/** One row of the top-functions-by-self-time table. */
export interface FunctionTotal {
  name: string;
  self: number;
  total: number;
  /** Number of tree positions this function occupied — > 1 for recursive
   * or repeatedly-called functions. */
  count: number;
}

/**
 * Aggregate self (and total) time by function name across every tree
 * position, sorted by self time descending — the "flat" view real
 * profilers (pprof's `top`, Speedscope's Sandwich) offer alongside the
 * call tree, for scanning "what's actually expensive" without wading
 * through tree structure. Self-time sums are exact (a call node's self
 * time is never double-counted elsewhere); total-time sums over a
 * recursive function double-count its own recursive calls, same
 * limitation pprof's flat view has — self is the number to trust here.
 */
export function topFunctionsBySelf(levels: FlameFrame[][]): FunctionTotal[] {
  const byName = new Map<string, FunctionTotal>();
  for (const level of levels) {
    for (const frame of level) {
      const existing = byName.get(frame.name);
      if (existing) {
        existing.self += frame.self;
        existing.total += frame.total;
        existing.count += 1;
      } else {
        byName.set(frame.name, {
          name: frame.name,
          self: frame.self,
          total: frame.total,
          count: 1,
        });
      }
    }
  }
  return Array.from(byName.values()).sort((a, b) => b.self - a.self);
}

const TIME_UNITS = new Set([
  "nanoseconds",
  "microseconds",
  "milliseconds",
  "seconds",
]);

const TO_NANOS: Record<string, number> = {
  nanoseconds: 1,
  microseconds: 1_000,
  milliseconds: 1_000_000,
  seconds: 1_000_000_000,
};

const BYTE_UNITS = new Set(["bytes", "byte"]);
const KIB = 1024;
const MIB = KIB * 1024;
const GIB = MIB * 1024;

/** Human value for a tick count: a duration for a time unit, a binary-scaled
 * size for a byte unit (memory profiles), else a plain count with the unit
 * appended. */
export function formatTicks(ticks: number, unit: string): string {
  if (TIME_UNITS.has(unit)) {
    const nanos = ticks * (TO_NANOS[unit] ?? 1);
    if (nanos >= 1_000_000_000) return `${(nanos / 1e9).toFixed(2)}s`;
    if (nanos >= 1_000_000) return `${(nanos / 1e6).toFixed(1)}ms`;
    if (nanos >= 1_000) return `${(nanos / 1e3).toFixed(1)}µs`;
    return `${Math.round(nanos)}ns`;
  }
  if (BYTE_UNITS.has(unit)) {
    const abs = Math.abs(ticks);
    if (abs >= GIB) return `${(ticks / GIB).toFixed(2)} GiB`;
    if (abs >= MIB) return `${(ticks / MIB).toFixed(1)} MiB`;
    if (abs >= KIB) return `${(ticks / KIB).toFixed(1)} KiB`;
    return `${Math.round(ticks)} B`;
  }
  const rounded = Math.round(ticks).toLocaleString();
  return unit ? `${rounded} ${unit}` : rounded;
}

export function formatPct(ticks: number, numTicks: number): string {
  if (numTicks <= 0) return "0%";
  return `${((ticks / numTicks) * 100).toFixed(1)}%`;
}

/**
 * Stable 0-based color bucket for a frame name, so the same function keeps
 * its color across renders. Callers map the bucket onto the `--svc-a..e`
 * palette; the root ("total") is special-cased to the accent by the view.
 */
export function colorBucket(name: string, buckets: number): number {
  let hash = 0;
  for (let i = 0; i < name.length; i++) {
    hash = (hash * 31 + name.charCodeAt(i)) | 0;
  }
  return Math.abs(hash) % buckets;
}

/** Index of a top-level (bracket-depth-0) " as " in `s`, or -1. Used to
 * split `<Type as Trait>` without getting confused by generics that
 * themselves contain " as " (they can't in Rust, but nested `<...>` must
 * still not shift what "top-level" means). */
function topLevelAsIndex(s: string): number {
  let depth = 0;
  for (let i = 0; i < s.length; i++) {
    const c = s[i];
    if (c === "<") depth++;
    else if (c === ">") depth = Math.max(0, depth - 1);
    else if (depth === 0 && s.startsWith(" as ", i)) return i;
  }
  return -1;
}

/** Index one past the `<...>` block opening at `s[openIdx]`, or -1 if
 * unbalanced. */
function matchingAngleClose(s: string, openIdx: number): number {
  let depth = 0;
  for (let i = openIdx; i < s.length; i++) {
    if (s[i] === "<") depth++;
    else if (s[i] === ">") {
      depth--;
      if (depth === 0) return i;
    }
  }
  return -1;
}

/** Drop every top-level `<...>` block (turbofish / generic parameter
 * lists) from `s`, wherever it occurs. */
function stripGenerics(s: string): string {
  let out = "";
  let depth = 0;
  for (const c of s) {
    if (c === "<") {
      depth++;
      continue;
    }
    if (c === ">") {
      if (depth > 0) depth--;
      continue;
    }
    if (depth === 0) out += c;
  }
  return out;
}

/** Drop `::{closure#N}`, `::{shim:vtable#N}`, and similar `::{...}`
 * compiler-generated segments. */
function stripCompilerNoise(s: string): string {
  return s.replace(/::\{[^{}]*\}/g, "").replace(/,?\s*\(\)/g, "");
}

function lastPathSegment(path: string): string {
  const parts = path
    .split("::")
    .map((p) => p.trim())
    .filter(Boolean);
  return parts[parts.length - 1] ?? path.trim();
}

/** Last one or two `::`-separated segments — two when there's a natural
 * "container::member" pair to preserve (so an already-short `Type::method`
 * name round-trips unchanged instead of losing its receiver), one for a
 * single bare name. */
function lastOneOrTwoSegments(path: string): string {
  const parts = path
    .split("::")
    .map((p) => p.trim())
    .filter(Boolean);
  if (parts.length === 0) return path.trim();
  return parts.slice(-2).join("::");
}

/**
 * Shorten a mangled/demangled symbol for on-bar display: a Rust name for a
 * monomorphized generic method (`<Type as Trait>::method::<Args>`) can run
 * to hundreds of characters, and — worse — the useful, distinguishing part
 * (the trailing generics) is exactly what a right-edge ellipsis cuts off
 * first, leaving unrelated frames looking identical once truncated.
 *
 * This keeps roughly `ReceiverType::method`, dropping the `as Trait`
 * qualifier, nested generic parameter lists, and closure/vtable-shim
 * noise. It's a heuristic: two distinct symbols can collapse to the same
 * short label. That's an acceptable trade — the full name is always still
 * available (hover, the detail panel, the top-functions table), this is
 * display-only.
 */
export function simplifyFrameName(name: string): string {
  const trimmed = name.trim();
  if (trimmed.length === 0) return trimmed;

  let receiver = "";
  let rest = trimmed;

  if (trimmed.startsWith("<")) {
    const close = matchingAngleClose(trimmed, 0);
    if (close !== -1) {
      const inner = trimmed.slice(1, close);
      const asIdx = topLevelAsIndex(inner);
      const typeExpr = asIdx === -1 ? inner : inner.slice(0, asIdx);
      receiver = lastPathSegment(stripCompilerNoise(stripGenerics(typeExpr)));
      rest = trimmed.slice(close + 1);
    }
  }

  rest = stripCompilerNoise(stripGenerics(rest)).replace(/^::+/, "");
  // When there's already a receiver from a leading <Type as Trait> block,
  // `rest` is just the method name; otherwise `rest` is the whole
  // remaining path and gets to keep one segment of its own context.
  const method = receiver ? lastPathSegment(rest) : lastOneOrTwoSegments(rest);

  if (receiver && method) return `${receiver}::${method}`;
  if (receiver) return receiver;
  if (method) return method;
  return trimmed;
}
