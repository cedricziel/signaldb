// Minimal line-level diff for the dry-run test panel's before/after view.
// The repo carries no diff library (see package.json) and the payloads here
// are small pretty-printed JSON, so a plain LCS line diff is enough — no
// need to pull in a dependency for word-level or unified-hunk output.

export type DiffLine =
  | { kind: "same"; text: string }
  | { kind: "removed"; text: string }
  | { kind: "added"; text: string };

export type DiffOp<T> = { kind: "same" | "removed" | "added"; item: T };

/** Diff of two sequences via longest-common-subsequence, as a flat run of
 * same/removed/added items (removed items from `a` precede the added items
 * from `b` at each divergence point). */
export function diffSequence<T>(a: readonly T[], b: readonly T[]): DiffOp<T>[] {
  let head = 0;
  while (head < a.length && head < b.length && a[head] === b[head]) head++;
  let tail = 0;
  while (
    tail < a.length - head &&
    tail < b.length - head &&
    a[a.length - 1 - tail] === b[b.length - 1 - tail]
  ) {
    tail++;
  }
  const same = (item: T): DiffOp<T> => ({ kind: "same", item });
  return [
    ...a.slice(0, head).map(same),
    ...diffMiddle(
      a.slice(head, a.length - tail),
      b.slice(head, b.length - tail),
    ),
    ...a.slice(a.length - tail).map(same),
  ];
}

function diffMiddle<T>(a: readonly T[], b: readonly T[]): DiffOp<T>[] {
  const n = a.length;
  const m = b.length;
  // Guard against pathological input: the DP matrix below is O(n*m) cells,
  // which can freeze the tab for a large input. Fall back to a cheap linear
  // diff (all removed, then all added) past this threshold.
  const maxCells = 1_000_000;
  if (n * m > maxCells) {
    return [
      ...a.map((item) => ({ kind: "removed" as const, item })),
      ...b.map((item) => ({ kind: "added" as const, item })),
    ];
  }
  // dp[i][j] = length of the LCS of a[i:] and b[j:]
  const dp: number[][] = Array.from({ length: n + 1 }, () =>
    new Array<number>(m + 1).fill(0),
  );
  for (let i = n - 1; i >= 0; i--) {
    for (let j = m - 1; j >= 0; j--) {
      dp[i]![j] =
        a[i] === b[j]
          ? dp[i + 1]![j + 1]! + 1
          : Math.max(dp[i + 1]![j]!, dp[i]![j + 1]!);
    }
  }
  const result: DiffOp<T>[] = [];
  let i = 0;
  let j = 0;
  while (i < n && j < m) {
    if (a[i] === b[j]) {
      result.push({ kind: "same", item: a[i]! });
      i++;
      j++;
    } else if (dp[i + 1]![j]! >= dp[i]![j + 1]!) {
      result.push({ kind: "removed", item: a[i]! });
      i++;
    } else {
      result.push({ kind: "added", item: b[j]! });
      j++;
    }
  }
  while (i < n) result.push({ kind: "removed", item: a[i++]! });
  while (j < m) result.push({ kind: "added", item: b[j++]! });
  return result;
}

/** Line-by-line diff of `before` vs `after` (see `diffSequence`). */
export function diffLines(before: string, after: string): DiffLine[] {
  return diffSequence(before.split("\n"), after.split("\n")).map(
    ({ kind, item }) => ({ kind, text: item }),
  );
}
