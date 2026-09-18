// Minimal line-level diff for the dry-run test panel's before/after view.
// The repo carries no diff library (see package.json) and the payloads here
// are small pretty-printed JSON, so a plain LCS line diff is enough — no
// need to pull in a dependency for word-level or unified-hunk output.

export type DiffLine =
  | { kind: "same"; text: string }
  | { kind: "removed"; text: string }
  | { kind: "added"; text: string };

/** Line-by-line diff of `before` vs `after` via longest-common-subsequence,
 * emitted as a flat sequence of same/removed/added lines (removed lines from
 * `before` precede the added lines from `after` at each divergence point). */
export function diffLines(before: string, after: string): DiffLine[] {
  const a = before.split("\n");
  const b = after.split("\n");
  const n = a.length;
  const m = b.length;
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
  const result: DiffLine[] = [];
  let i = 0;
  let j = 0;
  while (i < n && j < m) {
    if (a[i] === b[j]) {
      result.push({ kind: "same", text: a[i]! });
      i++;
      j++;
    } else if (dp[i + 1]![j]! >= dp[i]![j + 1]!) {
      result.push({ kind: "removed", text: a[i]! });
      i++;
    } else {
      result.push({ kind: "added", text: b[j]! });
      j++;
    }
  }
  while (i < n) result.push({ kind: "removed", text: a[i++]! });
  while (j < m) result.push({ kind: "added", text: b[j++]! });
  return result;
}
