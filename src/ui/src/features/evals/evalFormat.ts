// Display rules shared by the Evaluate pages.
import { meanOf, verdictOf, type EvalStats } from "./evalModel";

export function fmtScore(v: number | null): string {
  return v === null ? "—" : v.toFixed(2);
}

export function fmtPct(v: number | null, digits = 0): string {
  return v === null ? "—" : `${(v * 100).toFixed(digits)}%`;
}

export function fmtCount(n: number): string {
  return n.toLocaleString("en-US");
}

/** `▼ 0.07` / `▲ 0.01` / `0.00` (or `−0.07` / `+0.01` when `signed`),
 * with the tone the delta carries. */
export function fmtDelta(
  d: number | null,
  unit: "score" | "pp" = "score",
  signed = false,
): { text: string; tone: "bad" | "good" | "" } {
  if (d === null) return { text: "—", tone: "" };
  const v = unit === "pp" ? d * 100 : d;
  const digits = unit === "pp" ? 1 : 2;
  const suffix = unit === "pp" ? " pp" : "";
  if (Math.abs(v) < (unit === "pp" ? 0.05 : 0.005))
    return { text: `${(0).toFixed(digits)}${suffix}`, tone: "" };
  const abs = `${Math.abs(v).toFixed(digits)}${suffix}`;
  const [down, up] = signed ? ["−", "+"] : ["▼ ", "▲ "];
  return v < 0
    ? { text: `${down}${abs}`, tone: "bad" }
    : { text: `${up}${abs}`, tone: "good" };
}

/** One case/evaluator cell: the mean when scored, else the verdict. */
export function cellLabel(s: EvalStats | undefined): string {
  if (!s) return "—";
  const mean = meanOf(s);
  if (mean !== null) return fmtScore(mean);
  if (s.errors && !s.pass && !s.fail) return "error";
  return verdictOf(s) ?? "—";
}

export function fmtDay(ms: number): string {
  return new Date(ms).toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
  });
}

export function fmtDateTime(ms: number): string {
  const d = new Date(ms);
  return `${fmtDay(ms)} ${d.toLocaleTimeString("en-GB", {
    hour: "2-digit",
    minute: "2-digit",
  })}`;
}
