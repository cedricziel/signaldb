// Result-envelope → view selection. The view is chosen from the *declared*
// result envelope, before any results arrive, so the UI commits to the right
// presentation up front:
//   rows   → a log/span list
//   series → a time-series chart
//   table  → a topN table (aggregated)
export type EnvelopeView = "list" | "chart" | "table";

export function viewForResult(result: string): EnvelopeView {
  switch (result) {
    case "series":
      return "chart";
    case "table":
      return "table";
    case "rows":
    default:
      return "list";
  }
}
