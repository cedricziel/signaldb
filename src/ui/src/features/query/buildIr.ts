// Pure Query IR emitter. Builds a versioned IR document by appending structured
// stage objects — no dialect-string surgery in the browser. This is the seam
// that replaces the LogQL/PromQL string compilers (lib/filters.ts,
// features/metrics/buildPromQL.ts) for the native query surface.
import type {
  IrAgg,
  IrAggregate,
  IrComparisonOp,
  IrPredicate,
  IrStage,
  QueryIrRequest,
} from "../../api/gen";

export type IrSource = "logs" | "traces" | "profiles";
export type IrResult = "rows" | "series" | "table";

/** A structured predicate leaf, optionally negated (`not(leaf)`). */
export interface IrFilter {
  field: string;
  op: IrComparisonOp;
  value?: unknown;
  /** Wrap the leaf in `not(...)` (e.g. LogQL `!~`). */
  negate?: boolean;
}

export interface IrBuilderAggregate {
  by: string[];
  aggs: IrAgg[];
  /** A time-bucket width (`"1m"`). Present → the result is a `series`. */
  step?: string;
}

export interface IrBuilderState {
  source: IrSource;
  range: { from: string; to: string };
  result: IrResult;
  filters: IrFilter[];
  aggregate?: IrBuilderAggregate;
  fields?: string[];
}

function leaf(f: IrFilter): IrPredicate {
  const l: IrPredicate =
    f.op === "exists"
      ? { field: f.field, op: f.op }
      : { field: f.field, op: f.op, value: f.value };
  return f.negate ? { not: l } : l;
}

/**
 * Emit the IR document for a builder state. The `pipeline` is assembled by
 * pushing stage objects in order — `where` (from the filter chips), then an
 * optional `aggregate` — mirroring how the builder appends stages.
 */
export function buildIrDocument(state: IrBuilderState): QueryIrRequest {
  const pipeline: IrStage[] = [];

  if (state.filters.length > 0) {
    const leaves = state.filters.map(leaf);
    pipeline.push({
      where: leaves.length === 1 ? leaves[0]! : { and: leaves },
    });
  }

  if (state.aggregate) {
    const agg: IrAggregate = {
      by: state.aggregate.by,
      aggs: state.aggregate.aggs,
    };
    if (state.aggregate.step) agg.step = state.aggregate.step;
    pipeline.push({ aggregate: agg });
  }

  const doc: QueryIrRequest = {
    irVersion: 1,
    from: state.source,
    range: state.range,
    result: state.result,
    pipeline,
  };
  if (state.fields && state.fields.length > 0) doc.fields = state.fields;
  return doc;
}
