/**
 * How a discovered entity's signals and its trace-derived measurements are
 * rendered.
 *
 * Shared by the entity list and the entity detail page, so it lives in
 * neither: a detail page importing presentation helpers from a list page
 * inverts the dependency, and the next view that needs them would have to
 * import through a view module or re-derive them inline.
 */
import { formatRate } from "../../lib/traceGroups";
import { formatDurationMs } from "../../lib/waterfall";
import type { EntityObservation, EntityRed } from "../../api/catalog";

/**
 * Which signals cover this entity.
 *
 * Deliberately no counts. How many spans or metric points an entity produced
 * is a fact about our storage, not about the thing being observed — nobody
 * asks "how many samples does this host have". What *is* worth knowing is
 * which signals see it at all, because that is what explains a missing RED
 * measurement: a host covered only by metrics has no error rate because
 * nothing traces it, not because it is healthy.
 */
export function Observed({
  observations,
}: {
  observations: EntityObservation[];
}) {
  return (
    <span className="entity-observed">
      {observations.map((o) => (
        <span key={o.source} className="entity-signal">
          {o.source}
        </span>
      ))}
    </span>
  );
}

/**
 * The RED cells render "–" for an entity carrying no trace measurement, and a
 * real value — including a real zero — for one that does. The distinction is
 * the point: "–" means "no span ever carried this entity, so there is nothing
 * to measure", while "0%" means "spans measured it, and it was clean".
 * Collapsing both to zero reported uninstrumented entities as flawless.
 */
export function redRate(red: EntityRed | undefined, seconds: number): string {
  return red ? formatRate(red.traces, seconds) : "–";
}

export function redErrorRate(red: EntityRed | undefined): string {
  if (!red) return "–";
  // `traces` is the population the errors were counted among. An entity with
  // a `red` was seen in traces so it is never zero, but guarding the division
  // keeps that a local fact rather than an invariant held at a distance.
  return `${Math.round((100 * red.errors) / Math.max(1, red.traces))}%`;
}

/** Takes the percentile to read rather than its value: every caller had
 * `red` in hand and was plucking a field off it, so passing both meant
 * checking one fact twice. */
export function redDuration(
  red: EntityRed | undefined,
  field: "p50Ms" | "p95Ms",
): string {
  return red ? formatDurationMs(red[field]) : "–";
}

/** Marks a measured, non-zero error rate. Absent RED is not an error. */
export function redErrorClass(red: EntityRed | undefined): boolean {
  return (red?.errors ?? 0) > 0;
}
