/**
 * Splits a log row's fields into "This line" (the event itself) vs "Resource
 * · stream" (what emitted it).
 *
 * A real log row's `labels` carry only `service_name`/`level` (the router's
 * `batches_to_streams` — see `src/router/src/endpoints/logql.rs` — puts
 * everything else, resource attributes included, on per-line `metadata`
 * instead). So the split is not "labels vs metadata": a stream label is
 * always resource-level, but a metadata key only belongs in the resource
 * block when the schema registry says it identifies or describes an OTel
 * entity (`entity_roles`) — service, host, k8s.*, cloud, container, process,
 * telemetry.sdk, deployment, os. A metadata key with no entity role (or one
 * the registry hasn't answered for yet) describes the line itself and stays
 * there.
 */
import type { SemanticsMap } from "../../lib/semantics";

/** Kept on the line even if the registry ever attached an entity role to
 * one of them: they name the emitting event, not the process behind it. */
const ALWAYS_LINE = new Set(["trace_id", "span_id"]);

function hasEntityRole(semantics: SemanticsMap, key: string): boolean {
  return (semantics.get(key)?.primary.entity_roles?.length ?? 0) > 0;
}

export interface LogScopes {
  /** Per-line fields: trace_id/span_id, and metadata keys with no resolved
   * entity role (including keys the registry hasn't answered for). */
  line: [string, string][];
  /** Fields describing what emitted the line: every stream label, plus
   * metadata keys the registry says identify/describe an OTel entity. */
  resource: [string, string][];
}

/**
 * `labels` and `metadata` are each expected pre-sorted by the caller (see
 * `LogList.tsx`'s `sortedEntries`); this preserves that order within each
 * output scope rather than re-sorting.
 */
export function splitLogScopes(
  labels: readonly [string, string][],
  metadata: readonly [string, string][],
  semantics: SemanticsMap,
): LogScopes {
  const line: [string, string][] = [];
  const resource: [string, string][] = [...labels];
  for (const entry of metadata) {
    const [key] = entry;
    if (!ALWAYS_LINE.has(key) && hasEntityRole(semantics, key)) {
      resource.push(entry);
    } else {
      line.push(entry);
    }
  }
  return { line, resource };
}
