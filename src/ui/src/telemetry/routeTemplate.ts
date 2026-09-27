// Turns a matched route into a low-cardinality `url.template`: id-like params
// become `:<name>`, while params naming a small fixed set stay literal so pages
// remain distinguishable.

/** `signal` (`/logs`, `/traces`, ...), `entity` (catalog entity type) and
 * `kind` (schema `DefinitionKind`) each take a handful of values. */
const LOW_CARDINALITY_PARAMS = new Set(["signal", "entity", "kind"]);

const SPLAT_PARAM = "*";

function decodeSegment(segment: string): string {
  try {
    return decodeURIComponent(segment);
  } catch {
    return segment;
  }
}

/**
 * `buildRouteTemplate("/traces/7c1e...", { traceId: "7c1e..." })` →
 * `"/traces/:traceId"`. Params arrive decoded while the pathname keeps its
 * percent-encoding, so segments are compared decoded.
 */
export function buildRouteTemplate(
  pathname: string,
  params: Readonly<Record<string, string | undefined>>,
): string {
  const segments = pathname.split("/");
  const decoded = segments.map(decodeSegment);

  for (const [name, value] of Object.entries(params)) {
    if (name === SPLAT_PARAM || !value || LOW_CARDINALITY_PARAMS.has(name)) {
      continue;
    }
    decoded.forEach((segment, i) => {
      if (segment === value) segments[i] = `:${name}`;
    });
  }

  const splatSegments = (params[SPLAT_PARAM] ?? "")
    .split("/")
    .filter((segment) => segment !== "");
  if (splatSegments.length > 0) {
    segments.splice(
      segments.length - splatSegments.length,
      splatSegments.length,
      SPLAT_PARAM,
    );
  }

  return segments.join("/");
}
