// Builds `url.template` from the matched routes' declared paths, so ids never
// reach it. Params naming a small fixed set are filled in, keeping pages that
// share a pattern (`/:signal`) distinguishable.

/** `signal` (`/logs`, `/traces`, ...), `entity` (catalog entity type) and
 * `kind` (schema `DefinitionKind`) each take a handful of values. */
const LOW_CARDINALITY_PARAMS = new Set(["signal", "entity", "kind"]);

/**
 * `buildRouteTemplate(["/", "traces/:traceId"], params)` →
 * `"/traces/:traceId"`; `buildRouteTemplate(["/", ":signal"], { signal:
 * "logs" })` → `"/logs"`.
 */
export function buildRouteTemplate(
  routePaths: ReadonlyArray<string | undefined>,
  params: Readonly<Record<string, string | undefined>>,
): string {
  const segments = routePaths
    .flatMap((path) => (path ?? "").split("/"))
    .filter((segment) => segment !== "")
    .map((segment) => {
      const name = segment.startsWith(":") ? segment.slice(1) : undefined;
      const value = name ? params[name] : undefined;
      return name && value && LOW_CARDINALITY_PARAMS.has(name)
        ? value
        : segment;
    });
  return `/${segments.join("/")}`;
}
