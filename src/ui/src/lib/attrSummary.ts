/**
 * Collapsed one-line preview for a section of attribute rows (a log line's
 * stream labels, a span's resource attributes): a curated, ordered list of
 * fields to surface plus a trailing `+ N more` for whatever didn't make the
 * cut. Shared by LogList.tsx and TracesView.tsx, whose curated field lists
 * differ but whose summarizing rule is identical.
 */
export interface SummaryField {
  /** Spelling(s) to look for, in preference order; only the first present
   * spelling is shown. */
  keys: string[];
  /** Combines the found key(s) into the displayed part, e.g. an SDK
   * language + version pair shown together as one `sdk` / `go 1.28.0` part.
   * Defaults to the found key/value for the single found key. */
  render?: (found: ReadonlyMap<string, string>) => SummaryPart;
}

/** One rendered `key value` pair in the summary. */
export interface SummaryPart {
  key: string;
  value: string;
}

export interface AttributeSummaryResult {
  parts: SummaryPart[];
  /** Count of attributes present but not surfaced as a part; 0 when every
   * attribute made it into `parts`. */
  more: number;
}

/** `{ key, value }` for whichever single spelling in `field.keys` is present. */
function defaultRender(found: ReadonlyMap<string, string>): SummaryPart {
  const [key, value] = found.entries().next().value!;
  return { key, value };
}

/**
 * The process/resource identity fields every curated summary list leads
 * with — `LogList.tsx`'s `STREAM_SUMMARY_FIELDS` and `TracesView.tsx`'s
 * `RESOURCE_SUMMARY_FIELDS` each splice their own extra fields (`level`,
 * `service.version`, the sdk pair, ...) around this shared spine rather than
 * repeating it.
 */
export const RESOURCE_IDENTITY_FIELDS: SummaryField[] = [
  { keys: ["service_name", "service.name"] },
  { keys: ["service.namespace"] },
  { keys: ["deployment.environment.name"] },
  { keys: ["k8s.pod.name"] },
  { keys: ["host.name"] },
  { keys: ["cloud.region"] },
];

export function summarizeAttributes(
  entries: readonly [string, string][],
  fields: readonly SummaryField[],
): AttributeSummaryResult {
  const values = new Map(entries);
  const parts: SummaryPart[] = [];
  const used = new Set<string>();
  for (const field of fields) {
    const found = new Map<string, string>();
    for (const key of field.keys) {
      const value = values.get(key);
      if (value !== undefined) found.set(key, value);
    }
    if (found.size === 0) continue;
    if (field.render) {
      for (const key of found.keys()) used.add(key);
      parts.push(field.render(found));
    } else {
      // Only the first present spelling is shown, so an alternate spelling
      // that is also present still counts toward `more`.
      used.add(found.keys().next().value!);
      parts.push(defaultRender(found));
    }
  }
  return { parts, more: entries.length - used.size };
}
