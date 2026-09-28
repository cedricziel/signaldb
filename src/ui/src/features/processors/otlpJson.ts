// The processors:test endpoint round-trips the payload through the OTLP
// protobuf types, so its answer comes back in proto field order with every
// zero-valued field spelled out. Diffing that against the text the user
// submitted would flag reordered and defaulted fields as changes; both sides
// go through this first so only what the statements changed shows up.

// 64-bit integers are JSON strings in OTLP/JSON, so their zero is "0".
const UINT64_STRING_FIELD = /UnixNano$|^count$/;

function isDefault(key: string, value: unknown): boolean {
  if (value === null || value === false || value === 0 || value === "") {
    return true;
  }
  if (value === "0") return UINT64_STRING_FIELD.test(key);
  if (Array.isArray(value)) return value.length === 0;
  return typeof value === "object" && Object.keys(value).length === 0;
}

function canonical(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonical);
  if (value === null || typeof value !== "object") return value;
  const out: Record<string, unknown> = {};
  for (const key of Object.keys(value).sort()) {
    const v = canonical((value as Record<string, unknown>)[key]);
    if (!isDefault(key, v)) out[key] = v;
  }
  return out;
}

/** Pretty-printed OTLP/JSON with sorted keys and proto3 default values
 * dropped. */
export function canonicalOtlpJson(value: unknown): string {
  return JSON.stringify(canonical(value), null, 2);
}
