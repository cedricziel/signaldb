// A generic (language-agnostic) stacktrace line classifier: `exception.
// stacktrace` is opaque text — OTel does not mandate a format, and it varies
// by language/runtime — so this does not attempt to parse per-frame
// structure (function name vs. location). It only tells the header line
// apart from frame lines, and de-emphasizes frames whose location falls in a
// common dependency directory, so the caller's own code stands out.

export type StacktraceLineKind = "header" | "frame" | "vendor";

export interface StacktraceLine {
  kind: StacktraceLineKind;
  /** Leading whitespace stripped — the renderer supplies its own hanging
   * indent so wrapped continuation lines stay aligned under the frame text
   * rather than resetting to the left edge. */
  text: string;
}

const VENDOR_PATTERN =
  /node_modules|\.vite[\\/]deps|site-packages|\.cargo[\\/]registry|[\\/]vendor[\\/]/;

export function parseStacktraceLines(stacktrace: string): StacktraceLine[] {
  const raw = stacktrace.split("\n").map((line) => line.trimEnd());
  while (raw.length > 1 && raw[raw.length - 1] === "") raw.pop();
  return raw.map((line, i): StacktraceLine => {
    if (i === 0) return { kind: "header", text: line };
    const trimmed = line.trimStart();
    return {
      kind: VENDOR_PATTERN.test(trimmed) ? "vendor" : "frame",
      text: trimmed,
    };
  });
}
