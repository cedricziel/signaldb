// Client-side, best-effort sources of a stack frame's file/line, feeding the
// "View source" affordance (see docs/users/explore-ui.md's "View source
// (GitHub)" and openspec's "Where file/line come from" decision). All three
// functions here are pure and deliberately small — no per-language
// stacktrace parser, no path-prefix stripping; a frame the extraction can't
// read simply gets no source affordance.
import { RESOURCE_PREFIX } from "../features/traces/spanAttributes";

/** One file/line pair extracted from a stack-trace line or a structured
 * attribute pair. */
export interface FrameSourceLocation {
  path: string;
  line: number;
}

/** A path must name a file (has an extension) and can't contain whitespace —
 * cheap enough to rule out prose ("at index 5") without a real path parser. */
function isPlausiblePath(path: string): boolean {
  return path.includes(".") && !/\s/.test(path);
}

// Python's traceback frame header: `File "path/to/file.py", line 42, in fn`.
const PYTHON_FRAME_RE = /File "([^"]+)",\s*line\s+(\d+)/;

// `path.ext:line[:col]`, delimited by start/end-of-string, whitespace, a
// quote, or a paren/colon on the outside — covers JS/TS ("at fn
// (src/x.ts:12:5)"), Rust ("at src/main.rs:42:9"), Go
// ("/app/handler.go:77 +0x1f"), and Ruby ("app/x.rb:12:in `foo'") alike, plus
// a bare "Foo.java:12" with no directory.
const GENERIC_FRAME_RE =
  /(?<=^|[\s"'(])([^\s"'()]+\.[A-Za-z0-9]+):(\d+)(?::(\d+))?(?=$|[\s"':)])/;

/** Best-effort `path:line` extraction over one stack-trace line. Returns
 * `null` for a header line, or any line neither shape matches. */
export function extractFrameLocation(
  lineText: string,
): FrameSourceLocation | null {
  const python = PYTHON_FRAME_RE.exec(lineText);
  if (python) {
    const path = python[1]!.trim();
    const line = Number(python[2]);
    if (isPlausiblePath(path) && line > 0) return { path, line };
  }
  const generic = GENERIC_FRAME_RE.exec(lineText);
  if (generic) {
    const path = generic[1]!;
    const line = Number(generic[2]);
    if (isPlausiblePath(path) && line > 0) return { path, line };
  }
  return null;
}

/** A commit SHA `service.version` is allowed to look like — 7 to 40 hex
 * chars, short or full. */
const SHA_RE = /^[0-9a-f]{7,40}$/i;

/** Repository/ref hints read out of an attribute bag (resource + span/log
 * attributes, already flattened) for a `source-context` lookup: repository
 * from `vcs.repository.url.full`, ref from `vcs.ref.head.revision`, else
 * `vcs.ref.head.name`, else a `service.version` that looks like a commit
 * SHA. Each key is tried under every prefix in turn (`"resource."` before
 * the unprefixed span-level spelling by default), independently — a
 * resource-level repository can pair with a span-level ref or vice versa. */
export function repositoryHints(
  attrs: Record<string, unknown>,
  opts?: { prefixes?: string[] },
): { repository?: string; ref?: string } {
  const prefixes = opts?.prefixes ?? [RESOURCE_PREFIX, ""];
  const read = (key: string): string | undefined => {
    for (const prefix of prefixes) {
      const value = attrs[`${prefix}${key}`];
      if (typeof value === "string" && value !== "") return value;
    }
    return undefined;
  };

  const repository = read("vcs.repository.url.full");
  const version = read("service.version");
  const ref =
    read("vcs.ref.head.revision") ??
    read("vcs.ref.head.name") ??
    (version && SHA_RE.test(version) ? version : undefined);

  const hints: { repository?: string; ref?: string } = {};
  if (repository) hints.repository = repository;
  if (ref) hints.ref = ref;
  return hints;
}

function positiveInt(value: unknown): number | null {
  const n = typeof value === "number" ? value : Number(value);
  return Number.isInteger(n) && n > 0 ? n : null;
}

/** A frame's location from OTel's `code.*` semantic-convention attributes —
 * `code.file.path`/`code.line.number` (1.30+), falling back to the earlier
 * `code.filepath`/`code.lineno` spellings. */
export function codeLocationFromAttributes(
  attrs: Record<string, unknown>,
): FrameSourceLocation | null {
  const path = attrs["code.file.path"] ?? attrs["code.filepath"];
  if (typeof path !== "string" || path === "") return null;
  const line = positiveInt(attrs["code.line.number"] ?? attrs["code.lineno"]);
  if (line === null) return null;
  return { path, line };
}
