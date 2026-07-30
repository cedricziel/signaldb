// RUM session identity.
//
// A session groups a single user's activity so the frontend spans emitted
// during it can be followed as a unit and correlated with the backend traces
// they trigger. A `session.id` is stamped on every span (see
// `SessionSpanProcessor`); the same id flows to the backend implicitly through
// the trace it starts.
//
// Lifetime follows the common RUM convention: a session ends after
// `inactivityMs` of no activity (default 4h) or once it reaches the absolute
// `maxMs` cap (default 24h), whichever comes first. Each access slides the
// inactivity window forward, so an active user keeps one session until they go
// quiet or hit the cap.

const STORAGE_KEY = "signaldb.session";
const DEFAULT_INACTIVITY_MS = 4 * 60 * 60 * 1000; // 4 hours
const DEFAULT_MAX_MS = 24 * 60 * 60 * 1000; // 24 hours

export interface SessionRecord {
  id: string;
  /** Epoch ms the session began (drives the absolute cap). */
  startedAt: number;
  /** Epoch ms after which inactivity rolls a new session. */
  expiresAt: number;
}

export interface SessionManagerOptions {
  /** Clock, injectable for tests. Defaults to `Date.now`. */
  now?: () => number;
  /**
   * Persistence. `undefined` uses `localStorage` (so a session survives
   * reloads and spans across tabs); `null` disables persistence and keeps the
   * session in memory only.
   */
  storage?: Storage | null;
  /** Inactivity timeout before a new session starts. */
  inactivityMs?: number;
  /** Absolute cap on a single session's duration. */
  maxMs?: number;
  /** Id generator, injectable for tests. */
  newId?: () => string;
}

export interface SessionManager {
  /**
   * The current session id. Rolls a new session when the inactivity window has
   * lapsed or the absolute cap is reached, and otherwise slides the inactivity
   * window forward to mark this access as activity.
   */
  getId(): string;
  /** The full current record — for diagnostics and tests. */
  current(): SessionRecord;
}

function defaultNewId(): string {
  const c = globalThis.crypto;
  if (c && typeof c.randomUUID === "function") return c.randomUUID();
  // Fallback for older/embedded browsers without crypto.randomUUID.
  return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, (ch) => {
    const r = Math.floor(Math.random() * 16);
    const v = ch === "x" ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}

function defaultStorage(): Storage | null {
  try {
    // Access can throw in sandboxed iframes or when storage is disabled.
    return globalThis.localStorage ?? null;
  } catch {
    return null;
  }
}

function isRecord(value: unknown): value is SessionRecord {
  if (typeof value !== "object" || value === null) return false;
  const r = value as Record<string, unknown>;
  return (
    typeof r.id === "string" &&
    typeof r.startedAt === "number" &&
    typeof r.expiresAt === "number"
  );
}

export function createSessionManager(
  opts: SessionManagerOptions = {},
): SessionManager {
  const now = opts.now ?? (() => Date.now());
  const inactivityMs = opts.inactivityMs ?? DEFAULT_INACTIVITY_MS;
  const maxMs = opts.maxMs ?? DEFAULT_MAX_MS;
  const newId = opts.newId ?? defaultNewId;
  const storage = opts.storage === undefined ? defaultStorage() : opts.storage;

  // In-memory mirror: the source of truth when storage is unavailable, and a
  // cache otherwise (storage is still re-read so other tabs' writes win).
  let memory: SessionRecord | null = null;

  function load(): SessionRecord | null {
    if (storage) {
      try {
        const raw = storage.getItem(STORAGE_KEY);
        if (raw) {
          const parsed: unknown = JSON.parse(raw);
          if (isRecord(parsed)) return parsed;
        }
      } catch {
        // Corrupt/unreadable entry: fall through to memory / fresh start.
      }
    }
    return memory;
  }

  function save(record: SessionRecord): SessionRecord {
    memory = record;
    if (storage) {
      try {
        storage.setItem(STORAGE_KEY, JSON.stringify(record));
      } catch {
        // Quota or disabled storage: the in-memory mirror still holds.
      }
    }
    return record;
  }

  function start(at: number): SessionRecord {
    return save({ id: newId(), startedAt: at, expiresAt: at + inactivityMs });
  }

  function current(): SessionRecord {
    const t = now();
    const existing = load();
    if (!existing) return start(t);
    const idleExpired = t > existing.expiresAt;
    const capExpired = t - existing.startedAt >= maxMs;
    if (idleExpired || capExpired) return start(t);
    // Activity: slide the inactivity window forward, keep the same identity.
    return save({ ...existing, expiresAt: t + inactivityMs });
  }

  return { getId: () => current().id, current };
}

let singleton: SessionManager | null = null;

/** Process-wide session manager backed by `localStorage`. */
export function getDefaultSessionManager(): SessionManager {
  singleton ??= createSessionManager();
  return singleton;
}
