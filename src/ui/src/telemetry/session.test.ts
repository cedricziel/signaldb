import { describe, expect, it } from "vitest";
import { createSessionManager } from "./session";

/** In-memory Storage stand-in so tests control persistence explicitly. */
function memoryStorage(): Storage {
  const map = new Map<string, string>();
  return {
    get length() {
      return map.size;
    },
    clear: () => map.clear(),
    getItem: (k) => map.get(k) ?? null,
    key: (i) => [...map.keys()][i] ?? null,
    removeItem: (k) => map.delete(k),
    setItem: (k, v) => void map.set(k, v),
  };
}

/** Deterministic id source: id-0, id-1, ... so rotations are observable. */
function seqIds(): () => string {
  let n = 0;
  return () => `id-${n++}`;
}

describe("createSessionManager", () => {
  it("returns a stable id for activity within the inactivity window", () => {
    let now = 1_000;
    const s = createSessionManager({
      now: () => now,
      storage: memoryStorage(),
      inactivityMs: 100,
      newId: seqIds(),
    });
    expect(s.getId()).toBe("id-0");
    now = 1_050; // still inside the 100ms window
    expect(s.getId()).toBe("id-0");
  });

  it("slides the inactivity window forward on each access", () => {
    let now = 0;
    const s = createSessionManager({
      now: () => now,
      storage: memoryStorage(),
      inactivityMs: 100,
      newId: seqIds(),
    });
    s.getId(); // starts at 0, expires at 100
    now = 80;
    s.getId(); // slides expiry to 180
    now = 170; // would have expired at 100, but the window slid
    expect(s.getId()).toBe("id-0");
  });

  it("rolls a new session after the inactivity window lapses", () => {
    let now = 0;
    const s = createSessionManager({
      now: () => now,
      storage: memoryStorage(),
      inactivityMs: 100,
      newId: seqIds(),
    });
    expect(s.getId()).toBe("id-0");
    now = 201; // past the window with no activity
    expect(s.getId()).toBe("id-1");
  });

  it("rolls a new session at the absolute cap even under continuous activity", () => {
    let now = 0;
    const s = createSessionManager({
      now: () => now,
      storage: memoryStorage(),
      inactivityMs: 1_000,
      maxMs: 500,
      newId: seqIds(),
    });
    expect(s.getId()).toBe("id-0");
    now = 400; // active, inside inactivity window, under cap
    expect(s.getId()).toBe("id-0");
    now = 500; // hits the absolute cap
    expect(s.getId()).toBe("id-1");
  });

  it("shares the session across manager instances via storage", () => {
    const storage = memoryStorage();
    const now = 0;
    const a = createSessionManager({
      now: () => now,
      storage,
      inactivityMs: 1_000,
      newId: () => "shared",
    });
    expect(a.getId()).toBe("shared");
    // A second tab reads the same persisted record.
    const b = createSessionManager({
      now: () => now,
      storage,
      inactivityMs: 1_000,
      newId: () => "other",
    });
    expect(b.getId()).toBe("shared");
  });

  it("falls back to an in-memory session when storage is unavailable", () => {
    let now = 0;
    const s = createSessionManager({
      now: () => now,
      storage: null,
      inactivityMs: 100,
      newId: seqIds(),
    });
    expect(s.getId()).toBe("id-0");
    now = 50;
    expect(s.getId()).toBe("id-0");
  });

  it("starts fresh when persisted JSON is corrupt", () => {
    const storage = memoryStorage();
    storage.setItem("signaldb.session", "{not valid json");
    const s = createSessionManager({
      now: () => 0,
      storage,
      newId: () => "recovered",
    });
    expect(s.getId()).toBe("recovered");
  });
});
