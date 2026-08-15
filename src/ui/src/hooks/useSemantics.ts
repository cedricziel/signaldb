/**
 * `useSemantics()` — attribute keys → schema-registry semantics.
 *
 * Rendering sites hand in the keys they show; the hook batches them
 * (debounced, de-duplicated, chunked at the server's batch cap), resolves
 * them through the generated client, caches the answers for the session per
 * active tenant, and returns a map holding only the keys the registry knows.
 * The first snapshot is always synchronous — an empty map or the cached one —
 * so rows render at once with the raw key and pick the semantics up when they
 * land. Any failure degrades to "unknown"; the hook never throws.
 */
import { useEffect, useMemo, useSyncExternalStore } from "react";
import { getTenantContext } from "../api/http";
import {
  resolveAttributes,
  SCHEMA_BATCH_LIMIT,
  searchAttributes,
  type AttributeHit,
} from "../api/schema";
import {
  semanticsFromResolution,
  type AttributeSemantics,
  type SemanticsMap,
} from "../lib/semantics";

/** How long a render tick collects keys before one batched request goes out. */
export const SEMANTICS_DEBOUNCE_MS = 20;

/** `null` marks a key the registry answered "unknown" for (or a failed
 * lookup) so it is not asked again this session. */
type CacheEntry = AttributeSemantics | null;

interface TenantState {
  cache: Map<string, CacheEntry>;
  pending: Set<string>;
  inflight: Set<string>;
  timer: ReturnType<typeof setTimeout> | null;
}

const tenants = new Map<string, TenantState>();
const listeners = new Set<() => void>();
let version = 0;

const EMPTY: SemanticsMap = new Map();

/** Joins a key list into one stable dependency string. */
const SEP = "\u0000";

function tenantState(tenant: string): TenantState {
  let state = tenants.get(tenant);
  if (!state) {
    state = {
      cache: new Map(),
      pending: new Set(),
      inflight: new Set(),
      timer: null,
    };
    tenants.set(tenant, state);
  }
  return state;
}

function notify(): void {
  version += 1;
  for (const listener of listeners) listener();
}

function subscribe(listener: () => void): () => void {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

const getVersion = () => version;

async function flush(tenant: string): Promise<void> {
  const state = tenantState(tenant);
  state.timer = null;
  const keys = [...state.pending];
  state.pending.clear();
  for (const key of keys) state.inflight.add(key);

  const chunks: string[][] = [];
  for (let i = 0; i < keys.length; i += SCHEMA_BATCH_LIMIT) {
    chunks.push(keys.slice(i, i + SCHEMA_BATCH_LIMIT));
  }
  await Promise.all(
    chunks.map(async (chunk) => {
      try {
        const resolutions = await resolveAttributes(chunk);
        for (const res of resolutions) {
          state.cache.set(res.key, semanticsFromResolution(res) ?? null);
        }
      } catch {
        // Registry unavailable: degrade to raw keys for this session rather
        // than retrying on every render.
      } finally {
        for (const key of chunk) {
          state.inflight.delete(key);
          if (!state.cache.has(key)) state.cache.set(key, null);
        }
      }
    }),
  );
  notify();
}

/** Queue keys for resolution; only keys neither cached nor in flight go out. */
function request(tenant: string, keys: readonly string[]): void {
  const state = tenantState(tenant);
  let added = false;
  for (const key of keys) {
    if (!key || state.cache.has(key) || state.inflight.has(key)) continue;
    state.pending.add(key);
    added = true;
  }
  if (added && state.timer === null) {
    state.timer = setTimeout(() => {
      void flush(tenant);
    }, SEMANTICS_DEBOUNCE_MS);
  }
}

function snapshot(tenant: string, keys: readonly string[]): SemanticsMap {
  const cache = tenants.get(tenant)?.cache;
  if (!cache) return EMPTY;
  const out = new Map<string, AttributeSemantics>();
  for (const key of keys) {
    const entry = cache.get(key);
    if (entry) out.set(key, entry);
  }
  return out.size === 0 ? EMPTY : out;
}

/**
 * Semantics for `keys` under the active tenant. Returns a map containing
 * only the keys the registry knows; identity is stable until a resolution
 * lands, so it is safe as a memo/effect dependency.
 */
export function useSemantics(keys: readonly string[]): SemanticsMap {
  const tenant = getTenantContext().tenant;
  const keyList = keys.join(SEP);
  const seen = useSyncExternalStore(subscribe, getVersion, getVersion);

  useEffect(() => {
    request(tenant, keyList ? keyList.split(SEP) : []);
  }, [tenant, keyList]);

  return useMemo(
    () => snapshot(tenant, keyList ? keyList.split(SEP) : []),
    // `seen` invalidates the memo when a batch lands.
    [tenant, keyList, seen],
  );
}

// ---------- prefix search ----------

const NO_HITS: AttributeHit[] = [];
const searchCache = new Map<string, AttributeHit[]>();
const searchInflight = new Set<string>();

/** Cache/de-dupe key for one prefix search under one tenant. */
const searchKey = (tenant: string, prefix: string) => `${tenant}${SEP}${prefix}`;

/**
 * Registry prefix search for autocomplete: `[]` until the hits arrive (or
 * forever, on error), cached per tenant and prefix for the session.
 */
export function useAttributeSearch(prefix: string, limit = 20): AttributeHit[] {
  const tenant = getTenantContext().tenant;
  const trimmed = prefix.trim();
  const cacheKey = searchKey(tenant, trimmed);
  const seen = useSyncExternalStore(subscribe, getVersion, getVersion);

  useEffect(() => {
    if (!trimmed || searchCache.has(cacheKey) || searchInflight.has(cacheKey)) {
      return;
    }
    searchInflight.add(cacheKey);
    let fired = false;
    const timer = setTimeout(() => {
      fired = true;
      void (async () => {
        try {
          searchCache.set(cacheKey, await searchAttributes(trimmed, limit));
        } catch {
          searchCache.set(cacheKey, NO_HITS);
        } finally {
          searchInflight.delete(cacheKey);
          notify();
        }
      })();
    }, SEMANTICS_DEBOUNCE_MS);
    return () => {
      // A superseded prefix that never fired is released for a later retry.
      if (!fired) {
        clearTimeout(timer);
        searchInflight.delete(cacheKey);
      }
    };
  }, [cacheKey, trimmed, limit]);

  return useMemo(
    () => (trimmed ? (searchCache.get(cacheKey) ?? NO_HITS) : NO_HITS),
    [cacheKey, trimmed, seen],
  );
}

/** Test hook: forget every cached resolution and pending batch. */
export function resetSemanticsCache(): void {
  for (const state of tenants.values()) {
    if (state.timer !== null) clearTimeout(state.timer);
  }
  tenants.clear();
  searchCache.clear();
  searchInflight.clear();
  notify();
}
