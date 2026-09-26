// The centered ⌘K command palette: jump to a page, a service's catalog
// entry, a recent query, an action, or straight to a pasted trace id.

import { useQuery } from "@tanstack/react-query";
import { useEffect, useMemo, useRef, useState } from "react";
import { Link, useNavigate } from "react-router";
import { fetchCatalogEntities } from "../../api/catalog";
import { entityType } from "../catalog/entityTypes";
import { entityQueryKey, NAV_SORT } from "../catalog/CatalogView";
import { loadRecentQueries } from "../../lib/recentQueries";
import { compositeKey } from "../../lib/traceGroups";
import { rangeScopeKey, resolveRange } from "../../lib/time";
import {
  buildPath,
  crossSignalSearch,
  type ExploreState,
} from "../../lib/urlState";
import { NavIcon } from "./NavIcon";
import { MANAGE_PAGE, NAV_GROUPS, pageHref } from "./navModel";
import {
  buildPaletteGroups,
  type PaletteItem,
  type PaletteSources,
} from "./paletteModel";

interface Props {
  state: ExploreState;
  canManage: boolean;
  onClose: () => void;
}

export function CommandPalette({ state, canManage, onClose }: Props) {
  const navigate = useNavigate();
  const inputRef = useRef<HTMLInputElement>(null);
  const listRef = useRef<HTMLDivElement>(null);
  const [query, setQuery] = useState("");
  const [active, setActive] = useState(0);

  // Focus the input on open; hand focus back to whatever opened the
  // palette (the header's search field, a sidebar item, …) on close.
  useEffect(() => {
    const opener = document.activeElement as HTMLElement | null;
    inputRef.current?.focus();
    return () => opener?.focus?.();
  }, []);

  const services = useServiceItems(state, query.trim() !== "");
  const recent = useMemo(
    () =>
      loadRecentQueries().map((q) => ({
        label: q.text,
        meta: q.signal,
        href: q.href,
      })),
    [],
  );

  const sources: PaletteSources = useMemo(() => {
    const pages: PaletteItem[] = NAV_GROUPS.flatMap((g) =>
      g.pages.map((p) => ({
        label: p.label,
        meta: g.title.toLowerCase(),
        href: pageHref(p, state),
      })),
    );
    if (canManage) {
      pages.push({ label: MANAGE_PAGE.label, meta: "admin", href: "/manage" });
    }
    const actions: PaletteItem[] = [
      ...(canManage
        ? [
            { label: "Invite members", meta: "action", href: "/manage" },
            { label: "Create API key", meta: "action", href: "/api-keys" },
          ]
        : []),
      {
        label: "Instrument a service",
        meta: "action",
        href: "/instrumentation",
      },
      ...(canManage
        ? [
            {
              label: "Connect GitHub",
              meta: "action",
              href: "/integrations/github",
            },
          ]
        : []),
      { label: "Switch tenant", meta: "action", href: "/select-tenant" },
      {
        label: "Open setup checklist",
        meta: "action",
        href: withParam(`/overview${crossSignalSearch(state)}`, "setup"),
      },
    ];
    return { pages, services, recent, actions };
  }, [state, canManage, services, recent]);

  const groups = buildPaletteGroups(query, sources);
  const flat = groups.flatMap((g) => g.items);
  const activeIndex = Math.min(active, Math.max(0, flat.length - 1));

  // Keep the keyboard-selected row in view as ↑/↓ move past the edge.
  useEffect(() => {
    listRef.current
      ?.querySelector<HTMLElement>('[aria-selected="true"]')
      ?.scrollIntoView?.({ block: "nearest" });
  }, [activeIndex]);

  const open = (item: PaletteItem) => {
    onClose();
    navigate(item.href);
  };

  const onKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "ArrowDown") {
      e.preventDefault();
      setActive(Math.min(flat.length - 1, activeIndex + 1));
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      setActive(Math.max(0, activeIndex - 1));
    } else if (e.key === "Enter") {
      const item = flat[activeIndex];
      if (item) {
        e.preventDefault();
        open(item);
      }
    } else if (e.key === "Escape") {
      e.preventDefault();
      e.stopPropagation();
      onClose();
    }
  };

  let index = 0;
  return (
    <>
      <div className="palette-backdrop" onClick={onClose} aria-hidden="true" />
      <div
        className="palette"
        role="dialog"
        aria-modal="true"
        aria-label="Command palette"
        onKeyDown={onKeyDown}
      >
        <div className="palette-input-row">
          <NavIcon name="search" size={18} />
          <input
            ref={inputRef}
            type="search"
            className="palette-input"
            value={query}
            placeholder="Search pages, services, queries, or paste a trace ID…"
            aria-label="Search"
            aria-controls="palette-results"
            aria-activedescendant={
              flat.length > 0 ? `palette-opt-${activeIndex}` : undefined
            }
            autoComplete="off"
            spellCheck={false}
            onChange={(e) => {
              setQuery(e.target.value);
              setActive(0);
            }}
          />
          <kbd className="nav-kbd">esc</kbd>
        </div>
        <div
          id="palette-results"
          className="palette-results"
          role="listbox"
          aria-label="Results"
          ref={listRef}
        >
          {groups.map((g) => (
            <div key={g.title} role="group" aria-label={g.title}>
              <div className="nav-section-label palette-group-label">
                {g.title}
              </div>
              {g.items.map((item) => {
                const i = index++;
                const selected = i === activeIndex;
                return (
                  <Link
                    key={`${g.title}:${item.href}:${item.label}`}
                    id={`palette-opt-${i}`}
                    to={item.href}
                    role="option"
                    aria-selected={selected}
                    className="palette-row"
                    onMouseEnter={() => setActive(i)}
                    onClick={(e) => {
                      e.preventDefault();
                      open(item);
                    }}
                  >
                    <span className="palette-row-label">{item.label}</span>
                    <span className="palette-row-meta">{item.meta}</span>
                    {selected && (
                      <span className="palette-row-enter" aria-hidden="true">
                        ↵
                      </span>
                    )}
                  </Link>
                );
              })}
            </div>
          ))}
          {query.trim() !== "" && flat.length === 0 && (
            <div className="palette-empty">
              No matches for “{query.trim()}”. Paste a trace ID to jump straight
              to it.
            </div>
          )}
        </div>
        <div className="palette-footer" aria-hidden="true">
          <span>↑↓ navigate</span>
          <span>↵ open</span>
          <span>esc close</span>
        </div>
      </div>
    </>
  );
}

/** `href` with a bare `name` flag appended to its query string. */
function withParam(href: string, name: string): string {
  return `${href}${href.includes("?") ? "&" : "?"}${name}`;
}

/** The catalog's service list as palette rows — the same query (and cache
 * entry) the catalog's entity nav uses, fetched only once there's something
 * typed to match against. */
function useServiceItems(state: ExploreState, enabled: boolean): PaletteItem[] {
  const service = entityType("service");
  const rangeKey = rangeScopeKey(state);
  const { data } = useQuery({
    queryKey: entityQueryKey("service", rangeKey, NAV_SORT),
    queryFn: () =>
      fetchCatalogEntities(
        service!,
        resolveRange(state.range, Date.now()),
        NAV_SORT,
      ),
    enabled: enabled && service !== undefined && state.tenant !== "",
    staleTime: 30_000,
  });
  return useMemo(() => {
    if (!data) return [];
    const search = crossSignalSearch(state);
    return data.entities.map((e) => ({
      label: e.values.filter((v) => v !== null).join(" · ") || "(not set)",
      meta: "service",
      href: `${buildPath("catalog", "", {
        catalogEntity: "service",
        catalogPrimary: compositeKey(e.values),
        catalogSecondary: "",
      })}${search}`,
    }));
  }, [data, state]);
}
