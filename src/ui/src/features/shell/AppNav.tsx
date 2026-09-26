// The app navigation that replaced the top bar: a collapsible left sidebar
// (brand, tenant/dataset switcher, grouped pages, Manage, account, collapse
// toggle) at ≥ 720px, and a 48px top bar with a slide-in drawer below that.
// The command palette and its trigger live in `CommandPalette` and
// `PageHeader`; `useAppNavState` owns the shared open/collapsed state.

import { useCallback, useEffect, useState } from "react";
import { Link, useLocation } from "react-router";
import type { WhoamiResponse } from "../../api/session";
import { DEFAULT_DATASET, DEFAULT_TENANT } from "../../api/http";
import { useEscapeKey } from "../../hooks/useEscapeKey";
import { useMediaQuery } from "../../hooks/useMediaQuery";
import { crossSignalSearch, type ExploreState } from "../../lib/urlState";
import { BrandPulse, NavIcon } from "./NavIcon";
import {
  currentPageFor,
  HOME_PATH,
  MANAGE_PAGE,
  NAV_GROUPS,
  pageHref,
  type PageId,
} from "./navModel";
import { UserMenu } from "./UserMenu";
import "./AppNav.css";

const COLLAPSED_KEY = "sdb.sidebar.collapsed";
/** Below this viewport width the sidebar gives way to the mobile top bar. */
export const NARROW_QUERY = "(max-width: 719px)";
/** Below this the sidebar starts collapsed, unless the user chose. */
const TABLET_QUERY = "(max-width: 1023px)";

function loadCollapsedChoice(): boolean | null {
  try {
    const v = localStorage.getItem(COLLAPSED_KEY);
    return v === "true" ? true : v === "false" ? false : null;
  } catch {
    return null;
  }
}

function isTypingTarget(target: EventTarget | null): boolean {
  if (!(target instanceof HTMLElement)) return false;
  return (
    target.isContentEditable ||
    /^(input|textarea|select)$/i.test(target.tagName)
  );
}

export type NavOverlay = "palette" | "drawer" | null;

/**
 * Shell-level nav state: which overlay is open, whether the sidebar is
 * collapsed (the user's persisted choice wins; otherwise collapsed on
 * tablets), and the global shortcuts — ⌘K/Ctrl+K opens the palette, `[`
 * toggles the sidebar outside text fields, Esc closes an open overlay.
 */
export function useAppNavState() {
  const narrow = useMediaQuery(NARROW_QUERY);
  const tablet = useMediaQuery(TABLET_QUERY);
  const [choice, setChoice] = useState<boolean | null>(loadCollapsedChoice);
  const [overlay, setOverlay] = useState<NavOverlay>(null);
  const collapsed = !narrow && (choice ?? tablet);

  const toggleCollapsed = useCallback(() => {
    setChoice((prev) => {
      const next = !(prev ?? window.matchMedia(TABLET_QUERY).matches);
      try {
        localStorage.setItem(COLLAPSED_KEY, String(next));
      } catch {
        // localStorage unavailable — the choice lasts for this page only.
      }
      return next;
    });
  }, []);
  const openPalette = useCallback(() => setOverlay("palette"), []);
  const close = useCallback(() => setOverlay(null), []);
  const toggleDrawer = useCallback(
    () => setOverlay((o) => (o === "drawer" ? null : "drawer")),
    [],
  );

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "k") {
        e.preventDefault();
        setOverlay((o) => (o === "palette" ? null : "palette"));
      } else if (
        e.key === "[" &&
        !e.metaKey &&
        !e.ctrlKey &&
        !e.altKey &&
        !isTypingTarget(e.target) &&
        !window.matchMedia(NARROW_QUERY).matches
      ) {
        toggleCollapsed();
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [toggleCollapsed]);
  useEscapeKey(overlay !== null, close);

  // Navigating (a nav link, Back/Forward, a palette pick) dismisses the
  // drawer and palette.
  const location = useLocation();
  useEffect(() => {
    setOverlay(null);
  }, [location.pathname]);

  return {
    narrow,
    collapsed,
    overlay,
    toggleCollapsed,
    openPalette,
    close,
    toggleDrawer,
  };
}

interface NavProps {
  state: ExploreState;
  update: (patch: Partial<ExploreState>) => void;
  who: WhoamiResponse | undefined;
  canManage: boolean;
  nav: ReturnType<typeof useAppNavState>;
}

export function AppNav(props: NavProps) {
  return props.nav.narrow ? <MobileNav {...props} /> : <Sidebar {...props} />;
}

function Sidebar({ state, update, who, canManage, nav }: NavProps) {
  const { pathname } = useLocation();
  const current = currentPageFor(pathname).id;
  const expanded = !nav.collapsed;
  const collapseLabel = expanded ? "Collapse sidebar" : "Expand sidebar";
  return (
    <aside
      className={`app-sidebar${expanded ? "" : " collapsed"}`}
      aria-label="Main navigation"
    >
      <div className="app-sidebar-inner">
        <Link
          className="app-sidebar-brand"
          to={`${HOME_PATH}${crossSignalSearch(state)}`}
          aria-label="signaldb home"
        >
          <BrandPulse />
          {expanded && (
            <span>
              signal<b>db</b>
            </span>
          )}
        </Link>
        <TenantSwitcher
          state={state}
          update={update}
          who={who}
          expanded={expanded}
        />
        <nav className="app-sidebar-groups" aria-label="Pages">
          {NAV_GROUPS.map((g) => (
            <div key={g.title} className="app-nav-group">
              {expanded ? (
                <div className="nav-section-label app-nav-group-label">
                  {g.title}
                </div>
              ) : (
                <div className="app-nav-group-rule" aria-hidden="true" />
              )}
              {g.pages.map((p) => (
                <NavItem
                  key={p.id}
                  id={p.id}
                  label={p.label}
                  to={pageHref(p, state)}
                  current={current === p.id}
                  showLabel={expanded}
                />
              ))}
            </div>
          ))}
        </nav>
        <div className="app-sidebar-footer">
          {canManage && (
            <NavItem
              id="manage"
              label={MANAGE_PAGE.label}
              to={MANAGE_PAGE.path}
              current={current === "manage"}
              showLabel={expanded}
            />
          )}
          <UserMenu state={state} variant="sidebar" expanded={expanded} />
          <button
            type="button"
            className="app-nav-item app-sidebar-collapse"
            onClick={nav.toggleCollapsed}
            aria-label={collapseLabel}
            title={collapseLabel}
            aria-expanded={expanded}
          >
            <NavIcon name="collapse" />
            {expanded && (
              <>
                <span className="app-nav-item-label">Collapse</span>
                <kbd className="nav-kbd">[</kbd>
              </>
            )}
          </button>
        </div>
      </div>
    </aside>
  );
}

function NavItem({
  id,
  label,
  to,
  current,
  showLabel,
  large = false,
}: {
  id: PageId;
  label: string;
  to: string;
  current: boolean;
  showLabel: boolean;
  /** 44px touch rows in the mobile drawer. */
  large?: boolean;
}) {
  return (
    <Link
      to={to}
      className={`app-nav-item${large ? " app-nav-item--large" : ""}`}
      aria-current={current ? "page" : undefined}
      aria-label={showLabel ? undefined : label}
      title={showLabel ? undefined : label}
    >
      <NavIcon name={id} />
      {showLabel && <span className="app-nav-item-label">{label}</span>}
    </Link>
  );
}

function TenantSwitcher({
  state,
  update,
  who,
  expanded,
  large = false,
}: {
  state: ExploreState;
  update: (patch: Partial<ExploreState>) => void;
  who: WhoamiResponse | undefined;
  expanded: boolean;
  large?: boolean;
}) {
  const [open, setOpen] = useState(false);
  const close = useCallback(() => setOpen(false), []);
  useEscapeKey(open, close, { exclusive: true });

  const tenant = state.tenant || who?.tenant.id || DEFAULT_TENANT || "tenant";
  const dataset =
    state.dataset || who?.default_dataset || DEFAULT_DATASET || "default";
  const tenants = who?.memberships.map((m) => m.tenant_id) ?? [tenant];
  const datasets = who?.datasets.map((d) => d.id) ?? [dataset];

  return (
    <div className="tenant-switch">
      <button
        type="button"
        className={`tenant-switch-trigger${large ? " tenant-switch-trigger--large" : ""}`}
        onClick={() => setOpen((o) => !o)}
        aria-haspopup="listbox"
        aria-expanded={open}
        aria-label={`Switch tenant or dataset (${tenant} · ${dataset})`}
        title="Tenant / dataset context for all queries"
      >
        <span className="tenant-switch-tile">
          {tenant.charAt(0).toUpperCase()}
        </span>
        {expanded && (
          <>
            <span className="tenant-switch-text">
              <span className="tenant-switch-tenant">{tenant}</span>
              <span className="tenant-switch-dataset">{dataset}</span>
            </span>
            <NavIcon name="updown" size={14} />
          </>
        )}
      </button>
      {open && (
        <>
          <div
            className="tenant-switch-backdrop"
            onClick={close}
            aria-hidden="true"
          />
          <div className="tenant-switch-popover">
            <div
              role="listbox"
              aria-label="Tenant"
              className="tenant-switch-list"
            >
              <div className="nav-section-label tenant-switch-label">
                Tenant
              </div>
              {tenants.map((t) => (
                <TenantOption
                  key={t}
                  label={t}
                  selected={t === tenant}
                  onPick={() => {
                    // A different tenant's datasets aren't known until its
                    // whoami loads, so reset the dataset to its default.
                    // The popover stays open to pick one.
                    if (t !== tenant) update({ tenant: t, dataset: "" });
                  }}
                />
              ))}
            </div>
            <div
              role="listbox"
              aria-label="Dataset"
              className="tenant-switch-list tenant-switch-list--datasets"
            >
              <div className="nav-section-label tenant-switch-label">
                Dataset
              </div>
              {datasets.map((d) => (
                <TenantOption
                  key={d}
                  label={d}
                  mono
                  selected={d === dataset}
                  onPick={() => {
                    update({ dataset: d });
                    close();
                  }}
                />
              ))}
            </div>
          </div>
        </>
      )}
    </div>
  );
}

function TenantOption({
  label,
  selected,
  mono = false,
  onPick,
}: {
  label: string;
  selected: boolean;
  mono?: boolean;
  onPick: () => void;
}) {
  return (
    <button
      type="button"
      role="option"
      aria-selected={selected}
      className={`tenant-switch-option${mono ? " mono" : ""}`}
      onClick={onPick}
    >
      <span className="tenant-switch-option-label">{label}</span>
      {selected && <NavIcon name="check" size={14} />}
    </button>
  );
}

function MobileNav({ state, update, who, canManage, nav }: NavProps) {
  const { pathname } = useLocation();
  const page = currentPageFor(pathname);
  const drawerOpen = nav.overlay === "drawer";
  return (
    <>
      <header className="app-mobilebar">
        <button
          type="button"
          className="app-mobilebar-btn"
          aria-label="Open navigation"
          aria-expanded={drawerOpen}
          onClick={nav.toggleDrawer}
        >
          <NavIcon name="menu" size={18} />
        </button>
        <Link
          className="app-mobilebar-brand"
          to={`${HOME_PATH}${crossSignalSearch(state)}`}
          aria-label="signaldb home"
        >
          <BrandPulse />
        </Link>
        <span className="app-mobilebar-page" aria-current="page">
          {page.label}
        </span>
        <span className="app-mobilebar-spacer" />
        <button
          type="button"
          className="app-mobilebar-btn"
          aria-label="Search"
          aria-haspopup="dialog"
          onClick={nav.openPalette}
        >
          <NavIcon name="search" size={18} />
        </button>
        <UserMenu state={state} variant="compact" />
      </header>
      {drawerOpen && (
        <>
          <div
            className="app-drawer-backdrop"
            onClick={nav.close}
            aria-hidden="true"
          />
          <nav className="app-drawer" aria-label="Main navigation">
            <TenantSwitcher
              state={state}
              update={update}
              who={who}
              expanded
              large
            />
            {NAV_GROUPS.map((g) => (
              <div key={g.title} className="app-nav-group">
                <div className="nav-section-label app-drawer-group-label">
                  {g.title}
                </div>
                {g.pages.map((p) => (
                  <NavItem
                    key={p.id}
                    id={p.id}
                    label={p.label}
                    to={pageHref(p, state)}
                    current={page.id === p.id}
                    showLabel
                    large
                  />
                ))}
              </div>
            ))}
            {canManage && (
              <div className="app-drawer-footer">
                <NavItem
                  id="manage"
                  label={MANAGE_PAGE.label}
                  to={MANAGE_PAGE.path}
                  current={page.id === "manage"}
                  showLabel
                  large
                />
              </div>
            )}
          </nav>
        </>
      )}
    </>
  );
}
