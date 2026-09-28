// The Real users page (`/rum/{tab}`): an app switcher, a tab strip, and one
// tab body per selected tab. This group ships Overview and Setup only —
// Pages/Sessions/Errors/Network/Interactions are later groups (tasks.md
// groups 4-8) and are not rendered as placeholders here (per group 3's
// scope note).
import { useEffect, useMemo, useRef, useState } from "react";
import type { ExploreState, UpdateFn } from "../../lib/urlState";
import { rangeScopeKey, resolveRange } from "../../lib/time";
import { EmptyState } from "../../components/EmptyState";
import { RefreshButton } from "../../components/RefreshButton";
import { TimeRangePicker } from "../../components/TimeRangePicker";
import { NavIcon } from "../shell/NavIcon";
import { useRumApps } from "./useRumData";
import type { RumApp } from "../../api/rum";
import { RUM_TABS, type RumTab } from "./rumModel";
import { OverviewTab } from "./OverviewTab";
import { SetupTab } from "./SetupTab";
import "./rum.css";

export type { RumTab };

interface Props {
  state: ExploreState;
  update: UpdateFn;
  tab: RumTab;
  onTabChange: (tab: RumTab) => void;
}

export function RealUsersView({ state, update, tab, onTabChange }: Props) {
  const rangeKey = rangeScopeKey(state);
  const range = resolveRange(state.range, Date.now());
  const apps = useRumApps(range, rangeKey);

  // Default to the busiest app once the list loads, writing it into the URL
  // (the spec's "Opening the page picks the busiest app" scenario) — but
  // only when nothing is selected yet or the selection no longer exists.
  const appList = apps.data ?? [];
  const known = appList.some((a) => a.serviceName === state.rumApp);
  const selectedApp = known ? state.rumApp : (appList[0]?.serviceName ?? "");
  const current = appList.find((a) => a.serviceName === selectedApp);

  const scope = useMemo(
    () => ({ range, rangeKey, app: selectedApp }),
    [range, rangeKey, selectedApp],
  );

  // Writes the picked default into the URL (the spec's "Opening the page
  // picks the busiest app" scenario) once the app list is known — a plain
  // effect rather than folding it into render, since it's a navigation
  // side effect (rewriting `?app=`), not a derived value.
  useEffect(() => {
    if (!known && selectedApp !== "" && selectedApp !== state.rumApp) {
      update({ rumApp: selectedApp });
    }
  }, [known, selectedApp, state.rumApp, update]);

  function selectApp(app: string) {
    update({ rumApp: app });
  }

  const noAppsYet = !apps.isPending && appList.length === 0;
  const envVersion =
    current?.env && current?.version
      ? `${current.env} · ${current.version}`
      : current?.env || current?.version || "";

  return (
    <div className="rum-body">
      <div className="rum-header">
        <div className="rum-header-left">
          {!noAppsYet && (
            <AppSwitcher
              apps={appList}
              selected={selectedApp}
              onSelect={selectApp}
            />
          )}
          {envVersion && (
            <span className="mono dim rum-env-version">{envVersion}</span>
          )}
        </div>
        <div className="rum-header-right">
          <TimeRangePicker
            range={state.range}
            onChange={(r) => update({ range: r })}
          />
          <RefreshButton />
        </div>
      </div>

      <nav className="rum-tabs" aria-label="Real users tabs">
        {RUM_TABS.map((t) => (
          <button
            key={t.id}
            type="button"
            className={t.id === tab ? "on" : undefined}
            aria-current={t.id === tab ? "page" : undefined}
            onClick={() => onTabChange(t.id)}
          >
            {t.label}
          </button>
        ))}
      </nav>

      <div className="rum-main">
        {tab === "setup" ? (
          <SetupTab app={selectedApp} scope={scope} />
        ) : noAppsYet ? (
          <EmptyState title="No frontend app has sent real-user data yet">
            Instrument a browser app with the OpenTelemetry SDK to see sessions,
            Web Vitals and errors here.{" "}
            <button
              type="button"
              className="rum-empty-setup-link"
              onClick={() => onTabChange("setup")}
            >
              Open Setup
            </button>
          </EmptyState>
        ) : (
          <OverviewTab scope={scope} onOpenSetup={() => onTabChange("setup")} />
        )}
      </div>
    </div>
  );
}

/** The app's platform, from `resource.telemetry.sdk.language` — browser
 * only for now (other platforms are the `rum-explore-tabs` change's
 * "Platform-aware labels" requirement), so anything else reads as "Unknown". */
function platformLabel(sdkLanguage: string | null): string {
  return sdkLanguage === "webjs" ? "Browser · JS" : "Unknown platform";
}

/** A plain globe glyph standing in for a per-platform icon (iOS/Android
 * icons are a later change — see `rum-explore-tabs`). */
function PlatformIcon() {
  return (
    <svg
      width="14"
      height="14"
      viewBox="0 0 16 16"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.3"
      aria-hidden="true"
      className="rum-platform-icon"
    >
      <circle cx="8" cy="8" r="6.5" />
      <path d="M1.5 8 H14.5 M8 1.5 C10.3 4 10.3 12 8 14.5 C5.7 12 5.7 4 8 1.5" />
    </svg>
  );
}

/**
 * The app switcher: a button showing the current app's platform icon, name
 * and platform, opening a listbox menu of every frontend app with RUM data
 * (busiest first — the order `apps` already comes in). Closes on
 * outside-click or Escape.
 */
function AppSwitcher({
  apps,
  selected,
  onSelect,
}: {
  apps: RumApp[];
  selected: string;
  onSelect: (app: string) => void;
}) {
  const [open, setOpen] = useState(false);
  const rootRef = useRef<HTMLDivElement>(null);
  const current = apps.find((a) => a.serviceName === selected);

  useEffect(() => {
    if (!open) return;
    function onPointerDown(e: PointerEvent) {
      if (!rootRef.current?.contains(e.target as Node)) setOpen(false);
    }
    function onKeyDown(e: KeyboardEvent) {
      if (e.key === "Escape") setOpen(false);
    }
    document.addEventListener("pointerdown", onPointerDown);
    document.addEventListener("keydown", onKeyDown);
    return () => {
      document.removeEventListener("pointerdown", onPointerDown);
      document.removeEventListener("keydown", onKeyDown);
    };
  }, [open]);

  return (
    <div className="rum-appsel" ref={rootRef}>
      <button
        type="button"
        className="rum-appbtn"
        aria-haspopup="listbox"
        aria-expanded={open}
        onClick={() => setOpen((o) => !o)}
      >
        <PlatformIcon />
        <span>{current?.serviceName ?? "Select app"}</span>
        <span className="dim rum-appbtn-platform">
          {platformLabel(current?.sdkLanguage ?? null)}
        </span>
      </button>
      {open && (
        <div className="rum-menu" role="listbox" aria-label="Frontend apps">
          <span className="rum-eyebrow rum-menu-eyebrow">
            Frontend apps · service.name
          </span>
          {apps.map((a) => (
            <button
              key={a.serviceName}
              type="button"
              role="option"
              aria-selected={a.serviceName === selected}
              onClick={() => {
                onSelect(a.serviceName);
                setOpen(false);
              }}
            >
              <PlatformIcon />
              <span className="rum-menu-item-text">
                <span className="mono rum-menu-item-label">
                  {a.serviceName}
                </span>
                <span className="dim rum-menu-item-platform">
                  {platformLabel(a.sdkLanguage)}
                </span>
              </span>
              {a.serviceName === selected && <NavIcon name="check" size={14} />}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
