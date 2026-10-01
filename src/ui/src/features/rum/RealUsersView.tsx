// The Real users page (`/rum/{tab}`): an app switcher, a tab strip, and one
// tab body per selected tab.
import { useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import type { ExploreState, UpdateFn } from "../../lib/urlState";
import { rangeScopeKey, resolveRange } from "../../lib/time";
import { EmptyState } from "../../components/EmptyState";
import { RefreshButton } from "../../components/RefreshButton";
import { TimeRangePicker } from "../../components/TimeRangePicker";
import { NavIcon } from "../shell/NavIcon";
import { useRumApps } from "./useRumData";
import type { RumApp } from "../../api/rum";
import {
  detectPlatform,
  platformLabel,
  RUM_TABS,
  rumTabLabel,
  type RumPlatform,
  type RumTab,
} from "./rumModel";
import { ErrorsTab } from "./ErrorsTab";
import { InteractionsTab } from "./InteractionsTab";
import { NetworkTab } from "./NetworkTab";
import { OverviewTab } from "./OverviewTab";
import { PagesTab } from "./PagesTab";
import { SessionsTab } from "./SessionsTab";
import { SetupTab } from "./SetupTab";
import "./rum.css";

export type { RumTab };

interface Props {
  state: ExploreState;
  update: UpdateFn;
  tab: RumTab;
  onTabChange: (tab: RumTab) => void;
  /** Switches tab and sets a search param in one navigation — see
   * `RealUsersRoute`'s own doc comment for why `onTabChange` + `update()`
   * can't do this safely. */
  onTabChangeWith: (tab: RumTab, patch: Partial<ExploreState>) => void;
}

export function RealUsersView({
  state,
  update,
  tab,
  onTabChange,
  onTabChangeWith,
}: Props) {
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

  // Clears the previous app's selected route, error group and session in
  // the same navigation that sets the new app — the spec's "Switching apps"
  // scenario. `update()` already builds one URL from the whole patch, so
  // this doesn't need `onTabChangeWith`'s two-navigations workaround (that
  // one exists only because switching tab goes through path-based
  // `navigate()`, not `update()`).
  function selectApp(app: string) {
    update({
      rumApp: app,
      rumRoute: "",
      rumSession: "",
      rumErrorGroup: "",
    });
  }

  const noAppsYet = !apps.isPending && appList.length === 0;
  const envVersion =
    current?.env && current?.version
      ? `${current.env} · ${current.version}`
      : current?.env || current?.version || "";
  const platform = detectPlatform(
    current?.sdkLanguage ?? null,
    current?.osName ?? null,
  );

  return (
    <div className="rum-body">
      <div className="rum-header">
        <div className="rum-header-left">
          {!noAppsYet && (
            <AppSwitcher
              apps={appList}
              selected={selectedApp}
              platform={platform}
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
            {rumTabLabel(t.id, platform)}
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
        ) : tab === "network" ? (
          <NetworkTab
            scope={scope}
            platform={platform}
            onOpenSetup={() => onTabChange("setup")}
          />
        ) : tab === "pages" ? (
          <PagesTab
            scope={scope}
            platform={platform}
            route={state.rumRoute}
            onSelectRoute={(route) => update({ rumRoute: route })}
            onOpenSetup={() => onTabChange("setup")}
            onOpenNetwork={() => onTabChange("network")}
          />
        ) : tab === "interactions" ? (
          <InteractionsTab scope={scope} />
        ) : tab === "sessions" ? (
          <SessionsTab
            scope={scope}
            state={state}
            session={state.rumSession}
            onSelectSession={(session) => update({ rumSession: session })}
          />
        ) : tab === "errors" ? (
          <ErrorsTab
            scope={scope}
            state={state}
            currentVersion={current?.version ?? null}
            selected={state.rumErrorGroup}
            onSelectGroup={(groupKey) => update({ rumErrorGroup: groupKey })}
          />
        ) : (
          <OverviewTab
            scope={scope}
            platform={platform}
            currentVersion={current?.version ?? null}
            onOpenSetup={() => onTabChange("setup")}
            onOpenNetwork={() => onTabChange("network")}
            onOpenPages={(route) =>
              onTabChangeWith("pages", { rumRoute: route })
            }
            onOpenErrors={(groupKey) =>
              onTabChangeWith("errors", { rumErrorGroup: groupKey })
            }
          />
        )}
      </div>
    </div>
  );
}

/** The shared outer `<svg>` every `PlatformIcon` shape draws into. */
function PlatformIconSvg({ children }: { children: ReactNode }) {
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
      {children}
    </svg>
  );
}

/** A distinct inline glyph per platform: a globe for browser, a rounded
 * "phone" outline for iOS, and an angular one for Android — cheap enough to
 * draw inline rather than pulling in an icon set for three shapes. */
function PlatformIcon({ platform }: { platform: RumPlatform }) {
  if (platform === "ios") {
    return (
      <PlatformIconSvg>
        <rect x="4" y="1.5" width="8" height="13" rx="2" />
        <path d="M7 12.3 H9" strokeLinecap="round" />
      </PlatformIconSvg>
    );
  }
  if (platform === "android") {
    return (
      <PlatformIconSvg>
        <rect x="3" y="5" width="10" height="8" rx="1.5" />
        <path
          d="M5.5 5 3.8 2.8 M10.5 5 12.2 2.8 M5 8 V10.5 M11 8 V10.5"
          strokeLinecap="round"
        />
      </PlatformIconSvg>
    );
  }
  return (
    <PlatformIconSvg>
      <circle cx="8" cy="8" r="6.5" />
      <path d="M1.5 8 H14.5 M8 1.5 C10.3 4 10.3 12 8 14.5 C5.7 12 5.7 4 8 1.5" />
    </PlatformIconSvg>
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
  platform,
  onSelect,
}: {
  apps: RumApp[];
  selected: string;
  /** The selected app's platform — already computed by the caller from the
   * same `apps`/`selected` pair, so this avoids re-deriving it here. */
  platform: RumPlatform;
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
        <PlatformIcon platform={platform} />
        <span>{current?.serviceName ?? "Select app"}</span>
        <span className="dim rum-appbtn-platform">
          {platformLabel(current?.sdkLanguage ?? null, current?.osName ?? null)}
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
              <PlatformIcon
                platform={detectPlatform(a.sdkLanguage, a.osName)}
              />
              <span className="rum-menu-item-text">
                <span className="mono rum-menu-item-label">
                  {a.serviceName}
                </span>
                <span className="dim rum-menu-item-platform">
                  {platformLabel(a.sdkLanguage, a.osName)}
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
