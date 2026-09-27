// The app frame every page renders inside: the sidebar (or the mobile top
// bar), the sticky page header, the ⌘K palette and the main column. It only
// draws — who is signed in and the tenant/window context come in as props —
// so `App` owns the data and redirects, and the same frame can render on
// its own (stories, the design system) with no backend behind it.

import type { ReactNode } from "react";
import { MemoryRouter, useInRouterContext } from "react-router";
import type { WhoamiResponse } from "../../api/session";
import { ThrottleBanner } from "../../components/ThrottleBanner";
import { canManage as canManageFor } from "../../lib/useWhoami";
import {
  DEFAULT_STATE,
  type ExploreState,
  type UpdateFn,
} from "../../lib/urlState";
import { AppNav, useAppNavState } from "./AppNav";
import { CommandPalette } from "./CommandPalette";
import { HOME_PATH, pageById, type PageId } from "./navModel";
import { PageHeader } from "./PageHeader";

export interface AppShellProps {
  /** The page's own content, rendered in the main column. */
  children?: ReactNode;
  /**
   * Which page the sidebar and breadcrumb mark as current when the shell
   * renders outside a router. Inside one, the router's location decides.
   */
  page?: PageId;
  /** The signed-in identity: account row, tenant list, admin access. */
  who?: WhoamiResponse;
  /** Defaults to what `who` allows (instance admin or tenant admin). */
  canManage?: boolean;
  /** Shows the read-only demo banner and hides mutating menu entries. */
  isDemo?: boolean;
  /** Tenant, dataset and time window the nav links carry. */
  state?: ExploreState;
  update?: UpdateFn;
}

export function AppShell(props: AppShellProps) {
  if (useInRouterContext()) return <Frame {...props} />;
  return (
    <MemoryRouter
      initialEntries={[(props.page && pageById(props.page)?.path) || HOME_PATH]}
    >
      <Frame {...props} />
    </MemoryRouter>
  );
}

const noUpdate: UpdateFn = () => {};

function Frame({
  children,
  who,
  canManage = canManageFor(who),
  isDemo = false,
  state = DEFAULT_STATE,
  update = noUpdate,
}: AppShellProps) {
  const nav = useAppNavState();
  return (
    <div className="app-frame">
      {isDemo && (
        <div
          className="accent-banner demo-banner"
          title="Read-only public demo account"
        >
          Demo · read-only
        </div>
      )}
      <div className="app-body">
        <AppNav
          state={state}
          update={update}
          who={who}
          canManage={canManage}
          isDemo={isDemo}
          nav={nav}
        />
        <div className="app-column">
          {!nav.narrow && <PageHeader onOpenPalette={nav.openPalette} />}
          <ThrottleBanner />
          <main className="app-main">{children}</main>
        </div>
      </div>
      {nav.overlay === "palette" && (
        <CommandPalette
          state={state}
          canManage={canManage}
          onClose={nav.close}
        />
      )}
    </div>
  );
}
