// User menu dropdown for the top bar. Shows avatar with initials, user info,
// theme toggle, navigation items, and sign-out action.

import { useQueryClient } from "@tanstack/react-query";
import { useRef, useState } from "react";
import { Link, useNavigate } from "react-router";
import { clearPersistedTenantContext, toErrorMessage } from "../../api/http";
import { deleteSession, type WhoamiResponse } from "../../api/session";
import type { ExploreState } from "../../lib/urlState";
import { useIsDemo, useWhoami } from "../../lib/useWhoami";
import { isDarkTheme, toggleTheme } from "../../lib/theme";
import { useEscapeKey } from "../../hooks/useEscapeKey";
import "./UserMenu.css";

interface Props {
  state: ExploreState;
  /**
   * `topbar` (the default): avatar + name + caret, the popover dropping
   * below. `sidebar`: the nav sidebar's footer row — avatar, then name over
   * role when `expanded`, the popover opening beside the sidebar.
   * `compact`: the mobile top bar's bare 32px avatar.
   */
  variant?: "topbar" | "sidebar" | "compact";
  /** Sidebar variant only: whether the name/role text shows. */
  expanded?: boolean;
}

export function UserMenu({
  state,
  variant = "topbar",
  expanded = true,
}: Props) {
  const [open, setOpen] = useState(false);
  const { data: who, canManage } = useWhoami(state);
  const isDemo = useIsDemo();
  const toggle = () => setOpen((prev) => !prev);
  const close = () => setOpen(false);

  if (!who?.user) return null;

  const user = who.user;
  const initials = initialsFor(user.display_name || user.email);
  const role = who.memberships.find((m) => m.tenant_id === who.tenant.id)?.role;

  const name = user.display_name || user.email;

  return (
    <span className={`user-menu user-menu--${variant}`}>
      {variant === "topbar" ? (
        <button
          className="user-menu-toggle"
          onClick={toggle}
          aria-expanded={open}
          aria-haspopup="true"
        >
          <span className="user-avatar">{initials}</span>
          <span className="user-name">{name}</span>
          <span className="user-caret">▾</span>
        </button>
      ) : (
        <button
          type="button"
          className={`nav-account nav-account--${variant}`}
          onClick={toggle}
          aria-expanded={open}
          aria-haspopup="true"
          aria-label="Account"
          title={name}
        >
          <span className="nav-avatar">{initials}</span>
          {variant === "sidebar" && expanded && (
            <span className="nav-account-text">
              <span className="nav-account-name">{name}</span>
              {role && <span className="nav-account-role">{role}</span>}
            </span>
          )}
        </button>
      )}
      {open && (
        <UserMenuPopover
          who={who}
          role={role}
          canManage={canManage}
          isDemo={isDemo}
          onClose={close}
        />
      )}
    </span>
  );
}

interface PopoverProps {
  who: WhoamiResponse;
  role: string | undefined;
  canManage: boolean;
  /** Hides mutating surfaces (change: demo-mode): schema/processor editing
   * is reachable by every role today, so this is checked independently of
   * `canManage`. */
  isDemo: boolean;
  onClose: () => void;
}

function UserMenuPopover({
  who,
  role,
  canManage,
  isDemo,
  onClose,
}: PopoverProps) {
  const client = useQueryClient();
  const navigate = useNavigate();
  const backdropRef = useRef<HTMLSpanElement>(null);
  const [signOutError, setSignOutError] = useState<string | null>(null);
  const [isDark, setIsDark] = useState(isDarkTheme());

  // Close on backdrop click or Escape
  useEscapeKey(true, onClose);

  const handleBackdropClick = (e: React.MouseEvent) => {
    if (e.target === backdropRef.current) onClose();
  };

  const handleSignOut = async () => {
    setSignOutError(null);
    try {
      await deleteSession();
      clearPersistedTenantContext();
      client.clear();
      navigate("/login");
      window.location.reload();
    } catch (err) {
      // A failed sign-out leaves the session intact — report it and keep
      // the menu open rather than reloading into a page that still thinks
      // it's signed in.
      setSignOutError(toErrorMessage(err));
    }
  };

  const handleThemeToggle = () => {
    toggleTheme();
    setIsDark(isDarkTheme());
  };

  const user = who.user!;

  return (
    <span>
      <span
        ref={backdropRef}
        className="user-menu-backdrop"
        onClick={handleBackdropClick}
      />
      <span className="user-menu-popover" role="menu">
        {/* User info */}
        <span className="user-menu-info">
          <span className="user-menu-name">
            {user.display_name || user.email}
          </span>
          <span className="user-menu-email">{user.email}</span>
          {role && <span className="user-menu-role">{role}</span>}
        </span>

        {/* Menu items */}
        <span className="user-menu-items">
          <button className="user-menu-item" onClick={handleThemeToggle}>
            <span>Appearance</span>
            <span className="user-menu-hint">{isDark ? "Dark" : "Light"}</span>
          </button>
          <Link
            className="user-menu-item"
            to="/instrumentation"
            onClick={onClose}
          >
            <span>Send data</span>
            <span className="user-menu-hint">instrumentation</span>
          </Link>
          {canManage && (
            <Link className="user-menu-item" to="/api-keys" onClick={onClose}>
              <span>API keys</span>
              <span className="user-menu-hint">{who.tenant.id}</span>
            </Link>
          )}
          {canManage && (
            <Link
              className="user-menu-item"
              to="/integrations/github"
              onClick={onClose}
            >
              <span>GitHub</span>
              <span className="user-menu-hint">integration</span>
            </Link>
          )}
          {!isDemo && (
            <Link className="user-menu-item" to="/schema" onClick={onClose}>
              <span>Schema</span>
              <span className="user-menu-hint">conventions</span>
            </Link>
          )}
          {!isDemo && (
            <Link className="user-menu-item" to="/processors" onClick={onClose}>
              <span>Processors</span>
              <span className="user-menu-hint">OTTL</span>
            </Link>
          )}
          <a
            className="user-menu-item"
            href="https://signaldb.dev/docs"
            target="_blank"
            rel="noopener noreferrer"
          >
            <span>Docs</span>
            <span className="user-menu-hint">↗</span>
          </a>
        </span>

        {signOutError && (
          <p className="user-menu-alert error-text" role="alert">
            {signOutError}
          </p>
        )}

        {/* Bottom actions */}
        <span className="user-menu-actions">
          <Link
            className="user-menu-item"
            to="/select-tenant"
            onClick={onClose}
          >
            <span>Switch tenant</span>
          </Link>
          <button
            className="user-menu-item user-menu-signout"
            onClick={() => void handleSignOut()}
          >
            <span>Sign out</span>
          </button>
        </span>
      </span>
    </span>
  );
}

/** Extract initials from a name or email address. */
function initialsFor(name: string): string {
  const parts = name.split(/[\s@]+/).filter(Boolean);
  if (parts.length >= 2) {
    const first = parts[0]!;
    const last = parts[parts.length - 1]!;
    return (first.charAt(0) + last.charAt(0)).toUpperCase();
  }
  return name.slice(0, 2).toUpperCase();
}
