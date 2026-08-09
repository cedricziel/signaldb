// User menu dropdown for the top bar. Shows avatar with initials, user info,
// theme toggle, navigation items, and sign-out action.

import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useEffect, useRef, useState } from "react";
import { Link, useNavigate } from "react-router";
import { whoami, deleteSession } from "../../api/session";
import type { ExploreState } from "../../lib/urlState";
import "./UserMenu.css";

interface Props {
  state: ExploreState;
  update: (patch: Partial<ExploreState>) => void;
}

export function UserMenu({ state, update }: Props) {
  const [open, setOpen] = useState(false);
  const { data: who } = useQuery({
    queryKey: ["whoami", state.tenant, state.dataset],
    queryFn: () => whoami(),
    staleTime: 60_000,
    retry: false,
  });
  const toggle = () => setOpen((prev) => !prev);
  const close = () => setOpen(false);

  if (!who?.user) return null;

  const user = who.user;
  const initials = initialsFor(user.display_name || user.email);
  const role = who.memberships.find((m) => m.tenant_id === who.tenant.id)?.role;

  return (
    <span className="user-menu">
      <button
        className="user-menu-toggle"
        onClick={toggle}
        aria-expanded={open}
        aria-haspopup="true"
      >
        <span className="user-avatar">{initials}</span>
        <span className="user-name">{user.display_name || user.email}</span>
        <span className="user-caret">▾</span>
      </button>
      {open && (
        <UserMenuPopover
          who={who}
          role={role}
          onClose={close}
          update={update}
        />
      )}
    </span>
  );
}

interface PopoverProps {
  who: NonNullable<
    ReturnType<typeof whoami> extends Promise<infer T> ? T : never
  >;
  role: string | undefined;
  onClose: () => void;
  update: (patch: Partial<ExploreState>) => void;
}

function UserMenuPopover({
  who,
  role,
  onClose,
  update: _update,
}: PopoverProps) {
  const client = useQueryClient();
  const navigate = useNavigate();
  const backdropRef = useRef<HTMLSpanElement>(null);

  // Close on backdrop click or Escape
  useEffect(() => {
    const handleKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", handleKey);
    return () => window.removeEventListener("keydown", handleKey);
  }, [onClose]);

  const handleBackdropClick = (e: React.MouseEvent) => {
    if (e.target === backdropRef.current) onClose();
  };

  const handleSignOut = async () => {
    try {
      await deleteSession();
      client.clear();
      navigate("/logs");
      window.location.reload();
    } catch {
      // If session delete fails, still reload to clear stale state
      window.location.reload();
    }
  };

  const handleThemeToggle = () => {
    const root = document.documentElement;
    const current = root.getAttribute("data-theme");
    const isDark =
      current === "dark" ||
      (!current && window.matchMedia("(prefers-color-scheme: dark)").matches);
    root.setAttribute("data-theme", isDark ? "light" : "dark");
    try {
      localStorage.setItem("signaldb-theme", isDark ? "light" : "dark");
    } catch {
      // localStorage unavailable
    }
  };

  const isDark =
    document.documentElement.getAttribute("data-theme") === "dark" ||
    (!document.documentElement.getAttribute("data-theme") &&
      window.matchMedia("(prefers-color-scheme: dark)").matches);

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
          <Link className="user-menu-item" to="/api-keys" onClick={onClose}>
            <span>API keys</span>
            <span className="user-menu-hint">{who.tenant.id}</span>
          </Link>
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
            onClick={handleSignOut}
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
