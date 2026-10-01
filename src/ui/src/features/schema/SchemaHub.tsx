import { NavLink, Outlet, useLocation } from "react-router";
import { useOutletState } from "../../lib/outletState";
import { useSchemaSession } from "./useSchemaSession";
import "./schema.css";

/**
 * `/schema` — the schema hub. Two tabs backed by real routes: Conventions
 * (semantic-convention registries, every tenant user) and Storage (the
 * logical/physical storage schema explorer, instance admins only). Child
 * routes render into the outlet, so the browser back button walks views.
 */
export function SchemaHub() {
  // Forwards the shell's outlet context (tenant/dataset) to every schema
  // page below — an `<Outlet/>` that doesn't repeat `context` resets it to
  // `undefined` for descendants, which would leave those pages unable to
  // react to a tenant switch.
  const shell = useOutletState();
  const { isInstanceAdmin } = useSchemaSession();
  const { pathname } = useLocation();
  const tab = (to: string, label: string) => {
    const active = pathname.startsWith(to);
    return (
      <NavLink
        to={to}
        role="tab"
        className={active ? "active" : ""}
        aria-selected={active}
      >
        {label}
      </NavLink>
    );
  };
  return (
    <div className="schema-hub">
      <div className="schema-hub-tabs" role="tablist" aria-label="Schema">
        {tab("/schema/conventions", "Conventions")}
        {isInstanceAdmin && tab("/schema/storage", "Storage")}
      </div>
      <Outlet context={shell} />
    </div>
  );
}
