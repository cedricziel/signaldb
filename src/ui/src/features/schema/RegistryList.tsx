import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router";
import { listRegistries } from "./api";
import { CONVENTIONS, editorPath, registryPath } from "./paths";
import { RegistryLookup } from "./RegistryLookup";
import { useSchemaSession } from "./useSchemaSession";

function formatUpdated(value: string | null | undefined): string {
  if (!value) return "—";
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString();
}

/**
 * Conventions tab: every registry visible to the tenant in precedence order,
 * a global lookup box, and (for tenant admins) the entry points to create a
 * custom registry.
 */
export function RegistryList() {
  const { isTenantAdmin } = useSchemaSession();
  const registries = useQuery({
    queryKey: ["schema-registries"],
    queryFn: listRegistries,
    staleTime: 60_000,
  });

  return (
    <div className="schema-page">
      <div className="schema-page-head">
        <div>
          <h1 className="schema-title">Conventions</h1>
          <p className="schema-subtitle">
            Semantic-convention registries visible to this tenant. Custom
            registries take precedence over the bundled ones.
          </p>
        </div>
        {isTenantAdmin && (
          <div className="schema-actions">
            <Link className="schema-button" to={`${CONVENTIONS}/new?upload=1`}>
              Upload registry
            </Link>
            <Link className="schema-button primary" to={`${CONVENTIONS}/new`}>
              New
            </Link>
          </div>
        )}
      </div>

      <RegistryLookup />

      {registries.isPending && <p className="schema-note">Loading…</p>}
      {registries.isError && (
        <p className="schema-error">
          Failed to load registries:{" "}
          {registries.error instanceof Error
            ? registries.error.message
            : String(registries.error)}
        </p>
      )}

      {registries.data && (
        <>
          <p className="schema-precedence">
            Precedence: {registries.data.map((r) => r.namespace).join(" → ")}
          </p>
          <div className="schema-table-scroll">
            <table className="schema-table">
              <thead>
                <tr>
                  <th>Namespace</th>
                  <th>Version</th>
                  <th>Source</th>
                  <th className="num">Attributes</th>
                  <th className="num">Entities</th>
                  <th className="num">Metrics</th>
                  <th>Updated</th>
                  {isTenantAdmin && <th />}
                </tr>
              </thead>
              <tbody>
                {registries.data.map((r) => (
                  <tr key={`${r.namespace}@${r.version}`}>
                    <td>
                      <Link to={registryPath(r.namespace, r.version)}>
                        {r.namespace}
                      </Link>
                      {r.read_only && (
                        <span
                          className="schema-readonly"
                          role="img"
                          aria-label="read-only"
                          title="Read-only (bundled)"
                        >
                          🔒
                        </span>
                      )}
                    </td>
                    <td>
                      <code>{r.version}</code>
                    </td>
                    <td>
                      <span
                        className={`schema-source schema-source-${r.source}`}
                      >
                        {r.source}
                      </span>
                    </td>
                    <td className="num">{r.attribute_count}</td>
                    <td className="num">{r.entity_count}</td>
                    <td className="num">{r.metric_count}</td>
                    <td>{formatUpdated(r.updated_at)}</td>
                    {isTenantAdmin && (
                      <td className="schema-row-actions">
                        {!r.read_only && (
                          <Link to={editorPath(r.namespace, r.version)}>
                            Edit
                          </Link>
                        )}
                      </td>
                    )}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}
    </div>
  );
}
