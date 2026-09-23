import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Link } from "react-router";
import { deleteProcessor, listProcessors } from "./api";
import { PROCESSORS, editorPath } from "./paths";
import { useProcessorsSession } from "./useProcessorsSession";
import { ConfirmButton } from "../../components/ConfirmButton";
import { toErrorMessage } from "../../api/http";
import "./processors.css";

function formatUpdated(value: string | null | undefined): string {
  if (!value) return "—";
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString();
}

/**
 * `/processors` — every OTTL processor of the current tenant: name, signal,
 * dataset, enabled state, priority, compile status, and last update. Tenant
 * admins get create/edit/delete affordances; everyone else sees it
 * read-only (D6/D8, `explore-ui-processors` spec).
 */
export function ProcessorList() {
  const { isTenantAdmin, tenant } = useProcessorsSession();
  const queryClient = useQueryClient();
  const processors = useQuery({
    queryKey: ["processors", tenant],
    queryFn: listProcessors,
    staleTime: 30_000,
  });

  const remove = useMutation({
    mutationFn: deleteProcessor,
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["processors", tenant] });
    },
  });

  return (
    <div className="processors-page">
      <div className="processors-page-head">
        <div>
          <h1 className="processors-title">Processors</h1>
          <p className="processors-subtitle">
            OTTL statements applied to this tenant's telemetry at ingest, before
            anything is written.
          </p>
        </div>
        {isTenantAdmin && (
          <Link
            className="processors-button btn btn-primary"
            to={`${PROCESSORS}/new`}
          >
            New
          </Link>
        )}
      </div>

      {processors.isPending && <p className="processors-note">Loading…</p>}
      {processors.isError && (
        <p className="error-text" role="alert">
          Could not load processors: {toErrorMessage(processors.error)}
        </p>
      )}

      {processors.data && (
        <div className="table-scroll">
          <table className="processors-table">
            <thead>
              <tr>
                <th>Name</th>
                <th>Signal</th>
                <th>Dataset</th>
                <th>Enabled</th>
                <th className="num">Priority</th>
                <th>Status</th>
                <th>Updated</th>
                {isTenantAdmin && <th />}
              </tr>
            </thead>
            <tbody>
              {processors.data.map((p) => (
                <tr
                  key={p.name}
                  className={p.enabled ? undefined : "processors-row-disabled"}
                >
                  <td>
                    {isTenantAdmin ? (
                      <Link to={editorPath(p.name)}>{p.name}</Link>
                    ) : (
                      p.name
                    )}
                  </td>
                  <td>{p.signal}</td>
                  <td>{p.dataset ?? "all datasets"}</td>
                  <td>{p.enabled ? "yes" : "no"}</td>
                  <td className="num">{p.priority}</td>
                  <td>
                    <span
                      className={`processors-status processors-status-${p.status}`}
                    >
                      {p.status}
                    </span>
                  </td>
                  <td>{formatUpdated(p.updated_at)}</td>
                  {isTenantAdmin && (
                    <td className="processors-row-actions">
                      <ConfirmButton
                        label="Delete"
                        prompt={`Delete ${p.name}?`}
                        disabled={remove.isPending}
                        onConfirm={() => remove.mutate(p.name)}
                      />
                    </td>
                  )}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
