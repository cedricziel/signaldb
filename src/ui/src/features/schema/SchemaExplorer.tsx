import { useQuery } from "@tanstack/react-query";
import { Navigate } from "react-router";
import { getSchema, type ManagedSchema } from "../../api/management";
import { whoami } from "../../api/session";
import "./SchemaExplorer.css";

type LogicalField = ManagedSchema["logical"][number];
type PhysicalSchema = ManagedSchema["physical"][number];

function groupBySource<T extends { source: string }>(
  items: T[],
): [string, T[]][] {
  const groups = new Map<string, T[]>();
  for (const item of items) {
    const list = groups.get(item.source);
    if (list) list.push(item);
    else groups.set(item.source, [item]);
  }
  return [...groups.entries()].sort(([a], [b]) => a.localeCompare(b));
}

/** The dotted, query-facing name: level prefix (if any) plus the field's own name. */
function qualifiedName(field: LogicalField): string {
  return field.level ? `${field.level}.${field.name}` : field.name;
}

export function SchemaExplorer() {
  const { data: who, isLoading: whoLoading } = useQuery({
    queryKey: ["whoami"],
    queryFn: () => whoami(),
    staleTime: 60_000,
    retry: false,
  });

  const schema = useQuery({
    queryKey: ["schema"],
    queryFn: () => getSchema(),
    enabled: !!who?.user?.is_instance_admin,
    staleTime: 60_000,
  });

  if (whoLoading) return null;
  if (!who?.user?.is_instance_admin) {
    return <Navigate to="/logs" replace />;
  }

  return (
    <div className="schema-explorer-page">
      <h2 className="schema-explorer-title">Storage schema</h2>
      <p className="schema-explorer-subtitle">
        The registered logical (query-facing) field model and the resolved
        physical (storage) schema for every signal source.
        {schema.data && (
          <>
            {" "}
            Logical schema version{" "}
            <code>{schema.data.logical_schema_version}</code>.
          </>
        )}
      </p>

      {schema.isPending && <p className="schema-explorer-note">Loading…</p>}
      {schema.isError && (
        <p className="schema-explorer-note schema-explorer-error">
          Failed to load schema:{" "}
          {schema.error instanceof Error
            ? schema.error.message
            : String(schema.error)}
        </p>
      )}

      {schema.data && (
        <>
          <section className="schema-explorer-section">
            <h2>Logical schema</h2>
            <p className="schema-explorer-section-note">
              The OTel-native field model queries address. A field with no level
              is a plain record field or a SignalDB-defined join key/identity; a
              leveled field is an attribute reached by resolving its{" "}
              <code>resource.</code>/<code>scope.</code>/<code>record.</code>{" "}
              prefix (or unprefixed, by priority).
            </p>
            {groupBySource(schema.data.logical).map(([source, fields]) => (
              <details key={source} className="schema-source-card" open>
                <summary>
                  {source}{" "}
                  <span className="schema-source-count">
                    {fields.length} field{fields.length === 1 ? "" : "s"}
                  </span>
                </summary>
                <div className="table-scroll">
                  <table className="schema-explorer-table">
                    <thead>
                      <tr>
                        <th>Field</th>
                        <th>Level</th>
                        <th>Type</th>
                        <th>Kind</th>
                        <th>Filterable</th>
                      </tr>
                    </thead>
                    <tbody>
                      {[...fields]
                        .sort((a, b) =>
                          qualifiedName(a).localeCompare(qualifiedName(b)),
                        )
                        .map((field) => (
                          <tr key={`${field.level ?? ""}.${field.name}`}>
                            <td>
                              <code>{qualifiedName(field)}</code>
                              {field.non_native && (
                                <span
                                  className="schema-explorer-badge"
                                  title="Not addressable via the raw OTel name — a SignalDB-defined field"
                                >
                                  signaldb
                                </span>
                              )}
                            </td>
                            <td className="schema-dim">
                              {field.level ?? "—"}
                            </td>
                            <td className="schema-dim">
                              {field.value_type}
                            </td>
                            <td className="schema-dim">{field.kind}</td>
                            <td className="schema-dim">
                              {field.filterability === "filterable"
                                ? "yes"
                                : "retrieval only"}
                            </td>
                          </tr>
                        ))}
                    </tbody>
                  </table>
                </div>
              </details>
            ))}
          </section>

          <section className="schema-explorer-section">
            <h2>Physical schema</h2>
            <p className="schema-explorer-section-note">
              The resolved storage columns per table version. The current
              version is what ingestion writes and queries read by default;
              older versions stay readable for tables that haven't compacted
              onto the current layout yet.
            </p>
            {groupBySource(schema.data.physical).map(([source, versions]) => (
              <div key={source} className="schema-source-block">
                <h3>{source}</h3>
                {[...versions]
                  .sort((a, b) => a.version.localeCompare(b.version))
                  .map((version) => (
                    <PhysicalVersion key={version.version} schema={version} />
                  ))}
              </div>
            ))}
          </section>
        </>
      )}
    </div>
  );
}

function PhysicalVersion({ schema }: { schema: PhysicalSchema }) {
  return (
    <details className="schema-source-card" open={schema.is_current}>
      <summary>
        {schema.version}
        {schema.is_current && (
          <span className="schema-explorer-badge">current</span>
        )}{" "}
        <span className="schema-source-count">
          {schema.fields.length} field{schema.fields.length === 1 ? "" : "s"}
        </span>
      </summary>
      <p className="schema-explorer-section-note">{schema.description}</p>
      {schema.partition_by.length > 0 && (
        <p className="schema-explorer-section-note">
          Partitioned by <code>{schema.partition_by.join(", ")}</code>
        </p>
      )}
      <div className="table-scroll">
        <table className="schema-explorer-table">
          <thead>
            <tr>
              <th>Column</th>
              <th>Type</th>
              <th>Required</th>
              <th>Computed</th>
              <th>Physical only</th>
            </tr>
          </thead>
          <tbody>
            {schema.fields.map((field) => (
              <tr key={field.name}>
                <td>
                  <code>{field.name}</code>
                </td>
                <td className="schema-dim">{field.field_type}</td>
                <td className="schema-dim">{field.required ? "yes" : "no"}</td>
                <td className="schema-dim">{field.computed ?? "—"}</td>
                <td className="schema-dim">
                  {field.physical_only ? "yes" : "no"}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </details>
  );
}
