import { useMutation } from "@tanstack/react-query";
import { useState } from "react";
import { Link } from "react-router";
import { lookup, type LookupResult } from "./api";
import { definitionPath, kindLabel } from "./paths";
import { toErrorMessage } from "../../api/http";

/**
 * Global lookup box: resolves a name as attribute key, then entity, then
 * metric across every visible registry and lists the hits in precedence
 * order, the first marked primary.
 */
export function RegistryLookup() {
  const [name, setName] = useState("");
  const [asked, setAsked] = useState("");
  const resolve = useMutation({
    mutationFn: (value: string) => lookup(value),
  });

  const submit = (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const trimmed = name.trim();
    if (!trimmed) return;
    setAsked(trimmed);
    resolve.mutate(trimmed);
  };

  return (
    <section className="schema-lookup">
      <form onSubmit={submit} className="schema-lookup-form">
        <input
          aria-label="Lookup"
          placeholder="Look up an attribute key, entity, or metric…"
          value={name}
          onChange={(e) => setName(e.target.value)}
        />
        <button type="submit" disabled={resolve.isPending}>
          Resolve
        </button>
      </form>
      {resolve.isError && (
        <p className="schema-error">
          Could not complete the lookup: {toErrorMessage(resolve.error)}
        </p>
      )}
      {resolve.isSuccess && <LookupHits name={asked} result={resolve.data} />}
    </section>
  );
}

function LookupHits({ name, result }: { name: string; result: LookupResult }) {
  if (result === null) {
    return (
      <p className="schema-note">
        No definition of <code>{name}</code> in any visible registry.
      </p>
    );
  }
  return (
    <ul className="schema-hits" aria-label={`Definitions of ${name}`}>
      {result.hits.map((hit, index) => (
        <li
          key={`${hit.namespace}@${hit.version}`}
          className="schema-hit"
          aria-label={`${hit.namespace} hit`}
        >
          <span className="schema-hit-head">
            <Link
              to={definitionPath(hit.namespace, hit.version, result.kind, name)}
            >
              {hit.namespace}/{name}
            </Link>
            <span className="schema-hit-meta">
              {kindLabel(result.kind)} · {hit.version} · {hit.source}
            </span>
            {index === 0 && <span className="schema-badge">primary</span>}
          </span>
          <span className="schema-hit-brief">{hit.brief}</span>
        </li>
      ))}
    </ul>
  );
}
