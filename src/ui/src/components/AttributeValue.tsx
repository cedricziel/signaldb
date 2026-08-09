import { useState, type ReactNode } from "react";

import { CopyValueButton } from "./CopyValueButton";

interface Props {
  value: string;
  label: string;
}

function parseJsonContainer(value: string): object | null {
  try {
    const parsed: unknown = JSON.parse(value);
    return parsed !== null && typeof parsed === "object" ? parsed : null;
  } catch {
    return null;
  }
}

function highlightedJson(value: string) {
  const tokens: ReactNode[] = [];
  const token = /"(?:\\.|[^"\\])*"(?=\s*:)|"(?:\\.|[^"\\])*"|-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?|\b(?:true|false|null)\b/g;
  let previous = 0;

  for (const match of value.matchAll(token)) {
    const text = match[0];
    const start = match.index ?? 0;
    if (start > previous) tokens.push(value.slice(previous, start));
    const className = value.slice(start + text.length).trimStart().startsWith(":")
      ? "json-key"
      : text.startsWith('"')
        ? "json-string"
        : text === "true" || text === "false"
          ? "json-boolean"
          : text === "null"
            ? "json-null"
            : "json-number";
    tokens.push(
      <span className={className} key={start}>
        {text}
      </span>,
    );
    previous = start + text.length;
  }

  if (previous < value.length) tokens.push(value.slice(previous));
  return tokens;
}

export function AttributeValue({ value, label }: Props) {
  const parsed = parseJsonContainer(value);
  const [expanded, setExpanded] = useState(false);

  if (!parsed) {
    return (
      <span className="copy-value">
        <span className="copy-value-text">{value}</span>
        <CopyValueButton value={value} label={label} />
      </span>
    );
  }

  const compact = JSON.stringify(parsed);
  const formatted = JSON.stringify(parsed, null, 2);
  return (
    <span className="attribute-value" data-expanded={expanded || undefined}>
      <span className="copy-value">
        {expanded ? (
          <pre className="attribute-value-json">
            <code>{highlightedJson(formatted)}</code>
          </pre>
        ) : (
          <span className="copy-value-text attribute-value-preview">{compact}</span>
        )}
        <button
          type="button"
          className="attribute-value-toggle"
          aria-label={`${expanded ? "Collapse" : "Expand"} JSON`}
          aria-expanded={expanded}
          onClick={() => setExpanded((current) => !current)}
        >
          {expanded ? "Hide" : "JSON"}
        </button>
        <CopyValueButton value={value} label={label} />
      </span>
    </span>
  );
}
