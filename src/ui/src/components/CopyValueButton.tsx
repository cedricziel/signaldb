import { useEffect, useState } from "react";

interface Props {
  value: string;
  label: string;
}

export function CopyValueButton({ value, label }: Props) {
  const [copied, setCopied] = useState(false);

  useEffect(() => {
    if (!copied) return;
    const timeout = window.setTimeout(() => setCopied(false), 1_500);
    return () => window.clearTimeout(timeout);
  }, [copied]);

  async function copyValue() {
    if (!navigator.clipboard) return;
    await navigator.clipboard.writeText(value);
    setCopied(true);
  }

  return (
    <button
      type="button"
      className="copy-value-button"
      data-copied={copied || undefined}
      aria-label={`${copied ? "Copied" : "Copy"} ${label}`}
      onClick={() => void copyValue()}
    >
      {copied ? (
        <svg className="copy-value-icon" viewBox="0 0 16 16" aria-hidden="true">
          <path d="m3 8 3 3 7-7" />
        </svg>
      ) : (
        <svg className="copy-value-icon" viewBox="0 0 16 16" aria-hidden="true">
          <rect x="5" y="5" width="8" height="8" rx="1" />
          <path d="M11 5V3H3v8h2" />
        </svg>
      )}
      <span>{copied ? "Copied" : "Copy"}</span>
    </button>
  );
}
