import { useEffect, useState } from "react";

interface Props {
  value: string;
  label: string;
  /** Visible text before and after copying; defaults to Copy / Copied. */
  text?: [idle: string, done: string];
  /** Replaces the compact inline look, e.g. `btn` for a toolbar button. */
  className?: string;
}

export function CopyValueButton({
  value,
  label,
  text = ["Copy", "Copied"],
  className,
}: Props) {
  const [copied, setCopied] = useState(false);

  useEffect(() => {
    if (!copied) return;
    const timeout = window.setTimeout(() => setCopied(false), 1_500);
    return () => window.clearTimeout(timeout);
  }, [copied]);

  async function copyValue() {
    if (!navigator.clipboard) return;
    try {
      await navigator.clipboard.writeText(value);
      setCopied(true);
    } catch {
      setCopied(false);
    }
  }

  return (
    <button
      type="button"
      className={className ?? "copy-value-button"}
      data-copied={copied || undefined}
      aria-label={`${copied ? text[1] : text[0]} ${label}`}
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
      <span>{copied ? text[1] : text[0]}</span>
    </button>
  );
}
