import { useEffect, useRef, type ReactNode } from "react";

const FOCUSABLE_SELECTOR =
  'a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])';

interface Props {
  /** Accessible name, exposed as `aria-label` on the dialog. */
  label: string;
  /** When given: Escape and a backdrop click close the dialog. When absent
   * (the login gate, consent) the dialog isn't dismissible. */
  onClose?: () => void;
  /** Extra class on the panel for per-dialog sizing. */
  className?: string;
  /** `system` stacks above every content dialog: the sign-in gate, which
   * must cover whatever the user had open when their session expired. */
  layer?: "content" | "system";
  children: ReactNode;
}

/**
 * The one modal shell for SignalDB: a backdrop plus a focus-trapped,
 * `role="dialog"` panel. On mount it remembers the previously focused
 * element and moves focus into the panel; on unmount it restores focus.
 */
export function Dialog({
  label,
  onClose,
  className,
  layer = "content",
  children,
}: Props) {
  const panelRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const previouslyFocused = document.activeElement as HTMLElement | null;
    const panel = panelRef.current;
    const first = panel?.querySelector<HTMLElement>(FOCUSABLE_SELECTOR);
    (first ?? panel)?.focus();
    return () => {
      if (previouslyFocused && document.contains(previouslyFocused)) {
        previouslyFocused.focus();
      }
    };
  }, []);

  const handleKeyDown = (event: React.KeyboardEvent<HTMLDivElement>) => {
    if (event.key === "Escape") {
      onClose?.();
      return;
    }
    if (event.key !== "Tab") return;
    const focusables = Array.from(
      panelRef.current?.querySelectorAll<HTMLElement>(FOCUSABLE_SELECTOR) ?? [],
    );
    const first = focusables[0];
    const last = focusables[focusables.length - 1];
    // Nothing focusable (a message-only dialog): Tab must not walk out of
    // the modal, so keep focus on the panel itself.
    if (!first || !last) {
      event.preventDefault();
      panelRef.current?.focus();
      return;
    }
    if (event.shiftKey && document.activeElement === first) {
      event.preventDefault();
      last.focus();
    } else if (!event.shiftKey && document.activeElement === last) {
      event.preventDefault();
      first.focus();
    }
  };

  return (
    <div
      className={
        layer === "system"
          ? "dialog-backdrop dialog-backdrop--system"
          : "dialog-backdrop"
      }
      onMouseDown={(event) => {
        if (event.target === event.currentTarget) onClose?.();
      }}
    >
      <div
        ref={panelRef}
        className={className ? `dialog-panel ${className}` : "dialog-panel"}
        role="dialog"
        aria-modal="true"
        aria-label={label}
        tabIndex={-1}
        onKeyDown={handleKeyDown}
      >
        {children}
      </div>
    </div>
  );
}
