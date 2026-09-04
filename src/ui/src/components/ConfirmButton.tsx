import { useEffect, useRef, useState } from "react";
import { useEscapeKey } from "../hooks/useEscapeKey";

interface Props {
  /** The initial button's text, e.g. "Delete". */
  label: string;
  /** Shown alongside Confirm/Cancel once clicked, e.g. "Delete dataset staging?". */
  prompt: string;
  onConfirm: () => void;
  disabled?: boolean;
  /** Applied to the initial button. */
  className?: string;
}

/**
 * A button that requires a second click to act: click swaps it in place for
 * an inline "<prompt> Confirm Cancel" — used for every destructive
 * management action (delete dataset, revoke key, remove member).
 */
export function ConfirmButton({
  label,
  prompt,
  onConfirm,
  disabled,
  className,
}: Props) {
  const [confirming, setConfirming] = useState(false);
  const buttonRef = useRef<HTMLButtonElement>(null);
  const confirmRef = useRef<HTMLButtonElement>(null);
  const armedOnce = useRef(false);

  // Escape anywhere backs out of the confirmation and nothing else: the
  // exclusive listener claims the key before a surrounding Dialog can
  // treat the same press as "close", wherever focus happens to be.
  useEscapeKey(confirming, () => setConfirming(false), { exclusive: true });

  // Swapping the button for the prompt would otherwise drop keyboard focus
  // on the floor: move it onto Confirm, and back onto the button after a
  // cancel (after a confirm the row usually unmounts, which is a no-op).
  useEffect(() => {
    if (confirming) {
      armedOnce.current = true;
      confirmRef.current?.focus();
    } else if (armedOnce.current) {
      buttonRef.current?.focus();
    }
  }, [confirming]);

  if (!confirming) {
    return (
      <button
        ref={buttonRef}
        type="button"
        className={className}
        disabled={disabled}
        onClick={() => setConfirming(true)}
      >
        {label}
      </button>
    );
  }

  return (
    <span className="confirm-inline">
      {prompt}{" "}
      <button
        ref={confirmRef}
        type="button"
        disabled={disabled}
        onClick={() => {
          setConfirming(false);
          onConfirm();
        }}
      >
        Confirm
      </button>
      <button type="button" onClick={() => setConfirming(false)}>
        Cancel
      </button>
    </span>
  );
}
