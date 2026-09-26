// The header's "Setup 3/6" button and the checklist dialog it opens: each
// step adds coverage to the Overview itself.

import { Link } from "react-router";
import { Dialog } from "../../components/Dialog";
import type { SetupStep } from "./overviewModel";

function Progress({
  done,
  total,
  width,
}: {
  done: number;
  total: number;
  width: number | string;
}) {
  const pct = total > 0 ? (done / total) * 100 : 0;
  return (
    <span className="setup-progress" style={{ width }} aria-hidden="true">
      <span className="setup-progress-fill" style={{ width: `${pct}%` }} />
    </span>
  );
}

export function SetupButton({
  steps,
  onOpen,
}: {
  steps: SetupStep[];
  onOpen: () => void;
}) {
  const done = steps.filter((s) => s.done).length;
  return (
    <button
      type="button"
      className="btn overview-setup-btn"
      aria-haspopup="dialog"
      aria-label={`Setup checklist, ${done} of ${steps.length} done`}
      onClick={onOpen}
    >
      Setup
      <Progress done={done} total={steps.length} width={36} />
      <span className="overview-setup-count">
        {done}/{steps.length}
      </span>
    </button>
  );
}

export function SetupDialog({
  steps,
  onClose,
  linkSearch,
}: {
  steps: SetupStep[];
  onClose: () => void;
  /** Search string (tenant context) appended to the CTA routes. */
  linkSearch: string;
}) {
  const done = steps.filter((s) => s.done).length;
  const firstTodo = steps.find((s) => !s.done);
  return (
    <Dialog label="Setup checklist" onClose={onClose}>
      <div className="setup-dialog">
        <div className="setup-dialog-head">
          <div className="setup-dialog-title-row">
            <h2 className="setup-dialog-title">Finish setting up</h2>
            <span className="setup-dialog-count">
              {done} of {steps.length} done
            </span>
          </div>
          <p className="setup-dialog-copy">
            Each step adds coverage to this overview: more services on the map,
            more signals in ingest, more people who can act on it.
          </p>
          <Progress done={done} total={steps.length} width="100%" />
        </div>
        <ul className="setup-steps">
          {steps.map((s) => (
            <li key={s.id} className="setup-step">
              {s.done ? (
                <span
                  className="setup-status done"
                  role="img"
                  aria-label="done"
                >
                  ✓
                </span>
              ) : (
                <span
                  className="setup-status todo"
                  role="img"
                  aria-label="to do"
                />
              )}
              <span className="setup-step-text">
                <span className="setup-step-title">{s.title}</span>
                <span className="setup-step-detail">{s.detail}</span>
              </span>
              {!s.done && s.cta ? (
                <Link
                  className={`btn setup-step-cta${s === firstTodo ? " btn-primary" : ""}`}
                  to={`${s.cta.href}${linkSearch}`}
                  onClick={onClose}
                >
                  {s.cta.label}
                </Link>
              ) : (
                <span />
              )}
            </li>
          ))}
        </ul>
        <div className="setup-dialog-foot">
          <button type="button" className="btn btn-ghost" onClick={onClose}>
            Close
          </button>
        </div>
      </div>
    </Dialog>
  );
}
