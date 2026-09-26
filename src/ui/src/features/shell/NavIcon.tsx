// 16×16 line glyphs for the app navigation, stroked in `currentColor` so
// the caller's `color` (dim for idle items, accent for the current one)
// decides the tint.

import type { ReactNode } from "react";

export type NavIconName =
  | "overview"
  | "errors"
  | "catalog"
  | "logs"
  | "traces"
  | "metrics"
  | "profiles"
  | "query"
  | "schema"
  | "processors"
  | "instrumentation"
  | "manage"
  | "search"
  | "collapse"
  | "updown"
  | "check"
  | "menu";

const round = { strokeLinecap: "round", strokeLinejoin: "round" } as const;

const GLYPHS: Record<NavIconName, ReactNode> = {
  overview: (
    <>
      <rect x="2" y="2" width="5" height="5" rx="1" />
      <rect x="9" y="2" width="5" height="5" rx="1" />
      <rect x="2" y="9" width="5" height="5" rx="1" />
      <rect x="9" y="9" width="5" height="5" rx="1" />
    </>
  ),
  errors: (
    <>
      <path d="M8 2.5 L14 13.5 H2 Z" strokeLinejoin="round" />
      <line x1="8" y1="6.5" x2="8" y2="9.5" />
      <line
        x1="8"
        y1="11.5"
        x2="8"
        y2="11.6"
        strokeLinecap="round"
        strokeWidth="2"
      />
    </>
  ),
  catalog: (
    <>
      {[4, 8, 12].map((y) => (
        <g key={y}>
          <line
            x1="2.5"
            y1={y}
            x2="3"
            y2={y}
            strokeLinecap="round"
            strokeWidth="2"
          />
          <line x1="6" y1={y} x2="14" y2={y} />
        </g>
      ))}
    </>
  ),
  logs: (
    <>
      <line x1="2" y1="4" x2="14" y2="4" />
      <line x1="2" y1="8" x2="10" y2="8" />
      <line x1="2" y1="12" x2="12.5" y2="12" />
    </>
  ),
  traces: (
    <>
      <rect x="2" y="2.5" width="7" height="2.5" rx=".5" />
      <rect x="5" y="6.75" width="8" height="2.5" rx=".5" />
      <rect x="7.5" y="11" width="6" height="2.5" rx=".5" />
    </>
  ),
  metrics: <polyline points="2,12 6,7 9,10 14,4" {...round} />,
  profiles: (
    <>
      <rect x="2" y="11" width="12" height="3" rx=".5" />
      <rect x="3.5" y="7" width="9" height="3" rx=".5" />
      <rect x="5" y="3" width="5" height="3" rx=".5" />
    </>
  ),
  query: (
    <>
      <polyline points="3,5 6,8 3,11" {...round} />
      <line x1="8" y1="11" x2="13" y2="11" strokeLinecap="round" />
    </>
  ),
  schema: (
    <>
      <rect x="2" y="2.5" width="5" height="4" rx=".75" />
      <rect x="9" y="9.5" width="5" height="4" rx=".75" />
      <path d="M4.5 6.5 V11.5 H9" />
    </>
  ),
  processors: (
    <path d="M2 3 H14 L9.5 8.5 V13 L6.5 11.5 V8.5 Z" strokeLinejoin="round" />
  ),
  instrumentation: (
    <>
      <circle cx="8" cy="8" r="5.5" />
      <circle cx="8" cy="8" r="2" />
    </>
  ),
  manage: (
    <>
      <line x1="2" y1="5" x2="14" y2="5" />
      <circle cx="10" cy="5" r="1.8" fill="var(--surface)" />
      <line x1="2" y1="11" x2="14" y2="11" />
      <circle cx="6" cy="11" r="1.8" fill="var(--surface)" />
    </>
  ),
  search: (
    <>
      <circle cx="7" cy="7" r="4.5" />
      <line x1="10.4" y1="10.4" x2="14" y2="14" strokeLinecap="round" />
    </>
  ),
  collapse: (
    <>
      <rect x="2" y="2.5" width="12" height="11" rx="1.5" />
      <line x1="6.5" y1="2.5" x2="6.5" y2="13.5" />
    </>
  ),
  updown: (
    <>
      <polyline points="5,6 8,3 11,6" {...round} />
      <polyline points="5,10 8,13 11,10" {...round} />
    </>
  ),
  check: <polyline points="3,8.5 6.5,12 13,4.5" {...round} />,
  menu: (
    <>
      <line x1="2" y1="4" x2="14" y2="4" strokeLinecap="round" />
      <line x1="2" y1="8" x2="14" y2="8" strokeLinecap="round" />
      <line x1="2" y1="12" x2="14" y2="12" strokeLinecap="round" />
    </>
  ),
};

export function NavIcon({
  name,
  size = 16,
}: {
  name: NavIconName;
  size?: number;
}) {
  return (
    <svg
      className="nav-icon"
      width={size}
      height={size}
      viewBox="0 0 16 16"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.4"
      aria-hidden="true"
    >
      {GLYPHS[name]}
    </svg>
  );
}

/** The signaldb pulse mark, shared by the sidebar brand row and the
 * mobile top bar. */
export function BrandPulse() {
  return (
    <svg
      width="18"
      height="14"
      viewBox="0 0 18 14"
      fill="none"
      aria-hidden="true"
    >
      <path
        d="M1 7 L4 7 L6 2 L9 12 L12 4 L13.5 7 L17 7"
        stroke="var(--accent)"
        strokeWidth="1.8"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}
