// A single headline metric — label, big value, optional change figure and
// detail line, optional sparkline slot. Used on the catalog entity overview
// and anywhere else a row of key numbers needs the same look.
import type { ReactNode } from "react";
import "./KpiCard.css";

export interface KpiChange {
  text: string;
  direction: "up" | "down" | "flat";
  /** Up isn't always good (an error rate, say) — the caller decides the tone. */
  tone: "good" | "bad" | "neutral";
}

export interface KpiCardProps {
  label: string;
  value: string;
  unit?: string;
  valueTone?: "neutral" | "error";
  change?: KpiChange;
  detail?: string;
  /** A `Sparkline` or other small chart, rendered below the value. */
  children?: ReactNode;
}

export function KpiCard({
  label,
  value,
  unit,
  valueTone = "neutral",
  change,
  detail,
  children,
}: KpiCardProps) {
  return (
    <div className="kpi-card">
      <div className="kpi-label">{label}</div>
      <div className="kpi-value-row">
        <span className={`kpi-value kpi-value-${valueTone}`}>{value}</span>
        {unit && <span className="kpi-unit">{unit}</span>}
      </div>
      {change && (
        <div
          className={`kpi-change kpi-change-${change.tone} kpi-change-${change.direction}`}
        >
          {change.text}
        </div>
      )}
      {detail && <div className="kpi-detail">{detail}</div>}
      {children && <div className="kpi-chart">{children}</div>}
    </div>
  );
}

export function KpiStrip({ children }: { children: ReactNode }) {
  return <div className="kpi-strip">{children}</div>;
}
