import {
  RANGE_PRESETS,
  formatRangeLabel,
  type TimeRange,
} from "../../lib/time";

interface Props {
  range: TimeRange;
  onChange: (range: TimeRange) => void;
}

export function TimeRangePicker({ range, onChange }: Props) {
  const currentSeconds = range.type === "relative" ? range.seconds : null;
  return (
    <label className="time-picker">
      <span className="visually-hidden">Time range</span>
      <select
        value={currentSeconds ?? "absolute"}
        onChange={(e) => {
          const v = e.target.value;
          if (v !== "absolute") {
            onChange({ type: "relative", seconds: Number(v) });
          }
        }}
      >
        {range.type === "absolute" && (
          <option value="absolute">{formatRangeLabel(range)}</option>
        )}
        {RANGE_PRESETS.map((p) => (
          <option key={p.seconds} value={p.seconds}>
            {p.label}
          </option>
        ))}
      </select>
    </label>
  );
}
