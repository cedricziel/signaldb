import { RANGE_PRESETS, formatRangeLabel, type TimeRange } from "../lib/time";
// `.time-picker` lives in the explore views' shared stylesheet; own the
// import rather than relying on a parent to have loaded it.
import "../features/explore/explore.css";

interface Props {
  range: TimeRange;
  onChange: (range: TimeRange) => void;
}

export function TimeRangePicker({ range, onChange }: Props) {
  const currentSeconds = range.type === "relative" ? range.seconds : null;
  const isKnownPreset = RANGE_PRESETS.some((p) => p.seconds === currentSeconds);
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
        {/* A relative range whose seconds match no preset (e.g. `?range=30m`,
            or a value only PromQL/the query bar can express) still needs a
            selected option — otherwise the select shows nothing chosen even
            though a range is active. */}
        {currentSeconds !== null && !isKnownPreset && (
          <option value={currentSeconds}>{formatRangeLabel(range)}</option>
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
