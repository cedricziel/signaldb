import { describe, expect, it } from "vitest";
import {
  compactCount,
  formatRange,
  formatTimestamp,
  formatValue,
} from "./vizFormat";

describe("compactCount", () => {
  // Intl's `notation: "compact"` is locale-dependent — in `de` it does not
  // compact at all — so the axis uses an explicit, locale-stable formatter.
  it("compacts deterministically regardless of locale", () => {
    expect(compactCount(0)).toBe("0");
    expect(compactCount(610)).toBe("610");
    expect(compactCount(999)).toBe("999");
    expect(compactCount(1000)).toBe("1K");
    expect(compactCount(1500)).toBe("1.5K");
    expect(compactCount(9949)).toBe("9.9K");
    expect(compactCount(186_665)).toBe("187K");
    expect(compactCount(373_329)).toBe("373K");
    expect(compactCount(1_500_000)).toBe("1.5M");
    expect(compactCount(2_400_000_000)).toBe("2.4B");
  });
});

describe("formatTimestamp", () => {
  // 2024-03-05 14:07:09.250 local time.
  const ms = new Date(2024, 2, 5, 14, 7, 9, 250).getTime();

  it("shows date and time to the minute for coarse resolutions", () => {
    expect(formatTimestamp(ms, 60_000)).toBe("2024-03-05 14:07");
    expect(formatTimestamp(ms, 3_600_000)).toBe("2024-03-05 14:07");
  });

  it("adds seconds below a minute of resolution", () => {
    expect(formatTimestamp(ms, 15_000)).toBe("2024-03-05 14:07:09");
  });

  it("adds milliseconds below a second of resolution", () => {
    expect(formatTimestamp(ms, 100)).toBe("2024-03-05 14:07:09.250");
  });
});

describe("formatValue", () => {
  it("formats integers with grouping and the unit", () => {
    expect(formatValue(373_329, "lines")).toBe("373,329 lines");
    expect(formatValue(12)).toBe("12");
  });

  it("keeps a few significant decimals for fractional values", () => {
    expect(formatValue(0.12345, "s")).toBe("0.123 s");
    expect(formatValue(1234.5678)).toBe("1,234.57");
  });

  it("renders a dash for missing values", () => {
    expect(formatValue(null)).toBe("–");
    expect(formatValue(Number.NaN, "x")).toBe("–");
  });
});

describe("formatRange", () => {
  it("joins both ends with an en dash and the unit once each", () => {
    expect(formatRange(1, 2, "ms")).toBe("1 ms – 2 ms");
  });

  it("marks an open upper bound with a plus", () => {
    expect(formatRange(100, undefined, "ms")).toBe("100 ms+");
  });
});
