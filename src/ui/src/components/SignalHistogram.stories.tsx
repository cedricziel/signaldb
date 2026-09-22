import type { Meta, StoryObj } from "@storybook/react-vite";
import { SignalHistogram, type VolumeSeries } from "./SignalHistogram";

const FROM_MS = Date.parse("2026-01-01T00:00:00Z");
const TO_MS = FROM_MS + 60 * 60 * 1000;
const STEP_MS = 5 * 60 * 1000;

function points(base: number, wave: number): [number, number][] {
  const out: [number, number][] = [];
  for (let t = FROM_MS, i = 0; t <= TO_MS; t += STEP_MS, i++) {
    out.push([t, Math.max(0, Math.round(base + Math.sin(i) * wave))]);
  }
  return out;
}

const series: VolumeSeries[] = [
  { key: "info", points: points(40, 10) },
  { key: "warn", points: points(6, 4) },
  { key: "error", points: points(1, 1) },
];

const order = ["info", "warn", "error"];
const colors: Record<string, string> = {
  info: "var(--info-bar, #4f8cff)",
  warn: "var(--warn-bar, #e0a72e)",
  error: "var(--err-bar, #e05252)",
};

const meta = {
  title: "Components/SignalHistogram",
  component: SignalHistogram,
} satisfies Meta<typeof SignalHistogram>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Populated: Story = {
  args: {
    series,
    order,
    colors,
    rangeMs: { fromMs: FROM_MS, toMs: TO_MS },
    stepMs: STEP_MS,
    scale: "linear",
    unit: "lines",
    label: "Log volume over time by level",
  },
};

export const LogScale: Story = {
  args: {
    ...Populated.args,
    scale: "log",
    onScaleChange: () => {},
  },
};

export const Empty: Story = {
  args: {
    series: [],
    order,
    colors,
    rangeMs: { fromMs: FROM_MS, toMs: TO_MS },
    stepMs: STEP_MS,
    scale: "linear",
    unit: "lines",
    label: "Log volume over time by level",
  },
};
