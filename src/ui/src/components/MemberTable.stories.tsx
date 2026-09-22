import type { Meta, StoryObj } from "@storybook/react-vite";
import type { TraceGroupMember } from "../api/traceGroupMembers";
import { MemberTable } from "./MemberTable";

const START_MS = Date.parse("2026-01-01T00:00:00Z");

function member(
  traceId: string,
  spanName: string,
  serviceName: string,
  statusCode: string,
  offsetMs: number,
  durationMs: number,
): TraceGroupMember {
  return {
    traceId,
    spanId: `${traceId}-span`,
    parentSpanId: null,
    spanName,
    serviceName,
    statusCode,
    startNs: BigInt((START_MS + offsetMs) * 1_000_000).toString(),
    durationNanos: BigInt(durationMs * 1_000_000).toString(),
  };
}

const members: TraceGroupMember[] = [
  member("t1", "GET /checkout", "checkout", "Ok", 0, 42),
  member("t2", "POST /cart", "cart", "Error", 1000, 210),
  member("t3", "GET /inventory", "inventory", "Unset", 2000, 15),
];

const meta = {
  title: "Components/MemberTable",
  component: MemberTable,
} satisfies Meta<typeof MemberTable>;

export default meta;
type Story = StoryObj<typeof meta>;

const base = {
  what: "traces",
  identityLabel: "Root",
  emptyMessage: "No traces matched.",
  footnote: "Showing up to 500 traces, newest first.",
  onOpenTrace: () => {},
};

export const Populated: Story = {
  args: { ...base, members, error: null },
};

export const Loading: Story = {
  args: { ...base, members: undefined, error: null },
};

export const ErrorState: Story = {
  args: { ...base, members: undefined, error: new Error("query timed out") },
};

export const Empty: Story = {
  args: { ...base, members: [], error: null },
};
