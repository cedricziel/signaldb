import { QueryClientProvider, useQuery } from "@tanstack/react-query";
import type { Meta, StoryObj } from "@storybook/react-vite";
import { testQueryClient } from "../lib/queryClient";
import { RefreshButton } from "./RefreshButton";

function Idle() {
  return (
    <QueryClientProvider client={testQueryClient()}>
      <RefreshButton />
    </QueryClientProvider>
  );
}

function Loading() {
  return (
    <QueryClientProvider client={testQueryClient()}>
      <LoadingQuery />
      <RefreshButton />
    </QueryClientProvider>
  );
}

/** A range-scoped query (any key outside the button's non-range allowlist)
 * that never settles, so `RefreshButton` stays `aria-busy`. */
function LoadingQuery() {
  useQuery({ queryKey: ["data"], queryFn: () => new Promise(() => {}) });
  return null;
}

const meta = {
  title: "Components/RefreshButton",
} satisfies Meta;

export default meta;
type Story = StoryObj<typeof meta>;

export const IdleState: Story = {
  render: () => <Idle />,
};

export const LoadingState: Story = {
  render: () => <Loading />,
};
