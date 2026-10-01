import type { Meta, StoryObj } from "@storybook/react-vite";
import { EmptyState } from "./EmptyState";

const meta = {
  title: "Components/EmptyState",
  component: EmptyState,
} satisfies Meta<typeof EmptyState>;

export default meta;
type Story = StoryObj<typeof meta>;

export const TitleOnly: Story = {
  args: {
    title: "No log lines in this range",
  },
};

export const WithDetail: Story = {
  args: {
    title: "No profiles in this range",
    children: "Enable continuous profiling to start collecting them.",
  },
};
