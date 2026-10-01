import type { Meta, StoryObj } from "@storybook/react-vite";
import type { LabelFilter } from "../lib/filters";
import { FilterChips } from "./FilterChips";

const filters: LabelFilter[] = [
  { label: "service_name", op: "=", value: "checkout" },
  { label: "level", op: "=~", value: "error|warn" },
];

const labels = ["service_name", "level", "namespace", "pod"];

const meta = {
  title: "Components/FilterChips",
  component: FilterChips,
} satisfies Meta<typeof FilterChips>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Empty: Story = {
  args: { filters: [], labels, onChange: () => {} },
};

export const WithFilters: Story = {
  args: { filters, labels, onChange: () => {} },
};
