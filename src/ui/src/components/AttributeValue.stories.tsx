import type { Meta, StoryObj } from "@storybook/react-vite";
import { AttributeValue } from "./AttributeValue";

const meta = {
  title: "Components/AttributeValue",
  component: AttributeValue,
} satisfies Meta<typeof AttributeValue>;

export default meta;
type Story = StoryObj<typeof meta>;

export const PlainString: Story = {
  args: {
    value: "checkout",
    label: "value for service",
  },
};

export const JsonObject: Story = {
  args: {
    value: '{"service":"checkout","retries":2,"failed":false,"cause":null}',
    label: "value for attributes",
  },
};

export const LongPlainText: Story = {
  args: {
    value: "x".repeat(500),
    label: "value for arrow_schema",
  },
};
