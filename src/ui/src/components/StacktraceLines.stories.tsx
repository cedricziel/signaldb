import type { Meta, StoryObj } from "@storybook/react-vite";
import { StacktraceLines } from "./StacktraceLines";

const STACKTRACE =
  "PaymentError: card declined\n    at src/handler.rs:42:9\n    at <anonymous>\n    at node_modules/framework/run.js:10:1";

const meta = {
  title: "Components/StacktraceLines",
  component: StacktraceLines,
} satisfies Meta<typeof StacktraceLines>;

export default meta;
type Story = StoryObj<typeof meta>;

export const TraceVariant: Story = {
  args: {
    text: STACKTRACE,
    variant: "trace",
  },
};

export const ErrorVariant: Story = {
  args: {
    text: STACKTRACE,
    variant: "error",
  },
};
