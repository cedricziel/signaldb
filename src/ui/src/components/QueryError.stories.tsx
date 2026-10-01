import type { Meta, StoryObj } from "@storybook/react-vite";
import { QueryError } from "./QueryError";

const meta = {
  title: "Components/QueryError",
  component: QueryError,
} satisfies Meta<typeof QueryError>;

export default meta;
type Story = StoryObj<typeof meta>;

export const FromError: Story = {
  args: {
    what: "logs",
    error: new Error("boom"),
  },
};

export const FromNonErrorValue: Story = {
  args: {
    what: "traces",
    error: "plain string",
  },
};
