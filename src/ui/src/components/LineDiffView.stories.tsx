import type { Meta, StoryObj } from "@storybook/react-vite";
import { diffLines } from "../features/processors/lineDiff";
import { LineDiffView } from "./LineDiffView";

const before = `{
  "user": {
    "email": "alice@example.com",
    "plan": "trial"
  }
}`;

const after = `{
  "user": {
    "email": "[redacted]",
    "plan": "trial"
  }
}`;

const meta = {
  title: "Components/LineDiffView",
  component: LineDiffView,
} satisfies Meta<typeof LineDiffView>;

export default meta;
type Story = StoryObj<typeof meta>;

export const RedactedField: Story = {
  args: { diff: diffLines(before, after) },
};
