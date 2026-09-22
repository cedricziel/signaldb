import { useState } from "react";
import type { Meta, StoryObj } from "@storybook/react-vite";
import { AttributeKeyInput } from "./AttributeKeyInput";

const meta = {
  title: "Components/AttributeKeyInput",
  component: AttributeKeyInput,
} satisfies Meta<typeof AttributeKeyInput>;

export default meta;

function Controlled(props: {
  initial?: string;
  observed: string[];
  placeholder?: string;
}) {
  const [value, setValue] = useState(props.initial ?? "");
  return (
    <AttributeKeyInput
      value={value}
      onChange={setValue}
      onPick={setValue}
      observed={props.observed}
      ariaLabel="Attribute key"
      placeholder={props.placeholder}
    />
  );
}

export const Empty: StoryObj = {
  render: () => <Controlled observed={["level", "line"]} />,
};

export const WithPlaceholder: StoryObj = {
  render: () => (
    <Controlled observed={["level", "line"]} placeholder="Filter by key…" />
  ),
};

export const Prefilled: StoryObj = {
  render: () => <Controlled initial="level" observed={["level", "line"]} />,
};
