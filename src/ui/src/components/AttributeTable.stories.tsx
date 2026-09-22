import type { Meta, StoryObj } from "@storybook/react-vite";
import type { AttributeHit } from "../api/gen";
import { semanticsFromResolution, type SemanticsMap } from "../lib/semantics";
import {
  AttributeSection,
  AttributeSummary,
  AttributeTable,
  DescriptionsToggle,
} from "./AttributeTable";

const hit = (over: Partial<AttributeHit> = {}): AttributeHit => ({
  key: "k8s.pod.uid",
  brief: "The UID of the Pod.",
  type: "string",
  group_id: "registry.k8s.pod",
  group_display_name: "Kubernetes Attributes",
  namespace: "otel",
  version: "1.43.0",
  source: "bundled",
  stability: "development",
  ...over,
});

function semanticsOf(entries: [string, AttributeHit[]][]): SemanticsMap {
  const map = new Map();
  for (const [key, hits] of entries) {
    const sem = semanticsFromResolution({ key, hits, primary: hits[0] });
    if (sem) map.set(key, sem);
  }
  return map;
}

const semantics = semanticsOf([
  [
    "k8s.pod.uid",
    [
      hit({
        entity_roles: [
          { namespace: "otel", entity: "k8s.pod", role: "identifying" },
        ],
      }),
    ],
  ],
  [
    "k8s.pod.name",
    [
      hit({
        key: "k8s.pod.name",
        entity_roles: [
          { namespace: "otel", entity: "k8s.pod", role: "identifying" },
        ],
      }),
    ],
  ],
]);

const entries: [string, string][] = [
  ["k8s.pod.name", "web-1"],
  ["k8s.pod.uid", "275ecb36"],
  ["service.name", "checkout"],
];

const meta = {
  title: "Components/AttributeTable",
  component: AttributeTable,
} satisfies Meta<typeof AttributeTable>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Grid: Story = {
  args: {
    entries,
    semantics,
    layout: "grid",
    showDescriptions: false,
  },
};

export const Stacked: Story = {
  args: {
    entries,
    semantics,
    layout: "stacked",
    showDescriptions: false,
  },
};

export const WithDescriptions: Story = {
  args: {
    entries,
    semantics,
    layout: "grid",
    showDescriptions: true,
  },
};

export const WithActions: Story = {
  args: {
    entries,
    semantics,
    layout: "grid",
    showDescriptions: false,
    actions: (key, value) => [
      {
        label: "+ filter",
        ariaLabel: `Filter for ${key} = ${value}`,
        onClick: () => {},
      },
    ],
  },
};

export const Summary: StoryObj = {
  render: () => (
    <AttributeSummary
      summary={{
        parts: [
          { key: "service.name", value: "checkout" },
          { key: "level", value: "error" },
        ],
        more: 3,
      }}
    />
  ),
};

export const Toggle: StoryObj = {
  render: () => <DescriptionsToggle checked onToggle={() => {}} />,
};

export const Section: StoryObj = {
  render: () => (
    <AttributeSection title="Resource" count={12} expanded onToggle={() => {}}>
      <DescriptionsToggle checked={false} onToggle={() => {}} />
    </AttributeSection>
  ),
};
