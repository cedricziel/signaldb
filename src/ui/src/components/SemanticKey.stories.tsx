import type { Meta, StoryObj } from "@storybook/react-vite";
import type { AttributeHit } from "../api/gen";
import { semanticsFromResolution } from "../lib/semantics";
import { SemanticInfo, SemanticKeyLabel } from "./SemanticKey";

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
  examples: ["275ecb36-5aa8-4c2a-9c47-d8bb681b9aff"],
  ...over,
});

const semOf = (hits: AttributeHit[]) =>
  semanticsFromResolution({ key: hits[0]!.key, hits, primary: hits[0] });

const meta = {
  title: "Components/SemanticKey",
  component: SemanticKeyLabel,
} satisfies Meta<typeof SemanticKeyLabel>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Resolved: Story = {
  args: {
    name: "k8s.pod.uid",
    semantics: semOf([hit()]),
  },
};

export const Unresolved: Story = {
  args: {
    name: "app.order.id",
    semantics: undefined,
  },
};

export const Deprecated: Story = {
  args: {
    name: "http.status_code",
    semantics: semOf([
      hit({
        key: "http.status_code",
        deprecated: { renamed_to: "http.response.status_code" },
      }),
    ]),
  },
};

export const InfoGlyph: StoryObj = {
  render: () => <SemanticInfo name="k8s.pod.uid" semantics={semOf([hit()])} />,
};
