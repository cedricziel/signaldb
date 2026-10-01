import type { Meta, StoryObj } from "@storybook/react-vite";
import { QueryClientProvider } from "@tanstack/react-query";
import { within, userEvent } from "storybook/test";
import { testQueryClient } from "../lib/queryClient";
import { sourceContextAvailabilityKey } from "../lib/useSourceContextEnabled";
import { SourceSnippet } from "./SourceSnippet";

const TENANT = "acme";
const PATH = "src/handler.rs";
const LINE = 42;

// The component gates itself on the availability probe and fetches the
// snippet lazily on click — both via react-query. Seeding the cache under
// the exact keys the component's own useQuery calls use (see
// SourceSnippet.tsx and useSourceContextEnabled.ts) shows the populated
// panel without a network call, which Storybook has no backend for.
function seededClient() {
  const queryClient = testQueryClient();
  queryClient.setQueryData(sourceContextAvailabilityKey(TENANT), {
    configured: true,
    linked: true,
  });
  queryClient.setQueryData(["source-context", TENANT, "", "", PATH, LINE], {
    status: "available",
    snippet: {
      repository: "acme/api",
      ref: "main",
      path: PATH,
      line: LINE,
      start_line: 41,
      lines: ["fn handler() {", "    do_thing();", "}"],
      html_url: "https://github.com/acme/api/blob/main/src/handler.rs#L42",
      sha: "deadbeef",
    },
  });
  return queryClient;
}

const meta = {
  title: "Components/SourceSnippet",
  component: SourceSnippet,
} satisfies Meta<typeof SourceSnippet>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Available: Story = {
  args: {
    tenant: TENANT,
    path: PATH,
    line: LINE,
  },
  decorators: [
    (Story) => (
      <QueryClientProvider client={seededClient()}>
        <Story />
      </QueryClientProvider>
    ),
  ],
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement);
    await userEvent.click(
      await canvas.findByRole("button", { name: "View source" }),
    );
  },
};

export const Unavailable: Story = {
  args: {
    tenant: TENANT,
    path: PATH,
    line: LINE,
  },
};
