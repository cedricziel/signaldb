import { useEffect } from "react";
import type { Meta, StoryObj } from "@storybook/react-vite";
import { resetThrottleState, retryingFetch } from "../api/http";
import { ThrottleBanner } from "./ThrottleBanner";

/** Stubs `fetch` to always 429 with a `retry-after` just under the retry
 * policy's 10s per-attempt cap (a longer header makes `retryingFetch` fail
 * fast instead of waiting), then fires a `retryingFetch` that stays pending
 * well past the story's lifetime — enough to hold the banner's "pending"
 * state for a screenshot. */
function Pending() {
  useEffect(() => {
    const realFetch = globalThis.fetch;
    globalThis.fetch = (async () =>
      new Response("{}", {
        status: 429,
        headers: { "retry-after": "8" },
      })) as typeof fetch;
    resetThrottleState();
    void retryingFetch("/api/v1/query").catch(() => {});
    return () => {
      globalThis.fetch = realFetch;
      resetThrottleState();
    };
  }, []);
  return <ThrottleBanner />;
}

const meta = {
  title: "Components/ThrottleBanner",
  component: ThrottleBanner,
} satisfies Meta<typeof ThrottleBanner>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Hidden: Story = {};

export const Visible: Story = {
  render: () => <Pending />,
};
