import type { Meta, StoryObj } from "@storybook/react-vite";
import { userEvent, within } from "storybook/test";
import { StoryFetchStub } from "../stories/StoryFetchStub";
import { PasswordForm } from "./PasswordForm";

const meta = {
  title: "Components/PasswordForm",
  component: PasswordForm,
  args: { onAuthenticated: () => {}, primary: true },
} satisfies Meta<typeof PasswordForm>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Empty: Story = {};

export const Secondary: Story = {
  args: { primary: false },
};

export const ErrorState: Story = {
  decorators: [
    (Story) => (
      <StoryFetchStub
        routes={[
          {
            match: "/ui/session",
            method: "POST",
            status: 401,
            body: { error: "Invalid email or password." },
          },
        ]}
      >
        <Story />
      </StoryFetchStub>
    ),
  ],
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement);
    await userEvent.type(canvas.getByLabelText("Email"), "alice@example.com");
    await userEvent.type(canvas.getByLabelText("Password"), "wrong-password");
    await userEvent.click(canvas.getByRole("button", { name: "Sign in" }));
    await canvas.findByRole("alert");
  },
};
