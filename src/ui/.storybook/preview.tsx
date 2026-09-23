import type { Preview } from "@storybook/react-vite";
import { QueryClientProvider } from "@tanstack/react-query";
import { testQueryClient } from "../src/lib/queryClient";
import "../src/styles/global.css";

/** Storybook's `theme` toolbar values. "system" leaves `data-theme` unset so
 * `prefers-color-scheme` decides, matching the app's own default. */
type ThemeGlobal = "light" | "dark" | "system";

const preview: Preview = {
  parameters: {
    controls: {
      matchers: {
        color: /(background|color)$/i,
        date: /Date$/i,
      },
    },

    a11y: {
      // 'todo' - show a11y violations in the test UI only
      // 'error' - fail CI on a11y violations
      // 'off' - skip a11y checks entirely
      test: "todo",
    },
  },
  globalTypes: {
    theme: {
      description: "Color theme",
      defaultValue: "light" satisfies ThemeGlobal,
      toolbar: {
        title: "Theme",
        icon: "circlehollow",
        items: [
          { value: "light", title: "Light", icon: "sun" },
          { value: "dark", title: "Dark", icon: "moon" },
          { value: "system", title: "System", icon: "browser" },
        ],
        dynamicTitle: true,
      },
    },
  },
  initialGlobals: {
    theme: "light" satisfies ThemeGlobal,
  },
  decorators: [
    (Story, context) => {
      const theme = (context.globals.theme ?? "light") as ThemeGlobal;
      return (
        <div
          data-theme={theme === "system" ? undefined : theme}
          style={{ minHeight: "100vh" }}
        >
          <QueryClientProvider client={testQueryClient()}>
            <Story />
          </QueryClientProvider>
        </div>
      );
    },
  ],
};

export default preview;
