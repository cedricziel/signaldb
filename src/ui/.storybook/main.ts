import type { PluginOption } from "vite";
import type { StorybookConfig } from "@storybook/react-vite";

// @storybook/react-vite merges in the app's own vite.config.ts (for shared
// resolve/plugin setup), which includes VitePWA — a service-worker
// precache step meant for the deployed app, not this component explorer.
// It has no useful role here and chokes trying to precache Storybook's own
// (multi-MB) manager runtime, so it's dropped from the merged plugin list.
function withoutPwaPlugin(plugins: PluginOption[]): PluginOption[] {
  return plugins
    .map((plugin) =>
      Array.isArray(plugin) ? withoutPwaPlugin(plugin) : plugin,
    )
    .filter(
      (plugin) =>
        !(
          plugin &&
          typeof plugin === "object" &&
          "name" in plugin &&
          plugin.name.startsWith("vite-plugin-pwa")
        ),
    );
}

const config: StorybookConfig = {
  stories: ["../src/components/**/*.stories.tsx"],
  addons: [
    "@chromatic-com/storybook",
    "@storybook/addon-vitest",
    "@storybook/addon-a11y",
    "@storybook/addon-docs",
    "@storybook/addon-mcp",
  ],
  framework: "@storybook/react-vite",
  async viteFinal(viteConfig) {
    if (viteConfig.plugins) {
      viteConfig.plugins = withoutPwaPlugin(viteConfig.plugins);
    }
    return viteConfig;
  },
};
export default config;
