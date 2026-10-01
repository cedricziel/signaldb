// For more info, see https://github.com/storybookjs/eslint-plugin-storybook#configuration-flat-config-format
import storybook from "eslint-plugin-storybook";

import eslint from "@eslint/js";
import tseslint from "typescript-eslint";

export default tseslint.config(
  // Generated OpenAPI client — owned by @hey-api/openapi-ts, not hand-edited.
  { ignores: ["src/api/gen/**"] },
  eslint.configs.recommended,
  tseslint.configs.recommended,
  {
    rules: {
      "@typescript-eslint/no-unused-vars": [
        "error",
        { argsIgnorePattern: "^_", varsIgnorePattern: "^_" },
      ],
      // The UI reaches SignalDB only through the generated client
      // (`ui-generated-client-only`). A real transport (the generated
      // client's fetch, the service worker) disables this inline with a
      // reason rather than via a file-level exemption.
      "no-restricted-syntax": [
        "error",
        ...[
          "CallExpression[callee.name='fetch']",
          "CallExpression[callee.object.name='window'][callee.property.name='fetch']",
          "CallExpression[callee.object.name='globalThis'][callee.property.name='fetch']",
        ].map((selector) => ({
          selector,
          message:
            "Call SignalDB through the generated client (src/api/gen via src/api/client.ts), not raw fetch().",
        })),
      ],
    },
  },
  storybook.configs["flat/recommended"],
);
