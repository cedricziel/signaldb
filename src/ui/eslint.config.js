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
    },
  },
);
