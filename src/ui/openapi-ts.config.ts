import { defineConfig } from "@hey-api/openapi-ts";

// Generates the typed TypeScript client/SDK the UI consumes from SignalDB's
// code-first OpenAPI spec. The spec itself is emitted from the router's
// #[utoipa::path] annotations (api/signaldb-api.json). Regenerate via
// `cargo xtask generate` (or `pnpm --filter signaldb-ui generate:api`).
export default defineConfig({
  input: "../../api/signaldb-api.json",
  output: "src/api/gen",
  plugins: ["@hey-api/typescript", "@hey-api/sdk", "@hey-api/client-fetch"],
});
