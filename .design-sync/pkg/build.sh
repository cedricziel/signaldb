#!/usr/bin/env bash
# Builds a library-shaped dist of the src/ui components for design-sync.
# The UI is an app with no published package, so this stands in for one.
set -euo pipefail
cd "$(dirname "$0")"
COMPONENTS="AttributeKeyInput AttributeTable AttributeValue BrandMark ConfirmButton CopyValueButton Dialog EmptyState MobileSidebarDrawer QueryError SemanticKey SidebarResizer SourceSnippet StacktraceLines VizTooltip FilterChips TimeRangePicker SignalHistogram MemberTable"
PAGES="logs/LogsView traces/TracesView"
ln -sfn ../../src/ui/node_modules node_modules
rm -rf dist && mkdir -p dist
{
  for n in $COMPONENTS; do echo "export * from \"./types/components/$n\";"; done
  for p in $PAGES; do echo "export * from \"./types/features/$p\";"; done
  echo 'export { QueryClient, QueryClientProvider } from "@tanstack/react-query";'
  echo 'export { testQueryClient } from "./types/lib/queryClient";'
  echo 'export { MemoryRouter } from "react-router";'
  echo 'export { client } from "./types/api/gen/client.gen";'
  echo 'export declare const previewQueryClient: import("@tanstack/react-query").QueryClient;'
} > dist/index.d.ts
{
  echo 'import "../../../src/ui/src/styles/global.css";'
  for n in $COMPONENTS; do echo "export * from \"../../../src/ui/src/components/$n\";"; done
  for p in $PAGES; do echo "export * from \"../../../src/ui/src/features/$p\";"; done
  echo 'export { QueryClient, QueryClientProvider } from "@tanstack/react-query";'
  echo 'export { testQueryClient } from "../../../src/ui/src/lib/queryClient";'
  echo 'export { MemoryRouter } from "react-router";'
  echo 'export { client } from "../../../src/ui/src/api/gen/client.gen";'
  echo 'import { testQueryClient as makeClient } from "../../../src/ui/src/lib/queryClient";'
  echo 'export const previewQueryClient = makeClient();'
} > dist/entry.ts
../../src/ui/node_modules/.bin/tsc -p tsconfig.json
../../.ds-sync/node_modules/.bin/esbuild dist/entry.ts --bundle --format=esm --jsx=automatic \
  --external:react --external:react-dom --external:react/jsx-runtime \
  --outfile=dist/index.js --define:__SIGNALDB_DEFAULT_TENANT__=\"\" --define:__SIGNALDB_DEFAULT_DATASET__=\"\" --define:__SIGNALDB_OTLP_ENDPOINT__=\"\" --define:__SIGNALDB_TELEMETRY_SERVICE_NAME__=\"signaldb-ui\" --define:__SIGNALDB_UI_VERSION__=\"0\" --loader:.svg=dataurl --loader:.woff2=file --loader:.woff=file
