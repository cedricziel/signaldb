#!/usr/bin/env bash
# Builds a library-shaped dist of the src/ui components for design-sync.
# The UI is an app with no published package, so this stands in for one.
set -euo pipefail
cd "$(dirname "$0")"
UI=../../src/ui/src
# Every component with a story ships; no list to keep in sync.
COMPONENTS=$(for s in "$UI"/components/*.stories.tsx; do basename "$s" .stories.tsx; done)
# Page stories render a feature view; export it by name (export * would
# collide on shared helper names across feature modules).
PAGES="
CatalogView:features/catalog/CatalogView
ConsentView:features/consent/ConsentView
ErrorsView:features/errors/ErrorsView
ExploreView:features/explore/ExploreView
GitHubIntegration:features/integrations/GitHubIntegration
LogsView:features/logs/LogsView
ApiKeys:features/management/ApiKeys
Instrumentation:features/management/Instrumentation
ManagementPanel:features/management/ManagementPanel
SelectTenant:features/management/SelectTenant
MetricsView:features/metrics/MetricsView
OverviewView:features/overview/OverviewView
ProcessorList:features/processors/ProcessorList
ProfilesView:features/profiles/ProfilesView
QueryView:features/query/QueryView
SchemaHub:features/schema/SchemaHub
LoginRoute:features/shell/LoginRoute
TracesView:features/traces/TracesView
"
# Shared runtime the story wrappers must get from the bundle, not a second
# copy (router context, query cache, and the API client the fetch stub patches).
RUNTIME_DTS='
export * from "@tanstack/react-query";
export * from "react-router";
export { testQueryClient } from "./types/lib/queryClient";
export { client } from "./types/api/gen/client.gen";
export * from "./types/api/http";
export declare const previewQueryClient: import("@tanstack/react-query").QueryClient;'
ln -sfn ../../src/ui/node_modules node_modules
rm -rf dist && mkdir -p dist
{
  for n in $COMPONENTS; do echo "export * from \"./types/components/$n\";"; done
  for p in $PAGES; do echo "export { ${p%%:*} } from \"./types/${p#*:}\";"; done
  echo "$RUNTIME_DTS"
} > dist/index.d.ts
{
  # global.css first, as main.tsx does: loaded last its .btn rules override
  # feature CSS of equal specificity.
  echo "import \"../$UI/styles/global.css\";"
  # Stylesheets of modules no bundle export reaches - the app shell (sidebar,
  # page header, palette, user menu, banners) and route-only views like the
  # schema storage explorer. Story-local CSS compiles empty, so without these
  # lines any story rendering them previews unstyled. Order mirrors the app's
  # import order (UserMenu.css before AppNav.css).
  for c in features/shell/UserMenu.css features/shell/AppNav.css features/shell/UpdateBanner.css \
    features/shell/RouteErrorBoundary.css features/shell/UnsavedChangesGuard.css \
    features/schema/SchemaExplorer.css; do
    echo "import \"../$UI/$c\";"
  done
  for n in $COMPONENTS; do echo "export * from \"../$UI/components/$n\";"; done
  for p in $PAGES; do echo "export { ${p%%:*} } from \"../$UI/${p#*:}\";"; done
  echo "$RUNTIME_DTS" | sed "s#\./types/#../$UI/#; /export declare/d"
  echo "import { testQueryClient as makeClient } from \"../$UI/lib/queryClient\";"
  echo 'export const previewQueryClient = makeClient();'
} > dist/entry.ts
../../src/ui/node_modules/.bin/tsc -p tsconfig.json
../../.ds-sync/node_modules/.bin/esbuild dist/entry.ts --bundle --format=esm --jsx=automatic \
  --external:react --external:react-dom --external:react/jsx-runtime \
  --outfile=dist/index.js --define:__SIGNALDB_DEFAULT_TENANT__=\"\" --define:__SIGNALDB_DEFAULT_DATASET__=\"\" --define:__SIGNALDB_OTLP_ENDPOINT__=\"\" --define:__SIGNALDB_TELEMETRY_SERVICE_NAME__=\"signaldb-ui\" --define:__SIGNALDB_UI_VERSION__=\"0\" --loader:.svg=dataurl --loader:.woff2=file --loader:.woff=file --loader:.png=dataurl
