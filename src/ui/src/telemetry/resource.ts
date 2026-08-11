// Shared resource identity and runtime config, read once and used by both the
// trace pipeline (`index.ts`) and the log pipeline (`logs.ts`) so the two
// signals carry the same `service.name` / `service.version` / browser facts.
//
// Pure (no `zone.js`, no OTel SDK singletons) so it can be unit-tested — see
// `runtimeConfig.ts` for the same rule.

import {
  defaultResource,
  resourceFromAttributes,
  type Resource,
} from "@opentelemetry/resources";
import {
  ATTR_DEPLOYMENT_ENVIRONMENT_NAME,
  ATTR_SERVICE_NAME,
  ATTR_SERVICE_NAMESPACE,
  ATTR_SERVICE_VERSION,
} from "@opentelemetry/semantic-conventions";
import {
  resolveDeploymentEnvironment,
  resolveServerVersion,
  resolveServiceName,
  resolveServiceNamespace,
  type RuntimeConfig,
} from "./runtimeConfig";

// Runtime config the router injected via `/ui/runtime-config.js` before the app
// booted (see runtimeConfig.ts). Absent in tests/SSR and when the script 404s.
export const RUNTIME_CONFIG: RuntimeConfig | undefined =
  typeof window !== "undefined"
    ? window.__SIGNALDB_RUNTIME_CONFIG__
    : undefined;

const BUILD_TIME_SERVICE_NAME =
  typeof __SIGNALDB_TELEMETRY_SERVICE_NAME__ !== "undefined" &&
  __SIGNALDB_TELEMETRY_SERVICE_NAME__
    ? __SIGNALDB_TELEMETRY_SERVICE_NAME__
    : "signaldb-ui";

export const SERVICE_NAME = resolveServiceName(
  RUNTIME_CONFIG,
  BUILD_TIME_SERVICE_NAME,
);

export const SERVICE_VERSION =
  typeof __SIGNALDB_UI_VERSION__ !== "undefined"
    ? __SIGNALDB_UI_VERSION__
    : "0.0.0";

/**
 * Build the Resource shared by the trace and log providers.
 *
 * `namespace` / `serverVersion` / `deploymentEnvironment` mirror
 * `common::self_monitoring::build_resource`'s backend
 * `(service.namespace, service.version, deployment.environment.name)` facts,
 * read from the router-injected runtime config (see `runtimeConfig.ts`) —
 * omitted entirely (not just left `undefined`) when that config is absent,
 * rather than guessing a value. `serverVersion` lands on the custom
 * `signaldb.server.version` attribute, not `service.version`: this resource
 * describes the `signaldb-ui` service, whose own version is `serviceVersion`
 * (the UI bundle) — the backend that served it is a distinct fact.
 */
export function buildResource(
  serviceName: string = SERVICE_NAME,
  serviceVersion: string = SERVICE_VERSION,
  runtime: RuntimeConfig | undefined = RUNTIME_CONFIG,
): Resource {
  const namespace = resolveServiceNamespace(runtime);
  const serverVersion = resolveServerVersion(runtime);
  const deploymentEnvironment = resolveDeploymentEnvironment(runtime);
  return defaultResource().merge(
    resourceFromAttributes({
      [ATTR_SERVICE_NAME]: serviceName,
      [ATTR_SERVICE_VERSION]: serviceVersion,
      ...(namespace !== undefined
        ? { [ATTR_SERVICE_NAMESPACE]: namespace }
        : {}),
      ...(serverVersion !== undefined
        ? { "signaldb.server.version": serverVersion }
        : {}),
      ...(deploymentEnvironment !== undefined
        ? { [ATTR_DEPLOYMENT_ENVIRONMENT_NAME]: deploymentEnvironment }
        : {}),
      "browser.language": navigator.language,
      "browser.mobile": /Mobi/i.test(navigator.userAgent),
    }),
  );
}
