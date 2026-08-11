import { describe, expect, it } from "vitest";
import {
  ATTR_DEPLOYMENT_ENVIRONMENT_NAME,
  ATTR_SERVICE_NAME,
  ATTR_SERVICE_NAMESPACE,
  ATTR_SERVICE_VERSION,
} from "@opentelemetry/semantic-conventions";
import { buildResource } from "./resource";

describe("buildResource", () => {
  it("stamps service identity and stable browser facts", () => {
    const resource = buildResource("signaldb-ui", "1.2.3", undefined);
    expect(resource.attributes[ATTR_SERVICE_NAME]).toBe("signaldb-ui");
    expect(resource.attributes[ATTR_SERVICE_VERSION]).toBe("1.2.3");
    expect(resource.attributes["browser.language"]).toBe(navigator.language);
    expect(typeof resource.attributes["browser.mobile"]).toBe("boolean");
  });

  it("adds namespace, backend version, and deployment environment when the runtime config carries them", () => {
    const resource = buildResource("signaldb-ui", "1.2.3", {
      telemetry: {
        namespace: "signaldb",
        version: "0.2.2",
        deploymentEnvironment: "production",
      },
    });
    expect(resource.attributes[ATTR_SERVICE_NAMESPACE]).toBe("signaldb");
    expect(resource.attributes["signaldb.server.version"]).toBe("0.2.2");
    expect(resource.attributes[ATTR_DEPLOYMENT_ENVIRONMENT_NAME]).toBe(
      "production",
    );
  });

  it("omits those attributes entirely when the runtime config is absent", () => {
    const resource = buildResource("signaldb-ui", "1.2.3", undefined);
    expect(resource.attributes).not.toHaveProperty(ATTR_SERVICE_NAMESPACE);
    expect(resource.attributes).not.toHaveProperty("signaldb.server.version");
    expect(resource.attributes).not.toHaveProperty(
      ATTR_DEPLOYMENT_ENVIRONMENT_NAME,
    );
  });
});
