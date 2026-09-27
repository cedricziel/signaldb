import { afterEach, describe, expect, it, vi } from "vitest";
import {
  ATTR_DEPLOYMENT_ENVIRONMENT_NAME,
  ATTR_SERVICE_NAME,
  ATTR_SERVICE_NAMESPACE,
  ATTR_SERVICE_VERSION,
  ATTR_USER_AGENT_ORIGINAL,
} from "@opentelemetry/semantic-conventions";
import { buildResource } from "./resource";

afterEach(() => {
  vi.unstubAllGlobals();
});

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

  it("carries browser.brands and browser.platform for a Chrome-like navigator", () => {
    vi.stubGlobal("navigator", {
      language: "en-US",
      userAgent:
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
      userAgentData: {
        brands: [
          { brand: "Chromium", version: "129" },
          { brand: "Google Chrome", version: "129" },
        ],
        platform: "Windows",
      },
    });
    const resource = buildResource("signaldb-ui", "1.2.3", undefined);
    expect(resource.attributes[ATTR_USER_AGENT_ORIGINAL]).toContain(
      "Chrome/129",
    );
    expect(resource.attributes["browser.brands"]).toEqual([
      "Chromium 129",
      "Google Chrome 129",
    ]);
    expect(resource.attributes["browser.platform"]).toBe("Windows");
  });

  it("carries user_agent.original but omits browser.brands/platform for a Firefox-like navigator", () => {
    vi.stubGlobal("navigator", {
      language: "en-US",
      userAgent:
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:130.0) Gecko/20100101 Firefox/130.0",
    });
    const resource = buildResource("signaldb-ui", "1.2.3", undefined);
    expect(resource.attributes[ATTR_USER_AGENT_ORIGINAL]).toContain(
      "Firefox/130",
    );
    expect(resource.attributes).not.toHaveProperty("browser.brands");
    expect(resource.attributes).not.toHaveProperty("browser.platform");
  });
});
