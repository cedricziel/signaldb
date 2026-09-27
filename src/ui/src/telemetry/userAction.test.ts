import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { registerInstrumentations } from "@opentelemetry/instrumentation";
import {
  InMemoryLogRecordExporter,
  LoggerProvider,
  SimpleLogRecordProcessor,
} from "@opentelemetry/sdk-logs";
import { SvgAwareUserActionInstrumentation } from "./userAction";

const CLICK_EVENT_NAME = "browser.user_action.click";

function setup() {
  const exporter = new InMemoryLogRecordExporter();
  const provider = new LoggerProvider({
    processors: [new SimpleLogRecordProcessor({ exporter })],
  });
  const instrumentation = new SvgAwareUserActionInstrumentation();
  const unregister = registerInstrumentations({
    loggerProvider: provider,
    instrumentations: [instrumentation],
  });
  return { exporter, unregister };
}

function clickRecords(exporter: InMemoryLogRecordExporter) {
  return exporter
    .getFinishedLogRecords()
    .filter((record) => record.eventName === CLICK_EVENT_NAME);
}

let unregister: () => void;

beforeEach(() => {
  document.body.innerHTML = "";
});

afterEach(() => {
  unregister?.();
  document.body.innerHTML = "";
});

describe("SvgAwareUserActionInstrumentation", () => {
  it("attributes a click on an SVG icon to its closest HTMLElement ancestor", () => {
    const { exporter, unregister: cleanup } = setup();
    unregister = cleanup;
    document.body.innerHTML =
      '<button id="theme-toggle">Toggle theme<svg><path /></svg></button>';
    const path = document.querySelector("path")!;
    path.dispatchEvent(new MouseEvent("click", { bubbles: true }));

    const records = clickRecords(exporter);
    expect(records).toHaveLength(1);
    expect(records[0]!.attributes["browser.tag_name"]).toBe("BUTTON");
    expect(records[0]!.attributes["browser.css_selector"]).toContain(
      "theme-toggle",
    );
  });

  it("records a click on a plain button directly", () => {
    const { exporter, unregister: cleanup } = setup();
    unregister = cleanup;
    document.body.innerHTML = '<button id="save">Save</button>';
    document
      .querySelector("#save")!
      .dispatchEvent(new MouseEvent("click", { bubbles: true }));

    const records = clickRecords(exporter);
    expect(records).toHaveLength(1);
    expect(records[0]!.attributes["browser.tag_name"]).toBe("BUTTON");
  });

  it("never captures the clicked element's text", () => {
    const { exporter, unregister: cleanup } = setup();
    unregister = cleanup;
    document.body.innerHTML =
      '<button id="theme-toggle">Toggle theme<svg><path /></svg></button>';
    document
      .querySelector("path")!
      .dispatchEvent(new MouseEvent("click", { bubbles: true }));

    const records = clickRecords(exporter);
    expect(records).toHaveLength(1);
    expect(JSON.stringify(records[0]!.attributes)).not.toContain(
      "Toggle theme",
    );
  });
});
