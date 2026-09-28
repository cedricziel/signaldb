// The Real users page's Setup tab: instrumenting a browser app with the
// upstream OpenTelemetry SDK, exporting to a collector or backend the
// app's own operator runs (never a SignalDB API key in browser code — see
// design.md's "Keys in browser code" risk), and a live checklist. Layout
// mirrors the design prototype's `SetupTab` (`.rum-split`, `.rum-steps`,
// `.rum-kv`).
import { useQuery } from "@tanstack/react-query";
import { connectionInfo } from "../../api/connection";
import { CopyValueButton } from "../../components/CopyValueButton";
import {
  useRumKpis,
  useRumTracedShare,
  useRumVitals,
  type RumScope,
} from "./useRumData";
import { Panel } from "./Panel";

interface Props {
  app: string;
  scope: RumScope;
}

const COLLECTED_KV: [string, string][] = [
  [
    "logs",
    "browser.web_vital, browser.navigation, browser.navigation_timing, browser.resource_timing, exception, browser.user_action.click",
  ],
  [
    "spans",
    "fetch/XHR client spans, page-load spans, click spans (when instrumented)",
  ],
  [
    "session",
    "session.id, user.id (if your app sets one), browser resource attributes",
  ],
  [
    "not collected",
    "page content, form/input values, cookies or local storage",
  ],
];

export function SetupTab({ app, scope }: Props) {
  const connection = useQuery({
    queryKey: ["rum-setup-connection"],
    queryFn: connectionInfo,
    staleTime: 5 * 60_000,
  });

  const serviceName = app || "my-frontend-app";
  const kpis = useRumKpis(scope);
  const vitals = useRumVitals(scope);
  const tracedShare = useRumTracedShare(scope);

  const hasSession = app !== "" && (kpis.data?.sessions.value ?? 0) > 0;
  const hasVitals =
    app !== "" && Array.from(vitals.data?.values() ?? []).length > 0;
  const hasPageViews = app !== "" && (kpis.data?.pageViews.value ?? 0) > 0;
  const tracedSharePct =
    app !== "" && tracedShare.data?.hasData
      ? Math.round(tracedShare.data.value * 100)
      : null;

  const installSnippet = `npm install @opentelemetry/sdk-trace-web @opentelemetry/exporter-trace-otlp-http @opentelemetry/exporter-logs-otlp-http @opentelemetry/instrumentation-document-load`;

  const initSnippet = `import { WebTracerProvider } from "@opentelemetry/sdk-trace-web";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-http";
import { OTLPLogExporter } from "@opentelemetry/exporter-logs-otlp-http";
import { resourceFromAttributes } from "@opentelemetry/resources";
import { ATTR_SERVICE_NAME } from "@opentelemetry/semantic-conventions";

// This origin's own collector/backend endpoint — never point the SDK
// straight at SignalDB with a key baked into browser code.
const COLLECTOR_URL = "/otlp"; // proxied by this app's own backend

const resource = resourceFromAttributes({
  [ATTR_SERVICE_NAME]: "${serviceName}",
});

const traceExporter = new OTLPTraceExporter({
  url: \`\${COLLECTOR_URL}/v1/traces\`,
});
const logExporter = new OTLPLogExporter({
  url: \`\${COLLECTOR_URL}/v1/logs\`,
});
// No Authorization header, no SignalDB API key — this exporter carries
// only the app's own resource attributes to its own origin.`;

  const collectorSnippet = connection.data
    ? `# otel-collector-config.yaml — run by this app's operator, holds the key
receivers:
  otlp:
    protocols:
      http:
        endpoint: 0.0.0.0:4318 # matches COLLECTOR_URL above (proxied as /otlp)

exporters:
  otlphttp:
    endpoint: ${connection.data.ingest.otlp_http.url}
    headers:
      Authorization: "${connection.data.headers.authorization}"
      X-Tenant-ID: "${connection.data.headers["x-tenant-id"]}"
      X-Dataset-ID: "${connection.data.headers["x-dataset-id"]}"

service:
  pipelines:
    traces:
      receivers: [otlp]
      exporters: [otlphttp]
    logs:
      receivers: [otlp]
      exporters: [otlphttp]`
    : "";

  const corsSnippet = `# The collector/backend must propagate traceparent and allow it in CORS
# for every API origin the app calls, so client spans join their server
# children in the same trace (see the Network tab, a later group).
Access-Control-Allow-Headers: traceparent, tracestate, content-type`;

  return (
    <div className="rum-split">
      <Panel
        title="Instrument an app"
        meta="OpenTelemetry SDKs · OTLP/HTTP to SignalDB"
      >
        <ol className="rum-steps">
          <li>
            <span className="rum-stepn">1</span>
            <div className="rum-step-body">
              <div className="rum-step-title">Install the SDK</div>
              <SnippetBlock label="npm install" code={installSnippet} />
            </div>
          </li>
          <li>
            <span className="rum-stepn">2</span>
            <div className="rum-step-body">
              <div className="rum-step-title">
                Initialize with your service name and a collector endpoint
              </div>
              <p className="rum-setup-note">
                SignalDB API keys are bearer credentials with no origin
                restriction — any key shipped to a browser is public. Export to
                an OpenTelemetry Collector or your own backend instead; it
                forwards to SignalDB holding the key server-side.
              </p>
              <SnippetBlock label="init.ts" code={initSnippet} />
            </div>
          </li>
          <li>
            <span className="rum-stepn">3</span>
            <div className="rum-step-body">
              <div className="rum-step-title">
                Forward from the collector to SignalDB
              </div>
              {connection.isPending ? (
                <div className="rum-placeholder">
                  Loading connection details…
                </div>
              ) : (
                <SnippetBlock
                  label="otel-collector-config.yaml"
                  code={collectorSnippet}
                />
              )}
            </div>
          </li>
          <li>
            <span className="rum-stepn">4</span>
            <div className="rum-step-body">
              <div className="rum-step-title">
                Propagate traceparent, allow it in CORS
              </div>
              <SnippetBlock label="CORS" code={corsSnippet} />
            </div>
          </li>
        </ol>
      </Panel>

      <div className="rum-stack">
        <Panel title="Status">
          <ul className="rum-checklist">
            <ChecklistItem
              label="First session received"
              detail="session.id on any log record"
              done={hasSession}
              disabled={app === ""}
            />
            <ChecklistItem
              label="Page views received"
              detail="browser.navigation records"
              done={hasPageViews}
              disabled={app === ""}
            />
            <ChecklistItem
              label="Vitals received"
              detail="browser.web_vital records"
              done={hasVitals}
              disabled={app === ""}
            />
            <ChecklistItem
              label="Requests joined to backend traces"
              detail={
                tracedSharePct !== null
                  ? `${tracedSharePct}% of client spans have a server child`
                  : "client spans with a server child, via correlate"
              }
              done={(tracedSharePct ?? 0) > 0}
              disabled={app === ""}
            />
          </ul>
        </Panel>

        <Panel title="What gets collected">
          <dl className="rum-kv">
            {COLLECTED_KV.map(([term, desc]) => (
              <div className="rum-kv-row" key={term}>
                <dt className="mono dim rum-kv-term">{term}</dt>
                <dd className="rum-kv-desc">{desc}</dd>
              </div>
            ))}
          </dl>
        </Panel>
      </div>
    </div>
  );
}

function SnippetBlock({ label, code }: { label: string; code: string }) {
  return (
    <div className="rum-snippet-wrap">
      <pre className="rum-snippet">{code}</pre>
      <CopyValueButton
        value={code}
        label={`Copy ${label}`}
        className="copy-value-button rum-snippet-copy"
      />
    </div>
  );
}

function ChecklistItem({
  label,
  detail,
  done,
  disabled,
}: {
  label: string;
  detail: string;
  done: boolean;
  disabled: boolean;
}) {
  return (
    <li className="rum-checklist-item">
      <span
        className={done ? "rum-check rum-check-done" : "rum-check"}
        aria-hidden="true"
      >
        {done && (
          <svg viewBox="0 0 16 16" width="10" height="10" aria-hidden="true">
            <polyline
              points="3,8.5 6.5,12 13,4.5"
              fill="none"
              stroke="currentColor"
              strokeWidth="2"
              strokeLinecap="round"
              strokeLinejoin="round"
            />
          </svg>
        )}
      </span>
      <span className="rum-checklist-text">
        <b>{label}</b>
        <span className="mono dim rum-checklist-detail">
          {disabled ? "select an app" : detail}
        </span>
      </span>
    </li>
  );
}
