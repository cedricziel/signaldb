---
audience: user
type: how-to
status: living
sources:
  - src/ui/src/features/rum/SetupTab.tsx
  - src/ui/src/features/rum/rumModel.ts
  - src/ui/src/features/rum/useRumData.ts
---

# Instrument a browser app

This guide gets a browser app's sessions, Web Vitals, errors and
frontend-to-backend traces onto SignalDB's **Real users** page. It uses the
upstream OpenTelemetry browser SDK; SignalDB ships no SDK of its own. The
steps match the **Setup** tab on the Real users page.

## Prerequisites

- A SignalDB API key, tenant and dataset. [Send OTLP data to
  SignalDB](sending-otlp.md) covers endpoints and headers.
- An OTLP endpoint you run that the browser can reach: an OpenTelemetry
  Collector, or a route on your app's own backend that forwards OTLP.

Do not put a SignalDB API key in browser code. Anyone who loads the page can
read it, and a key works from any non-browser client (no `Origin` header)
even when it is restricted to certain origins. The browser exports to your
endpoint; your endpoint adds the key and forwards to SignalDB.

## 1. Install the SDK

```bash
npm install @opentelemetry/api-logs @opentelemetry/sdk-trace-base \
  @opentelemetry/sdk-trace-web @opentelemetry/sdk-logs \
  @opentelemetry/exporter-trace-otlp-http @opentelemetry/exporter-logs-otlp-http \
  @opentelemetry/resources @opentelemetry/semantic-conventions \
  @opentelemetry/instrumentation @opentelemetry/instrumentation-fetch \
  @opentelemetry/instrumentation-document-load \
  @opentelemetry/browser-instrumentation
```

`@opentelemetry/browser-instrumentation` (0.7) emits the log records the Real
users page reads. The trace packages emit the page-load and fetch spans.

## 2. Initialize

Set the resource attributes first. The page identifies an app by them:

| Attribute                     | Use                                                                          |
| ----------------------------- | ---------------------------------------------------------------------------- |
| `service.name`                | Required. The app switcher lists every `service.name` that sent RUM records. |
| `service.version`             | Recommended. Tells releases apart.                                           |
| `deployment.environment.name` | Recommended. Tells production from staging.                                  |

```ts
import { WebTracerProvider } from "@opentelemetry/sdk-trace-web";
import {
  LoggerProvider,
  BatchLogRecordProcessor,
} from "@opentelemetry/sdk-logs";
import { BatchSpanProcessor } from "@opentelemetry/sdk-trace-base";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-http";
import { OTLPLogExporter } from "@opentelemetry/exporter-logs-otlp-http";
import { resourceFromAttributes } from "@opentelemetry/resources";
import {
  ATTR_SERVICE_NAME,
  ATTR_SERVICE_VERSION,
} from "@opentelemetry/semantic-conventions";
import { logs } from "@opentelemetry/api-logs";
import { registerInstrumentations } from "@opentelemetry/instrumentation";
import { FetchInstrumentation } from "@opentelemetry/instrumentation-fetch";
import { DocumentLoadInstrumentation } from "@opentelemetry/instrumentation-document-load";
import { WebVitalsInstrumentation } from "@opentelemetry/browser-instrumentation/experimental/web-vitals";
import { NavigationInstrumentation } from "@opentelemetry/browser-instrumentation/experimental/navigation";
import { NavigationTimingInstrumentation } from "@opentelemetry/browser-instrumentation/experimental/navigation-timing";
import { ResourceTimingInstrumentation } from "@opentelemetry/browser-instrumentation/experimental/resource-timing";
import { ErrorsInstrumentation } from "@opentelemetry/browser-instrumentation/experimental/errors";
import { UserActionInstrumentation } from "@opentelemetry/browser-instrumentation/experimental/user-action";

// Your own endpoint on this origin. No Authorization header, no SignalDB key.
const COLLECTOR_URL = "/otlp";

const resource = resourceFromAttributes({
  [ATTR_SERVICE_NAME]: "my-frontend-app",
  [ATTR_SERVICE_VERSION]: "1.4.2",
  "deployment.environment.name": "production",
});

const tracerProvider = new WebTracerProvider({
  resource,
  spanProcessors: [
    new BatchSpanProcessor(
      new OTLPTraceExporter({ url: `${COLLECTOR_URL}/v1/traces` }),
    ),
  ],
});
tracerProvider.register();

const loggerProvider = new LoggerProvider({
  resource,
  processors: [
    new SessionProcessor(), // see "Stamp session.id" below
    new BatchLogRecordProcessor(
      new OTLPLogExporter({ url: `${COLLECTOR_URL}/v1/logs` }),
    ),
  ],
});
logs.setGlobalLoggerProvider(loggerProvider);

registerInstrumentations({
  tracerProvider,
  loggerProvider,
  instrumentations: [
    new DocumentLoadInstrumentation(),
    new FetchInstrumentation({
      propagateTraceHeaderCorsUrls: [/^https:\/\/api\.example\.com\//],
    }),
    new WebVitalsInstrumentation(),
    new NavigationInstrumentation(),
    new NavigationTimingInstrumentation(),
    new ResourceTimingInstrumentation({ ignoreUrls: [/\/v1\/(traces|logs)$/] }),
    new ErrorsInstrumentation(),
    new UserActionInstrumentation(),
  ],
});
```

These instrumentations emit log records with these `event_name` values:

| `event_name`                | Feeds                                               |
| --------------------------- | --------------------------------------------------- |
| `browser.web_vital`         | Core Web Vitals (lowercase names, ms; CLS unitless) |
| `browser.navigation`        | Page views                                          |
| `browser.navigation_timing` | Page-load timing                                    |
| `browser.resource_timing`   | Network tab's Resources table                       |
| `browser.user_action.click` | Clicks                                              |
| `exception`                 | Errors and the sessions-with-errors share           |

`ignoreUrls` keeps the SDK's own export requests out of the resource timings.

### Stamp `session.id`

Sessions are grouped by `session.id` on each record. Add it with a log record
processor, and the same on spans with a span processor if you want spans in
the session too:

```ts
import type { LogRecordProcessor, SdkLogRecord } from "@opentelemetry/sdk-logs";

class SessionProcessor implements LogRecordProcessor {
  onEmit(record: SdkLogRecord): void {
    record.setAttribute("session.id", getSessionId()); // your session logic
    const userId = currentUserId(); // optional
    if (userId) record.setAttribute("user.id", userId);
  }
  forceFlush() {
    return Promise.resolve();
  }
  shutdown() {
    return Promise.resolve();
  }
}
```

`user.id` is optional; it feeds the Users count. Don't put personal data in it
that you wouldn't store in SignalDB.

### Set `url.template` (optional)

The Pages tab groups by route. Set `url.template` to the route pattern, such
as `/products/:id`, from your router in the same processor. Without it,
SignalDB derives a path template from `url.full` when the record has one;
page views with neither don't appear on the Pages tab.

## 3. Forward to SignalDB

Point your endpoint at SignalDB with the key attached. With a Collector:

```yaml
receivers:
  otlp:
    protocols:
      http:
        endpoint: 0.0.0.0:4318 # your app proxies /otlp here

exporters:
  otlphttp:
    endpoint: <SignalDB OTLP/HTTP URL>
    headers:
      Authorization: "Bearer <api-key>"
      X-Tenant-ID: "<tenant>"
      X-Dataset-ID: "<dataset>"

service:
  pipelines:
    traces:
      receivers: [otlp]
      exporters: [otlphttp]
    logs:
      receivers: [otlp]
      exporters: [otlphttp]
```

The Setup tab fills in the URL and headers for your tenant. Ports and header
rules are in [Send OTLP data to SignalDB](sending-otlp.md).

## 4. Join frontend requests to backend traces

`propagateTraceHeaderCorsUrls` sends a `traceparent` header on fetches to the
listed origins; same-origin fetches get it without the option. For
cross-origin APIs, each API must allow the header in its CORS response:

```http
Access-Control-Allow-Headers: traceparent, tracestate, content-type
```

The backend must also be instrumented with OpenTelemetry and send its traces
to the same SignalDB tenant and dataset. The Network tab then splits each
request's time into client+network and backend, and shows how many requests
were traced. An origin with no traced requests gets a callout.

## Verify

1. Load a few pages of your app.
2. Open the **Real users** page (`/rum`) and pick your app in the switcher.
3. Open **Setup** and check the status list: first session received, page
   views received, vitals received, and requests joined to backend traces.

Web Vitals such as LCP and INP are reported as the page is used or hidden,
so vitals can lag the first page view. What each tab shows is in [Explore
UI: Real users](explore-ui.md#real-users).

## Troubleshooting

- **App missing from the switcher.** No RUM record arrived with that
  `service.name`. Check the browser's network panel for failed `/v1/logs`
  requests, then your endpoint's logs for rejected forwards.
- **Sessions stay at zero.** Records arrive without `session.id`; check the
  processor is registered before the exporter's processor.
- **Requests not joined.** Check the request carries `traceparent`, the API's
  preflight allows it, and the backend sends traces to the same dataset.

This guide covers browser apps only.
