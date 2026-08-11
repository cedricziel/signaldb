import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import type { ExploreState } from "../../lib/urlState";
import { whoami } from "../../api/session";
import { CopyValueButton } from "../../components/CopyValueButton";
import "./Instrumentation.css";

// The acceptor's OTLP ports (see docs/users/sending-otlp.md); distinct from
// the router's HTTP port, which only serves queries. Snippets append the
// protocol-appropriate port to a bare host themselves, rather than baking a
// single "host:port" string that some templates would then append a second
// port to.
const OTLP_GRPC_PORT = 4317;
const OTLP_HTTP_PORT = 4318;

type SourceId =
  | "otel-sdk"
  | "otel-collector"
  | "kubernetes"
  | "docker"
  | "journald"
  | "prometheus";

interface Source {
  id: SourceId;
  label: string;
  title: string;
  description: string;
  steps: string[];
  /** `host` is a bare hostname (no port) — the acceptor's ingest host. */
  snippet: (tenant: string, dataset: string, host: string) => string;
}

const SOURCES: Source[] = [
  {
    id: "otel-sdk",
    label: "OTel SDK",
    title: "OpenTelemetry SDK",
    description: "Application-level instrumentation using OpenTelemetry SDK.",
    steps: [
      "Install the OpenTelemetry SDK for your language",
      "Configure the OTLP exporter to point to SignalDB",
      "Set authentication and tenant headers",
    ],
    snippet: (tenant, dataset, host) => `// Example for Go
package main

import (
    "context"
    "log"

    "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
)

func main() {
    ctx := context.Background()
    exp, err := otlptracegrpc.New(ctx,
        otlptracegrpc.WithEndpoint("${host}:${OTLP_GRPC_PORT}"),
        otlptracegrpc.WithHeaders(map[string]string{
            "Authorization": "Bearer YOUR_API_KEY",
            "X-Tenant-ID":   "${tenant}",
            "X-Dataset-ID":  "${dataset}",
        }),
    )
    if err != nil {
        log.Fatalf("failed to create exporter: %v", err)
    }
    defer exp.Shutdown(ctx)
    // Register exp with a TracerProvider — see the OpenTelemetry Go docs
    // for a full SDK setup: https://opentelemetry.io/docs/languages/go/
}

// Environment variables (equivalent to the code above):
export OTEL_EXPORTER_OTLP_ENDPOINT="http://${host}:${OTLP_GRPC_PORT}"
export OTEL_EXPORTER_OTLP_HEADERS="Authorization=Bearer YOUR_API_KEY,X-Tenant-ID=${tenant},X-Dataset-ID=${dataset}"`,
  },
  {
    id: "otel-collector",
    label: "OTel Collector",
    title: "OpenTelemetry Collector",
    description: "Standalone collector/agent for forwarding telemetry.",
    steps: [
      "Deploy OpenTelemetry Collector (Helm, Docker, etc.)",
      "Configure OTLP exporter pointing to SignalDB",
      "Set authentication headers",
    ],
    snippet: (tenant, dataset, host) => `# otel-collector-config.yaml
receivers:
  otlp:
    protocols:
      grpc:
      http:

exporters:
  otlp:
    endpoint: ${host}:${OTLP_GRPC_PORT}
    tls:
      insecure: true
    headers:
      Authorization: "Bearer YOUR_API_KEY"
      X-Tenant-ID: "${tenant}"
      X-Dataset-ID: "${dataset}"

service:
  pipelines:
    traces:
      receivers: [otlp]
      exporters: [otlp]
    metrics:
      receivers: [otlp]
      exporters: [otlp]
    logs:
      receivers: [otlp]
      exporters: [otlp]`,
  },
  {
    id: "kubernetes",
    label: "Kubernetes",
    title: "Kubernetes",
    description: "Instrument Kubernetes workloads using Helm or DaemonSet.",
    steps: [
      "Install OpenTelemetry Collector via Helm",
      "Configure OTLP exporter to SignalDB",
      "Set authentication and tenant headers",
    ],
    snippet: (
      tenant,
      dataset,
      host,
    ) => `# values.yaml for opentelemetry-collector Helm chart
config:
  receivers:
    otlp:
      protocols:
        grpc:
        http:
  exporters:
    otlp:
      endpoint: ${host}:${OTLP_GRPC_PORT}
      tls:
        insecure: true
      headers:
        Authorization: "Bearer YOUR_API_KEY"
        X-Tenant-ID: "${tenant}"
        X-Dataset-ID: "${dataset}"
  service:
    pipelines:
      traces:
        receivers: [otlp]
        exporters: [otlp]
      metrics:
        receivers: [otlp]
        exporters: [otlp]
      logs:
        receivers: [otlp]
        exporters: [otlp]`,
  },
  {
    id: "docker",
    label: "Docker",
    title: "Docker",
    description:
      "Instrument containers with OpenTelemetry sidecar or env vars.",
    steps: [
      "Run OpenTelemetry Collector as sidecar container",
      "Configure OTLP exporter to SignalDB",
      "Pass environment variables to application",
    ],
    snippet: (tenant, dataset, host) => `# docker-compose.yml
version: '3'
services:
  app:
    image: myapp:latest
    environment:
      OTEL_EXPORTER_OTLP_ENDPOINT: http://${host}:${OTLP_GRPC_PORT}
      OTEL_EXPORTER_OTLP_HEADERS: "Authorization=Bearer YOUR_API_KEY,X-Tenant-ID=${tenant},X-Dataset-ID=${dataset}"
  otel-collector:
    image: otel/opentelemetry-collector-contrib:latest
    volumes:
      - ./otel-config.yaml:/etc/otel/config.yaml
    ports:
      - "${OTLP_GRPC_PORT}:${OTLP_GRPC_PORT}"`,
  },
  {
    id: "journald",
    label: "journald",
    title: "systemd journal",
    description: "Forward system logs from journald to SignalDB.",
    steps: [
      "Install the OpenTelemetry Collector (journald receiver requires otelcol-contrib)",
      "Configure the journald receiver and an OTLP exporter to SignalDB",
      "Set authentication and tenant headers",
    ],
    snippet: (tenant, dataset, host) => `# otel-collector-config.yaml
receivers:
  journald:
    directory: /var/log/journal
    units:
      - ssh
      - kubelet

exporters:
  otlp:
    endpoint: ${host}:${OTLP_GRPC_PORT}
    tls:
      insecure: true
    headers:
      Authorization: "Bearer YOUR_API_KEY"
      X-Tenant-ID: "${tenant}"
      X-Dataset-ID: "${dataset}"

service:
  pipelines:
    logs:
      receivers: [journald]
      exporters: [otlp]`,
  },
  {
    id: "prometheus",
    label: "Prometheus",
    title: "Prometheus",
    description: "Scrape metrics and remote_write to SignalDB.",
    steps: [
      "Configure Prometheus remote_write to SignalDB",
      "Set authentication headers",
      "Optional: use Prometheus Agent mode",
    ],
    snippet: (tenant, dataset, host) => `# prometheus.yml
global:
  scrape_interval: 15s

remote_write:
  - url: http://${host}:${OTLP_HTTP_PORT}/api/v1/write
    authorization:
      type: Bearer
      credentials: YOUR_API_KEY
    headers:
      X-Tenant-ID: ${tenant}
      X-Dataset-ID: ${dataset}

scrape_configs:
  - job_name: node
    static_configs:
      - targets:
        - localhost:9100`,
  },
];

interface Props {
  state: Pick<ExploreState, "tenant" | "dataset">;
}

export function Instrumentation({ state }: Props) {
  const [selectedSource, setSelectedSource] = useState<SourceId>("otel-sdk");
  const { data: who } = useQuery({
    queryKey: ["whoami", state.tenant, state.dataset],
    queryFn: () => whoami(),
  });

  const tenant = who?.tenant.id ?? state.tenant;
  const dataset = state.dataset;
  // The acceptor (ingest) typically shares a host with the router (this
  // page) but listens on different ports — see OTLP_GRPC_PORT/OTLP_HTTP_PORT
  // above. Default to the browser's current hostname rather than an
  // internal-looking service name; users on a different ingest host can
  // substitute their own.
  const host = window.location.hostname;

  const source = SOURCES.find((s) => s.id === selectedSource)!;

  return (
    <div className="instrumentation-page">
      <header>
        <h1>Send data</h1>
        <p className="description">
          Configure your application to send telemetry.
        </p>
      </header>
      <div className="instrumentation-grid">
        <aside className="source-selector">
          {SOURCES.map((src) => (
            <button
              key={src.id}
              className={`source-button ${selectedSource === src.id ? "selected" : ""}`}
              onClick={() => setSelectedSource(src.id)}
            >
              {src.label}
            </button>
          ))}
        </aside>
        <main className="content-area">
          <div className="content-panel">
            <h2>{source.title}</h2>
            <p>{source.description}</p>
            <ol className="steps">
              {source.steps.map((step, idx) => (
                <li key={idx}>{step}</li>
              ))}
            </ol>
            <div className="code-snippet">
              <div className="code-header">
                <span>Configuration snippet</span>
                <CopyValueButton
                  value={source.snippet(tenant, dataset, host)}
                  label="Copy snippet"
                />
              </div>
              <pre>
                <code>{source.snippet(tenant, dataset, host)}</code>
              </pre>
            </div>
            <div className="verification">
              <h3>Verification</h3>
              <div className="status-list">
                <div className="status-item">
                  <span className="status-icon waiting">·</span>
                  <span>Traces</span>
                  <span className="status-text">Waiting for data</span>
                </div>
                <div className="status-item">
                  <span className="status-icon waiting">·</span>
                  <span>Logs</span>
                  <span className="status-text">Waiting for data</span>
                </div>
                <div className="status-item">
                  <span className="status-icon waiting">·</span>
                  <span>Metrics</span>
                  <span className="status-text">Waiting for data</span>
                </div>
                <div className="status-item">
                  <span className="status-icon waiting">·</span>
                  <span>Profiles</span>
                  <span className="status-text">Waiting for data</span>
                </div>
              </div>
            </div>
          </div>
        </main>
      </div>
    </div>
  );
}
