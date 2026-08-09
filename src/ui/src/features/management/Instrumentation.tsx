import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import type { ExploreState } from "../../lib/urlState";
import { whoami } from "../../api/session";
import { CopyValueButton } from "../../components/CopyValueButton";
import "./Instrumentation.css";

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
  snippet: (tenant: string, dataset: string, endpoint: string) => string;
  envVars?: {
    OTEL_EXPORTER_OTLP_ENDPOINT?: string;
    OTEL_EXPORTER_OTLP_HEADERS?: string;
    // others
  };
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
    snippet: (tenant, dataset, endpoint) => `// Example for Go
package main

import (
    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
)

func main() {
    ctx := context.Background()
    exp, err := otlptracegrpc.New(ctx,
        otlptracegrpc.WithEndpoint("${endpoint}"),
        otlptracegrpc.WithHeaders(map[string]string{
            "Authorization": "Bearer YOUR_API_KEY",
            "X-Tenant-ID":   "${tenant}",
            "X-Dataset-ID":  "${dataset}",
        }),
    )
    // ...
}

// Environment variables:
export OTEL_EXPORTER_OTLP_ENDPOINT="http://${endpoint}"
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
    snippet: (tenant, dataset, endpoint) => `# otel-collector-config.yaml
receivers:
  otlp:
    protocols:
      grpc:
      http:

exporters:
  otlp:
    endpoint: ${endpoint}:4317
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
      endpoint,
    ) => `# values.yaml for opentelemetry-collector Helm chart
config:
  receivers:
    otlp:
      protocols:
        grpc:
        http:
  exporters:
    otlp:
      endpoint: ${endpoint}:4317
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
    snippet: (tenant, dataset, endpoint) => `# docker-compose.yml
version: '3'
services:
  app:
    image: myapp:latest
    environment:
      OTEL_EXPORTER_OTLP_ENDPOINT: http://${endpoint}
      OTEL_EXPORTER_OTLP_HEADERS: "Authorization=Bearer YOUR_API_KEY,X-Tenant-ID=${tenant},X-Dataset-ID=${dataset}"
  otel-collector:
    image: otel/opentelemetry-collector-contrib:latest
    volumes:
      - ./otel-config.yaml:/etc/otel/config.yaml
    ports:
      - "4317:4317"`,
  },
  {
    id: "journald",
    label: "journald",
    title: "systemd journal",
    description: "Forward system logs from journald to SignalDB.",
    steps: [
      "Install Promtail or Vector for journal scraping",
      "Configure OTLP exporter to SignalDB",
      "Set authentication headers",
    ],
    snippet: (tenant, dataset, endpoint) => `# promtail-config.yaml
server:
  http_listen_port: 9080
  grpc_listen_port: 0

positions:
  filename: /tmp/positions.yaml

clients:
  - url: http://${endpoint}:4317/v1/logs
    headers:
      Authorization: Bearer YOUR_API_KEY
      X-Tenant-ID: ${tenant}
      X-Dataset-ID: ${dataset}

scrape_configs:
  - job_name: journal
    journal:
      path: /var/log/journal
      labels:
        job: journal
    relabel_configs:
      - source_labels: [__journal__hostname]
        target_label: host
    batch:
      timeout: -1`,
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
    snippet: (tenant, dataset, endpoint) => `# prometheus.yml
global:
  scrape_interval: 15s

remote_write:
  - url: http://${endpoint}/api/v1/prometheus/write
    headers:
      Authorization: Bearer YOUR_API_KEY
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
  const dataset = state.dataset ?? "default";
  // In a real app we might get endpoint from config, but default to router:3000
  const endpoint = "router:3000";

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
                  value={source.snippet(tenant, dataset, endpoint)}
                  label="Copy snippet"
                />
              </div>
              <pre>
                <code>{source.snippet(tenant, dataset, endpoint)}</code>
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
