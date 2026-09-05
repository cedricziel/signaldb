import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import type { ExploreState } from "../../lib/urlState";
import {
  connectionInfo,
  type ConnectionInfoResponse,
} from "../../api/connection";
import { toErrorMessage } from "../../api/http";
import { CopyValueButton } from "../../components/CopyValueButton";
import { SkeletonLines } from "../explore/Skeleton";
import "./Instrumentation.css";

/** YAML `tls:` block for a collector-style OTLP exporter, indented to match
 * the surrounding `endpoint:`/`headers:` lines. Omitted entirely for a TLS
 * endpoint — the exporter's default (verify) is what you want there. */
function tlsBlock(tls: boolean, indent: string): string {
  if (tls) return "";
  return `\n${indent}tls:\n${indent}  insecure: true`;
}

/** Go SDK line pairing a plaintext gRPC endpoint with `WithInsecure()`;
 * empty for a TLS endpoint, where the exporter's default (verify) applies. */
function grpcInsecureLine(tls: boolean): string {
  if (tls) return "";
  return "\n        otlptracegrpc.WithInsecure(),";
}

/** The credential a client config's `credentials:`/`Bearer `-style fields
 * take, extracted from the server's `Authorization: Bearer <placeholder>`
 * header contract. */
function bearerCredential(authorization: string): string {
  return authorization.replace(/^Bearer\s+/, "");
}

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
  snippet: (info: ConnectionInfoResponse) => string;
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
    snippet: (info) => `// Example for Go
package main

import (
    "context"
    "log"

    "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
)

func main() {
    ctx := context.Background()
    exp, err := otlptracegrpc.New(ctx,
        otlptracegrpc.WithEndpoint("${info.ingest.otlp_grpc.authority}"),${grpcInsecureLine(info.ingest.otlp_grpc.tls)}
        otlptracegrpc.WithHeaders(map[string]string{
            "Authorization": "${info.headers.authorization}",
            "X-Tenant-ID":   "${info.headers["x-tenant-id"]}",
            "X-Dataset-ID":  "${info.headers["x-dataset-id"]}",
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
export OTEL_EXPORTER_OTLP_ENDPOINT="${info.otel_env.OTEL_EXPORTER_OTLP_ENDPOINT}"
export OTEL_EXPORTER_OTLP_HEADERS="${info.otel_env.OTEL_EXPORTER_OTLP_HEADERS}"`,
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
    snippet: (info) => `# otel-collector-config.yaml
receivers:
  otlp:
    protocols:
      grpc:
      http:

exporters:
  otlp:
    endpoint: ${info.ingest.otlp_grpc.authority}${tlsBlock(info.ingest.otlp_grpc.tls, "    ")}
    headers:
      Authorization: "${info.headers.authorization}"
      X-Tenant-ID: "${info.headers["x-tenant-id"]}"
      X-Dataset-ID: "${info.headers["x-dataset-id"]}"

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
    snippet: (info) => `# values.yaml for opentelemetry-collector Helm chart
config:
  receivers:
    otlp:
      protocols:
        grpc:
        http:
  exporters:
    otlp:
      endpoint: ${info.ingest.otlp_grpc.authority}${tlsBlock(info.ingest.otlp_grpc.tls, "      ")}
      headers:
        Authorization: "${info.headers.authorization}"
        X-Tenant-ID: "${info.headers["x-tenant-id"]}"
        X-Dataset-ID: "${info.headers["x-dataset-id"]}"
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
    snippet: (info) => `# docker-compose.yml
version: '3'
services:
  app:
    image: myapp:latest
    environment:
      OTEL_EXPORTER_OTLP_ENDPOINT: ${info.otel_env.OTEL_EXPORTER_OTLP_ENDPOINT}
      OTEL_EXPORTER_OTLP_HEADERS: "${info.otel_env.OTEL_EXPORTER_OTLP_HEADERS}"
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
      "Install the OpenTelemetry Collector (journald receiver requires otelcol-contrib)",
      "Configure the journald receiver and an OTLP exporter to SignalDB",
      "Set authentication and tenant headers",
    ],
    snippet: (info) => `# otel-collector-config.yaml
receivers:
  journald:
    directory: /var/log/journal
    units:
      - ssh
      - kubelet

exporters:
  otlp:
    endpoint: ${info.ingest.otlp_grpc.authority}${tlsBlock(info.ingest.otlp_grpc.tls, "    ")}
    headers:
      Authorization: "${info.headers.authorization}"
      X-Tenant-ID: "${info.headers["x-tenant-id"]}"
      X-Dataset-ID: "${info.headers["x-dataset-id"]}"

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
    snippet: (info) => `# prometheus.yml
global:
  scrape_interval: 15s

remote_write:
  - url: ${info.ingest.prometheus_remote_write}
    authorization:
      type: Bearer
      credentials: ${bearerCredential(info.headers.authorization)}
    headers:
      X-Tenant-ID: ${info.headers["x-tenant-id"]}
      X-Dataset-ID: ${info.headers["x-dataset-id"]}

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
  const connection = useQuery({
    queryKey: ["connection", state.tenant, state.dataset],
    queryFn: () => connectionInfo(),
    staleTime: 5 * 60_000,
    retry: false,
  });

  const notes = connection.data?.notes ?? [];
  const source = SOURCES.find((s) => s.id === selectedSource)!;
  const snippet = useMemo(
    () => (connection.data ? source.snippet(connection.data) : ""),
    [source, connection.data],
  );

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
            {notes.length > 0 && (
              <div className="warn-callout instrumentation-note" role="note">
                {notes.map((note, idx) => (
                  <p key={idx}>{note}</p>
                ))}
              </div>
            )}
            {connection.isError ? (
              <div className="instrumentation-error" role="alert">
                <p>
                  Could not load connection details:{" "}
                  {toErrorMessage(connection.error)}
                </p>
                <button
                  type="button"
                  onClick={() => void connection.refetch()}
                >
                  Retry
                </button>
              </div>
            ) : (
              <>
                <div className="code-snippet">
                  <div className="code-header">
                    <span>Configuration snippet</span>
                    <CopyValueButton value={snippet} label="Copy snippet" />
                  </div>
                  {connection.isLoading ? (
                    <SkeletonLines lines={8} />
                  ) : (
                    <pre>
                      <code>{snippet}</code>
                    </pre>
                  )}
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
              </>
            )}
          </div>
        </main>
      </div>
    </div>
  );
}
