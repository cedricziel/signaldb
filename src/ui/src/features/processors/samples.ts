// Small, realistic OTLP/JSON export payloads preloaded into the dry-run test
// panel, one per signal — each carries a PII-ish attribute so the default
// redaction example ("set user.email to [redacted]") has something to hit.

export const SAMPLE_PAYLOADS: Record<"traces" | "logs" | "metrics", unknown> =
  {
    traces: {
      resourceSpans: [
        {
          resource: {
            attributes: [
              { key: "service.name", value: { stringValue: "checkout" } },
            ],
          },
          scopeSpans: [
            {
              scope: { name: "checkout-handler" },
              spans: [
                {
                  traceId: "5b8aa5a2d2c872e8321cf37308d69df2",
                  spanId: "051581bf3cb55c13",
                  name: "POST /checkout",
                  kind: 2,
                  startTimeUnixNano: "1700000000000000000",
                  endTimeUnixNano: "1700000000123000000",
                  attributes: [
                    {
                      key: "user.email",
                      value: { stringValue: "alice@example.com" },
                    },
                    {
                      key: "url.full",
                      value: {
                        stringValue: "https://shop.example/checkout?ref=ad1",
                      },
                    },
                  ],
                  status: { code: 1 },
                },
              ],
            },
          ],
        },
      ],
    },
    logs: {
      resourceLogs: [
        {
          resource: {
            attributes: [
              { key: "service.name", value: { stringValue: "checkout" } },
            ],
          },
          scopeLogs: [
            {
              scope: { name: "checkout-handler" },
              logRecords: [
                {
                  timeUnixNano: "1700000000000000000",
                  severityText: "INFO",
                  severityNumber: 9,
                  body: { stringValue: "order placed" },
                  attributes: [
                    {
                      key: "user.email",
                      value: { stringValue: "alice@example.com" },
                    },
                  ],
                },
              ],
            },
          ],
        },
      ],
    },
    metrics: {
      resourceMetrics: [
        {
          resource: {
            attributes: [
              { key: "service.name", value: { stringValue: "checkout" } },
            ],
          },
          scopeMetrics: [
            {
              scope: { name: "checkout-handler" },
              metrics: [
                {
                  name: "checkout.active_sessions",
                  description: "Active checkout sessions",
                  unit: "1",
                  gauge: {
                    dataPoints: [
                      {
                        timeUnixNano: "1700000000000000000",
                        asInt: "42",
                        attributes: [
                          {
                            key: "user.email",
                            value: { stringValue: "alice@example.com" },
                          },
                        ],
                      },
                    ],
                  },
                },
              ],
            },
          ],
        },
      ],
    },
  };
