// What `POST /api/v1/processors:test` sends back for the logs sample after
// `set(attributes["user.email"], "[redacted]")`: the server decodes the
// payload into the OTLP protobuf types and encodes it again, so every
// field comes back in proto order, zero values included.
export const REDACTED_LOGS_ECHO = {
  resourceLogs: [
    {
      resource: {
        attributes: [
          { key: "service.name", value: { stringValue: "checkout" } },
        ],
        droppedAttributesCount: 0,
        entityRefs: [],
      },
      scopeLogs: [
        {
          scope: {
            name: "checkout-handler",
            version: "",
            attributes: [],
            droppedAttributesCount: 0,
          },
          logRecords: [
            {
              timeUnixNano: "1700000000000000000",
              observedTimeUnixNano: "0",
              severityNumber: 9,
              severityText: "INFO",
              body: { stringValue: "order placed" },
              attributes: [
                { key: "user.email", value: { stringValue: "[redacted]" } },
              ],
              droppedAttributesCount: 0,
              flags: 0,
              traceId: "",
              spanId: "",
              eventName: "",
            },
          ],
          schemaUrl: "",
        },
      ],
      schemaUrl: "",
    },
  ],
};
