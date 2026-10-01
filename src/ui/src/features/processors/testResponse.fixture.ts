// What `POST /api/v1/processors:test` sends back for the logs sample and
// `set(attributes["user.email"], "[redacted]")`. The server decodes the
// payload into the OTLP protobuf types and encodes it again, so both `input`
// and `payload` come back in proto field order, zero values included.
function logsEcho(email: string) {
  return {
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
                  { key: "user.email", value: { stringValue: email } },
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
}

export const REDACT_EMAILS_TEST_RESPONSE = {
  input: logsEcho("alice@example.com"),
  payload: logsEcho("[redacted]"),
  statements: [{ processor: "redact-emails", index: 0, matched: 1, errors: 0 }],
};
