// Shared fixtures for the processors feature tests. Shapes mirror the
// generated types in `api/gen/types.gen.ts`.
import { outletContextRoute } from "../../test/render";
import { DEFAULT_STATE } from "../../lib/urlState";

export function shellOutlet(tenant = "acme", dataset = "") {
  return outletContextRoute({ ...DEFAULT_STATE, tenant, dataset });
}

export const WHOAMI_TENANT_ADMIN = {
  user: {
    id: "user-1",
    email: "alice@example.com",
    display_name: "Alice",
    is_instance_admin: false,
  },
  memberships: [{ tenant_id: "acme", role: "admin" }],
  tenant: { id: "acme", slug: "acme", name: "Acme Corp" },
  datasets: [
    { id: "production", slug: "production", is_default: true },
    { id: "staging", slug: "staging", is_default: false },
  ],
  default_dataset: "production",
};

export const WHOAMI_MEMBER = {
  ...WHOAMI_TENANT_ADMIN,
  memberships: [{ tenant_id: "acme", role: "member" }],
};

export const PROCESSORS_LIST = {
  processors: [
    {
      name: "redact-emails",
      description: "Redact user emails",
      signal: "logs",
      dataset: "production",
      enabled: true,
      priority: 100,
      error_mode: "ignore",
      statements: ['set(attributes["user.email"], "[redacted]")'],
      tenant_id: "acme",
      created_at: "2026-08-01T00:00:00Z",
      updated_at: "2026-08-14T10:00:00Z",
      status: "ok",
    },
    {
      name: "strip-query",
      description: null,
      signal: "traces",
      dataset: null,
      enabled: false,
      priority: 50,
      error_mode: "silent",
      statements: ['replace_pattern(attributes["url.full"], "\\\\?.*$", "")'],
      tenant_id: "acme",
      created_at: "2026-07-01T00:00:00Z",
      updated_at: "2026-07-02T00:00:00Z",
      status: "invalid",
    },
  ],
};

export const VALIDATION_ERROR = {
  errors: [
    { statement: 1, column: 0, message: "merge_maps is not supported" },
  ],
};

export const VALIDATION_OK = { errors: [] };

export const TEST_RESPONSE = {
  payload: {
    resourceLogs: [
      {
        resource: { attributes: [] },
        scopeLogs: [
          {
            scope: {},
            logRecords: [
              {
                body: { stringValue: "order placed" },
                attributes: [
                  {
                    key: "user.email",
                    value: { stringValue: "[redacted]" },
                  },
                ],
              },
            ],
          },
        ],
      },
    ],
  },
  statements: [{ processor: "redact-emails", index: 0, matched: 1, errors: 0 }],
};
