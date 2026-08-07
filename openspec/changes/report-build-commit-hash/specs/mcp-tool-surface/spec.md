## ADDED Requirements

### Requirement: server_info reports the MCP server's build commit

The `server_info` MCP tool SHALL include the git commit hash the `signaldb-mcp` server binary was built from, alongside its existing `version` field, following the same build-provenance and fallback rules as the CLI (dirty-build marker, `unknown` placeholder for non-git builds).

#### Scenario: MCP client calls server_info

- **WHEN** an MCP client invokes the `server_info` tool
- **THEN** the response includes both the server's semantic version and the git commit hash it was built from
