## MODIFIED Requirements

### Requirement: MCP tools cover the full client capability set

The MCP server SHALL additionally expose processor tools: `list_processors`,
`get_processor`, `validate_processor`, `test_processor` (require
`processors:read`) and `create_processor`, `replace_processor`,
`delete_processor` (require `processors:write`). Each takes the `tenant`
parameter validated against the caller's tenant scope and calls the generated
SDK.

#### Scenario: Processor management is available as tools

- **WHEN** an MCP client lists available tools
- **THEN** the list includes the seven processor tools above, with
  descriptions that state the OTTL subset and that changes apply at ingest
  within the reload interval
