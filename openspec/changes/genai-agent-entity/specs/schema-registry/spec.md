# Spec Delta

## ADDED Requirements

### Requirement: Bundled SignalDB registry defines the GenAI agent entity

The bundled `signaldb` registry SHALL define an entity type named `gen_ai.agent`
whose identifying attribute is `gen_ai.agent.id` and whose descriptive attributes
are `gen_ai.agent.name`, `gen_ai.agent.description` and `gen_ai.agent.version`. The
entity SHALL reference the attribute definitions visible to every tenant rather than
define new ones, so an attribute lookup for any of these keys returns the same
definitions as before this change. The entity's description SHALL state that
`gen_ai.agent.id` identifies a hosted agent by a provider-assigned, stable
identifier and is not an in-memory instance id.

#### Scenario: Agent entity resolves for a fresh tenant

- **WHEN** a newly created tenant with no custom registries resolves entity type
  `gen_ai.agent`
- **THEN** the response contains one hit tagged with the `signaldb` namespace and
  bundled source, listing `gen_ai.agent.id` as identifying and
  `gen_ai.agent.name`, `gen_ai.agent.description` and `gen_ai.agent.version` as
  descriptive

#### Scenario: Agent attributes report their entity role

- **WHEN** a client resolves attribute `gen_ai.agent.id`
- **THEN** the hit lists `gen_ai.agent` among the entities in which it plays a role,
  with role `identifying`

#### Scenario: Tenant can extend the agent entity

- **WHEN** a tenant registry defines an entity that extends `gen_ai.agent` and adds
  a descriptive attribute
- **THEN** the registry validates, and resolving `gen_ai.agent` for that tenant
  lists the tenant's entity among those extending it
