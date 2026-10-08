## RENAMED Requirements

- FROM: `### Requirement: Root path redirects to the logs view`
- TO: `### Requirement: Root path redirects to the overview`

## MODIFIED Requirements

### Requirement: Root path redirects to the overview

Navigating to the site root SHALL redirect to `/overview`, preserving any
query string from the original URL.

#### Scenario: Root redirects to the overview

- **WHEN** a user opens `/`
- **THEN** the browser URL becomes `/overview` and the overview renders

#### Scenario: Root redirect preserving query params

- **WHEN** a user opens `/?tenant=acme&dataset=prod`
- **THEN** the browser URL becomes `/overview?tenant=acme&dataset=prod`
