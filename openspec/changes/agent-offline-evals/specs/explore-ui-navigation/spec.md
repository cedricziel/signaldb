## ADDED Requirements

### Requirement: Evaluate navigation group

The navigation sidebar, mobile drawer and command palette SHALL include an
Evaluate group, placed between Investigate and Configure, with the pages
Agents & scores (`/evals`), Compare (`/evals/compare`), Runs
(`/evals/runs`) and Evaluators (`/evals/evaluators`); Eval sets
(`/evals/sets`) joins the group with the `agent-eval-sets` capability. The current page SHALL be the item
with the longest path prefix of the location, so nested Evaluate pages
keep their own item current. Evaluate links SHALL carry the time range and
tenant/dataset context.

#### Scenario: Nested paths highlight their own item

- **WHEN** a user opens `/evals/runs`
- **THEN** "Runs" is the current page in the sidebar, not "Agents & scores"

#### Scenario: The case drilldown belongs to Compare

- **WHEN** a user opens `/evals/compare/case?case=case-117`
- **THEN** "Compare" is the current page in the sidebar
