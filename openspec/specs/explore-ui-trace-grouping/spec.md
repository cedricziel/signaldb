# explore-ui-trace-grouping Specification

## Purpose
Guarantees that the traces tab's group table describes the whole selected window
— its counts and latency percentiles measured across every matching record, not
derived from the sample of rows the view happened to fetch — and that what a row
counts is stated rather than assumed.
## Requirements
### Requirement: Group aggregates are exact for the window

Each group's record count and latency percentiles SHALL be computed across every
record matching the active filters in the selected window. They SHALL NOT be
derived from a row-limited result set.

#### Scenario: Aggregates are independent of how many rows the view fetches

- **WHEN** the group table's row budget is changed
- **THEN** each group's count and percentiles are unchanged
- **AND** they still reflect every matching record in the window

#### Scenario: Percentiles describe the window, not the newest records

- **WHEN** the window holds more matching records than the view displays rows
- **THEN** a group's p95 is the p95 of all its records in the window
- **AND** it is not the p95 of only the most recent ones

#### Scenario: The window's own bounds are the only time filter

- **WHEN** the selected time range changes
- **THEN** every group's aggregates are recomputed for the new range

### Requirement: Each group line reports rate, errors and duration

A group line SHALL carry the RED measures for its records in the window: the
request count and the rate derived from it over the window, the number of those
records in error, and duration percentiles. The percentiles SHALL be computed
over the group's records as a whole, not over a subset of them presented as the
whole.

#### Scenario: A group line carries all of RED

- **WHEN** the group table is presented
- **THEN** each line shows a count, a rate over the selected window, an error
  count, and duration percentiles

#### Scenario: A group with no errors reports zero, and is still listed

- **WHEN** no record in a group is in error
- **THEN** the group is listed with an error count of zero
- **AND** it is not omitted from the table

#### Scenario: Percentiles cover the whole group

- **WHEN** a group holds records in more than one status
- **THEN** its percentiles are over all of the group's records
- **AND** they are not the percentiles of any single status presented as the
  group's

#### Scenario: Rate follows the window

- **WHEN** the selected window is widened or narrowed with the same matching
  records per unit time
- **THEN** each group's rate stays comparable across the two windows

### Requirement: The row budget buys groups, not records

The view's row budget SHALL bound the number of groups presented, not the number
of records the aggregates are computed over.

#### Scenario: A busy window still fills the table with groups

- **WHEN** the window holds far more matching records than the row budget
- **THEN** the table presents up to the budget in groups
- **AND** every presented group's aggregates cover all of its records

### Requirement: Sorting reranks the whole window, not the presented page

Changing the sort SHALL rerank every group in the window, not reorder the groups
already presented. The presented set SHALL be those ranking highest under the
sort in effect.

#### Scenario: Sorting by latency shows the slowest groups

- **WHEN** the window holds more groups than the row budget and a user sorts by
  a latency percentile
- **THEN** the groups presented are the slowest in the window
- **AND** they are not merely the slowest among the previously presented groups

#### Scenario: A sort the presented page cannot answer still resolves

- **WHEN** the presented groups were selected under one sort and a user picks a
  different one
- **THEN** the table reflects the new ranking over the whole window

#### Scenario: Sorting by rate agrees with sorting by count

- **WHEN** a user sorts by rate over a fixed window
- **THEN** the ordering matches sorting by count

### Requirement: Loading states preserve the table's shape

While results are pending the view SHALL hold the shape of the content it is
loading rather than collapsing, so the surrounding layout does not shift when
rows arrive. The pending state SHALL be conveyed to assistive technology rather
than presented as a run of empty cells.

#### Scenario: Re-sorting does not collapse the table

- **WHEN** a sort change refetches the groups
- **THEN** the table keeps its header and row shape while the query is in flight

#### Scenario: The pending state is announced, not spelled out

- **WHEN** the group table is loading
- **THEN** the region is marked busy for assistive technology
- **AND** the placeholder content is not announced as data

### Requirement: A truncated group set says so

When more groups exist than the view presents, the omission SHALL be stated. The
presented groups SHALL be those ranking highest under the active sort.

#### Scenario: More groups exist than are shown

- **WHEN** the window yields more distinct groups than the row budget
- **THEN** the view states that the group list is truncated
- **AND** the groups shown are the highest-ranked under the active sort

#### Scenario: The group set fits

- **WHEN** every distinct group fits within the row budget
- **THEN** no truncation is indicated

### Requirement: The grain of a group row is selectable and stated

The view SHALL offer a grain selecting what one row counts: traces, where only a
trace's root record contributes and duration is the trace's end-to-end duration;
or spans, where every matching record contributes. The active grain SHALL be
evident from the table, and the count column SHALL be labelled for it.

#### Scenario: Trace grain counts traces

- **WHEN** the grain is traces
- **THEN** a group's count is the number of traces whose root falls in the group
- **AND** its percentiles are over end-to-end trace durations

#### Scenario: Span grain counts spans

- **WHEN** the grain is spans
- **THEN** a group's count is the number of matching spans
- **AND** the count agrees with the span-level totals the tab's volume chart and
  facet counts report for the same window and filters

#### Scenario: The count column names what it counts

- **WHEN** the grain changes
- **THEN** the count column's label changes to match

### Requirement: Filters apply to the records that are counted

Active filters SHALL narrow the records the aggregates are computed over, under
the same grain the table presents. A filter that no record satisfies SHALL
produce an empty table rather than an unfiltered one.

#### Scenario: A filter narrows every group

- **WHEN** a filter is added
- **THEN** each group's count and percentiles cover only records satisfying it

#### Scenario: Trace grain restricts matching to the root record

- **WHEN** the grain is traces and a filter names a field carried only by
  non-root records
- **THEN** the table is empty rather than showing unfiltered groups

### Requirement: An unresolvable grouping dimension is not presented as data

A dimension the backend cannot resolve SHALL NOT be rendered as a single group
holding the window's total. The view SHALL distinguish "this dimension is not
queryable" from "no record carries a value for it".

#### Scenario: An unknown dimension is reported, not counted

- **WHEN** the chosen grouping dimension cannot be resolved for the tenant
- **THEN** the view reports the dimension as unavailable
- **AND** it does not present a group whose only label is a placeholder for the
  absent value

#### Scenario: A resolvable dimension absent from some records

- **WHEN** the dimension resolves but some records carry no value for it
- **THEN** those records form a distinct group labelled as having no value
- **AND** records that do carry values form their own groups

### Requirement: Drilling into a group narrows to that group's traces

Selecting a group SHALL present the traces belonging to it, matching the same
filters, grain and window as the aggregate that produced the group.

#### Scenario: The drill-in agrees with the group row

- **WHEN** a group is selected
- **THEN** the traces listed all carry that group's dimension values
- **AND** they satisfy the filters active when the group was computed

#### Scenario: The drill-in list is bounded and says so

- **WHEN** a group holds more traces than the drill-in presents
- **THEN** the list states that it is bounded
- **AND** the group row's count still reports the group's full size

