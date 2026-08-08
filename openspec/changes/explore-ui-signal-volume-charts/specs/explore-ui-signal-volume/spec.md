## Purpose

Guarantees that the explore UI's signal-volume charts tell the truth about the
selected window and let a user read the value of any bucket they can see — so
that "the chart looks flat" means "the data is flat", never "the renderer lost
it" or "a row limit truncated it".

## ADDED Requirements

### Requirement: Volume charts cover the full selected window

A signal-volume chart SHALL be driven by a server-side aggregate evaluated over
the entire selected time window. It SHALL NOT be derived by bucketing a
result set that a row limit has truncated.

#### Scenario: The row limit does not bound the chart

- **WHEN** a tab's row query returns exactly its configured limit of rows
- **THEN** the volume chart still shows every bucket in the selected window that
  has data
- **AND** changing the row limit does not change the chart

#### Scenario: Buckets without data render as empty, not absent

- **WHEN** the window contains buckets for which the aggregate returned no rows
- **THEN** those buckets occupy their position in the chart at zero height
- **AND** the buckets that do have data keep their true position on the time
  axis

### Requirement: Bucket magnitude is legible across the distribution

A bucket's rendered height SHALL be a function of the bucket total, floored once
per bar rather than once per stacked segment, so that buckets differing by
orders of magnitude are visually distinguishable.

#### Scenario: Small buckets are distinguishable from each other

- **WHEN** one bucket holds two orders of magnitude more than another and both
  are far below the window maximum
- **THEN** the two bars render at different heights

#### Scenario: A non-empty bucket is always visible

- **WHEN** a bucket's total is greater than zero but negligible against the
  window maximum
- **THEN** the bar renders at no less than one pixel
- **AND** the floor is applied once to the bar, not once per stacked series

### Requirement: A scale control governs the chart's vertical mapping

The chart SHALL offer a linear and a logarithmic vertical scale, defaulting to
linear. In logarithmic mode the transform SHALL apply to the bucket total, with
each stacked segment taking its linear share of the resulting bar height, so
segment heights always sum to the bar.

#### Scenario: Log mode preserves stack composition

- **WHEN** the chart is in logarithmic mode and a bucket contains several series
- **THEN** each series' segment occupies its proportional share of the bar
- **AND** the segments sum to exactly the bar's height

#### Scenario: The selected scale travels with a shared link

- **WHEN** a user selects a scale and shares the resulting URL
- **THEN** opening that URL reproduces the same scale
- **AND** a URL that does not specify a scale opens in linear mode

### Requirement: Chart axes are labelled

The chart SHALL render a vertical axis conveying the window maximum, and a time
axis whose labels identify each end of the window unambiguously.

#### Scenario: A flat region is readable as a magnitude

- **WHEN** every bucket except one is far below the window maximum
- **THEN** the vertical axis states the maximum
- **AND** a user can distinguish a genuinely-empty region from a low-but-
  non-zero one

#### Scenario: A multi-day window has unambiguous end labels

- **WHEN** the selected window spans more than one calendar day
- **THEN** the time-axis labels include the date
- **AND** the two ends of the axis do not render as the same string

### Requirement: Every bucket's values are interrogable

Hovering or focusing any bucket SHALL reveal that bucket's timestamp, the value
of each series present, and the bucket total. The interaction target SHALL span
the full plot height, independent of the bar's rendered height.

#### Scenario: A one-pixel bar is interrogable

- **WHEN** a bucket renders at its minimum height
- **THEN** pointing anywhere in that bucket's column, at any height, reveals its
  values

#### Scenario: The breakdown reports every series

- **WHEN** a bucket contains several series
- **THEN** the revealed detail lists each series with its own value
- **AND** states the bucket total

#### Scenario: Values are reachable without a pointer

- **WHEN** a user moves through the chart with the keyboard
- **THEN** each bucket can take focus and reveals the same detail as hovering

### Requirement: The traces tab presents trace volume

The traces tab SHALL render a volume chart for the selected window, stacked by
span status, subject to every requirement above.

#### Scenario: Trace volume is stacked by status

- **WHEN** the traces tab loads with a selected window
- **THEN** a volume chart renders with one stacked series per span status
- **AND** failing and non-failing volume are separately visible

#### Scenario: Trace volume is independent of the trace list

- **WHEN** the trace list is truncated by its row limit
- **THEN** the volume chart still reflects every trace in the window

### Requirement: Row-count indicators are distinct from the chart

A row-count or row-limit indicator SHALL NOT be presented as part of a volume
chart, so that a chart's appearance is never attributed to a row limit that does
not govern it.

#### Scenario: The limit badge does not read as a chart caption

- **WHEN** a tab shows both a row-limit indicator and a volume chart
- **THEN** the indicator is visually separate from the chart
