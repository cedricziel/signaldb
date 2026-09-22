# explore-ui-catalog Specification

## Purpose

Defines the Catalog tab's entity detail behavior: navigating from a matching
span into the trace waterfall, surfacing per-entity-type read-only detail
tables (e.g. top database statements), color-coding the trace waterfall by
span kind, and showing a service's time-by-dependency-category breakdown.

## Requirements

### Requirement: Matching-span rows open the trace waterfall

A "recent matching spans" row on a Catalog entity detail page SHALL, when
clicked, switch to the Traces signal and open that span's trace in the
waterfall view.

#### Scenario: Opening a matching span

- **WHEN** a user clicks a row in an entity detail page's recent matching
  spans list
- **THEN** the explore UI switches to the Traces signal and renders the
  waterfall for that span's trace id

### Requirement: Read-only top-values table for supporting entity types

An entity type MAY define a read-only top-values table, distinct from the
drillable breakdown table, that ranks distinct values of one field by
frequency within the entity's pinned identity and time window. Clicking a
row in this table SHALL NOT drill into a secondary pin or otherwise change
navigation state.

#### Scenario: Top statements for a database entity

- **WHEN** a user views a database entity's detail page
- **THEN** a "Top statements" table lists distinct `db.query.text` values
  observed for that database, ranked by frequency, and clicking a row does
  not navigate

### Requirement: Trace waterfall spans are color-coded by kind

The trace waterfall SHALL color-code each span's bar according to its
`span.kind` (SERVER, CLIENT, INTERNAL, PRODUCER, CONSUMER), display a
legend when kind data is available, and show the kind in the span detail
panel. Kind data SHALL be sourced via a dedicated Query IR query and SHALL
NOT be added to the Tempo-compatible trace response.

#### Scenario: Waterfall bars reflect span kind

- **WHEN** a trace's spans have resolvable `span.kind` values
- **THEN** each waterfall bar is colored according to its kind, a legend
  enumerating the kinds present is shown, and the span detail panel
  displays the selected span's kind

### Requirement: Service time-by-dependency-category breakdown

A service's own Catalog entity detail page SHALL show a breakdown of
summed CLIENT-span duration by dependency category — database, HTTP, RPC,
messaging — derived from the presence of `db.system.name`,
`http.request.method`, `rpc.system`, and `messaging.system` respectively,
with any remaining CLIENT-span duration not matching a known category
shown as "Other".

#### Scenario: Viewing a service's dependency breakdown

- **WHEN** a user views a service's own Catalog entity detail page and that
  service has outbound CLIENT-kind spans in the selected window
- **THEN** a proportional bar and legend show the share of total CLIENT
  duration attributable to each dependency category and to "Other"

#### Scenario: No dependency traffic

- **WHEN** a service has no outbound CLIENT-kind spans in the selected
  window
- **THEN** the breakdown section shows an explanatory empty state instead
  of an empty bar

### Requirement: Registry-derived metrics on an entity detail page

A Catalog entity detail page SHALL show the metrics the tenant's schema
registries associate with that entity type, discovered from the registry's
metric-to-entity associations rather than from a list maintained in the UI.
An entity type the registries associate no metric with SHALL show no metrics
section, rather than an empty one.

#### Scenario: A host's system metrics

- **WHEN** a user opens the detail page of a `host` entity, and the tenant's
  registries associate `system.*` metrics with the `host` entity
- **THEN** those metrics are the ones offered on the page, without the UI
  naming any metric itself

#### Scenario: An entity type with no associated metrics

- **WHEN** a user opens the detail page of an entity type the registries
  associate no metric with
- **THEN** the page shows no metrics section at all

#### Scenario: A tenant's own registry

- **WHEN** a tenant publishes a registry associating its own metrics with an
  entity type
- **THEN** that entity type's detail page offers those metrics on the same
  terms as bundled OpenTelemetry ones, with no change to the UI

### Requirement: Entity metric series are pinned to the entity's identity

Metric series shown on an entity detail page SHALL be filtered to the entity
being viewed, using the same identity dimensions and values that pin the
page's other measurements. A page SHALL NOT show a metric aggregated across
every entity of its type.

#### Scenario: One process among many

- **WHEN** a user opens the detail page of a process entity identified by
  `process.pid` and `host.name`
- **THEN** each metric series shown is restricted to that `process.pid` on
  that `host.name`

#### Scenario: Drilled into a breakdown row

- **WHEN** a user has drilled from an entity into one of its breakdown rows
- **THEN** the metrics shown remain those of the entity itself, pinned to the
  entity's identity

### Requirement: Only metrics observed in the window are charted

An entity detail page SHALL chart only those associated metrics that have data
in the selected time window, and SHALL NOT render an associated-but-unobserved
metric as a series of zeroes. A metric that is associated and observed SHALL be
charted over the selected window.

#### Scenario: Associated metric with no data in the window

- **WHEN** a metric is associated with the entity type but has no points for
  this entity in the selected window
- **THEN** no chart is rendered for it, and it is not shown as a flat zero
  series

#### Scenario: Window narrowed past the data

- **WHEN** a user narrows the time range until an entity's metrics have no
  points in it
- **THEN** the page reports that the entity has no metric data in this window
  rather than charting zeroes

### Requirement: Metrics-only entity rows carry a sparkline

The Catalog entity list SHALL show a sparkline for an entity type that has an
associated headline metric, so that an entity type no trace ever carried is not
presented as entirely unmeasured. An entity type with no associated headline
metric, or a row with no data for it in the window, SHALL leave the column
empty rather than draw a flat line.

#### Scenario: Listing containers

- **WHEN** a user lists an entity type whose registries associate a headline
  metric with it, and rows have data for it in the window
- **THEN** each such row carries a sparkline of that metric alongside its
  existing columns

#### Scenario: No headline metric for the entity type

- **WHEN** a user lists an entity type the registries associate no metric with
- **THEN** no sparkline column is shown

### Requirement: Metric provenance is stated on the page

An entity detail page's metrics section SHALL state that its metric selection
came from the schema registries' entity associations, in the same way the
entity list states which attributes and signals fed it.

#### Scenario: Reading where the metrics came from

- **WHEN** a user views an entity detail page's metrics section
- **THEN** the section names the registry association it was discovered
  through, so the selection is explainable without reading the code

### Requirement: Catalog entity types come from the tenant's schema registries

The catalog SHALL derive the set of entity types it can discover from the entity
definitions visible to the authenticated tenant. An entity type SHALL NOT
require a code change to become catalogable, and a tenant's own registry SHALL
contribute its entity types alongside the bundled ones.

An entity type's identity SHALL be its registry-declared identifying
attributes, except where the catalog supplies its own identity for that entity
type, which SHALL take precedence. A supplied identity is warranted where the
registry declares none, and where the declared identity is unusable in
practice; the catalog SHALL NOT be limited to entity types the registry
identifies. An entity type with neither a declared nor a supplied identity SHALL
be excluded, since nothing distinguishes one instance from another.

Presentation detail that the registry does not express — display label,
ordering, which secondary dimension to break down by, and any span-kind scoping
— MAY be supplied per entity type. Such detail SHALL NOT determine whether an
entity type is discoverable: an entity type with no supplied presentation SHALL
still be discovered and listed, labelled from its registry name.

#### Scenario: A registry-declared entity type is catalogable without code change

- **WHEN** a tenant's visible registries declare an entity type with at least
  one identifying attribute, and telemetry in the window carries that attribute
- **THEN** the catalog offers that entity type, identified by its declared
  identifying attributes, with no frontend change required

#### Scenario: An entity the registry leaves unidentified is still catalogable

- **WHEN** a registry declares an entity type whose attributes are all
  descriptive, and the catalog supplies an identity for it
- **THEN** the catalog offers that entity type, identified by the supplied
  attributes

> Verification note: the OTel 1.43 registry declares `host` and `container`
> with no identifying attributes — `host.name` and `container.name` are
> descriptive. Excluding unidentified entity types therefore removes two of
> the most useful pages in the catalog, which is why supplied identity exists.

#### Scenario: An entity with no identity at all is excluded

- **WHEN** a registry declares an entity type whose attributes are all
  descriptive, and the catalog supplies no identity for it
- **THEN** the catalog does not offer that entity type

#### Scenario: A supplied identity takes precedence over the declared one

- **WHEN** an entity type's registry-declared identity differs from the
  identity the catalog supplies for it
- **THEN** instances are identified by the supplied attributes

> Verification note: a Kubernetes pod is declared as identified by
> `k8s.pod.uid` — unique, since names repeat across namespaces and restarts,
> but opaque to a reader and frequently absent from real telemetry. Precedence
> is what lets the catalog key it by name and namespace instead.

#### Scenario: A custom registry contributes entity types

- **WHEN** a tenant publishes a custom registry declaring an entity type
- **THEN** that entity type appears in the catalog on the same terms as a
  bundled one

### Requirement: Entity presence is detected across every signal source

The catalog SHALL determine which entity types are present from maintained
metadata, considering every signal source the tenant can query rather than a
fixed subset, and SHALL attribute each entity type's presence to the sources it
was observed in. An entity type SHALL be reported as present in a source when
that source's metadata reports its primary identifying attribute as present.
Detection SHALL NOT read signal data.

An entity observable only through a non-trace signal SHALL be discovered on the
same terms as one observable through traces.

This requirement constrains the observable properties — every signal covered, no
signal-data scan, presence attributed per source — not the metadata that answers
it. Any maintained source of that metadata satisfies it.

#### Scenario: An entity reporting only through metrics is discovered

- **WHEN** maintained metadata reports `process.pid` present on metrics and
  absent on traces and logs
- **THEN** the catalog reports the process entity type as present, attributing
  its presence to metrics

#### Scenario: Detection reads no signal data

- **WHEN** the catalog determines which entity types are present
- **THEN** the answer is produced from maintained metadata, and the response's
  reported cost states that no signal data was read

#### Scenario: The entity type list reflects what was observed

- **WHEN** the tenant's registries declare many entity types and telemetry in
  the window carries the identifying attributes of only some
- **THEN** the catalog lists the observed entity types, and does not present the
  unobserved ones as empty pages

### Requirement: Instance listing is a separate, on-demand read over the identity tuple

Listing an entity type's instances SHALL be a distinct operation from detecting
that the entity type is present, performed when the user opens that entity type
rather than for every entity type up front. An instance SHALL be identified by
the combination of its identifying attribute values, grouped as a tuple, so that
instances are never synthesized from independently-discovered values of separate
attributes.

Identifying attributes absent from a source SHALL be dropped from the tuple for
that source rather than grouping every instance under an empty value. When the
primary identifying attribute is absent from a source, that source SHALL
contribute no instances.

#### Scenario: Instances are grouped by the identity tuple

- **WHEN** an entity type declares two identifying attributes and both are
  present
- **THEN** each listed instance corresponds to an observed combination of the
  two values, and no combination is listed that was not observed together

#### Scenario: An absent identity dimension is dropped, not grouped as empty

- **WHEN** an entity type's secondary identifying attribute is absent from a
  source
- **THEN** instances from that source are identified by the remaining
  attributes, rather than every instance sharing one empty-valued dimension

#### Scenario: Opening one entity type does not list the others

- **WHEN** a user opens the catalog and selects one entity type
- **THEN** instances are listed for that entity type only

### Requirement: Rate, error, and duration measurements are trace-derived and stated as such

Error rate and duration percentiles SHALL be derived from trace data only, since
span status and span duration have no counterpart in other signals. Where an
entity's observations come from non-trace signals, these measurements SHALL be
reported as unavailable rather than as zero. Volume observed through different
signals SHALL NOT be summed into a single figure presented as request volume.

#### Scenario: An entity observed only outside traces reports no percentiles

- **WHEN** an entity's observations in the window come only from metrics
- **THEN** its error rate and duration percentiles are shown as unavailable, not
  as zero or `0ms`

#### Scenario: Volume from different signals is not conflated

- **WHEN** an entity is observed through more than one signal
- **THEN** the catalog does not present a single summed count as that entity's
  request volume

### Requirement: An unanalyzed entity type is reported distinctly from an absent one

When no maintained metadata covers an entity type's identifying attributes for a
source, the catalog SHALL report that the source has not been analyzed, rather
than reporting the entity type as absent or rendering an empty result. The
catalog SHALL make the distinction visible to the user and SHALL state the age
of the metadata it relied on.

#### Scenario: Missing metadata is not reported as missing entities

- **WHEN** no maintained metadata covers an entity type's identifying attributes
- **THEN** the catalog states that the data has not been analyzed yet, rather
  than showing an empty list that reads as "none exist"

#### Scenario: Metadata age is visible

- **WHEN** the catalog reports entity types detected from maintained metadata
- **THEN** the age or as-of time of that metadata is available to the user
