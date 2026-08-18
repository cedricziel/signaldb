## ADDED Requirements

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
