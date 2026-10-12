## MODIFIED Requirements

### Requirement: AnyValue fidelity requires fixing lossy conversion at the OTLP boundary

Lossless preservation SHALL hold from the OTLP boundary, not merely from the
storage write. The OTLP→internal conversion SHALL preserve `BytesValue` as bytes
(distinct from a string). `StringValueStrindex` is an OTLP Profiles-only variant:
the Profiles converter SHALL resolve it through the request's `ProfilesDictionary`
rather than dropping it to null; non-Profiles receivers SHALL process it as a
non-fatal absent value, as required by OTLP. When a record travels on the typed
carrier (see `ingest-wire-format`), storage SHALL additionally preserve:

- non-finite doubles as doubles;
- a key-value list whose keys resemble a SignalDB carrier object as a
  key-value list;
- every occurrence of a duplicated key, in sent order;
- values nested up to the configured depth bound (`[acceptor].max_value_depth`).

A container without duplicate keys SHALL be stored exactly as before this
requirement changed. The relative order of distinct keys across typed homes
is not required to be preserved, and SHALL be documented as such.

#### Scenario: Bytes are not degraded to a string

- **WHEN** a `BytesValue` attribute is ingested
- **THEN** it is retrievable as bytes, distinguishable from a string attribute, and
  not corrupted by a UTF-8 conversion

#### Scenario: NaN attribute survives storage

- **WHEN** a double attribute with value NaN is ingested over the typed carrier
- **THEN** a raw read returns a NaN double, not an empty value

#### Scenario: Duplicate occurrences survive storage

- **WHEN** a container with a duplicated key is ingested over the typed carrier
  and later compacted
- **THEN** a raw duplicate-preserving read returns every occurrence of that key
  in sent order, before and after compaction

### Requirement: Residue values are read through an explicit raw accessor

Because a logical field resolves to one registry-owned canonical type, a residue
value (off-type, array, kvlist, or bytes) SHALL NOT be surfaced as that field's
canonical-typed value. Reading residue content SHALL be an explicit raw/any-typed
retrieval that returns the original `AnyValue`; a canonical-typed read of a field
SHALL return the typed value or null, never a coerced residue value.

The per-container raw bag SHALL keep one entry per key, holding that key's
last occurrence. A separate retrieval-only per-container list accessor SHALL
return every occurrence of every key, duplicates included, in sent order
relative to one another. Where a JSON result renders an `AnyValue`, a value
that JSON cannot express SHALL be rendered as a documented carrier object
rather than collapsed. This applies to a non-finite double, and to a nested
key-value list with duplicate keys or with a key that collides with the
carrier discriminator. These rendering changes SHALL be gated on an IR
version, so documents declaring an earlier version keep their previous
rendering.

#### Scenario: Off-type value is invisible to the typed read, visible to the raw read

- **WHEN** a field canonically typed integer has an off-type string occurrence in
  the residue
- **THEN** a typed read of the field returns null for that row, and an explicit raw
  read returns the original string `AnyValue`

#### Scenario: The list accessor exposes duplicates the bag cannot

- **WHEN** a span carries key `k` twice and a query projects both the
  container's raw bag and its list accessor
- **THEN** the bag has one `k` entry holding the last value, and the list has
  two `k` entries in sent order

#### Scenario: A NaN renders as a carrier, not null

- **WHEN** a raw bag containing a NaN double is returned under an IR version
  that has the carrier rendering
- **THEN** the value renders as the documented non-finite double carrier, and
  under an earlier IR version it renders as before
