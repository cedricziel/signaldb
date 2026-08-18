/**
 * Entity types, derived from the tenant's schema registries rather than a
 * hand-written list.
 *
 * The registry declares what an entity type *is* — its name and the
 * attributes that identify one. It cannot declare how we choose to present
 * it (nav label, which second dimension to break down by, whether a query
 * must be scoped to server-kind spans): those are statements about
 * SignalDB's UI and query surface, not about the OTel entity, and belong in
 * `entityTypes.ts`'s curated list. So the two are merged, curated first.
 *
 * Two facts about the real registry shape this merge, and both would bite a
 * simpler "just read the registry" implementation:
 *
 * 1. **`host` and `container` declare no identifying attributes at all.**
 *    In OTel 1.43 every attribute they carry — `host.name`, `container.name`
 *    — is *descriptive*. Filtering to entities with an identifying attribute
 *    therefore deletes two entity types that work today. Where we curate an
 *    identity, it stands in.
 *
 * 2. **The registry's identity is often not the useful one.** A pod is
 *    identified by `k8s.pod.uid`: correct (names repeat across namespaces
 *    and restarts) but opaque, and absent from a lot of real telemetry.
 *    The curated identity — name plus namespace — is what a person reads and
 *    what the data actually carries. Curated identity wins where it exists;
 *    the registry's is used for every type we have not curated.
 *
 * The result is that adding an entity type still needs no code change — a
 * tenant's own registry contributes its types on the same terms as the
 * bundled one — while none of the eight curated types regress.
 */
import {
  ENTITY_TYPES,
  RESOURCE_SOURCES,
  type EntityTypeDef,
} from "./entityTypes";

/** A registry entity definition, narrowed to what deriving a type needs. */
export interface RegistryEntity {
  /** Entity type name, dotted (`k8s.pod`). */
  name: string;
  /** Attributes the registry declares as identifying, in declared order. */
  identifying: string[];
}

/** Route ids are underscored; registry names are dotted. */
function idOf(name: string): string {
  return name.replace(/\./g, "_");
}

/** "telemetry.sdk" -> "Telemetry sdks" / "telemetry sdk". A registry-only
 * type has no curated label, and its own name is a better fallback than a
 * placeholder: it is exactly what the user would search the schema hub for. */
function labelOf(name: string): { label: string; singular: string } {
  const singular = name.replace(/[._]/g, " ");
  return {
    label: singular.charAt(0).toUpperCase() + singular.slice(1) + "s",
    singular,
  };
}

/**
 * Merge the curated entity types with everything else the registry declares.
 *
 * Curated types keep their position and every curated field; a registry
 * entity matching one by id contributes nothing, because the curated entry
 * is strictly more specific. Registry-only entities follow, in registry
 * order, identified by their declared identifying attributes.
 *
 * An entity with neither a curated identity nor a declared one is dropped:
 * with nothing to group by there is no instance to list.
 */
export function deriveEntityTypes(
  registry: RegistryEntity[],
  curated: EntityTypeDef[] = ENTITY_TYPES,
): EntityTypeDef[] {
  const byId = new Map(curated.map((e) => [e.id, e]));
  const derived: EntityTypeDef[] = [...curated];

  for (const entity of registry) {
    const id = idOf(entity.name);
    if (byId.has(id)) continue;
    if (entity.identifying.length === 0) continue;
    derived.push({
      id,
      ...labelOf(entity.name),
      identity: entity.identifying,
      // A registry entity is a resource entity: its identifying attributes
      // ride on every signal the SDK emits, so it is discoverable from all
      // of them. `observedEntityTypes` narrows this to the sources that
      // actually carry the attribute.
      sources: RESOURCE_SOURCES,
    });
  }

  return derived;
}

/**
 * Keep the entity types some signal actually carries, and narrow each one to
 * the sources that carry it.
 *
 * Both halves matter. Without the filter, registry breadth turns the nav
 * into dozens of empty tabs — the OTel registry alone declares 36 types with
 * an identity, and a deployment carries a handful. Without the narrowing,
 * every type queries every source, so listing processes costs four round
 * trips against signals that cannot match plus the one that can.
 *
 * Presence is judged on the *primary* identity attribute only: a source
 * carrying `process.pid` but not `process.creation.time` still knows about
 * processes, just with a coarser identity.
 */
export function observedEntityTypes(
  types: EntityTypeDef[],
  fieldsBySource: Map<string, Set<string>>,
): EntityTypeDef[] {
  const observed: EntityTypeDef[] = [];

  for (const type of types) {
    const primary = type.identity[0];
    if (primary === undefined) continue;
    const declared = type.sources ?? ["traces"];
    const carrying = declared.filter((source) =>
      fieldsBySource.get(source)?.has(primary),
    );
    if (carrying.length === 0) continue;
    observed.push({ ...type, sources: carrying });
  }

  return observed;
}
