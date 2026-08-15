// URL helpers for the schema hub. Definition pages are addressable so
// tooltips, MCP results, and shared links can point at one definition.
import type { DefinitionKind } from "./api";

export const CONVENTIONS = "/schema/conventions";

export const registryPath = (namespace: string, version: string) =>
  `${CONVENTIONS}/${encodeURIComponent(namespace)}/${encodeURIComponent(version)}`;

export const editorPath = (namespace: string, version: string) =>
  `${registryPath(namespace, version)}/edit`;

export const definitionPath = (
  namespace: string,
  version: string,
  kind: DefinitionKind,
  name: string,
) => `${registryPath(namespace, version)}/${kind}/${encodeURIComponent(name)}`;

export const KINDS: DefinitionKind[] = ["attributes", "entities", "metrics"];

export function isKind(value: string | undefined): value is DefinitionKind {
  return value !== undefined && (KINDS as string[]).includes(value);
}

/** Singular, human label for a definition kind. */
export function kindLabel(kind: DefinitionKind): string {
  switch (kind) {
    case "attributes":
      return "attribute";
    case "entities":
      return "entity";
    case "metrics":
      return "metric";
  }
}
