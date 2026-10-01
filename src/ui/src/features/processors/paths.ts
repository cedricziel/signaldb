// URL helpers for the processors feature.

export const PROCESSORS = "/processors";

export const editorPath = (name: string) =>
  `${PROCESSORS}/${encodeURIComponent(name)}/edit`;
