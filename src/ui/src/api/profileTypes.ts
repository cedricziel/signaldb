// Flamebearer render-response types, shared by api/profilesIr.ts and the
// FlameGraph component. Discovery (profile types, services, label
// keys/values) is served through the Query IR — see api/ir/discovery.ts.

/** A profile kind, e.g. `{ID: "cpu:nanoseconds", sampleType: "cpu", ...}`. */
export interface ProfileType {
  ID: string;
  name: string;
  sampleType: string;
  sampleUnit: string;
  periodType?: string;
  periodUnit?: string;
}

/**
 * Flamebearer profile, as Grafana/Pyroscope render it. `levels` is a
 * flattened tree, one array per depth: each frame is a delta-encoded
 * quadruple `[offset, total, self, nameIndex]` where `offset` is the gap in
 * ticks from the end of the previous frame at that level, and `nameIndex`
 * points into `names`.
 */
export interface Flamebearer {
  names: string[];
  levels: number[][];
  numTicks: number;
  maxSelf: number;
}

export interface RenderResponse {
  flamebearer: Flamebearer;
  metadata: {
    format: "single" | "double";
    spyName?: string;
    sampleRate: number;
    units: string;
    name: string;
  };
}
