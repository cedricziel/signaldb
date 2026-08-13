import { useQuery } from "@tanstack/react-query";
import {
  pyroscopeLabelNames,
  pyroscopeLabelValues,
  pyroscopeProfileTypes,
  pyroscopeServices,
} from "../../api/pyroscope";
import { fetchFlamegraph, fetchFlamegraphById } from "../../api/profilesIr";
import { rangeToParam, resolveRange, type TimeRange } from "../../lib/time";
import type { ExploreState, UpdateFn } from "../../lib/urlState";
import { TimeRangePicker } from "../shell/TimeRangePicker";
import { FlameGraph, FlamePane } from "./FlameGraph";
import { decodeFlamebearer } from "../../lib/flamebearer";
import "./profiles.css";

interface Props {
  state: ExploreState;
  update: UpdateFn;
}

export function ProfilesView({ state, update }: Props) {
  if (state.profileId) {
    return <SingleProfileView profileId={state.profileId} update={update} />;
  }
  if (state.profileCompare) {
    return <CompareView state={state} update={update} />;
  }
  return <SingleRangeView state={state} update={update} />;
}

/** The service/type/attribute selectors shared by the single and compare
 * views — everything except the time range and the rendered graph(s). */
function useProfileSelectors(state: ExploreState) {
  const rangeKey = `${rangeToParam(state.range)}|${state.tenant}|${state.dataset}`;

  const typesQuery = useQuery({
    queryKey: ["pyro-types", rangeKey],
    queryFn: () => pyroscopeProfileTypes(resolveRange(state.range, Date.now())),
  });
  const servicesQuery = useQuery({
    queryKey: ["pyro-services", rangeKey],
    queryFn: () => pyroscopeServices(resolveRange(state.range, Date.now())),
  });
  const labelNamesQuery = useQuery({
    queryKey: ["pyro-labelnames", rangeKey],
    queryFn: () => pyroscopeLabelNames(resolveRange(state.range, Date.now())),
  });
  const labelValuesQuery = useQuery({
    queryKey: ["pyro-labelvalues", state.profileMatcherLabel, rangeKey],
    queryFn: () =>
      pyroscopeLabelValues(
        state.profileMatcherLabel,
        resolveRange(state.range, Date.now()),
      ),
    enabled: state.profileMatcherLabel !== "",
  });

  const types = typesQuery.data ?? [];
  // Fall back to the first available type until the user picks one, without
  // rewriting the URL.
  const selectedType =
    types.find((t) => t.ID === state.profileType)?.ID ?? types[0]?.ID ?? "";
  const selectedTypeMeta = types.find((t) => t.ID === selectedType);

  return {
    typesQuery,
    servicesQuery,
    labelNamesQuery,
    labelValuesQuery,
    types,
    selectedType,
    selectedTypeMeta,
    unit: selectedTypeMeta?.sampleUnit ?? "",
  };
}

interface FlamegraphRenderArgs {
  /** Disambiguates the query key when the same view runs two renders in
   * parallel (the compare view's baseline vs. comparison side). */
  label: string;
  range: TimeRange;
  /** Cache-key form of `range` — a relative range resolves "now" on every
   * fetch, so the key can't be derived from `range` itself. */
  queryKeyRange: string;
  service: string;
  sampleType: string | undefined;
  matcher: { label: string; value: string } | undefined;
  enabled: boolean;
  refetchInterval?: number | false;
}

function useFlamegraphRender({
  label,
  range,
  queryKeyRange,
  service,
  sampleType,
  matcher,
  enabled,
  refetchInterval = false,
}: FlamegraphRenderArgs) {
  return useQuery({
    queryKey: [
      "pyro-render",
      label,
      sampleType ?? "",
      service,
      matcher?.label ?? "",
      matcher?.value ?? "",
      queryKeyRange,
    ],
    queryFn: () =>
      fetchFlamegraph(resolveRange(range, Date.now()), {
        service: service || undefined,
        sampleType,
        matcher: matcher?.label ? matcher : undefined,
      }),
    enabled,
    refetchInterval,
  });
}

function SelectorControls({
  state,
  update,
  selectors,
}: {
  state: ExploreState;
  update: UpdateFn;
  selectors: ReturnType<typeof useProfileSelectors>;
}) {
  const {
    servicesQuery,
    labelNamesQuery,
    labelValuesQuery,
    types,
    selectedType,
  } = selectors;
  return (
    <div className="profiles-controls">
      <label className="profiles-field">
        Service
        <select
          aria-label="Profile service"
          value={state.profileService}
          onChange={(e) => update({ profileService: e.target.value })}
        >
          <option value="">All services</option>
          {(servicesQuery.data ?? []).map((svc) => (
            <option key={svc} value={svc}>
              {svc}
            </option>
          ))}
        </select>
      </label>
      <label className="profiles-field">
        Profile type
        <select
          aria-label="Profile type"
          value={selectedType}
          disabled={types.length === 0}
          onChange={(e) => update({ profileType: e.target.value })}
        >
          {types.length === 0 && <option value="">No profile types</option>}
          {types.map((t) => (
            <option key={t.ID} value={t.ID}>
              {t.sampleType} · {t.sampleUnit}
            </option>
          ))}
        </select>
      </label>
      <label className="profiles-field">
        Attribute
        <select
          aria-label="Attribute label"
          value={state.profileMatcherLabel}
          onChange={(e) =>
            update({
              profileMatcherLabel: e.target.value,
              profileMatcherValue: "",
            })
          }
        >
          <option value="">None</option>
          {(labelNamesQuery.data ?? []).map((name) => (
            <option key={name} value={name}>
              {name}
            </option>
          ))}
        </select>
      </label>
      {state.profileMatcherLabel !== "" && (
        <label className="profiles-field">
          Value
          <select
            aria-label="Attribute value"
            value={state.profileMatcherValue}
            onChange={(e) => update({ profileMatcherValue: e.target.value })}
          >
            <option value="">Any value</option>
            {(labelValuesQuery.data ?? []).map((value) => (
              <option key={value} value={value}>
                {value}
              </option>
            ))}
          </select>
        </label>
      )}
      <label className="profiles-field profiles-compare-toggle">
        <input
          type="checkbox"
          checked={state.profileCompare}
          onChange={(e) => update({ profileCompare: e.target.checked })}
        />
        Compare
      </label>
    </div>
  );
}

function SingleRangeView({ state, update }: Props) {
  const selectors = useProfileSelectors(state);
  const { typesQuery, servicesQuery, selectedType, selectedTypeMeta, unit } =
    selectors;
  const rangeKey = `${rangeToParam(state.range)}|${state.tenant}|${state.dataset}`;

  const renderQuery = useFlamegraphRender({
    label: "single",
    range: state.range,
    queryKeyRange: rangeKey,
    service: state.profileService,
    sampleType: selectedTypeMeta?.sampleType,
    matcher: state.profileMatcherLabel
      ? { label: state.profileMatcherLabel, value: state.profileMatcherValue }
      : undefined,
    enabled: selectedType !== "",
    refetchInterval: state.live ? 15_000 : false,
  });

  const isEmpty =
    renderQuery.data !== undefined &&
    renderQuery.data.render.flamebearer.numTicks === 0;

  return (
    <div className="profilesview">
      <SelectorControls state={state} update={update} selectors={selectors} />

      {(typesQuery.isError || servicesQuery.isError || renderQuery.isError) && (
        <div className="query-error" role="alert">
          Query failed:{" "}
          {
            (
              (typesQuery.error ??
                servicesQuery.error ??
                renderQuery.error) as Error
            ).message
          }
        </div>
      )}

      {selectedType === "" && !typesQuery.isFetching && (
        <div className="profiles-note">
          No profiles in the selected range. Enable{" "}
          <code>[self_monitoring].profiles_enabled</code> to have SignalDB
          profile itself, or send profiles over OTLP.
        </div>
      )}

      {renderQuery.isFetching && !renderQuery.data && (
        <div className="profiles-note">Loading…</div>
      )}

      {renderQuery.data?.truncated && (
        <div className="profiles-note">
          Too many matching profiles — showing an aggregate of the first batch
          only. Narrow the range or add a filter for the full picture.
        </div>
      )}

      {isEmpty && (
        <div className="profiles-note">
          No profiles in the selected range for this filter.
        </div>
      )}

      {renderQuery.data && !isEmpty && (
        <FlameGraph render={renderQuery.data.render} unit={unit} />
      )}
    </div>
  );
}

function CompareView({ state, update }: Props) {
  const selectors = useProfileSelectors(state);
  const { typesQuery, servicesQuery, selectedType, selectedTypeMeta, unit } =
    selectors;
  const rangeKey = `${rangeToParam(state.range)}|${state.tenant}|${state.dataset}`;
  const baselineKey = rangeToParam(state.profileBaseline);

  const matcher = state.profileMatcherLabel
    ? { label: state.profileMatcherLabel, value: state.profileMatcherValue }
    : undefined;
  const baselineQuery = useFlamegraphRender({
    label: "baseline",
    range: state.profileBaseline,
    queryKeyRange: baselineKey,
    service: state.profileService,
    sampleType: selectedTypeMeta?.sampleType,
    matcher,
    enabled: selectedType !== "",
  });
  const comparisonQuery = useFlamegraphRender({
    label: "comparison",
    range: state.range,
    queryKeyRange: rangeKey,
    service: state.profileService,
    sampleType: selectedTypeMeta?.sampleType,
    matcher,
    enabled: selectedType !== "",
  });

  const error =
    typesQuery.error ??
    servicesQuery.error ??
    baselineQuery.error ??
    comparisonQuery.error;

  return (
    <div className="profilesview">
      <SelectorControls state={state} update={update} selectors={selectors} />
      <div className="profiles-controls profiles-compare-ranges">
        <label className="profiles-field">
          Baseline
          <TimeRangePicker
            range={state.profileBaseline}
            onChange={(range) => update({ profileBaseline: range })}
          />
        </label>
        <span className="profiles-field">Comparison: current range</span>
      </div>

      {error && (
        <div className="query-error" role="alert">
          Query failed: {(error as Error).message}
        </div>
      )}

      {selectedType === "" && !typesQuery.isFetching && (
        <div className="profiles-note">
          No profiles in the selected range. Enable{" "}
          <code>[self_monitoring].profiles_enabled</code> to have SignalDB
          profile itself, or send profiles over OTLP.
        </div>
      )}

      <div className="profiles-compare">
        <ComparePane title="Baseline" query={baselineQuery} unit={unit} />
        <ComparePane title="Comparison" query={comparisonQuery} unit={unit} />
      </div>
    </div>
  );
}

function ComparePane({
  title,
  query,
  unit,
}: {
  title: string;
  query: ReturnType<
    typeof useQuery<Awaited<ReturnType<typeof fetchFlamegraph>>>
  >;
  unit: string;
}) {
  if (query.isFetching && !query.data) {
    return (
      <div className="profiles-compare-pane">
        <div className="flame-title">{title}</div>
        <div className="profiles-note">Loading…</div>
      </div>
    );
  }
  if (!query.data || query.data.render.flamebearer.numTicks === 0) {
    return (
      <div className="profiles-compare-pane">
        <div className="flame-title">{title}</div>
        <div className="profiles-note">No profiles in this window.</div>
      </div>
    );
  }
  const levels = decodeFlamebearer(query.data.render.flamebearer);
  return (
    <div className="profiles-compare-pane">
      <FlamePane
        levels={levels}
        totalTicks={query.data.render.flamebearer.numTicks}
        unit={unit}
        title={title}
      />
    </div>
  );
}

function SingleProfileView({
  profileId,
  update,
}: {
  profileId: string;
  update: UpdateFn;
}) {
  const renderQuery = useQuery({
    queryKey: ["pyro-byid", profileId],
    queryFn: () => fetchFlamegraphById(profileId),
  });

  return (
    <div className="profilesview">
      <div className="profiles-controls">
        <button
          type="button"
          className="backbtn"
          onClick={() => update({ profileId: "" })}
        >
          ← profiles
        </button>
        <span className="profiles-field">
          Profile <code>{profileId}</code>
        </span>
      </div>

      {renderQuery.isError && (
        <div className="query-error" role="alert">
          {(renderQuery.error as Error).message}
        </div>
      )}
      {renderQuery.isFetching && !renderQuery.data && (
        <div className="profiles-note">Loading…</div>
      )}
      {renderQuery.data && <FlameGraph render={renderQuery.data} unit="" />}
    </div>
  );
}
