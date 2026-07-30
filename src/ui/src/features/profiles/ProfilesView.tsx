import { useQuery } from "@tanstack/react-query";
import {
  pyroscopeProfileTypes,
  pyroscopeRender,
  pyroscopeServices,
} from "../../api/pyroscope";
import { rangeToParam, resolveRange } from "../../lib/time";
import type { ExploreState } from "../../lib/urlState";
import { FlameGraph } from "./FlameGraph";
import "./profiles.css";

interface Props {
  state: ExploreState;
  update: (patch: Partial<ExploreState>) => void;
}

export function ProfilesView({ state, update }: Props) {
  const rangeKey = `${rangeToParam(state.range)}|${state.tenant}|${state.dataset}`;

  const typesQuery = useQuery({
    queryKey: ["pyro-types", rangeKey],
    queryFn: () => pyroscopeProfileTypes(resolveRange(state.range, Date.now())),
  });
  const servicesQuery = useQuery({
    queryKey: ["pyro-services", rangeKey],
    queryFn: () => pyroscopeServices(resolveRange(state.range, Date.now())),
  });

  const types = typesQuery.data ?? [];
  // Fall back to the first available type until the user picks one, without
  // rewriting the URL.
  const selectedType =
    types.find((t) => t.ID === state.profileType)?.ID ?? types[0]?.ID ?? "";
  const selectedTypeMeta = types.find((t) => t.ID === selectedType);
  const unit = selectedTypeMeta?.sampleUnit ?? "";

  const renderQuery = useQuery({
    queryKey: ["pyro-render", selectedType, state.profileService, rangeKey],
    queryFn: () =>
      pyroscopeRender(
        selectedType,
        state.profileService,
        resolveRange(state.range, Date.now()),
      ),
    enabled: selectedType !== "",
    refetchInterval: state.live ? 15_000 : false,
  });

  const isEmpty =
    renderQuery.data !== undefined &&
    renderQuery.data.flamebearer.levels.length <= 1;

  return (
    <div className="profilesview">
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
      </div>

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

      {isEmpty && (
        <div className="profiles-note">
          No profiles in the selected range for this service and type.
        </div>
      )}

      {renderQuery.data && !isEmpty && (
        <FlameGraph render={renderQuery.data} unit={unit} />
      )}
    </div>
  );
}
