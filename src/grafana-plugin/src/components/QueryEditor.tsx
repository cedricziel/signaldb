import React, { ChangeEvent } from 'react';
import { InlineField, Input, Select, Stack, TextArea } from '@grafana/ui';
import { QueryEditorProps, SelectableValue } from '@grafana/data';
import { DataSource } from '../datasource';
import { SignalDBDataSourceOptions, SignalDBQuery, SignalType } from '../types';

type Props = QueryEditorProps<DataSource, SignalDBQuery, SignalDBDataSourceOptions>;

interface SignalTypeMeta {
  label: string;
  description: string;
  queryLabel: string;
  placeholder: string;
}

const SIGNAL_TYPE_META: Record<SignalType, SignalTypeMeta> = {
  [SignalType.Traces]: {
    label: 'Traces',
    description: 'Query trace data using TraceQL',
    queryLabel: 'TraceQL Query',
    placeholder: '{ service.name = "my-service" } | duration > 100ms',
  },
  [SignalType.Metrics]: {
    label: 'Metrics',
    description: 'Query metrics using PromQL',
    queryLabel: 'PromQL Query',
    placeholder: 'up{job="my-job"}',
  },
  [SignalType.Logs]: {
    label: 'Logs',
    description: 'Query logs using LogQL',
    queryLabel: 'LogQL Query',
    placeholder: '{app="my-app"} |= "error"',
  },
  [SignalType.Profiles]: {
    label: 'Profiles',
    description: 'Query profiles as a flamegraph',
    queryLabel: 'Profile Query',
    placeholder: 'cpu{service_name="my-service"}',
  },
};

const DEFAULT_SIGNAL_TYPE_META: Pick<SignalTypeMeta, 'queryLabel' | 'placeholder'> = {
  queryLabel: 'Query',
  placeholder: 'Enter your query',
};

const SIGNAL_TYPE_OPTIONS: Array<SelectableValue<SignalType>> = Object.entries(SIGNAL_TYPE_META).map(
  ([value, meta]) => ({ label: meta.label, value: value as SignalType, description: meta.description })
);

export function QueryEditor({ query, onChange, onRunQuery }: Props) {
  const onSignalTypeChange = (value: SelectableValue<SignalType>) => {
    onChange({ ...query, signalType: value.value! });
    onRunQuery();
  };

  const onQueryTextChange = (event: ChangeEvent<HTMLTextAreaElement>) => {
    onChange({ ...query, queryText: event.target.value });
  };

  const onLimitChange = (event: ChangeEvent<HTMLInputElement>) => {
    const limit = parseInt(event.target.value, 10);
    onChange({ ...query, limit: isNaN(limit) ? undefined : limit });
  };

  const { signalType, queryText, limit } = query;
  const { queryLabel, placeholder } = SIGNAL_TYPE_META[signalType] ?? DEFAULT_SIGNAL_TYPE_META;

  return (
    <Stack gap={2} direction="column">
      <InlineField label="Signal Type" labelWidth={16} tooltip="Select the type of observability signal to query">
        <Select options={SIGNAL_TYPE_OPTIONS} value={signalType} onChange={onSignalTypeChange} width={32} />
      </InlineField>

      <InlineField
        label={queryLabel}
        labelWidth={16}
        tooltip="Enter query expression for the selected signal type"
        grow
      >
        <TextArea
          id="query-editor-query-text"
          onChange={onQueryTextChange}
          onBlur={onRunQuery}
          value={queryText || ''}
          placeholder={placeholder}
          rows={3}
        />
      </InlineField>

      <InlineField label="Limit" labelWidth={16} tooltip="Maximum number of results to return">
        <Input
          id="query-editor-limit"
          onChange={onLimitChange}
          onBlur={onRunQuery}
          value={limit || ''}
          width={16}
          type="number"
          placeholder="100"
        />
      </InlineField>
    </Stack>
  );
}
