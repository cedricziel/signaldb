import React, { ChangeEvent } from 'react';
import { InlineField, Input, Select, Stack, TextArea } from '@grafana/ui';
import { QueryEditorProps, SelectableValue } from '@grafana/data';
import { DataSource } from '../datasource';
import { SignalDBDataSourceOptions, SignalDBQuery, SignalType } from '../types';

type Props = QueryEditorProps<DataSource, SignalDBQuery, SignalDBDataSourceOptions>;

const SIGNAL_TYPE_OPTIONS: Array<SelectableValue<SignalType>> = [
  { label: 'Traces', value: SignalType.Traces, description: 'Query trace data using TraceQL' },
  { label: 'Metrics', value: SignalType.Metrics, description: 'Query metrics using PromQL' },
  { label: 'Logs', value: SignalType.Logs, description: 'Query logs using LogQL' },
];

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

  // Determine placeholder text based on signal type
  const getPlaceholder = () => {
    switch (signalType) {
      case SignalType.Traces:
        return '{ service.name = "my-service" } | duration > 100ms';
      case SignalType.Metrics:
        return 'up{job="my-job"}';
      case SignalType.Logs:
        return '{app="my-app"} |= "error"';
      default:
        return 'Enter your query';
    }
  };

  const getQueryLabel = () => {
    switch (signalType) {
      case SignalType.Traces:
        return 'TraceQL Query';
      case SignalType.Metrics:
        return 'PromQL Query';
      case SignalType.Logs:
        return 'LogQL Query';
      default:
        return 'Query';
    }
  };

  return (
    <Stack gap={2} direction="column">
      <InlineField label="Signal Type" labelWidth={16} tooltip="Select the type of observability signal to query">
        <Select
          options={SIGNAL_TYPE_OPTIONS}
          value={signalType}
          onChange={onSignalTypeChange}
          width={32}
        />
      </InlineField>

      <InlineField
        label={getQueryLabel()}
        labelWidth={16}
        tooltip="Enter query expression for the selected signal type"
        grow
      >
        <TextArea
          id="query-editor-query-text"
          onChange={onQueryTextChange}
          onBlur={onRunQuery}
          value={queryText || ''}
          placeholder={getPlaceholder()}
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
