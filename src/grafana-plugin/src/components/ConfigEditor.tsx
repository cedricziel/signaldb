import React, { ChangeEvent } from 'react';
import { InlineField, Input, Select, SecretInput } from '@grafana/ui';
import { DataSourcePluginOptionsEditorProps, SelectableValue } from '@grafana/data';
import { SignalDBDataSourceOptions, SignalDBSecureJsonData } from '../types';

interface Props extends DataSourcePluginOptionsEditorProps<SignalDBDataSourceOptions, SignalDBSecureJsonData> {}

const PROTOCOL_OPTIONS: Array<SelectableValue<string>> = [
  { label: 'HTTP', value: 'http', description: 'HTTP API (default port 3001)' },
  { label: 'Flight', value: 'flight', description: 'Arrow Flight (default port 50053)' },
];

export function ConfigEditor(props: Props) {
  const { onOptionsChange, options } = props;
  const { jsonData, secureJsonFields, secureJsonData } = options;

  const onRouterUrlChange = (event: ChangeEvent<HTMLInputElement>) => {
    onOptionsChange({
      ...options,
      jsonData: {
        ...jsonData,
        routerUrl: event.target.value,
      },
    });
  };

  const onProtocolChange = (value: SelectableValue<string>) => {
    onOptionsChange({
      ...options,
      jsonData: {
        ...jsonData,
        protocol: value.value as 'http' | 'flight',
      },
    });
  };

  const onTimeoutChange = (event: ChangeEvent<HTMLInputElement>) => {
    const timeout = parseInt(event.target.value, 10);
    onOptionsChange({
      ...options,
      jsonData: {
        ...jsonData,
        timeout: isNaN(timeout) ? undefined : timeout,
      },
    });
  };

  const onAPIKeyChange = (event: ChangeEvent<HTMLInputElement>) => {
    onOptionsChange({
      ...options,
      secureJsonData: {
        apiKey: event.target.value,
      },
    });
  };

  const onResetAPIKey = () => {
    onOptionsChange({
      ...options,
      secureJsonFields: {
        ...options.secureJsonFields,
        apiKey: false,
      },
      secureJsonData: {
        ...options.secureJsonData,
        apiKey: '',
      },
    });
  };

  return (
    <>
      <InlineField
        label="Router URL"
        labelWidth={20}
        interactive
        tooltip="URL of the SignalDB router service (e.g., http://localhost:3001)"
      >
        <Input
          id="config-editor-router-url"
          onChange={onRouterUrlChange}
          value={jsonData.routerUrl || ''}
          placeholder="http://localhost:3001"
          width={50}
        />
      </InlineField>

      <InlineField label="Protocol" labelWidth={20} interactive tooltip="Communication protocol">
        <Select
          options={PROTOCOL_OPTIONS}
          value={jsonData.protocol || 'http'}
          onChange={onProtocolChange}
          width={30}
        />
      </InlineField>

      <InlineField label="Timeout (seconds)" labelWidth={20} interactive tooltip="Query timeout in seconds">
        <Input
          id="config-editor-timeout"
          onChange={onTimeoutChange}
          value={jsonData.timeout || ''}
          placeholder="30"
          width={20}
          type="number"
        />
      </InlineField>

      <InlineField label="API Key" labelWidth={20} interactive tooltip="Optional API key for authentication (stored securely)">
        <SecretInput
          isConfigured={secureJsonFields?.apiKey || false}
          id="config-editor-api-key"
          value={secureJsonData?.apiKey || ''}
          placeholder="Optional API key"
          width={50}
          onReset={onResetAPIKey}
          onChange={onAPIKeyChange}
        />
      </InlineField>
    </>
  );
}
