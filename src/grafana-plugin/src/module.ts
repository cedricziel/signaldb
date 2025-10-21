import { DataSourcePlugin } from '@grafana/data';
import { DataSource } from './datasource';
import { ConfigEditor } from './components/ConfigEditor';
import { QueryEditor } from './components/QueryEditor';
import { SignalDBQuery, SignalDBDataSourceOptions } from './types';

export const plugin = new DataSourcePlugin<DataSource, SignalDBQuery, SignalDBDataSourceOptions>(DataSource)
  .setConfigEditor(ConfigEditor)
  .setQueryEditor(QueryEditor);
