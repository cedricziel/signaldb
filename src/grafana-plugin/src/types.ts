import { DataSourceJsonData } from '@grafana/data';
import { DataQuery } from '@grafana/schema';

/**
 * Signal types supported by SignalDB
 */
export enum SignalType {
  Traces = 'traces',
  Metrics = 'metrics',
  Logs = 'logs',
}

/**
 * Query definition for SignalDB datasource
 */
export interface SignalDBQuery extends DataQuery {
  /**
   * Type of signal to query (traces, metrics, logs)
   */
  signalType: SignalType;

  /**
   * Query expression (varies by signal type)
   * - For traces: TraceQL query
   * - For metrics: PromQL query or metric name
   * - For logs: LogQL query or search expression
   */
  queryText: string;

  /**
   * Time range for the query (optional, uses dashboard time range by default)
   */
  timeRange?: {
    from?: string;
    to?: string;
  };

  /**
   * Additional query options
   */
  limit?: number;
}

export const DEFAULT_QUERY: Partial<SignalDBQuery> = {
  signalType: SignalType.Traces,
  queryText: '',
  limit: 100,
};

/**
 * Configuration options for the SignalDB datasource
 */
export interface SignalDBDataSourceOptions extends DataSourceJsonData {
  /**
   * URL of the SignalDB router
   * Default: http://localhost:3001 (HTTP API)
   * Alternative: http://localhost:50053 (Flight API)
   */
  routerUrl?: string;

  /**
   * Connection protocol
   */
  protocol?: 'http' | 'flight';

  /**
   * Timeout for queries in seconds
   */
  timeout?: number;
}

/**
 * Secure configuration (not sent to frontend)
 */
export interface SignalDBSecureJsonData {
  /**
   * API key or authentication token (if needed in future)
   */
  apiKey?: string;
}
