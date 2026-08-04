import { test, expect } from '@grafana/plugin-e2e';

// Field labels below must match src/components/QueryEditor.tsx. The query
// text field's accessible name is the signal-type-specific label from
// getQueryLabel() (e.g. "TraceQL Query" for the default Traces signal
// type), not a generic "Query Text"; there is no "Constant" field — the
// only numeric input is "Limit".

test('smoke: should render query editor', async ({ panelEditPage, readProvisionedDataSource }) => {
  const ds = await readProvisionedDataSource({ fileName: 'datasources.yml' });
  await panelEditPage.datasource.set(ds.name);
  await expect(panelEditPage.getQueryEditorRow('A').getByRole('textbox', { name: 'TraceQL Query' })).toBeVisible();
});

test('should trigger new query when Limit field is changed', async ({ panelEditPage, readProvisionedDataSource }) => {
  const ds = await readProvisionedDataSource({ fileName: 'datasources.yml' });
  await panelEditPage.datasource.set(ds.name);
  await panelEditPage.getQueryEditorRow('A').getByRole('textbox', { name: 'TraceQL Query' }).fill('{}');
  const queryReq = panelEditPage.waitForQueryDataRequest();
  await panelEditPage.getQueryEditorRow('A').getByRole('spinbutton', { name: 'Limit' }).fill('10');
  await expect(await queryReq).toBeTruthy();
});

test('a valid TraceQL query round-trips through the live backend', async ({
  panelEditPage,
  readProvisionedDataSource,
}) => {
  // Unlike the stock @grafana/create-plugin scaffold this replaced, the
  // Rust backend's handle_query (src/grafana-plugin/backend/src/main.rs)
  // dispatches the query text as a real TraceQL query over Flight to a
  // live router/querier — there is no hardcoded fake response. `{}` is the
  // minimal syntactically valid TraceQL query (matches all spans), so this
  // only asserts the round trip succeeds, not any specific result content.
  const ds = await readProvisionedDataSource({ fileName: 'datasources.yml' });
  await panelEditPage.datasource.set(ds.name);
  await panelEditPage.getQueryEditorRow('A').getByRole('textbox', { name: 'TraceQL Query' }).fill('{}');
  await panelEditPage.setVisualization('Table');
  await expect(panelEditPage.refreshPanel()).toBeOK();
});
