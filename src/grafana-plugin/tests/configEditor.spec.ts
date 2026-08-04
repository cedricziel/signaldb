import { test, expect } from '@grafana/plugin-e2e';
import { SignalDBDataSourceOptions, SignalDBSecureJsonData } from '../src/types';

// Field labels below must match src/components/ConfigEditor.tsx exactly
// (Router URL / Protocol / Timeout (seconds) / Tenant ID / Dataset ID / API
// Key) — the plugin does not have a "Path" field, that was leftover
// @grafana/create-plugin scaffold boilerplate.

test('smoke: should render config editor', async ({ createDataSourceConfigPage, readProvisionedDataSource, page }) => {
  const ds = await readProvisionedDataSource({ fileName: 'datasources.yml' });
  await createDataSourceConfigPage({ type: ds.type });
  await expect(page.getByLabel('Router URL')).toBeVisible();
});

test('"Save & test" should be successful when configuration is valid', async ({
  createDataSourceConfigPage,
  readProvisionedDataSource,
  page,
}) => {
  const ds = await readProvisionedDataSource<SignalDBDataSourceOptions, SignalDBSecureJsonData>({
    fileName: 'datasources.yml',
  });
  const configPage = await createDataSourceConfigPage({ type: ds.type });
  await page.getByRole('textbox', { name: 'Router URL' }).fill(ds.jsonData.routerUrl ?? '');
  await page.getByRole('textbox', { name: 'API Key' }).fill(ds.secureJsonData?.apiKey ?? '');
  await expect(configPage.saveAndTest()).toBeOK();
});

// Note: there is no test for an "invalid configuration" rejection here. The
// Rust backend (src/grafana-plugin/backend/src/main.rs) does not implement
// `backend::CheckHealth` at all, so "Save & test" has no real validation
// path to exercise — a prior version of this test asserted a specific
// error message ("API key is missing") that no code in this plugin ever
// produces. Add that scenario back once CheckHealth validation exists.
