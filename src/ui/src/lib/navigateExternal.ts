// Navigates the browser away from the SPA to an external URL — split out of
// its one caller (GitHubIntegration's "Connect GitHub" button) so a test can
// stub it and assert the target URL without jsdom actually attempting a
// cross-origin navigation.
export function navigateExternal(url: string): void {
  window.location.assign(url);
}
