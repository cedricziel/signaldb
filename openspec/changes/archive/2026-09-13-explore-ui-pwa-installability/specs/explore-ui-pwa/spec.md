# explore-ui-pwa — Delta Spec

## Purpose

The Explore UI is installable as a standalone app on mobile and desktop,
with a service worker that makes the app shell load instantly and offline
without ever serving stale query or telemetry data, and that updates itself
without depending on the browser's own re-check to happen in time.

## ADDED Requirements

### Requirement: The UI is installable as a standalone app

The UI SHALL serve a web app manifest declaring its name, themed icons
(including a maskable variant), and `display: standalone`, and SHALL serve
the iOS-specific `apple-touch-icon` link and meta tags iOS reads instead of
the manifest for "Add to Home Screen".

#### Scenario: Installing from a desktop browser

- **WHEN** a user opens the Explore UI in a browser that supports the
  installable-web-app manifest (e.g. Chrome, Edge)
- **THEN** the browser offers to install it, and the installed app opens in
  its own standalone window with the SignalDB icon, not a browser tab

#### Scenario: Adding to a mobile home screen

- **WHEN** a user adds the Explore UI to their home screen from iOS Safari
  or Android Chrome
- **THEN** the resulting home-screen icon uses the maskable/themed icon
  artwork and opens the app standalone, without browser chrome

### Requirement: The service worker never serves live data from cache

The service worker SHALL precache only the application shell (JavaScript,
CSS, HTML, the manifest, and the favicon). It SHALL NOT define any caching
strategy for query, telemetry, or authentication requests — those requests
SHALL always be served by the network, exactly as they would be without a
service worker installed.

#### Scenario: A query is made while the app is installed

- **WHEN** an installed instance of the UI issues a query, log search, or
  telemetry export request
- **THEN** the request goes to the network unconditionally; no service
  worker cache is consulted or populated for it

#### Scenario: A query is made while offline

- **WHEN** the device has no network connectivity and a query is attempted
- **THEN** the request fails as a normal network error — it is never served
  a cached (and potentially stale) result

### Requirement: Backend routes are never served the cached app shell

The service worker's SPA navigation fallback SHALL exclude every path the
UI's own backend proxy handles (API, Loki/Tempo/Prometheus/Pyroscope
compatibility endpoints, OAuth endpoints, runtime config), using the same
segment-anchored path matching the UI's backend proxy configuration already
uses, so that a backend route is never confused with a same-prefixed SPA
route.

#### Scenario: A backend route is not swallowed by a similarly-prefixed SPA route

- **WHEN** the service worker evaluates whether `/api/v1/whoami` is a
  backend route to exclude from the cached-shell fallback
- **THEN** it is excluded (served by the network)
- **WHEN** the service worker evaluates the SPA route `/api-keys`
- **THEN** it is NOT excluded (served the cached app shell like any other
  client-side route), even though it shares the `/api` prefix

### Requirement: New builds activate without stranding a long-lived tab

The UI SHALL install and activate a new service worker version
automatically, without requiring a user-facing prompt to be built or
dismissed, and SHALL reload the page once the new version takes control. A
tab that remains open and visible without navigating SHALL still check for
a new version at least once per hour; a tab that becomes visible after being
hidden past that interval SHALL check immediately.

#### Scenario: A new build is deployed while a tab is open and active

- **WHEN** a new UI build is deployed and a browser tab has been open,
  visible, and un-navigated for longer than the update-check interval
- **THEN** the tab detects the new version within one interval, activates
  it, and reloads onto the new build without any user action

#### Scenario: A tab is hidden, then regains focus

- **WHEN** a browser tab has been hidden (backgrounded or minimized) for
  longer than the update-check interval and then regains focus
- **THEN** an update check runs immediately on regaining focus, rather than
  waiting for the next scheduled interval

#### Scenario: A tab is hidden and never regains focus

- **WHEN** a browser tab is hidden and stays hidden
- **THEN** no update check runs against it (a background tab gains nothing
  from a fresher build nobody is viewing, and would otherwise force a
  network revalidation for no benefit)
