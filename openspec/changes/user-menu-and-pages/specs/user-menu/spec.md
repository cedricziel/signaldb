## Purpose

Provides authenticated users with a dropdown menu in the top bar for quick access to account information, theme preferences, navigation shortcuts, and session management.

## ADDED Requirements

### Requirement: Display user identity in the top bar

The system SHALL display a user menu trigger in the top bar when a user is authenticated. The trigger SHALL show the user's initials as an avatar and their display name.

#### Scenario: Authenticated user sees the menu trigger

- **WHEN** the user is logged in (whoami returns a user object)
- **THEN** the top bar displays a button with initials and display name

#### Scenario: Unauthenticated user sees nothing

- **WHEN** the user is not logged in (whoami returns no user)
- **THEN** no user menu element is rendered

#### Scenario: Initials are derived correctly

- **WHEN** the user has a display name "Jane Doe"
- **THEN** the avatar shows "JD"

#### Scenario: Email is used as fallback

- **WHEN** the user has no display name but has email "jane@acme.com"
- **THEN** the display name shows the email and initials derive from the email local-part

### Requirement: Popover reveals user actions on click

The system SHALL open a popover menu when the user clicks the trigger. The popover SHALL be dismissed by clicking outside, pressing Escape, or navigating away.

#### Scenario: Popover opens on click

- **WHEN** the user clicks the menu trigger
- **THEN** a popover appears showing user info, menu items, and actions

#### Scenario: Popover closes on Escape

- **WHEN** the popover is open and the user presses Escape
- **THEN** the popover closes

#### Scenario: Popover closes on backdrop click

- **WHEN** the popover is open and the user clicks outside the popover
- **THEN** the popover closes

### Requirement: Popover displays user information

The popover SHALL show the user's display name, email address, and tenant role.

#### Scenario: User info visible in popover

- **WHEN** the popover is open
- **THEN** the user's name, email, and role (if available) are displayed

### Requirement: Theme toggle persists user preference

The popover SHALL include an appearance toggle that switches between light and dark themes. The selection SHALL persist across sessions via localStorage.

#### Scenario: Toggle switches theme

- **WHEN** the user clicks the Appearance menu item
- **THEN** the page theme toggles between light and dark, and the value is saved to localStorage

#### Scenario: Theme persists across reloads

- **WHEN** the user sets a theme and reloads the page
- **THEN** the saved theme is restored from localStorage

### Requirement: Navigation links provide quick access

The popover SHALL include links to: Send data (/instrumentation), API keys (/api-keys), and external documentation.

#### Scenario: Internal navigation links work

- **WHEN** the user clicks "Send data" or "API keys"
- **THEN** the app navigates to the corresponding route and the popover closes

#### Scenario: Docs link opens in new tab

- **WHEN** the user clicks "Docs"
- **THEN** the SignalDB docs open in a new browser tab

### Requirement: Tenant switching navigates to selection page

The popover SHALL include a "Switch tenant" link that navigates to the tenant selection page.

#### Scenario: Switch tenant navigates

- **WHEN** the user clicks "Switch tenant"
- **THEN** the app navigates to /select-tenant and the popover closes

### Requirement: Sign out terminates session

The popover SHALL include a "Sign out" action that deletes the session, clears the query cache, and reloads the page.

#### Scenario: Sign out clears session

- **WHEN** the user clicks "Sign out"
- **THEN** the session is deleted, query cache is cleared, and the page reloads

#### Scenario: Sign out works even if API fails

- **WHEN** the user clicks "Sign out" and the delete session API fails
- **THEN** the page still reloads to clear stale state
