// Generates one HTML file per login-page state from a single template so
// every preview shares markup. Run: node build.mjs
import { writeFileSync } from "node:fs";

const mark = `<svg viewBox="0 0 18 18" aria-hidden="true"><rect width="18" height="18" rx="4" fill="#14181e"/><path d="M2 9 L5 9 L7 4 L10 14 L13 6 L14.5 9 L16 9" stroke="#f58a3c" stroke-width="1.8" fill="none" stroke-linecap="round" stroke-linejoin="round"/></svg>`;
const warn = `<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="9"/><path d="M12 8v4M12 16h.01"/></svg>`;
const key = `<svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="8" cy="15" r="4"/><path d="m10.9 12.1 9.1-9.1M15 8l3 3"/></svg>`;

const sso = (name) =>
  `<a class="login-sso" href="/ui/session/oidc/start?redirect=%2Flogs">${key}<span>Continue with ${name}</span></a>`;
const divider = `<div class="login-divider" aria-hidden="true">or</div>`;
const form = ({ primary, error }) => `
<form class="login-form" method="post" action="/ui/session">
  <label>Email<input name="email" type="email" autocomplete="username" value="${error ? "alice@acme.dev" : ""}" ${error ? 'aria-invalid="true"' : ""} placeholder="you@company.dev"></label>
  <label>Password<input name="password" type="password" autocomplete="current-password" value="${error ? "••••••••" : ""}" ${error ? 'aria-invalid="true"' : ""}></label>
  ${error ? `<p class="login-error" role="alert">Invalid email or password</p>` : ""}
  <button class="login-submit ${primary ? "login-submit--primary" : "login-submit--soft"}" type="submit">Sign in</button>
</form>`;

const page = ({ title, theme = "light", body, hint, alert }) => `<!-- @dsCard group="Auth" -->
<!doctype html>
<html lang="en" data-theme="${theme}">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>${title} · SignalDB</title>
<link rel="stylesheet" href="login.css">
</head>
<body>
<div class="login-page">
  <main class="login-main">
    <div class="login-brand">${mark}<span>SignalDB</span></div>
    <section class="login-card">
      ${alert ? `<div class="login-alert" role="alert">${warn}<span>${alert}</span></div>` : ""}
      ${body({ hint })}
    </section>
  </main>
  <footer class="login-foot"><a href="https://signaldb.dev/docs">Docs</a><span>·</span><a href="https://signaldb.dev">signaldb.dev</a></footer>
</div>
</body>
</html>`;

const credential = ({ password = true, oidc = null, error = false, notice = null }) => ({ hint }) => `
<h1>Sign in</h1>
<p class="login-hint">${hint ?? "Use your account to explore logs, traces and metrics."}</p>
${notice ? `<p class="login-notice">${notice}</p>` : ""}
${oidc ? sso(oidc) : ""}
${oidc && password ? divider : ""}
${password ? form({ primary: !oidc, error }) : ""}
${!password && oidc ? `<p class="login-hint">Password sign-in is off on this instance.</p>` : ""}`;

const tenants = () => () => `
<h1>Choose a tenant</h1>
<p class="login-hint">Your account belongs to several tenants. Pick the one to explore — you can switch later from the top bar.</p>
<ul class="login-tenants">
  ${[["AC","Acme Inc","acme","admin"],["GX","Globex Corp","globex","member"],["IN","Initech","initech","viewer"]]
    .map(([i,n,id,r]) => `<li><button type="button"><span class="login-tenant-avatar">${i}</span><span><span class="login-tenant-name">${n}</span><br><span class="login-tenant-meta">${id}</span></span><span class="login-tenant-role">${r}</span></button></li>`).join("")}
</ul>
<p class="login-account">Signed in as alice@acme.dev <a href="#">Sign out</a></p>`;

const noAccess = () => () => `
<h1>No tenant access yet</h1>
<p class="login-hint">Your account <strong>alice@acme.dev</strong> isn't a member of any tenant. Ask a tenant admin to add you, or see the <a href="https://signaldb.dev/docs">bootstrap guide</a> for a new instance.</p>
<p class="login-account"><a href="#">Sign out</a></p>`;

const files = {
  "01-password-only.html":  page({ title: "Password only",  body: credential({}) }),
  "02-both.html":           page({ title: "SSO + password", body: credential({ oidc: "Authentik" }) }),
  "03-sso-only.html":       page({ title: "SSO only",       body: credential({ oidc: "Authentik", password: false }) }),
  "04-sso-failed.html":     page({ title: "SSO failed",     body: credential({ oidc: "Authentik" }), alert: "Single sign-on failed. Try again, or sign in with your email and password." }),
  "05-bad-password.html":   page({ title: "Bad password",   body: credential({ error: true }) }),
  "06-probe-failed.html":   page({ title: "Probe failed",   body: credential({ notice: "Couldn't load sign-in options — password sign-in is shown as a fallback." }) }),
  "07-tenant-step.html":    page({ title: "Choose tenant",  body: tenants() }),
  "08-no-access.html":      page({ title: "No access",      body: noAccess() }),
  "09-both-dark.html":      page({ title: "SSO + password (dark)", theme: "dark", body: credential({ oidc: "Authentik" }) }),
};
for (const [name, html] of Object.entries(files)) writeFileSync(name, html);
console.log(Object.keys(files).join("\n"));
