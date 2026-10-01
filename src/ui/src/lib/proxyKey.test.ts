import { describe, expect, it } from "vitest";
import { proxyKey } from "./proxyKey";

const matches = (path: string, url: string) =>
  new RegExp(proxyKey(path)).test(url);

describe("proxyKey", () => {
  it("matches the path itself, nested paths, and query strings", () => {
    expect(matches("/api", "/api")).toBe(true);
    expect(matches("/api", "/api/v1/whoami")).toBe(true);
    expect(matches("/api", "/api?tenant=acme")).toBe(true);
    expect(matches("/ui/session", "/ui/session?next=%2Flogs")).toBe(true);
  });

  it("rejects near-prefix SPA routes", () => {
    expect(matches("/api", "/api-keys")).toBe(false);
    expect(matches("/api", "/api-keys?tenant=acme")).toBe(false);
    expect(matches("/oauth/consent/context", "/oauth/consent")).toBe(false);
  });

  it("treats every regex metacharacter in the path literally", () => {
    expect(matches("/runtime-config.js", "/runtime-config.js")).toBe(true);
    expect(matches("/runtime-config.js", "/runtime-configXjs")).toBe(false);
    for (const path of ["/a+b", "/a(b)", "/a[b]", "/a|b", "/a$b", "/a\\b"]) {
      expect(matches(path, path)).toBe(true);
      expect(matches(path, path.replace(/[+()[\]|$\\]/g, ""))).toBe(false);
    }
  });
});
