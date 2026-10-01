import { describe, expect, it } from "vitest";
import {
  codeLocationFromAttributes,
  extractFrameLocation,
  repositoryHints,
} from "./sourceLocation";

describe("extractFrameLocation", () => {
  it("reads a Python traceback frame", () => {
    expect(
      extractFrameLocation('  File "app/main.py", line 42, in run'),
    ).toEqual({ path: "app/main.py", line: 42 });
  });

  it("reads a JS/TS frame with a column", () => {
    expect(extractFrameLocation("at fn (src/x.ts:12:5)")).toEqual({
      path: "src/x.ts",
      line: 12,
    });
  });

  it("reads a Rust frame with no parens", () => {
    expect(extractFrameLocation("   at src/main.rs:42:9")).toEqual({
      path: "src/main.rs",
      line: 42,
    });
  });

  it("reads a Go frame with a trailing offset", () => {
    expect(extractFrameLocation("/app/handler.go:77 +0x1f")).toEqual({
      path: "/app/handler.go",
      line: 77,
    });
  });

  it("reads a Ruby frame with a trailing :in clause", () => {
    expect(extractFrameLocation("app/x.rb:12:in `foo'")).toEqual({
      path: "app/x.rb",
      line: 12,
    });
  });

  it("reads a bare basename with no directory", () => {
    expect(extractFrameLocation("Foo.java:12")).toEqual({
      path: "Foo.java",
      line: 12,
    });
  });

  it("returns null for a header line", () => {
    expect(
      extractFrameLocation("PaymentError: card declined"),
    ).toBeNull();
  });

  it("returns null for a line with no path", () => {
    expect(extractFrameLocation("    at Object.<anonymous>")).toBeNull();
  });

  it("returns null for a path with no extension", () => {
    expect(extractFrameLocation("at handler:12")).toBeNull();
  });
});

describe("repositoryHints", () => {
  it("reads resource-prefixed repository and revision by default", () => {
    expect(
      repositoryHints({
        "resource.vcs.repository.url.full": "https://github.com/acme/api",
        "resource.vcs.ref.head.revision": "deadbeefcafe",
      }),
    ).toEqual({ repository: "https://github.com/acme/api", ref: "deadbeefcafe" });
  });

  it("falls back to the unprefixed spelling", () => {
    expect(
      repositoryHints({
        "vcs.repository.url.full": "acme/api",
        "vcs.ref.head.name": "main",
      }),
    ).toEqual({ repository: "acme/api", ref: "main" });
  });

  it("prefers revision over ref name over service.version", () => {
    expect(
      repositoryHints({
        "vcs.ref.head.revision": "cafebabe0",
        "vcs.ref.head.name": "main",
        "service.version": "1234567",
      }),
    ).toEqual({ ref: "cafebabe0" });
  });

  it("uses service.version only when it looks like a commit SHA", () => {
    expect(repositoryHints({ "service.version": "abc1234" })).toEqual({
      ref: "abc1234",
    });
    expect(repositoryHints({ "service.version": "2.4.1" })).toEqual({});
  });

  it("returns an empty object when nothing matches", () => {
    expect(repositoryHints({ "some.other.key": "x" })).toEqual({});
  });

  it("honors a custom prefix list", () => {
    expect(
      repositoryHints(
        { "span.vcs.repository.url.full": "acme/api" },
        { prefixes: ["span."] },
      ),
    ).toEqual({ repository: "acme/api" });
  });
});

describe("codeLocationFromAttributes", () => {
  it("reads the 1.30+ spelling", () => {
    expect(
      codeLocationFromAttributes({
        "code.file.path": "src/handler.rs",
        "code.line.number": 42,
      }),
    ).toEqual({ path: "src/handler.rs", line: 42 });
  });

  it("reads the pre-1.30 spelling", () => {
    expect(
      codeLocationFromAttributes({
        "code.filepath": "src/handler.rs",
        "code.lineno": "42",
      }),
    ).toEqual({ path: "src/handler.rs", line: 42 });
  });

  it("returns null without a valid line", () => {
    expect(
      codeLocationFromAttributes({ "code.file.path": "src/handler.rs" }),
    ).toBeNull();
    expect(
      codeLocationFromAttributes({
        "code.file.path": "src/handler.rs",
        "code.line.number": 0,
      }),
    ).toBeNull();
  });

  it("returns null without a path", () => {
    expect(codeLocationFromAttributes({ "code.line.number": 42 })).toBeNull();
  });
});
