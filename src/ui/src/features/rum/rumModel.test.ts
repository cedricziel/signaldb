import { describe, expect, it } from "vitest";
import {
  formatVitalValue,
  isSdkExportPath,
  NO_VITAL_DATA,
  rateVital,
  ratingSwatchColorVar,
  ratingTextColorVar,
  RUM_TABS,
  rumTabFromParam,
  splitKpiSeries,
  urlTemplate,
  vitalFigure,
  vitalShares,
  vitalThresholdBound,
} from "./rumModel";

describe("rateVital", () => {
  it("rates LCP against the Web Vitals thresholds (ms)", () => {
    expect(rateVital("lcp", 2000)).toBe("good");
    expect(rateVital("lcp", 2900)).toBe("needs-improvement");
    expect(rateVital("lcp", 5000)).toBe("poor");
  });

  it("rates CLS unitless", () => {
    expect(rateVital("cls", 0.05)).toBe("good");
    expect(rateVital("cls", 0.2)).toBe("needs-improvement");
    expect(rateVital("cls", 0.4)).toBe("poor");
  });

  it("rates INP in milliseconds", () => {
    expect(rateVital("inp", 100)).toBe("good");
    expect(rateVital("inp", 300)).toBe("needs-improvement");
    expect(rateVital("inp", 800)).toBe("poor");
  });
});

describe("formatVitalValue", () => {
  it("shows LCP/FCP/TTFB in seconds", () => {
    expect(formatVitalValue("lcp", 2900)).toBe("2.9 s");
    expect(formatVitalValue("fcp", 1200)).toBe("1.2 s");
    expect(formatVitalValue("ttfb", 900)).toBe("0.9 s");
  });

  it("shows INP in milliseconds", () => {
    expect(formatVitalValue("inp", 104)).toBe("104 ms");
  });

  it("shows CLS unitless with two decimals", () => {
    expect(formatVitalValue("cls", 0.123)).toBe("0.12");
  });
});

describe("vitalFigure", () => {
  it("formats a vital card with a distribution", () => {
    const figure = vitalFigure("lcp", 2900, {
      good: 68,
      "needs-improvement": 22,
      poor: 10,
    });
    expect(figure.formatted).toBe("2.9 s");
    expect(figure.rating).toBe("needs-improvement");
    expect(figure.shares).toEqual([
      { rating: "good", count: 68, share: 0.68, threshold: "" },
      {
        rating: "needs-improvement",
        count: 22,
        share: 0.22,
        threshold: "",
      },
      { rating: "poor", count: 10, share: 0.1, threshold: "" },
    ]);
  });

  it("shows — with no rating for a vital with no records", () => {
    const figure = vitalFigure("inp", undefined, {});
    expect(figure.formatted).toBe(NO_VITAL_DATA);
    expect(figure.rating).toBeUndefined();
    expect(figure.p75).toBeUndefined();
  });
});

describe("vitalShares", () => {
  it("shares are zero, not NaN, with no records at all", () => {
    expect(vitalShares({})).toEqual([
      { rating: "good", count: 0, share: 0, threshold: "" },
      { rating: "needs-improvement", count: 0, share: 0, threshold: "" },
      { rating: "poor", count: 0, share: 0, threshold: "" },
    ]);
  });
});

describe("splitKpiSeries", () => {
  it("splits a bucketed series at the midpoint into previous/current", () => {
    const points = [
      { tMs: 0, value: 10 },
      { tMs: 10, value: 20 },
      { tMs: 20, value: 5 },
      { tMs: 30, value: 7 },
    ];
    const figure = splitKpiSeries(points, 20);
    expect(figure.previous).toBe(30);
    expect(figure.value).toBe(12);
    expect(figure.series).toEqual([
      { tMs: 20, value: 5 },
      { tMs: 30, value: 7 },
    ]);
  });

  it("has no previous figure when the earlier half is empty", () => {
    const figure = splitKpiSeries([{ tMs: 20, value: 5 }], 20);
    expect(figure.previous).toBeUndefined();
  });
});

describe("urlTemplate", () => {
  it("strips the query and keeps a plain path as-is", () => {
    expect(urlTemplate("https://api.example.com/health?verbose=1")).toEqual({
      origin: "api.example.com",
      template: "/health",
    });
  });

  it("replaces a numeric id segment", () => {
    expect(urlTemplate("https://api.example.com/orders/48213")).toEqual({
      origin: "api.example.com",
      template: "/orders/:id",
    });
  });

  it("replaces a UUID id segment", () => {
    expect(
      urlTemplate(
        "https://api.example.com/users/9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d/profile",
      ),
    ).toEqual({
      origin: "api.example.com",
      template: "/users/:id/profile",
    });
  });

  it("replaces a long hex id segment", () => {
    expect(
      urlTemplate("https://api.example.com/sessions/8f14e45fceea167a"),
    ).toEqual({ origin: "api.example.com", template: "/sessions/:id" });
  });

  it("is null for a URL that doesn't parse", () => {
    expect(urlTemplate("not-a-url")).toBeNull();
  });
});

describe("isSdkExportPath", () => {
  it("names the telemetry export endpoints", () => {
    expect(isSdkExportPath("/v1/traces")).toBe(true);
    expect(isSdkExportPath("/otlp/v1/logs")).toBe(true);
    expect(isSdkExportPath("/v1/metrics")).toBe(true);
  });

  it("is false for an ordinary API path", () => {
    expect(isSdkExportPath("/api/checkout")).toBe(false);
  });
});

describe("RUM_TABS / rumTabFromParam", () => {
  it("has the tabs this build ships, in display order", () => {
    expect(RUM_TABS.map((t) => t.id)).toEqual(["overview", "network", "setup"]);
  });

  it("recognizes a known tab", () => {
    expect(rumTabFromParam("setup")).toBe("setup");
  });

  it("settles an unknown or missing tab on overview", () => {
    expect(rumTabFromParam("bogus")).toBe("overview");
    expect(rumTabFromParam(undefined)).toBe("overview");
  });
});

describe("vitalThresholdBound", () => {
  it("formats the good/poor bound in the vital's own display unit", () => {
    expect(vitalThresholdBound("lcp", "good")).toBe("2.5 s");
    expect(vitalThresholdBound("lcp", "poor")).toBe("4.0 s");
    expect(vitalThresholdBound("cls", "good")).toBe("0.10");
  });
});

describe("ratingTextColorVar / ratingSwatchColorVar", () => {
  it("gives good/poor distinct text and swatch tokens", () => {
    expect(ratingTextColorVar("good")).toBe("var(--ok-text)");
    expect(ratingTextColorVar("poor")).toBe("var(--err)");
    expect(ratingTextColorVar("needs-improvement")).toBe(
      "var(--warn-banner-text)",
    );
    expect(ratingSwatchColorVar("good")).toBe("var(--ok)");
    expect(ratingSwatchColorVar("poor")).toBe("var(--err)");
    expect(ratingSwatchColorVar("needs-improvement")).toBe("var(--warn)");
  });
});
