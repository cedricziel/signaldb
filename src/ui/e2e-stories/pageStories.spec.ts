import AxeBuilder from "@axe-core/playwright";
import { expect, test, type Page } from "@playwright/test";
import { ALLOWLIST, type AllowEntry } from "./allowlist";

/** Phone, tablet and the design-sync capture size. */
const VIEWPORTS = [
  { width: 390, height: 844 },
  { width: 768, height: 1024 },
  { width: 1280, height: 800 },
] as const;

/** Each width's stories are split across this many tests so workers share
 * the ~80 page stories evenly. */
const SHARDS = 4;

const PAGE_STORY = /^(pages|shell)-/;

type Finding = { story: string; width: number; message: string };

interface IndexJson {
  entries: Record<string, { id: string; type: string }>;
}

async function pageStoryIds(page: Page): Promise<string[]> {
  const res = await page.request.get("/index.json");
  expect(res.ok(), "storybook index.json").toBe(true);
  const index = (await res.json()) as IndexJson;
  return Object.values(index.entries)
    .filter((e) => e.type === "story" && PAGE_STORY.test(e.id))
    .map((e) => e.id)
    .sort();
}

function allowed(
  story: string,
  width: number,
  check: AllowEntry["check"],
): AllowEntry | undefined {
  return ALLOWLIST.find(
    (e) =>
      e.check === check &&
      matches(e, story) &&
      (!e.widths || e.widths.includes(width)),
  );
}

function matches(entry: AllowEntry, story: string): boolean {
  return entry.story.endsWith("*")
    ? story.startsWith(entry.story.slice(0, -1))
    : story === entry.story;
}

interface PreviewWindow {
  __STORYBOOK_PREVIEW__?: {
    currentRender?: { id?: string; phase?: string };
  };
  __STORYBOOK_ADDONS_CHANNEL__?: {
    emit: (event: string, payload: unknown) => void;
  };
}

/** Shows `story` and waits for Storybook to finish it (loaders, render,
 * play), then for the DOM to stop changing (stubbed queries settle a few
 * ticks later). The first story loads `iframe.html`; later ones switch over
 * Storybook's channel, which unmounts the previous story (restoring its
 * fetch stub and theme) without re-evaluating the whole bundle. */
async function showStory(page: Page, story: string) {
  if (page.url().includes("/iframe.html")) {
    await page.evaluate((storyId) => {
      (window as PreviewWindow).__STORYBOOK_ADDONS_CHANNEL__?.emit(
        "setCurrentStory",
        { storyId, viewMode: "story" },
      );
    }, story);
  } else {
    await page.goto(`/iframe.html?id=${story}&viewMode=story`);
  }
  await page.waitForFunction((storyId) => {
    const render = (window as PreviewWindow).__STORYBOOK_PREVIEW__
      ?.currentRender;
    if (render?.id !== storyId) return false;
    return (
      render.phase === "completed" ||
      render.phase === "finished" ||
      render.phase === "errored" ||
      document.body.classList.contains("sb-show-errordisplay")
    );
  }, story);
  let last = "";
  for (let i = 0; i < 20; i++) {
    const html = await page.evaluate(
      () => document.getElementById("storybook-root")?.innerHTML ?? "",
    );
    if (html === last) return;
    last = html;
    await page.waitForTimeout(100);
  }
}

async function checkStory(
  page: Page,
  story: string,
  width: number,
  used: Set<AllowEntry>,
): Promise<Finding[]> {
  const findings: Finding[] = [];
  const errors: string[] = [];
  const onError = (err: Error) => errors.push(err.message);
  page.on("pageerror", onError);
  try {
    await showStory(page, story);

    const renderError = await page.evaluate(() =>
      document.body.classList.contains("sb-show-errordisplay")
        ? (document.getElementById("error-message")?.textContent ?? "error")
        : null,
    );
    if (renderError) errors.push(renderError);
    for (const message of errors) {
      findings.push({ story, width, message: `threw: ${message}` });
    }

    const { scrollWidth, clientWidth } = await page.evaluate(() => ({
      scrollWidth: document.documentElement.scrollWidth,
      clientWidth: document.documentElement.clientWidth,
    }));
    if (scrollWidth > clientWidth + 1) {
      const entry = allowed(story, width, "overflow");
      if (entry) used.add(entry);
      else
        findings.push({
          story,
          width,
          message: `horizontal overflow: scrollWidth ${scrollWidth} > clientWidth ${clientWidth}`,
        });
    }

    const axe = await new AxeBuilder({ page })
      .include("#storybook-root")
      .withRules(["color-contrast"])
      .analyze();
    const nodes = axe.violations.flatMap((v) => v.nodes);
    if (nodes.length > 0) {
      const entry = allowed(story, width, "color-contrast");
      if (entry && nodes.length <= (entry.nodes ?? Infinity)) used.add(entry);
      else
        findings.push({
          story,
          width,
          message:
            `${nodes.length} color-contrast violation(s)` +
            (entry ? ` (allowlist permits ${entry.nodes})` : "") +
            ":\n" +
            nodes
              .map(
                (n) =>
                  `      ${n.target.join(" ")}: ${(n.failureSummary ?? "").split("\n").slice(1).join(" ").trim()}`,
              )
              .join("\n"),
        });
    }
  } finally {
    page.off("pageerror", onError);
  }
  return findings;
}

for (const viewport of VIEWPORTS) {
  for (let shard = 0; shard < SHARDS; shard++) {
    test(`page stories @${viewport.width}px [${shard + 1}/${SHARDS}]`, async ({
      page,
    }) => {
      test.setTimeout(300_000);
      await page.setViewportSize(viewport);
      const ids = (await pageStoryIds(page)).filter(
        (_, i) => i % SHARDS === shard,
      );
      expect(ids.length, "page stories in this shard").toBeGreaterThan(0);

      const used = new Set<AllowEntry>();
      const findings: Finding[] = [];
      for (const id of ids) {
        findings.push(...(await checkStory(page, id, viewport.width, used)));
      }
      test.info().annotations.push({
        type: "checked",
        description: `${ids.length} stories`,
      });
      for (const e of used) {
        test.info().annotations.push({
          type: "allowlisted",
          description: `${e.story} ${e.check}: ${e.reason}`,
        });
      }
      for (const e of ALLOWLIST) {
        const applies =
          (!e.widths || e.widths.includes(viewport.width)) &&
          ids.some((id) => matches(e, id));
        if (applies && !used.has(e)) {
          const description = `${e.story} ${e.check} @${viewport.width}px no longer fails; delete it from allowlist.ts`;
          test
            .info()
            .annotations.push({ type: "stale-allowlist", description });
          console.warn(`stale allowlist entry: ${description}`);
        }
      }

      expect(
        findings.map((f) => `${f.story} @${f.width}px: ${f.message}`),
      ).toEqual([]);
    });
  }
}

/** Every allowlist entry must still match a story in the build, so a
 * renamed story doesn't leave a dead entry behind. (Entries that match but
 * no longer fail are reported as `stale-allowlist` warnings above.) */
test("allowlist entries name existing stories", async ({ page }) => {
  const ids = await pageStoryIds(page);
  const dead = ALLOWLIST.filter((e) => !ids.some((id) => matches(e, id))).map(
    (e) => `${e.story} (${e.check})`,
  );
  expect(dead).toEqual([]);
});
