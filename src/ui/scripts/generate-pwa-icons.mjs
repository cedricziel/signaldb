// Regenerates public/*.png from public/favicon.svg. Run after changing the
// mark (see also src/components/BrandMark.tsx, which draws it for the app
// itself) — nothing else keeps these derivatives in sync.
import sharp from "sharp";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

const svg = readFileSync(new URL("../public/favicon.svg", import.meta.url));
const bg = "#14181e";
const out = (name) =>
  fileURLToPath(new URL(`../public/${name}`, import.meta.url));

async function run() {
  await sharp(svg).resize(192, 192).png().toFile(out("pwa-192x192.png"));
  await sharp(svg).resize(512, 512).png().toFile(out("pwa-512x512.png"));
  // apple-touch-icon: iOS ignores the manifest for "Add to Home Screen" and
  // reads this link instead (see index.html).
  await sharp(svg).resize(180, 180).png().toFile(out("apple-touch-icon.png"));

  // Maskable icon: OS masks (circle, squircle, etc.) crop up to ~20% off
  // each edge, so artwork sits in the safe zone at ~80% scale on an opaque
  // background rather than filling the canvas.
  const maskableArt = await sharp(svg).resize(410, 410).toBuffer();
  await sharp({
    create: { width: 512, height: 512, channels: 4, background: bg },
  })
    .composite([{ input: maskableArt, gravity: "center" }])
    .png()
    .toFile(out("pwa-512x512-maskable.png"));
}

await run();
