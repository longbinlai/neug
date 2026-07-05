import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "/Users/longbinlai/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules/playwright/index.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const outDir = path.join(__dirname, "outputs");
const fileUrl = `file://${path.join(outDir, "neug-blueprint-slides.html")}`;
const viewportWidth = Number(process.env.VIEWPORT_WIDTH || 1440);
const viewportHeight = Number(process.env.VIEWPORT_HEIGHT || 900);

const browser = await chromium.launch({
  headless: true,
  executablePath: "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
});
const page = await browser.newPage({ viewport: { width: viewportWidth, height: viewportHeight }, deviceScaleFactor: 1 });

await page.goto(`${fileUrl}#/1`);
const count = await page.locator(".slide").count();

for (const slide of Array.from({ length: count }, (_, i) => i + 1)) {
  await page.goto(`${fileUrl}#/${slide}`);
  await page.screenshot({
    path: path.join(outDir, `neug-blueprint-slide-${slide}.png`),
    fullPage: true,
  });
}

const activeCount = await page.locator(".slide.active").count();
await browser.close();

console.log(JSON.stringify({ count, activeCount, viewport: { width: viewportWidth, height: viewportHeight } }, null, 2));
