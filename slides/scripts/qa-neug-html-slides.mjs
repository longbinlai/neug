import { chromium } from "/Users/robeenly/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules/playwright/index.mjs";

const fileUrl = "file:///Users/robeenly/Documents/neug/outputs/neug-llm-agent-graph-engine-slides.html";
const browser = await chromium.launch({
  headless: true,
  executablePath: "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
});
const page = await browser.newPage({ viewport: { width: 1440, height: 900 }, deviceScaleFactor: 1 });

for (const slide of [1, 2, 3, 4, 5, 6, 15]) {
  await page.goto(`${fileUrl}#/${slide}`);
  await page.screenshot({
    path: `/Users/robeenly/Documents/neug/outputs/neug-html-slide-${slide}.png`,
    fullPage: true,
  });
}

const count = await page.locator(".slide").count();
const activeCount = await page.locator(".slide.active").count();
await browser.close();

console.log(JSON.stringify({ count, activeCount }, null, 2));
