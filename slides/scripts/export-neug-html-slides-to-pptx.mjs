import fs from "node:fs/promises";
import path from "node:path";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";

const require = createRequire(import.meta.url);
const pptxgen = require("/Users/longbinlai/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules/pptxgenjs");

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, "..");
const outputDir = path.join(root, "outputs");
const finalPptx = path.join(outputDir, "neug-llm-agent-graph-engine.pptx");

const pptx = new pptxgen();
pptx.layout = "LAYOUT_WIDE";
pptx.author = "NeuG";
pptx.subject = "NeuG LLM Agent Graph Engine";
pptx.title = "NeuG | 面向 LLM 与 Agent 的嵌入式图数据引擎";
pptx.company = "GraphScope";
pptx.lang = "zh-CN";
pptx.theme = {
  headFontFace: "Arial",
  bodyFontFace: "Arial",
  lang: "zh-CN",
};

for (let i = 1; i <= 13; i += 1) {
  const imagePath = path.join(outputDir, `neug-html-slide-${i}.png`);
  await fs.access(imagePath);
  const slide = pptx.addSlide();
  slide.background = { color: "0B0D0E" };
  slide.addImage({ path: imagePath, x: 0, y: 0, w: 13.333333, h: 7.5 });
}

await pptx.writeFile({ fileName: finalPptx });
console.log(finalPptx);
