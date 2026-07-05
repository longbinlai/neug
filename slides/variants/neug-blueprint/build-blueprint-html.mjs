import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const repoSlidesRoot = path.resolve(__dirname, "../..");
const baseHtmlPath = path.join(repoSlidesRoot, "outputs", "neug-llm-agent-graph-engine-slides.html");
const outDir = path.join(__dirname, "outputs");
const outFile = path.join(outDir, "neug-blueprint-slides.html");

const pageTopics = [
  "封面",
  "上下文需求",
  "混合检索",
  "查询性能",
  "轻量嵌入",
  "能力地图",
  "组织知识案例",
  "增量维护",
  "业务图建模",
  "图搜索下钻",
  "个人记忆探索",
  "Roadmap",
  "Q&A",
];

const sections = [
  { label: "定位", from: 1, to: 3 },
  { label: "基础能力", from: 4, to: 6 },
  { label: "组织知识", from: 7, to: 8 },
  { label: "业务分析", from: 9, to: 10 },
  { label: "探索与路线", from: 11, to: 13 },
];

const blueprintCss = String.raw`
  <style id="neug-blueprint-theme">
    :root {
      --bg: #05070c;
      --panel: #0b111b;
      --panel-2: #101a28;
      --text: #f6f9ff;
      --muted: #a8b5c8;
      --faint: #52647e;
      --line: #1c3352;
      --green: #1478ff;
      --blue: #43dbff;
      --amber: #93a4bd;
      --neug-blue: #1478ff;
      --cyan: #43dbff;
      --steel: #93a4bd;
      --panel-glow: rgba(20, 120, 255, .12);
      --grid-line: rgba(67, 219, 255, .055);
    }
    body::before {
      background:
        radial-gradient(circle at 82% 12%, rgba(20, 120, 255, .16), transparent 24%),
        radial-gradient(circle at 12% 88%, rgba(67, 219, 255, .10), transparent 28%),
        #03060b;
    }
    .deck {
      background: #05070c;
      box-shadow: 0 28px 90px rgba(0, 0, 0, .68);
    }
    .slide {
      padding: 66px 72px 64px;
      background:
        linear-gradient(rgba(67, 219, 255, .038) 1px, transparent 1px),
        linear-gradient(90deg, rgba(67, 219, 255, .038) 1px, transparent 1px),
        radial-gradient(circle at 78% 16%, rgba(20, 120, 255, .13), transparent 28%),
        linear-gradient(135deg, rgba(20, 120, 255, .05), transparent 36%),
        #05070c;
      background-size: 42px 42px, 42px 42px, auto, auto, auto;
    }
    .slide::after {
      content: "";
      position: absolute;
      inset: 18px;
      border: 1px solid rgba(67, 219, 255, .10);
      pointer-events: none;
    }
    .slide.active { display: block; }
    .cover {
      padding: 58px 72px;
      background:
        linear-gradient(rgba(67, 219, 255, .05) 1px, transparent 1px),
        linear-gradient(90deg, rgba(67, 219, 255, .05) 1px, transparent 1px),
        radial-gradient(circle at 72% 28%, rgba(20, 120, 255, .20), transparent 22%),
        #04070d;
      background-size: 46px 46px, 46px 46px, auto, auto;
    }
    .cover::after {
      content: "";
      position: absolute;
      right: 92px;
      bottom: 96px;
      width: 390px;
      height: 250px;
      border: 1px solid rgba(67, 219, 255, .22);
      background:
        linear-gradient(90deg, transparent 49%, rgba(67, 219, 255, .18) 50%, transparent 51%),
        linear-gradient(transparent 49%, rgba(67, 219, 255, .12) 50%, transparent 51%);
      background-size: 64px 64px;
      opacity: .42;
      clip-path: polygon(0 0, 86% 0, 100% 18%, 100% 100%, 14% 100%, 0 82%);
    }
    .cover .orb {
      display: none;
    }
    .cover-logo {
      width: 170px;
      filter: drop-shadow(0 0 18px rgba(20, 120, 255, .38));
    }
    .kicker {
      color: var(--cyan);
      font-size: 12px;
      letter-spacing: .12em;
      margin-bottom: 22px;
    }
    h1 {
      margin-top: 118px;
      max-width: 790px;
      font-size: 64px;
      line-height: 1.08;
      letter-spacing: 0;
      text-shadow: 0 0 28px rgba(20, 120, 255, .18);
    }
    h2 {
      max-width: 1060px;
      font-size: 38px;
      line-height: 1.2;
      letter-spacing: 0;
    }
    .subtitle,
    .cover-footer,
    .context-lead,
    .hybrid-lead,
    .benchmark-lead,
    .embedded-lead,
    .core-lead,
    .scenario-lead {
      color: var(--muted);
    }
    .rule {
      background: linear-gradient(90deg, var(--neug-blue), var(--cyan));
      height: 3px;
      box-shadow: 0 0 22px rgba(20, 120, 255, .52);
    }
    .bp-nav {
      position: absolute;
      left: 72px;
      right: 72px;
      top: 24px;
      z-index: 20;
      display: flex;
      align-items: center;
      justify-content: space-between;
      pointer-events: none;
    }
    .bp-brand,
    .bp-page {
      display: flex;
      align-items: baseline;
      gap: 10px;
      color: var(--muted);
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
      font-size: 11px;
      letter-spacing: .08em;
      text-transform: uppercase;
    }
    .bp-brand strong {
      color: var(--neug-blue);
      font-size: 14px;
      letter-spacing: .04em;
    }
    .bp-brand span,
    .bp-page span {
      color: var(--faint);
    }
    .bp-page strong {
      color: var(--text);
      font-size: 14px;
      letter-spacing: .04em;
    }
    .bp-page em {
      color: var(--cyan);
      font-style: normal;
      font-size: 11px;
      letter-spacing: .06em;
    }
    .bp-rail {
      position: absolute;
      left: 72px;
      right: 72px;
      bottom: 20px;
      z-index: 20;
      display: grid;
      grid-template-columns: repeat(5, minmax(0, 1fr));
      gap: 10px;
      pointer-events: none;
    }
    .bp-segment {
      min-width: 0;
    }
    .bp-segment-line {
      display: block;
      height: 3px;
      border-radius: 999px;
      background: rgba(82, 100, 126, .34);
      overflow: hidden;
    }
    .bp-segment.active .bp-segment-line {
      background: linear-gradient(90deg, var(--neug-blue), var(--cyan));
      box-shadow: 0 0 18px rgba(20, 120, 255, .42);
    }
    .bp-segment-label {
      display: block;
      margin-top: 7px;
      color: rgba(168, 181, 200, .52);
      font-size: 10px;
      line-height: 1;
      letter-spacing: .05em;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .bp-segment.active .bp-segment-label {
      color: var(--cyan);
    }
    .progress {
      display: none;
    }
    .footer {
      right: 72px;
      bottom: 48px;
      color: rgba(168, 181, 200, .38);
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
      z-index: 21;
    }
    .cover .footer,
    .cover .bp-nav,
    .cover .bp-rail {
      display: none;
    }
    .card,
    .context-card,
    .action-card,
    .benchmark-panel,
    .embedded-card,
    .capability-chip,
    .core-center,
    .cli-node,
    .query-chip,
    .case-question,
    .case-path-step,
    .schema-node,
    .delta-item,
    .call-sequence,
    .finding,
    .business-path,
    .search-map,
    .frontier-row,
    .fit-item,
    .future-boundary,
    .future-signal,
    .future-entry,
    .roadmap-lane,
    .stat-tile {
      background:
        linear-gradient(135deg, rgba(20, 120, 255, .08), transparent 44%),
        rgba(8, 14, 24, .86) !important;
      border-color: rgba(70, 120, 190, .55) !important;
      box-shadow: inset 0 1px 0 rgba(255, 255, 255, .035), 0 12px 38px rgba(0, 0, 0, .18);
    }
    .green,
    .card.green,
    .context-card.green,
    .action-card.green,
    .stat-tile.green,
    .future-entry.green,
    .future-signal.green,
    .roadmap-lane.green {
      border-color: rgba(20, 120, 255, .78) !important;
    }
    .blue,
    .card.blue,
    .context-card.blue,
    .action-card.blue,
    .stat-tile.blue,
    .future-entry.blue,
    .future-signal.blue,
    .roadmap-lane.blue {
      border-color: rgba(67, 219, 255, .72) !important;
    }
    .amber,
    .card.amber,
    .context-card.amber,
    .action-card.amber,
    .stat-tile.amber,
    .future-entry.amber,
    .future-signal.amber,
    .roadmap-lane.amber {
      border-color: rgba(147, 164, 189, .72) !important;
    }
    .context-node,
    .schema-node,
    .frontier-nodes strong,
    .path-chain strong,
    .query-chip,
    .scenario-step,
    .compact-card {
      background: rgba(4, 9, 17, .78) !important;
      border-color: rgba(70, 120, 190, .5) !important;
    }
    .context-edges line {
      stroke: rgba(67, 219, 255, .44);
    }
    .engine-stack,
    .core-diagram,
    .knowledge-schema,
    .business-model {
      background:
        linear-gradient(rgba(67, 219, 255, .035) 1px, transparent 1px),
        linear-gradient(90deg, rgba(67, 219, 255, .035) 1px, transparent 1px),
        rgba(5, 10, 18, .78) !important;
      background-size: 28px 28px, 28px 28px, auto !important;
      border-color: rgba(20, 120, 255, .68) !important;
    }
    .engine-stack::before {
      color: var(--cyan);
    }
    .metric strong,
    .stat-tile strong,
    .benchmark-callout strong,
    .duel strong,
    .lift-strip strong,
    .proof-metric strong,
    .roadmap-takeaway,
    .future-entry em,
    .search-root,
    .analysis-question {
      color: var(--cyan) !important;
    }
    .stat-tile.amber strong,
    .future-entry.amber em {
      color: #c5d1e4 !important;
    }
    .bar-fill.neug,
    .progress {
      background: linear-gradient(90deg, var(--neug-blue), var(--cyan)) !important;
    }
    .connection-quote {
      border-left-color: var(--neug-blue);
    }
    .slide[data-section="组织知识"] .kicker,
    .slide[data-section="业务分析"] .kicker,
    .slide[data-section="个人与设备记忆"] .kicker {
      color: var(--cyan);
    }
    .cover-footer {
      color: #b8c4d6;
    }
    .slide:not(.cover) {
      padding: 114px 54px 40px;
    }
    .slide::after {
      inset: 14px;
    }
    .bp-nav {
      left: 54px;
      right: 54px;
      top: 22px;
    }
    .bp-rail {
      left: 54px;
      right: 54px;
      bottom: 16px;
    }
    .footer {
      right: 54px;
      bottom: 42px;
    }
    .kicker {
      margin-bottom: 14px;
    }
    h2 {
      max-width: 1140px;
      font-size: 43px;
      line-height: 1.14;
    }
    .context-lead,
    .embedded-lead {
      margin-top: 20px;
      max-width: 1120px;
      font-size: 24px;
      line-height: 1.38;
    }
    .hybrid-lead,
    .benchmark-lead,
    .core-lead,
    .scenario-lead,
    .knowledge-case-slide .scenario-lead,
    .knowledge-update-slide .scenario-lead,
    .business-case-slide .scenario-lead,
    .business-search-slide .scenario-lead,
    .future-entry-slide .scenario-lead {
      max-width: 1120px;
      font-size: 21.8px;
      line-height: 1.3;
    }
    .context-layout {
      grid-template-columns: 560px 1fr;
      gap: 46px;
      margin-top: 32px;
    }
    .context-left,
    .context-graph {
      width: 560px;
    }
    .context-graph {
      height: 358px;
    }
    .context-card {
      padding: 20px 24px;
    }
    .context-card h3 {
      font-size: 22px;
    }
    .context-card p {
      font-size: 16.3px;
      line-height: 1.38;
    }
    .connection-quote {
      margin-top: 20px;
      padding: 20px 24px;
    }
    .hybrid-layout {
      grid-template-columns: 420px 170px 1fr;
      gap: 26px;
      margin-top: 44px;
    }
    .benchmark-layout {
      gap: 32px;
      margin-top: 34px;
    }
    .embedded-layout {
      grid-template-columns: 590px 1fr;
      gap: 38px;
      margin-top: 44px;
    }
    .embedded-card {
      padding: 19px 24px;
    }
    .embedded-card h3 {
      font-size: 23px;
    }
    .embedded-card p {
      font-size: 16.8px;
      line-height: 1.34;
    }
    .knowledge-case-slide .knowledge-layout,
    .knowledge-update-slide .update-layout,
    .business-case-slide .business-layout,
    .search-layout,
    .future-layout {
      margin-top: 30px;
    }
    .knowledge-case-slide .case-question strong,
    .business-case-slide .case-question strong {
      font-size: 21.5px;
    }
    .knowledge-case-slide .case-path-step h3,
    .knowledge-update-slide .finding h3 {
      font-size: 17.8px;
    }
    .knowledge-case-slide .case-path-step p,
    .knowledge-update-slide .finding p {
      font-size: 13.8px;
    }
    .knowledge-case-slide .schema-node strong,
    .business-case-slide .business-model .schema-node strong {
      font-size: 16.2px;
    }
    .knowledge-case-slide .schema-node span,
    .business-case-slide .business-model .schema-node span {
      font-size: 12.9px;
    }
    .knowledge-case-slide .stat-tile,
    .knowledge-update-slide .stat-tile,
    .business-case-slide .stat-tile,
    .business-search-slide .stat-tile {
      min-height: 104px;
    }
    .knowledge-update-slide .delta-item {
      min-height: 116px;
    }
    .knowledge-update-slide .call-sequence {
      padding: 18px 20px;
    }
    .knowledge-update-slide .finding {
      min-height: 94px;
      padding: 16px 18px;
    }
    .business-case-slide .business-layout {
      grid-template-columns: 570px 1fr;
      gap: 30px;
    }
    .business-case-slide .business-path {
      padding: 18px 18px;
    }
    .business-case-slide .path-chain strong {
      min-height: 43px;
      font-size: 13.9px;
    }
    .business-case-slide .business-model .schema-node {
      min-height: 66px;
      padding: 12px 14px;
    }
    .search-map {
      padding: 24px 26px;
    }
    .fit-item {
      min-height: 91px;
      padding: 17px 20px;
    }
    .frontier-row {
      padding: 16px 18px;
    }
    .roadmap-lanes {
      margin-top: 72px;
    }
  </style>`;

const navScript = String.raw`
  <script id="neug-blueprint-nav">
    (() => {
      const pageTopics = ${JSON.stringify(pageTopics)};
      const sections = ${JSON.stringify(sections)};
      const slides = [...document.querySelectorAll(".slide")];
      const total = slides.length;
      const sectionForPage = (page) => sections.find((section) => page >= section.from && page <= section.to) ?? sections[0];

      slides.forEach((slide, index) => {
        const page = index + 1;
        const topic = pageTopics[index] ?? "Slide";
        const section = sectionForPage(page);
        slide.dataset.topic = topic;
        slide.dataset.section = section.label;

        if (!slide.classList.contains("cover")) {
          const nav = document.createElement("div");
          nav.className = "bp-nav";
          nav.innerHTML = [
            '<div class="bp-brand"><strong>NeuG</strong><span>' + section.label + '</span></div>',
            '<div class="bp-page"><strong>' + String(page).padStart(2, "0") + '</strong><span>/ ' + String(total).padStart(2, "0") + '</span><em>' + topic + '</em></div>',
          ].join("");
          slide.append(nav);

          const rail = document.createElement("div");
          rail.className = "bp-rail";
          rail.innerHTML = sections.map((item) => {
            const active = page >= item.from && page <= item.to ? " active" : "";
            return '<div class="bp-segment' + active + '"><span class="bp-segment-line"></span><span class="bp-segment-label">' + item.label + '</span></div>';
          }).join("");
          slide.append(rail);
        }
      });
    })();
  </script>`;

const baseHtml = await fs.readFile(baseHtmlPath, "utf8");
const html = baseHtml
  .replace("</head>", `${blueprintCss}\n</head>`)
  .replace("</body>", `${navScript}\n</body>`);

await fs.mkdir(outDir, { recursive: true });
await fs.writeFile(outFile, html, "utf8");
console.log(outFile);
