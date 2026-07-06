import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, "..");
const outFile = path.join(root, "outputs", "neug-llm-agent-graph-engine-slides.html");
const pipInstallImagePath = path.join(root, "outputs", "assets", "neug-pip-install.png");
const neugLogoPath = "/Users/longbinlai/.agents/skills/neug-wiki/raw/neug-logo/horizontal-logo-dark.png";

const slides = [
  {
    kind: "cover",
    kicker: "NeuG",
    title: "NeuG: 面向 Agent 应用的\n嵌入式图数据引擎",
    subtitle: "支撑企业知识库、自动化分析与端侧关联索引",
    presenter: "汇报者：赖龙彬（尤林）",
    github: "GitHub: https://github.com/alibaba/neug",
    footer: "高性能图查询  |  轻量嵌入  |  企业知识库  |  自动化分析  |  个人与设备记忆",
  },
  {
    kind: "context",
    kicker: "WHY NOW",
    title: "LLM 与 Agent 需要关联复杂、可维护的上下文",
    lead: "问题不是给模型塞进更多 token，而是维护一层能持续更新、检索和分析的上下文关系网。",
    cards: [
      ["组织知识", "LLM Wiki 说明了一个方向：组织知识不能只靠临时切片召回，需要把原始资料整理成可维护的知识层。企业里还要同时维护文档、代码、会议、项目和人员关系。", "green"],
      ["业务分析", "经营分析需要沿客户、商品、渠道、组织和时间连续追问。难点不是生成一次 SQL，而是维护可复用的分析上下文和中间结果。", "blue"],
      ["个人与设备记忆", "个人工具和设备需要维护人、地点、事件、资料、任务和时间线的本地关联上下文。重点不是设备形态，而是端上持续积累的关系网络。", "amber"],
    ],
    quote: "In the extreme view, the world can be seen as only connections, nothing else.",
    quoteBy: "Tim Berners-Lee，万维网之父",
  },
  {
    kind: "hybrid",
    kicker: "POSITIONING",
    title: "复杂上下文需要具备混合检索能力的图引擎",
    lead: "Agent 不只是召回文本，还要在关系、语义和关键词之间来回切换。",
    actions: [
      ["沿关系找", "谁参与了这个项目？这个决策来自哪次会议？这个任务依赖哪些代码和文档？", "green"],
      ["按语义找", "哪些历史讨论和当前问题相似？哪些客户问题表达不同但本质相同？", "blue"],
      ["按关键词找", "精确定位代码符号、业务字段、产品名、指标名和错误信息。", "amber"],
    ],
    stack: [
      ["Graph Core", "实体、关系、路径与持续更新"],
      ["Vector + Full-text", "语义相似与关键词精确匹配"],
      ["GDS + Sampling", "重要性、社区、路径分析与大规模时效性"],
      ["CLI + Skill", "面向 Agent 调用的工具入口"],
    ],
  },
  {
    kind: "benchmark",
    kicker: "FOUNDATION 1",
    title: "NeuG 是单机形态下性能最强的图数据引擎",
    lead: "一组来自 Mac 单机对比 Neo4j，一组来自 LDBC-300 标准图压力测试。",
    mac: {
      title: "Mac 单机 LDBC SNB Interactive",
      subtitle: "Apple Silicon Mac · LDBC SNB SF1 · 4 clients · 300s",
      neugQps: "617",
      neo4jQps: "≈12.2",
      speedup: "50.6×",
      latency: "P95 20.6ms vs Neo4j 1728ms",
      stability: "185,156 queries · 0 failures",
    },
    ldbc300: {
      title: "LDBC-300 标准图测试",
      current: "110,000",
      official: "80,511",
      lift: "+36.6%",
      ops: "609,988,373 operations",
      threads: "192 worker threads",
      duration: "current best result",
    },
  },
  {
    kind: "embedded",
    kicker: "FOUNDATION 2",
    title: "21.5MB 的 pip 包，让图引擎可以进入应用和 Agent runtime",
    lead: "NeuG 的轻量不是为了“小”，而是为了让图数据能力可以像一个库一样被嵌入到应用、工具链、个人和端侧环境里。",
    image: "assets/neug-pip-install.png",
    cards: [
      ["低部署成本", "无需单独图数据库集群、服务进程和复杂运维，更接近 DuckDB / SQLite 的使用方式。", "green"],
      ["贴近 Agent 执行环境", "Agent 可以在本地工具链、CLI、Skill 和数据分析脚本里直接调用图查询与混合检索能力。", "blue"],
      ["为端侧生态预留入口", "轻量嵌入形态可面向手机、眼镜、可穿戴等设备，为个人与设备记忆保留端侧关联索引入口。", "amber"],
    ],
  },
  {
    kind: "core",
    kicker: "CAPABILITY MAP",
    title: "NeuG Graph Core 统一承载扩展、分析与 CLI 查询",
    lead: "Vector Extension 和 Graph Analytics 接入 NeuG Graph，并通过 CALL 与 CLI 对外提供统一查询入口。",
    extensions: [
      ["Vector Extension", "向量索引、相似检索、memory embedding", "CALL vector.*", "blue"],
      ["Graph Analytics", "PageRank、BFS / DFS、community detection、sampling", "CALL graph.*", "amber"],
    ],
    core: ["NeuG Graph Core", "统一管理实体、关系、属性、索引与执行计划"],
    cli: ["NeuG CLI", "同一个 CLI 同时支持 graph query / vector retrieval / graph analytics"],
    queries: [
      ["Graph Query", "MATCH / traversal"],
      ["Vector Retrieval", "ANN / similarity"],
      ["Graph Analytics", "CALL graph.algorithm"],
    ],
    scenarios: [
      ["组织知识", "green"],
      ["业务分析", "blue"],
      ["个人与设备记忆", "amber"],
    ],
  },
  {
    kind: "knowledge-case",
    kicker: "组织知识",
    title: "组织知识｜先从研发知识库切入",
    lead: "组织知识覆盖面广，研发知识库是最容易验证的切入点：代码、文档、模块、概念和版本天然有关系，最能体现 NeuG 维护上下文图的价值。",
    caseLabel: "研发知识库案例",
    question: "对比 MySQL / PostgreSQL / NeuG 在 MVCC、WAL、隔离级别和并发控制上的实现差异。",
    graphNodes: [
      ["Repo", "MySQL / PostgreSQL / NeuG", "green"],
      ["Module", "Transaction / WAL / Optimizer", "blue"],
      ["Concept", "MVCC / WAL / isolation", "amber"],
      ["Wiki", "模块摘要与对比结论", "green"],
      ["Raw", "源码、README、设计文档", "muted"],
    ],
    path: [
      ["问题解析", "从问题里抽取 MVCC、WAL、隔离级别等 concept"],
      ["图扩展", "MATCH concept → module → repo，补齐三个仓库的相关模块"],
      ["混合检索", "向量找相近设计，全文锁定关键符号和术语"],
      ["证据回溯", "答案从 wiki 摘要返回，必要时沿 source 回到 raw"],
    ],
    stats: [
      ["3w vs 11w", "使用 wiki 后上下文显著变短", "green"],
      ["≈6×", "token / credit 开销降低", "blue"],
      ["4.33×", "回答时间从 13min 降到 3min", "amber"],
    ],
  },
  {
    kind: "knowledge-update",
    kicker: "组织知识",
    title: "研发知识库｜增量维护避免知识腐化",
    lead: "研发知识不是静态文档。代码、接口和设计持续变化，如果 wiki 不跟着更新，Agent 会引用过期概念，最后让知识库变得不可用。",
    delta: [
      ["代码变更", "新增模块、接口、函数调用"],
      ["知识漂移", "wiki 摘要和 concept 开始落后"],
      ["答案失真", "Agent 继续引用过期上下文"],
    ],
    calls: [
      "1. 解析 PR / release diff，得到变更实体和关系",
      "2. 用 COPY TEMP 临时载入，不污染正式知识图",
      "3. 在 NeuG 中分析影响范围：哪些模块、concept、wiki 页受影响",
      "4. 生成 wiki 更新建议，并保留 source 回溯链路",
      "5. PR 审查通过后，再写入正式知识库",
    ],
    findings: [
      ["先隔离", "变更先进入临时图，避免错误更新污染正式知识库", "green"],
      ["再定位", "沿函数、模块、concept、wiki 页找出需要维护的范围", "blue"],
      ["后入库", "人审查 diff 后再合并，知识更新变成可控流程", "amber"],
    ],
    stats: [
      ["不维护", "知识会腐化，答案越来越不可信", "green"],
      ["临时图", "先分析，不污染正式库", "blue"],
      ["可审查", "wiki / concept diff 后再入库", "amber"],
    ],
  },
  {
    kind: "business-case",
    kicker: "业务分析",
    title: "业务分析｜先从招聘与岗位变化切入",
    lead: "业务分析覆盖面广，招聘与岗位变化适合作为第一类验证场景：岗位、职业、职级、地区和时间天然相连，NeuG 可以先把这些口径建成业务图。",
    caseLabel: "NeuGBI 案例问题",
    question: "AI 对美国就业的冲击有多大？",
    modelNodes: [
      ["Job", "岗位记录 / 招聘数量", "green"],
      ["Occupation", "职业分类 / 软件开发", "blue"],
      ["Seniority", "L1-L7 / Junior vs Senior", "amber"],
      ["Region", "州 / 地区变化", "blue"],
      ["Time", "2021-2025 趋势", "green"],
    ],
    paths: [
      ["问题拆解", "AI 冲击", "招聘数量", "岗位结构"],
      ["对比口径", "Junior", "Senior", "L1-L7"],
      ["证据落点", "职业分类", "软件开发", "地区 / 时间"],
    ],
    stats: [
      ["60GB", "Revelio Lab 美国就业数据", "green"],
      ["3 亿条", "记录规模超过 prompt 上下文", "blue"],
      ["业务图", "岗位、职业、职级、地区、时间放在一张图里", "amber"],
    ],
  },
  {
    kind: "business-search",
    kicker: "业务分析",
    title: "业务分析｜多轮下钻天然适合图搜索",
    lead: "每一轮分析都会从当前发现出发，扩展候选维度、保留路径状态、剪掉无效分支；这和图中的 BFS / DFS 分析负载很像，所以适合用图数据库承载。",
    root: "AI 对就业影响？",
    levels: [
      ["扩展层 1", "整体趋势", "职业", "地区"],
      ["扩展层 2", "计算机职业", "Junior / Senior", "主要州"],
      ["扩展层 3", "软件开发", "L1 / L2 / L3+", "结论回写"],
    ],
    graphFit: [
      ["前沿扩展", "当前候选维度就是下一轮要扩展的前沿节点", "green"],
      ["路径状态", "每轮发现和中间分组需要沿分析路径保留下来", "blue"],
      ["分支剪枝", "先用采样判断方向，剪掉低价值分支", "amber"],
    ],
    stats: [
      ["-29.4%", "Junior 岗位记录数下降", "green"],
      ["-5.8%", "Senior 岗位记录数下降", "blue"],
      ["280万→145万", "软件开发 L2 接近腰斩", "amber"],
    ],
  },
  {
    kind: "future-entry",
    kicker: "个人与设备记忆",
    title: "个人与设备记忆｜从小场景验证端侧关联索引",
    lead: "这个方向还在探索阶段，先从个人高频、数据边界清楚的小场景验证 NeuG 对事件、资料和任务关系的管理价值。",
    boundary: {
      label: "验证方式",
      title: "先选数据边界清楚的小场景",
      body: "下面三个入口都有明确的数据来源和可回答的问题，后续可以选择一个做演示验证，判断本地关系索引是否真的能提升 Agent 记忆能力。",
    },
    entries: [
      [
        "会后任务记忆",
        "把会议纪要、IM、日历和 PR 关联到人、项目、截止时间。",
        "可回答：上次周会提到的 blocker，后来是谁在跟？",
        "green",
      ],
      [
        "个人资料回溯",
        "本地文档、代码片段、浏览记录只抽取摘要、来源和任务关系。",
        "可回答：这个设计决策当时参考了哪几份材料？",
        "blue",
      ],
      [
        "设备事件索引",
        "手机或眼镜只记录轻量事件：见了谁、在哪、关联哪件事。",
        "可回答：上周在客户现场提到的需求是什么？",
        "amber",
      ],
    ],
    signals: [
      ["轻量嵌入", "先在桌面 Agent 或个人工具链里验证，不依赖新硬件。", "green"],
      ["本地优先", "个人高频数据可以留在本机，只把必要摘要交给模型。", "blue"],
      ["关系清楚", "人、事、时间、资料天然是图，比 prompt 历史更可维护。", "amber"],
    ],
  },
  {
    kind: "roadmap",
    kicker: "ROADMAP",
    title: "下一步先打磨两个可展示闭环，保留个人记忆探索入口",
    lead: "汇报结论：NeuG 不是单点图数据库，而是面向 LLM / Agent 的嵌入式图数据底座。",
    lanes: [
      ["组织知识闭环", "Vector + full-text + GDS + CLI，做企业知识库 / 代码知识库演示验证。", "green"],
      ["业务分析闭环", "数据湖接入 + 采样 + NeuGBI，把招聘/经营分析跑成端到端演示验证。", "blue"],
      ["个人记忆探索", "围绕会后任务、资料回溯或设备事件选择小场景，验证端侧关联索引价值。", "amber"],
    ],
  },
  {
    kind: "qa",
    kicker: "Q&A",
    title: "Q&A",
    lead: "欢迎讨论 NeuG 的定位、下一步验证闭环和 Agent 应用接入方式。",
    questions: [
      ["场景优先级", "组织知识和 NeuGBI 两个闭环，哪个更适合作为第一阶段演示？"],
      ["技术验证", "混合检索、图分析、采样和 CLI，哪些能力需要优先打磨成稳定接口？"],
      ["应用接入", "NeuG 作为嵌入式图引擎，应该先进入哪些 Agent runtime 或工具链？"],
    ],
    github: "https://github.com/alibaba/neug",
  },
];

function esc(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function cls(accent) {
  return accent ? ` ${accent}` : "";
}

function renderCards(cards, layout = "cards") {
  return `<div class="${layout}">${cards
    .map(
      ([title, body, accent]) => `
        <article class="card${cls(accent)}">
          <h3>${esc(title)}</h3>
          <p>${esc(body)}</p>
        </article>`,
    )
    .join("")}</div>`;
}

function renderMetrics(metrics) {
  return `<div class="metrics">${metrics
    .map(
      ([value, label, accent]) => `
        <div class="metric${cls(accent)}">
          <strong>${esc(value)}</strong>
          <span>${esc(label)}</span>
        </div>`,
    )
    .join("")}</div>`;
}

function renderScenarioSteps(steps) {
  return `<div class="scenario-steps">${steps
    .map(
      ([title, body], i) => `
        <div class="scenario-step">
          <span>${String(i + 1).padStart(2, "0")}</span>
          <h3>${esc(title)}</h3>
          <p>${esc(body)}</p>
        </div>`,
    )
    .join('<div class="scenario-step-line"></div>')}</div>`;
}

function renderStatTiles(stats) {
  return `<div class="stat-tiles">${stats
    .map(
      ([value, label, accent]) => `
        <div class="stat-tile${cls(accent)}">
          <strong>${esc(value)}</strong>
          <span>${esc(label)}</span>
        </div>`,
    )
    .join("")}</div>`;
}

function renderCompactCards(cards) {
  return `<div class="compact-cards">${cards
    .map(
      ([title, body, accent]) => `
        <article class="compact-card${cls(accent)}">
          <h3>${esc(title)}</h3>
          <p>${esc(body)}</p>
        </article>`,
    )
    .join("")}</div>`;
}

function renderContextGraph() {
  const nodes = [
    ["agent", "Agent / User", 220, 112, "core"],
    ["docs", "文档", 70, 36, "green"],
    ["code", "代码", 282, 28, "blue"],
    ["meeting", "会议", 406, 92, "amber"],
    ["project", "项目", 72, 176, "blue"],
    ["customer", "客户", 394, 200, "green"],
    ["task", "任务", 232, 226, "amber"],
    ["time", "时间", 32, 252, "muted"],
    ["device", "设备事件", 416, 278, "blue"],
    ["dialog", "对话", 164, 284, "green"],
  ];
  const edges = [
    ["agent", "docs"],
    ["agent", "code"],
    ["agent", "meeting"],
    ["agent", "project"],
    ["agent", "customer"],
    ["agent", "task"],
    ["task", "meeting"],
    ["task", "time"],
    ["project", "code"],
    ["project", "docs"],
    ["customer", "meeting"],
    ["dialog", "agent"],
    ["dialog", "time"],
    ["device", "time"],
    ["device", "customer"],
  ];
  const lookup = Object.fromEntries(nodes.map(([id, , x, y]) => [id, { x, y }]));
  const lines = edges.map(([from, to]) => {
    const a = lookup[from];
    const b = lookup[to];
    return `<line x1="${a.x + 48}" y1="${a.y + 26}" x2="${b.x + 48}" y2="${b.y + 26}" />`;
  });
  return `
    <div class="context-graph" aria-label="动态上下文关系网">
      <svg class="context-edges" viewBox="0 0 530 340" role="presentation">${lines.join("")}</svg>
      ${nodes
        .map(
          ([id, label, x, y, accent]) => `
            <div class="context-node ${accent}" style="left:${x}px;top:${y}px" data-node="${id}">
              ${esc(label)}
            </div>`,
        )
        .join("")}
    </div>`;
}

function renderPlaceholder(label, className = "") {
  return `<div class="placeholder ${className}"><span>${esc(label)}</span></div>`;
}

let pipInstallImageDataUri = "";
let neugLogoDataUri = "";

function assetSrc(src) {
  if (src === "assets/neug-pip-install.png" && pipInstallImageDataUri) {
    return pipInstallImageDataUri;
  }
  return src;
}

function renderFooter(page) {
  return `<div class="footer"><span>${page}</span></div>`;
}

function renderSlide(slide, index) {
  const page = String(index + 1).padStart(2, "0");
  if (slide.kind === "cover") {
    return `
      <section class="slide cover" data-page="${page}">
        <div class="orb orb-a"></div>
        <div class="orb orb-b"></div>
        ${neugLogoDataUri ? `<img class="cover-logo" src="${neugLogoDataUri}" alt="NeuG" />` : ""}
        <div class="kicker">${esc(slide.kicker)}</div>
        <h1>${esc(slide.title).replaceAll("\n", "<br>")}</h1>
        <p class="subtitle">${esc(slide.subtitle)}</p>
        <p class="cover-presenter">${esc(slide.presenter)}</p>
        <p class="cover-github">${esc(slide.github)}</p>
        <div class="rule"></div>
        <p class="cover-footer">${esc(slide.footer)}</p>
      </section>`;
  }
  if (slide.kind === "context") {
    return `
      <section class="slide context-slide" data-page="${page}">
        <div class="kicker">${esc(slide.kicker)}</div>
        <h2>${esc(slide.title)}</h2>
        <p class="context-lead">${esc(slide.lead)}</p>
        <div class="context-layout">
          <div class="context-left">
            ${renderContextGraph()}
            <figure class="connection-quote">
              <blockquote>${esc(slide.quote)}</blockquote>
              <figcaption>— ${esc(slide.quoteBy)}</figcaption>
            </figure>
          </div>
          <div class="context-cards">
            ${slide.cards
              .map(
                ([title, body, accent]) => `
                  <article class="context-card ${accent}">
                    <h3>${esc(title)}</h3>
                    <p>${esc(body)}</p>
                  </article>`,
              )
              .join("")}
          </div>
        </div>
        ${renderFooter(page)}
      </section>`;
  }
  if (slide.kind === "hybrid") {
    return `
      <section class="slide hybrid-slide" data-page="${page}">
        <div class="kicker">${esc(slide.kicker)}</div>
        <h2>${esc(slide.title)}</h2>
        <p class="hybrid-lead">${esc(slide.lead)}</p>
        <div class="hybrid-layout">
          <div class="hybrid-actions">
            ${slide.actions
              .map(
                ([title, body, accent]) => `
                  <article class="action-card ${accent}">
                    <h3>${esc(title)}</h3>
                    <p>${esc(body)}</p>
                  </article>`,
              )
              .join("")}
          </div>
          <div class="hybrid-arrow" aria-hidden="true">
            <span>Context graph</span>
            <strong>→</strong>
            <span>Hybrid retrieval engine</span>
          </div>
          <div class="engine-stack">
            ${slide.stack
              .map(
                ([title, body], i) => `
                  <div class="stack-layer layer-${i}">
                    <h3>${esc(title)}</h3>
                    <p>${esc(body)}</p>
                  </div>`,
              )
              .join("")}
          </div>
        </div>
        ${renderFooter(page)}
      </section>`;
  }
  if (slide.kind === "benchmark") {
    return `
      <section class="slide benchmark-slide" data-page="${page}">
        <div class="kicker">${esc(slide.kicker)}</div>
        <h2>${esc(slide.title)}</h2>
        <p class="benchmark-lead">${esc(slide.lead)}</p>
        <div class="benchmark-layout">
          <article class="benchmark-panel mac-panel">
            <div class="panel-head">
              <h3>${esc(slide.mac.title)}</h3>
              <p>${esc(slide.mac.subtitle)}</p>
            </div>
            <div class="qps-bars">
              <div class="bar-row">
                <span>NeuG</span>
                <div class="bar-track"><div class="bar-fill neug" style="width:100%"></div></div>
                <strong>${esc(slide.mac.neugQps)} QPS</strong>
              </div>
              <div class="bar-row">
                <span>Neo4j</span>
                <div class="bar-track"><div class="bar-fill neo4j" style="width:2.0%"></div></div>
                <strong>${esc(slide.mac.neo4jQps)} QPS</strong>
              </div>
            </div>
            <div class="benchmark-callout">
              <strong>${esc(slide.mac.speedup)}</strong>
              <span>吞吐提升</span>
            </div>
            <div class="benchmark-facts">
              <span>${esc(slide.mac.latency)}</span>
              <span>${esc(slide.mac.stability)}</span>
            </div>
          </article>
          <article class="benchmark-panel ldbc-panel">
            <div class="panel-head">
              <h3>${esc(slide.ldbc300.title)}</h3>
              <p>当前测试结果 vs 之前 LDBC 官方打榜成绩</p>
            </div>
            <div class="duel-metrics">
              <div class="duel current">
                <span>当前测试</span>
                <strong>${esc(slide.ldbc300.current)}</strong>
                <em>op/s</em>
              </div>
              <div class="duel official">
                <span>官方打榜</span>
                <strong>${esc(slide.ldbc300.official)}</strong>
                <em>ops</em>
              </div>
            </div>
            <div class="lift-strip">
              <span>相对提升</span>
              <strong>${esc(slide.ldbc300.lift)}</strong>
            </div>
            <div class="benchmark-facts three">
              <span>${esc(slide.ldbc300.ops)}</span>
              <span>${esc(slide.ldbc300.threads)}</span>
              <span>${esc(slide.ldbc300.duration)}</span>
            </div>
          </article>
        </div>
        ${renderFooter(page)}
      </section>`;
  }
  if (slide.kind === "embedded") {
    return `
      <section class="slide embedded-slide" data-page="${page}">
        <div class="kicker">${esc(slide.kicker)}</div>
        <h2>${esc(slide.title)}</h2>
        <p class="embedded-lead">${esc(slide.lead)}</p>
        <div class="embedded-layout">
          <div class="install-proof">
            <div class="proof-topline">
              <span>pip install neug</span>
              <strong>v0.1.3</strong>
            </div>
            <img src="${esc(assetSrc(slide.image))}" alt="NeuG pip install terminal screenshot">
            <div class="proof-metric">
              <strong>21.5MB</strong>
              <span>macOS arm64 wheel</span>
            </div>
          </div>
          <div class="embedded-cards">
            ${slide.cards
              .map(
                ([title, body, accent]) => `
                  <article class="embedded-card ${accent}">
                    <h3>${esc(title)}</h3>
                    <p>${esc(body)}</p>
                  </article>`,
              )
              .join("")}
          </div>
        </div>
        ${renderFooter(page)}
      </section>`;
  }
  if (slide.kind === "core") {
    return `
      <section class="slide core-slide" data-page="${page}">
        <div class="kicker">${esc(slide.kicker)}</div>
        <h2>${esc(slide.title)}</h2>
        <p class="core-lead">${esc(slide.lead)}</p>
        <div class="core-system">
          <div class="core-column extensions-column">
            <div class="column-label">Extensions via CALL</div>
            ${slide.extensions
              .map(
                ([title, body, call, accent]) => `
                  <article class="system-module ${accent}">
                    <h3>${esc(title)}</h3>
                    <p>${esc(body)}</p>
                    <code>${esc(call)}</code>
                  </article>`,
              )
              .join("")}
          </div>
          <div class="system-arrow" aria-hidden="true">
            <span>CALL</span>
            <strong>→</strong>
          </div>
          <div class="graph-core-panel">
            <div class="core-ring">
              <span>${esc(slide.core[0])}</span>
              <p>${esc(slide.core[1])}</p>
            </div>
          </div>
          <div class="system-arrow" aria-hidden="true">
            <span>Unified API</span>
            <strong>→</strong>
          </div>
          <div class="core-column cli-column">
            <div class="cli-panel">
              <span>${esc(slide.cli[0])}</span>
              <p>${esc(slide.cli[1])}</p>
            </div>
            <div class="query-strip">
              ${slide.queries.map(([title]) => `<span>${esc(title)}</span>`).join("")}
            </div>
          </div>
        </div>
        <div class="scenario-bridge">
          <span class="bridge-label">支撑三类业务场景</span>
          ${slide.scenarios
            .map(
              ([title, accent]) => `
                <div class="scenario-chip ${accent}">
                  <strong>${esc(title)}</strong>
                </div>`,
            )
            .join("")}
        </div>
        ${renderFooter(page)}
      </section>`;
  }
  if (slide.kind === "knowledge-case") {
    return `
      <section class="slide knowledge-case-slide" data-page="${page}">
        <div class="kicker">${esc(slide.kicker)}</div>
        <h2>${esc(slide.title)}</h2>
        <p class="scenario-lead">${esc(slide.lead)}</p>
        <div class="knowledge-layout">
          <div class="case-left">
            <div class="case-question">
              <span>${esc(slide.caseLabel ?? "具体问题")}</span>
              <strong>${esc(slide.question)}</strong>
            </div>
            <div class="case-path">
              ${slide.path
                .map(
                  ([title, body], i) => `
                    <div class="case-path-step">
                      <em>${String(i + 1).padStart(2, "0")}</em>
                      <h3>${esc(title)}</h3>
                      <p>${esc(body)}</p>
                    </div>`,
                )
                .join("")}
            </div>
          </div>
          <div class="case-right">
            <div class="schema-title">NeuG 中的知识图建模</div>
            <div class="knowledge-schema">
              ${slide.graphNodes
                .map(
                  ([title, body, accent]) => `
                    <div class="schema-node${cls(accent)}">
                      <strong>${esc(title)}</strong>
                      <span>${esc(body)}</span>
                    </div>`,
                )
                .join("")}
            </div>
            ${renderStatTiles(slide.stats)}
          </div>
        </div>
        ${renderFooter(page)}
      </section>`;
  }
  if (slide.kind === "knowledge-update") {
    return `
      <section class="slide knowledge-update-slide" data-page="${page}">
        <div class="kicker">${esc(slide.kicker)}</div>
        <h2>${esc(slide.title)}</h2>
        <p class="scenario-lead">${esc(slide.lead)}</p>
        <div class="update-layout">
          <div class="update-left">
            <div class="delta-stack">
              ${slide.delta
                .map(
                  ([title, body], i) => `
                    <div class="delta-item">
                      <span>${String(i + 1).padStart(2, "0")}</span>
                      <strong>${esc(title)}</strong>
                      <p>${esc(body)}</p>
                    </div>`,
                )
                .join("")}
            </div>
            <div class="call-sequence">
              <span>NeuG 调用路径</span>
              ${slide.calls.map((call) => `<code>${esc(call)}</code>`).join("")}
            </div>
          </div>
          <div class="update-right">
            <div class="finding-list">
              ${slide.findings
                .map(
                  ([title, body, accent]) => `
                    <article class="finding${cls(accent)}">
                      <h3>${esc(title)}</h3>
                      <p>${esc(body)}</p>
                    </article>`,
                )
                .join("")}
            </div>
            ${renderStatTiles(slide.stats)}
          </div>
        </div>
        ${renderFooter(page)}
      </section>`;
  }
  if (slide.kind === "future-entry") {
    return `
      <section class="slide future-entry-slide" data-page="${page}">
        <div class="kicker">${esc(slide.kicker)}</div>
        <h2>${esc(slide.title)}</h2>
        <p class="scenario-lead">${esc(slide.lead)}</p>
        <div class="future-layout">
          <div class="future-boundary">
            <span>${esc(slide.boundary.label)}</span>
            <strong>${esc(slide.boundary.title)}</strong>
            <p>${esc(slide.boundary.body)}</p>
            <div class="future-signals">
              ${slide.signals
                .map(
                  ([title, body, accent]) => `
                    <article class="future-signal${cls(accent)}">
                      <h3>${esc(title)}</h3>
                      <p>${esc(body)}</p>
                    </article>`,
                )
                .join("")}
            </div>
          </div>
          <div class="future-entry-list">
            ${slide.entries
              .map(
                ([title, body, example, accent]) => `
                  <article class="future-entry${cls(accent)}">
                    <h3>${esc(title)}</h3>
                    <p>${esc(body)}</p>
                    <em>${esc(example)}</em>
                  </article>`,
              )
              .join("")}
          </div>
        </div>
        ${renderFooter(page)}
      </section>`;
  }
  if (slide.kind === "scenario") {
    return `
      <section class="slide scenario-slide" data-page="${page}">
        <div class="kicker">${esc(slide.kicker)}</div>
        <h2>${esc(slide.title)}</h2>
        <p class="scenario-lead">${esc(slide.lead)}</p>
        <div class="scenario-layout">
          <div class="scenario-flow-panel">
            ${renderScenarioSteps(slide.steps)}
          </div>
          <div class="scenario-evidence-panel">
            <div class="evidence-label">关键证据 / 落地条件</div>
            ${renderStatTiles(slide.stats)}
          </div>
        </div>
        ${renderFooter(page)}
      </section>`;
  }
  if (slide.kind === "business-case") {
    return `
      <section class="slide business-case-slide" data-page="${page}">
        <div class="kicker">${esc(slide.kicker)}</div>
        <h2>${esc(slide.title)}</h2>
        <p class="scenario-lead">${esc(slide.lead)}</p>
        <div class="business-layout">
          <div class="business-left">
            <div class="case-question">
              <span>${esc(slide.caseLabel)}</span>
              <strong>${esc(slide.question)}</strong>
            </div>
            <div class="business-paths">
              ${slide.paths
                .map(
                  ([label, ...items], pathIndex) => `
                    <div class="business-path ${pathIndex === 0 ? "green" : "blue"}">
                      <span>${esc(label)}</span>
                      <div class="path-chain">
                        ${items
                          .map((item, itemIndex) => `<strong>${esc(item)}</strong>${itemIndex < items.length - 1 ? "<em>→</em>" : ""}`)
                          .join("")}
                      </div>
                    </div>`,
                )
                .join("")}
            </div>
          </div>
          <div class="business-right">
            <div class="schema-title">NeuG 中的业务图建模</div>
            <div class="business-model">
              ${slide.modelNodes
                .map(
                  ([title, body, accent]) => `
                    <div class="schema-node${cls(accent)}">
                      <strong>${esc(title)}</strong>
                      <span>${esc(body)}</span>
                    </div>`,
                )
                .join("")}
            </div>
            ${renderStatTiles(slide.stats)}
          </div>
        </div>
        ${renderFooter(page)}
      </section>`;
  }
  if (slide.kind === "business-search") {
    return `
      <section class="slide business-search-slide" data-page="${page}">
        <div class="kicker">${esc(slide.kicker)}</div>
        <h2>${esc(slide.title)}</h2>
        <p class="scenario-lead">${esc(slide.lead)}</p>
        <div class="search-layout">
          <div class="search-map">
            <div class="search-root">${esc(slide.root)}</div>
            <div class="frontier-list">
              ${slide.levels
                .map(
                  ([label, ...nodes], i) => `
                    <div class="frontier-row frontier-${i}">
                      <span>${esc(label)}</span>
                      <div class="frontier-nodes">
                        ${nodes.map((node) => `<strong>${esc(node)}</strong>`).join("")}
                      </div>
                    </div>`,
                )
                .join("")}
            </div>
          </div>
          <div class="search-proof">
            <div class="fit-list">
              ${slide.graphFit
                .map(
                  ([title, body, accent]) => `
                    <article class="fit-item${cls(accent)}">
                      <h3>${esc(title)}</h3>
                      <p>${esc(body)}</p>
                    </article>`,
                )
                .join("")}
            </div>
            <div class="search-stats">
              ${renderStatTiles(slide.stats)}
            </div>
          </div>
        </div>
        ${renderFooter(page)}
      </section>`;
  }
  if (slide.kind === "business-loop") {
    return `
      <section class="slide business-loop-slide" data-page="${page}">
        <div class="kicker">${esc(slide.kicker)}</div>
        <h2>${esc(slide.title)}</h2>
        <p class="scenario-lead">${esc(slide.lead)}</p>
        <div class="business-loop-layout">
          <div class="loop-left">
            <div class="risk-list">
              ${slide.risks
                .map(
                  ([title, body], i) => `
                    <div class="risk-item">
                      <span>${String(i + 1).padStart(2, "0")}</span>
                      <strong>${esc(title)}</strong>
                      <p>${esc(body)}</p>
                    </div>`,
                )
                .join("")}
            </div>
            <div class="call-sequence business-steps">
              <span>NeuG 分析闭环</span>
              ${slide.steps.map(([title, body]) => `<code><b>${esc(title)}</b> · ${esc(body)}</code>`).join("")}
            </div>
          </div>
          <div class="loop-right">
            <div class="evidence-label">案例结论</div>
            ${renderStatTiles(slide.stats)}
          </div>
        </div>
        ${renderFooter(page)}
      </section>`;
  }
  if (slide.kind === "analysis") {
    return `
      <section class="slide analysis-slide" data-page="${page}">
        <div class="kicker">${esc(slide.kicker)}</div>
        <h2>${esc(slide.title)}</h2>
        <p class="scenario-lead">${esc(slide.lead)}</p>
        <div class="analysis-layout">
          <div class="analysis-tree">
            <div class="analysis-question">${esc(slide.center)}</div>
            <div class="analysis-paths">
              ${slide.paths
                .map(
                  ([label, ...items], pathIndex) => `
                    <div class="analysis-path ${pathIndex === 0 ? "green" : "blue"}">
                      <span>${esc(label)}</span>
                      <div class="path-chain">
                        ${items
                          .map((item, itemIndex) => `<strong>${esc(item)}</strong>${itemIndex < items.length - 1 ? "<em>→</em>" : ""}`)
                          .join("")}
                      </div>
                    </div>`,
                )
                .join("")}
            </div>
          </div>
          <div class="analysis-proof">
            <div class="evidence-label">案例证据</div>
            ${renderStatTiles(slide.stats)}
          </div>
        </div>
        ${renderFooter(page)}
      </section>`;
  }
  if (slide.kind === "roadmap") {
    return `
      <section class="slide roadmap-slide" data-page="${page}">
        <div class="kicker">${esc(slide.kicker)}</div>
        <h2>${esc(slide.title)}</h2>
        <p class="scenario-lead">${esc(slide.lead)}</p>
        <div class="roadmap-lanes">
          ${slide.lanes
            .map(
              ([title, body, accent], i) => `
                <article class="roadmap-lane${cls(accent)}">
                  <span>${String(i + 1).padStart(2, "0")}</span>
                  <h3>${esc(title)}</h3>
                  <p>${esc(body)}</p>
                </article>`,
            )
            .join("")}
        </div>
        ${slide.takeaway ? `<p class="roadmap-takeaway">${esc(slide.takeaway)}</p>` : ""}
        ${renderFooter(page)}
      </section>`;
  }
  if (slide.kind === "qa") {
    return `
      <section class="slide qa-slide" data-page="${page}">
        <div class="kicker">${esc(slide.kicker)}</div>
        <div class="qa-layout">
          <div class="qa-main">
            <h2>${esc(slide.title)}</h2>
            <p class="scenario-lead">${esc(slide.lead)}</p>
            <p class="qa-github">${esc(slide.github)}</p>
          </div>
          <div class="qa-questions">
            ${slide.questions
              .map(
                ([title, body], i) => `
                  <article class="qa-question">
                    <span>${String(i + 1).padStart(2, "0")}</span>
                    <h3>${esc(title)}</h3>
                    <p>${esc(body)}</p>
                  </article>`,
              )
              .join("")}
          </div>
        </div>
        ${renderFooter(page)}
      </section>`;
  }

  let body = "";
  if (slide.metrics) body += renderMetrics(slide.metrics);
  if (slide.lead) body += `<p class="lead">${esc(slide.lead)}</p>`;
  if (slide.orbit) {
    body += `<div class="orbit">${slide.orbit
      .map((item, i) => `<div class="node ${i % 2 ? "blue" : "green"}">${esc(item)}</div>`)
      .join('<div class="edge"></div>')}</div>`;
  }
  if (slide.cards) body += renderCards(slide.cards, slide.cards.length === 2 ? "cards two" : slide.cards.length === 5 ? "cards five" : "cards");
  if (slide.flow) {
    body += `<div class="flow">${slide.flow
      .map(
        ([title, bodyText], i) => `
          <div class="flow-item">
            <h3>${esc(title)}</h3>
            <p>${esc(bodyText)}</p>
          </div>${i < slide.flow.length - 1 ? '<div class="flow-line"></div>' : ""}`,
      )
      .join("")}</div>`;
  }
  if (slide.placeholders) {
    body += `<div class="placeholder-row">${slide.placeholders.map((p) => renderPlaceholder(p)).join("")}</div>`;
  }
  if (slide.placeholder && slide.bullets) {
    body += `<div class="split">${renderPlaceholder(slide.placeholder)}<div class="text-block">
      ${slide.bulletsTitle ? `<h3>${esc(slide.bulletsTitle)}</h3>` : ""}
      ${slide.lead ? "" : ""}
      <ul>${slide.bullets.map((b) => `<li>${esc(b)}</li>`).join("")}</ul>
    </div></div>`;
  } else if (slide.placeholder) {
    body += renderPlaceholder(slide.placeholder, "wide");
  }
  if (slide.bullets && !slide.placeholder) {
    body += `<ul class="bullets">${slide.bullets.map((b) => `<li>${esc(b)}</li>`).join("")}</ul>`;
  }
  if (slide.note) body += `<p class="note">${esc(slide.note)}</p>`;

  return `
    <section class="slide" data-page="${page}">
      <div class="kicker">${esc(slide.kicker)}</div>
      <h2>${esc(slide.title)}</h2>
      <div class="content">${body}</div>
      ${renderFooter(page)}
    </section>`;
}

pipInstallImageDataUri = `data:image/png;base64,${await fs.readFile(pipInstallImagePath, "base64")}`;
neugLogoDataUri = `data:image/png;base64,${await fs.readFile(neugLogoPath, "base64")}`;

const html = `<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>NeuG | 面向 LLM 与 Agent 的嵌入式图数据引擎</title>
  <style>
    :root {
      color-scheme: dark;
      --deck-scale: 1;
      --deck-width: 1440px;
      --deck-height: 900px;
      --bg: #0b0d0e;
      --panel: #171b1f;
      --panel-2: #20262b;
      --text: #f4f0e8;
      --muted: #aaa49b;
      --faint: #5b625f;
      --line: #30383d;
      --green: #9be7c5;
      --blue: #76d6ff;
      --amber: #e8c77e;
    }
    * { box-sizing: border-box; }
    html, body {
      margin: 0;
      width: 100%;
      height: 100%;
      overflow: hidden;
      background: #050606;
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", "PingFang SC", "Microsoft YaHei", sans-serif;
    }
    body::before {
      content: "";
      position: fixed;
      inset: 0;
      background:
        radial-gradient(circle at 80% 15%, rgba(155, 231, 197, .09), transparent 22%),
        radial-gradient(circle at 18% 85%, rgba(118, 214, 255, .07), transparent 24%),
        #050606;
      pointer-events: none;
    }
    .deck {
      position: fixed;
      left: 50%;
      top: 50%;
      width: var(--deck-width);
      height: var(--deck-height);
      transform: translate(-50%, -50%) scale(var(--deck-scale));
      transform-origin: center center;
      background: var(--bg);
      box-shadow: 0 28px 80px rgba(0,0,0,.56);
      overflow: hidden;
    }
    .slide {
      position: absolute;
      inset: 0;
      padding: 56px 72px;
      display: none;
      color: var(--text);
      background:
        linear-gradient(135deg, rgba(255,255,255,.018), transparent 36%),
        var(--bg);
    }
    .slide.active { display: block; }
    .kicker {
      color: var(--green);
      font-size: 14px;
      font-weight: 700;
      letter-spacing: .04em;
      text-transform: uppercase;
      margin-bottom: 24px;
    }
    h1, h2, h3, p { margin: 0; }
    h1 {
      margin-top: 104px;
      max-width: 760px;
      font-size: 62px;
      line-height: 1.12;
      letter-spacing: -0.02em;
      font-weight: 720;
    }
    h2 {
      max-width: 1040px;
      font-size: 39px;
      line-height: 1.22;
      letter-spacing: -0.015em;
      font-weight: 680;
    }
    .subtitle {
      margin-top: 28px;
      font-size: 23px;
      color: var(--muted);
    }
    .cover-presenter {
      margin-top: 18px;
      color: var(--text);
      font-size: 19px;
      line-height: 1.3;
      font-weight: 620;
    }
    .cover-github {
      margin-top: 10px;
      color: var(--muted);
      font-size: 17px;
      line-height: 1.3;
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
    }
    .rule {
      margin-top: 28px;
      width: 180px;
      height: 2px;
      background: var(--green);
    }
    .cover-footer {
      margin-top: 28px;
      font-size: 17px;
      color: var(--muted);
    }
    .cover-logo {
      position: absolute;
      right: 72px;
      top: 58px;
      width: 164px;
      height: auto;
      object-fit: contain;
      opacity: .98;
      z-index: 2;
    }
    .orb {
      position: absolute;
      border-radius: 999px;
      filter: saturate(1.05);
    }
    .orb-a { right: 170px; top: 150px; width: 168px; height: 168px; background: var(--green); opacity: .92; }
    .orb-b { right: 112px; top: 282px; width: 116px; height: 116px; background: var(--blue); opacity: .9; }
    .content { margin-top: 34px; }
    .lead {
      max-width: 940px;
      font-size: 25px;
      line-height: 1.42;
      color: var(--text);
    }
    .context-lead {
      margin-top: 24px;
      max-width: 1040px;
      color: var(--muted);
      font-size: 23px;
      line-height: 1.45;
    }
    .context-layout {
      display: grid;
      grid-template-columns: 530px 1fr;
      gap: 58px;
      align-items: start;
      margin-top: 26px;
    }
    .context-left {
      width: 530px;
    }
    .context-graph {
      position: relative;
      width: 530px;
      height: 340px;
      border: 1px solid rgba(48, 56, 61, .9);
      border-radius: 18px;
      background:
        radial-gradient(circle at 50% 38%, rgba(155,231,197,.12), transparent 30%),
        linear-gradient(135deg, rgba(255,255,255,.028), rgba(255,255,255,.006));
      overflow: hidden;
    }
    .context-graph::before {
      content: "";
      position: absolute;
      inset: 22px;
      border: 1px dashed rgba(91,98,95,.32);
      border-radius: 999px;
    }
    .context-edges {
      position: absolute;
      inset: 0;
      width: 100%;
      height: 100%;
    }
    .context-edges line {
      stroke: rgba(167, 162, 154, .34);
      stroke-width: 1.4;
    }
    .context-node {
      position: absolute;
      display: grid;
      place-items: center;
      width: 88px;
      height: 46px;
      border-radius: 999px;
      background: rgba(23, 27, 31, .92);
      border: 1px solid var(--line);
      color: var(--text);
      font-size: 15px;
      font-weight: 700;
      box-shadow: 0 10px 30px rgba(0,0,0,.26);
    }
    .context-node.core {
      width: 100px;
      height: 100px;
      border-radius: 999px;
      background: rgba(155,231,197,.16);
      border-color: rgba(155,231,197,.78);
      color: var(--green);
      font-size: 17px;
      text-align: center;
      line-height: 1.15;
    }
    .context-node.green { border-color: rgba(155,231,197,.7); color: var(--green); }
    .context-node.blue { border-color: rgba(118,214,255,.7); color: var(--blue); }
    .context-node.amber { border-color: rgba(232,199,126,.72); color: var(--amber); }
    .context-node.muted { color: var(--muted); }
    .context-cards {
      display: grid;
      gap: 14px;
    }
    .context-card {
      padding: 18px 22px 18px;
      border-radius: 12px;
      background: var(--panel);
      border: 1px solid var(--line);
      border-left: 4px solid var(--line);
    }
    .context-card.green { border-left-color: var(--green); }
    .context-card.blue { border-left-color: var(--blue); }
    .context-card.amber { border-left-color: var(--amber); }
    .context-card h3 {
      color: var(--text);
      font-size: 21px;
      line-height: 1.2;
      margin-bottom: 8px;
    }
    .context-card p {
      color: var(--muted);
      font-size: 15.6px;
      line-height: 1.42;
    }
    .connection-quote {
      margin: 18px 0 0;
      padding: 18px 22px 18px;
      border-left: 3px solid var(--green);
      background: rgba(23, 27, 31, .58);
      border-radius: 0 12px 12px 0;
    }
    .connection-quote blockquote {
      margin: 0;
      color: var(--text);
      font-size: 18px;
      line-height: 1.42;
      font-weight: 650;
    }
    .connection-quote figcaption {
      margin-top: 10px;
      color: var(--muted);
      font-size: 13px;
      line-height: 1.35;
    }
    .hybrid-lead {
      margin-top: 20px;
      max-width: 960px;
      color: var(--muted);
      font-size: 21px;
      line-height: 1.34;
    }
    .hybrid-layout {
      display: grid;
      grid-template-columns: 390px 180px 1fr;
      gap: 28px;
      align-items: center;
      margin-top: 38px;
    }
    .hybrid-actions {
      display: grid;
      gap: 14px;
    }
    .action-card {
      min-height: 106px;
      padding: 17px 20px;
      border-radius: 12px;
      background: var(--panel);
      border: 1px solid var(--line);
      border-left: 4px solid var(--line);
    }
    .action-card.green { border-left-color: var(--green); }
    .action-card.blue { border-left-color: var(--blue); }
    .action-card.amber { border-left-color: var(--amber); }
    .action-card h3 {
      color: var(--text);
      font-size: 20px;
      line-height: 1.2;
      margin-bottom: 9px;
    }
    .action-card p {
      color: var(--muted);
      font-size: 15.2px;
      line-height: 1.34;
    }
    .hybrid-arrow {
      display: grid;
      place-items: center;
      gap: 14px;
      color: var(--faint);
      text-align: center;
      font-size: 13px;
      letter-spacing: .03em;
      text-transform: uppercase;
    }
    .hybrid-arrow strong {
      display: grid;
      place-items: center;
      width: 58px;
      height: 58px;
      border-radius: 999px;
      border: 1px solid rgba(155,231,197,.52);
      color: var(--green);
      font-size: 32px;
      font-weight: 500;
      background: rgba(155,231,197,.08);
    }
    .engine-stack {
      position: relative;
      display: grid;
      gap: 10px;
      padding: 48px 22px 18px;
      border-radius: 18px;
      border: 2px solid rgba(155,231,197,.72);
      background:
        radial-gradient(circle at 80% 12%, rgba(118,214,255,.12), transparent 28%),
        rgba(23,27,31,.72);
      box-shadow: 0 0 0 1px rgba(155,231,197,.08), 0 28px 70px rgba(0,0,0,.22);
    }
    .engine-stack::before {
      content: "NeuG";
      position: absolute;
      left: 24px;
      top: 18px;
      color: var(--green);
      font-size: 18px;
      font-weight: 800;
      letter-spacing: .04em;
    }
    .engine-stack::after {
      content: "Hybrid retrieval graph engine";
      position: absolute;
      right: 24px;
      top: 21px;
      color: var(--faint);
      font-size: 12px;
      letter-spacing: .06em;
      text-transform: uppercase;
    }
    .stack-layer {
      padding: 16px 21px;
      border-radius: 12px;
      border: 1px solid rgba(48,56,61,.92);
      background: rgba(11,13,14,.62);
    }
    .stack-layer h3 {
      margin-bottom: 6px;
      color: var(--text);
      font-size: 20px;
      line-height: 1.15;
    }
    .stack-layer p {
      color: var(--muted);
      font-size: 15px;
      line-height: 1.28;
    }
    .stack-layer.layer-0 { border-color: rgba(155,231,197,.6); }
    .stack-layer.layer-1 { border-color: rgba(118,214,255,.52); }
    .stack-layer.layer-2 { border-color: rgba(232,199,126,.48); }
    .stack-layer.layer-3 { border-color: rgba(155,231,197,.44); }
    .benchmark-lead {
      margin-top: 20px;
      max-width: 980px;
      color: var(--muted);
      font-size: 21px;
      line-height: 1.34;
    }
    .benchmark-layout {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 30px;
      align-items: stretch;
      margin-top: 28px;
    }
    .benchmark-panel {
      min-height: 326px;
      border-radius: 18px;
      border: 1px solid var(--line);
      background:
        radial-gradient(circle at 85% 8%, rgba(118,214,255,.1), transparent 26%),
        var(--panel);
      padding: 22px 28px;
    }
    .benchmark-panel.ldbc-panel {
      background:
        radial-gradient(circle at 88% 8%, rgba(155,231,197,.11), transparent 26%),
        var(--panel);
    }
    .panel-head h3 {
      color: var(--text);
      font-size: 23px;
      line-height: 1.2;
      margin-bottom: 8px;
    }
    .panel-head p {
      color: var(--muted);
      font-size: 14.5px;
      line-height: 1.35;
    }
    .qps-bars {
      display: grid;
      gap: 18px;
      margin-top: 25px;
    }
    .bar-row {
      display: grid;
      grid-template-columns: 62px 1fr 96px;
      gap: 16px;
      align-items: center;
      color: var(--muted);
      font-size: 15px;
      font-weight: 700;
    }
    .bar-row strong {
      color: var(--text);
      font-size: 16px;
      text-align: right;
    }
    .bar-track {
      position: relative;
      height: 18px;
      border-radius: 999px;
      background: rgba(255,255,255,.06);
      overflow: hidden;
    }
    .bar-fill {
      height: 100%;
      border-radius: 999px;
    }
    .bar-fill.neug { background: linear-gradient(90deg, var(--green), var(--blue)); }
    .bar-fill.neo4j { background: var(--amber); min-width: 5px; }
    .benchmark-callout {
      display: flex;
      align-items: baseline;
      gap: 16px;
      margin-top: 26px;
      padding-top: 18px;
      border-top: 1px solid var(--line);
    }
    .benchmark-callout strong {
      color: var(--green);
      font-size: 50px;
      line-height: 1;
      letter-spacing: -0.02em;
    }
    .benchmark-callout span {
      color: var(--muted);
      font-size: 18px;
      font-weight: 700;
    }
    .benchmark-facts {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 12px;
      margin-top: 14px;
    }
    .benchmark-facts.three {
      grid-template-columns: repeat(3, 1fr);
      margin-top: 16px;
    }
    .benchmark-facts span {
      display: block;
      padding: 8px 12px;
      border-radius: 9px;
      border: 1px solid rgba(48,56,61,.9);
      color: var(--muted);
      font-size: 13.5px;
      background: rgba(11,13,14,.45);
    }
    .benchmark-facts.three span {
      font-size: 12.5px;
      line-height: 1.25;
      min-height: 44px;
      display: flex;
      align-items: center;
    }
    .duel-metrics {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 16px;
      margin-top: 22px;
    }
    .duel {
      padding: 18px 18px 16px;
      border-radius: 14px;
      border: 1px solid var(--line);
      background: rgba(11,13,14,.5);
    }
    .duel span {
      display: block;
      color: var(--muted);
      font-size: 14px;
      font-weight: 700;
      margin-bottom: 14px;
    }
    .duel strong {
      display: block;
      color: var(--green);
      font-size: 36px;
      line-height: 1;
      letter-spacing: -0.02em;
    }
    .duel.official strong { color: var(--blue); }
    .duel em {
      display: block;
      margin-top: 10px;
      color: var(--muted);
      font-style: normal;
      font-size: 14px;
    }
    .lift-strip {
      display: flex;
      align-items: center;
      justify-content: space-between;
      margin-top: 14px;
      padding: 14px 20px;
      border-radius: 13px;
      background: rgba(155,231,197,.1);
      border: 1px solid rgba(155,231,197,.36);
    }
    .lift-strip span {
      color: var(--muted);
      font-size: 16px;
      font-weight: 700;
    }
    .lift-strip strong {
      color: var(--green);
      font-size: 34px;
      line-height: 1;
    }
    .embedded-lead {
      margin-top: 24px;
      max-width: 1040px;
      color: var(--muted);
      font-size: 23px;
      line-height: 1.45;
    }
    .embedded-layout {
      display: grid;
      grid-template-columns: 560px 1fr;
      gap: 44px;
      align-items: center;
      margin-top: 46px;
    }
    .install-proof {
      overflow: hidden;
      border-radius: 18px;
      border: 1px solid var(--line);
      background:
        radial-gradient(circle at 84% 18%, rgba(155,231,197,.12), transparent 28%),
        var(--panel);
      box-shadow: 0 24px 70px rgba(0,0,0,.22);
    }
    .proof-topline {
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 18px 22px;
      border-bottom: 1px solid var(--line);
      color: var(--muted);
      font-size: 15px;
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
    }
    .proof-topline strong {
      color: var(--green);
      font-size: 14px;
    }
    .install-proof img {
      display: block;
      width: 100%;
      height: 132px;
      object-fit: contain;
      object-position: left center;
      background: #1f2022;
      filter: saturate(.94) contrast(1.05);
    }
    .proof-metric {
      display: flex;
      align-items: baseline;
      gap: 18px;
      padding: 26px 28px 30px;
    }
    .proof-metric strong {
      color: var(--green);
      font-size: 64px;
      line-height: 1;
      letter-spacing: -0.03em;
    }
    .proof-metric span {
      color: var(--muted);
      font-size: 18px;
      font-weight: 700;
    }
    .embedded-cards {
      display: grid;
      gap: 18px;
    }
    .embedded-card {
      padding: 22px 24px;
      border-radius: 12px;
      background: var(--panel);
      border: 1px solid var(--line);
      border-left: 4px solid var(--line);
    }
    .embedded-card.green { border-left-color: var(--green); }
    .embedded-card.blue { border-left-color: var(--blue); }
    .embedded-card.amber { border-left-color: var(--amber); }
    .embedded-card h3 {
      color: var(--text);
      font-size: 22px;
      line-height: 1.2;
      margin-bottom: 10px;
    }
    .embedded-card p {
      color: var(--muted);
      font-size: 16.4px;
      line-height: 1.45;
    }
    .core-lead {
      margin-top: 24px;
      max-width: 1040px;
      color: var(--muted);
      font-size: 23px;
      line-height: 1.45;
    }
    .core-arch {
      position: relative;
      margin-top: 30px;
      display: grid;
      grid-template-rows: 146px 30px 134px 78px 72px;
      gap: 0;
      min-height: 460px;
    }
    .extension-row {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 36px;
      align-items: stretch;
    }
    .extension-card {
      position: relative;
      padding: 18px 24px 18px;
      border-radius: 14px;
      border: 1px solid var(--line);
      background:
        radial-gradient(circle at 84% 18%, rgba(118,214,255,.10), transparent 30%),
        rgba(23,27,31,.9);
      overflow: hidden;
    }
    .extension-card::after {
      content: "";
      position: absolute;
      left: 50%;
      bottom: -34px;
      width: 1px;
      height: 34px;
      background: var(--line);
    }
    .extension-card.blue {
      border-color: rgba(118,214,255,.44);
      border-top: 3px solid var(--blue);
    }
    .extension-card.amber {
      border-color: rgba(232,199,126,.44);
      border-top: 3px solid var(--amber);
      background:
        radial-gradient(circle at 84% 18%, rgba(232,199,126,.10), transparent 30%),
        rgba(23,27,31,.9);
    }
    .extension-type {
      color: var(--faint);
      font-size: 12px;
      font-weight: 800;
      letter-spacing: .08em;
      text-transform: uppercase;
    }
    .extension-card h3 {
      margin-top: 10px;
      color: var(--text);
      font-size: 24px;
      line-height: 1.18;
    }
    .extension-card p {
      margin-top: 9px;
      color: var(--muted);
      font-size: 15.5px;
      line-height: 1.38;
    }
    .extension-card strong {
      position: absolute;
      right: 20px;
      bottom: 18px;
      display: inline-grid;
      place-items: center;
      min-width: 136px;
      height: 34px;
      border-radius: 999px;
      border: 1px solid rgba(155,231,197,.45);
      color: var(--green);
      background: rgba(155,231,197,.08);
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
      font-size: 13px;
      letter-spacing: .01em;
    }
    .call-rail {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 36px;
      align-items: center;
      text-align: center;
      color: var(--green);
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
      font-size: 13px;
      font-weight: 800;
      letter-spacing: .08em;
    }
    .call-rail span {
      justify-self: center;
      width: 74px;
      padding: 6px 0;
      border-radius: 999px;
      border: 1px solid rgba(155,231,197,.42);
      background: rgba(11,13,14,.95);
    }
    .core-node {
      position: relative;
      display: grid;
      place-items: center;
      align-content: center;
      justify-self: center;
      width: 690px;
      min-height: 134px;
      border-radius: 18px;
      border: 2px solid rgba(155,231,197,.78);
      background:
        radial-gradient(circle at 50% 0%, rgba(155,231,197,.18), transparent 42%),
        rgba(17,22,22,.96);
      box-shadow: 0 24px 80px rgba(0,0,0,.28), 0 0 0 1px rgba(155,231,197,.1);
      text-align: center;
    }
    .core-node::before,
    .cli-node::before {
      content: "";
      position: absolute;
      top: 0;
      left: 50%;
      width: 1px;
      height: 40px;
      background: var(--line);
      transform: translate(-50%, -40px);
    }
    .core-node span {
      color: var(--green);
      font-size: 32px;
      line-height: 1.1;
      font-weight: 780;
      letter-spacing: .01em;
    }
    .core-node p {
      margin-top: 12px;
      color: var(--muted);
      font-size: 17px;
      line-height: 1.38;
    }
    .cli-node {
      position: relative;
      align-self: center;
      justify-self: center;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 24px;
      width: 860px;
      min-height: 64px;
      padding: 14px 24px;
      border-radius: 13px;
      border: 1px solid rgba(118,214,255,.5);
      background: rgba(20,29,34,.92);
    }
    .cli-node::before {
      top: 0;
      left: 50%;
      transform: translate(-50%, -40px);
      height: 40px;
    }
    .cli-node::after {
      content: "";
      position: absolute;
      left: 50%;
      bottom: -28px;
      width: 1px;
      height: 28px;
      background: var(--line);
    }
    .cli-node span {
      flex: 0 0 auto;
      color: var(--blue);
      font-size: 22px;
      line-height: 1.1;
      font-weight: 780;
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
    }
    .cli-node p {
      color: var(--muted);
      font-size: 16px;
      line-height: 1.35;
      text-align: right;
    }
    .query-row {
      display: grid;
      grid-template-columns: repeat(3, 1fr);
      gap: 22px;
      align-items: end;
    }
    .query-pill {
      min-height: 66px;
      padding: 13px 18px;
      border-radius: 12px;
      border: 1px solid var(--line);
      background: rgba(17,20,23,.9);
      text-align: center;
    }
    .query-pill strong {
      display: block;
      color: var(--text);
      font-size: 18px;
      line-height: 1.2;
    }
    .query-pill span {
      display: block;
      margin-top: 7px;
      color: var(--muted);
      font-size: 13.5px;
      line-height: 1.25;
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
    }
    .core-system {
      margin-top: 34px;
      display: grid;
      grid-template-columns: 340px 76px 360px 76px 1fr;
      gap: 0;
      align-items: center;
      min-height: 320px;
      padding: 22px 30px;
      border-radius: 18px;
      border: 1px solid rgba(48,56,61,.86);
      background:
        radial-gradient(circle at 50% 46%, rgba(155,231,197,.10), transparent 34%),
        linear-gradient(135deg, rgba(255,255,255,.035), rgba(255,255,255,.008));
    }
    .core-column {
      display: grid;
      gap: 14px;
      min-width: 0;
    }
    .column-label {
      color: var(--faint);
      font-size: 12px;
      font-weight: 800;
      letter-spacing: .08em;
      text-transform: uppercase;
    }
    .system-module {
      min-height: 108px;
      padding: 18px 22px;
      border-radius: 12px;
      border: 1px solid var(--line);
      background: rgba(17,20,23,.88);
    }
    .system-module.blue {
      border-color: rgba(118,214,255,.5);
      box-shadow: inset 0 3px 0 var(--blue);
    }
    .system-module.amber {
      border-color: rgba(232,199,126,.5);
      box-shadow: inset 0 3px 0 var(--amber);
    }
    .system-module h3 {
      color: var(--text);
      font-size: 22px;
      line-height: 1.18;
    }
    .system-module p {
      margin-top: 10px;
      color: var(--muted);
      font-size: 14.6px;
      line-height: 1.38;
    }
    .system-module code {
      display: inline-flex;
      margin-top: 12px;
      padding: 7px 12px;
      border-radius: 999px;
      border: 1px solid rgba(155,231,197,.42);
      color: var(--green);
      background: rgba(155,231,197,.08);
      font-size: 13px;
      font-weight: 800;
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
    }
    .system-arrow {
      display: grid;
      place-items: center;
      gap: 12px;
      color: var(--faint);
      text-align: center;
      font-size: 11px;
      font-weight: 800;
      letter-spacing: .06em;
      text-transform: uppercase;
    }
    .system-arrow strong {
      display: grid;
      place-items: center;
      width: 44px;
      height: 44px;
      border-radius: 999px;
      border: 1px solid rgba(155,231,197,.5);
      color: var(--green);
      background: rgba(155,231,197,.08);
      font-size: 24px;
      font-weight: 520;
    }
    .graph-core-panel {
      display: grid;
      gap: 18px;
      justify-items: center;
    }
    .core-ring {
      display: grid;
      place-items: center;
      align-content: center;
      width: 286px;
      aspect-ratio: 1;
      border-radius: 999px;
      border: 2px solid rgba(155,231,197,.78);
      background:
        radial-gradient(circle at 50% 42%, rgba(155,231,197,.2), transparent 52%),
        rgba(12,17,17,.96);
      text-align: center;
      box-shadow: 0 28px 86px rgba(0,0,0,.28);
    }
    .core-ring span {
      width: 220px;
      color: var(--green);
      font-size: 30px;
      line-height: 1.12;
      font-weight: 780;
    }
    .core-ring p {
      margin-top: 14px;
      width: 230px;
      color: var(--muted);
      font-size: 14.6px;
      line-height: 1.38;
    }
    .cli-panel {
      padding: 20px 22px;
      border-radius: 12px;
      border: 1px solid rgba(118,214,255,.52);
      background: rgba(15,27,33,.86);
      box-shadow: inset 0 3px 0 var(--blue);
    }
    .cli-panel span {
      color: var(--blue);
      font-size: 25px;
      line-height: 1.1;
      font-weight: 820;
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
    }
    .cli-panel p {
      margin-top: 12px;
      color: var(--muted);
      font-size: 15.2px;
      line-height: 1.4;
    }
    .query-strip {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
    }
    .query-strip span {
      padding: 8px 10px;
      border-radius: 999px;
      border: 1px solid rgba(48,56,61,.92);
      color: var(--muted);
      background: rgba(17,20,23,.86);
      font-size: 12.8px;
      font-weight: 760;
    }
    .query-entry {
      display: flex;
      align-items: baseline;
      justify-content: space-between;
      gap: 16px;
      min-height: 58px;
      padding: 15px 18px;
      border-radius: 11px;
      border: 1px solid rgba(48,56,61,.92);
      background: rgba(17,20,23,.86);
    }
    .scenario-bridge {
      display: grid;
      grid-template-columns: 190px repeat(3, 1fr);
      gap: 14px;
      align-items: center;
      margin-top: 16px;
      padding: 14px 16px;
      border-radius: 15px;
      border: 1px solid rgba(48,56,61,.92);
      background:
        radial-gradient(circle at 86% 50%, rgba(155,231,197,.07), transparent 30%),
        rgba(15,18,20,.82);
    }
    .bridge-label {
      color: var(--muted);
      font-size: 15px;
      font-weight: 780;
      line-height: 1.25;
    }
    .scenario-chip {
      display: grid;
      place-items: center;
      min-height: 54px;
      padding: 12px 18px;
      border-radius: 12px;
      border: 1px solid rgba(48,56,61,.92);
      background: rgba(17,20,23,.82);
    }
    .scenario-chip.green {
      border-color: rgba(155,231,197,.56);
      background: rgba(155,231,197,.08);
    }
    .scenario-chip.blue {
      border-color: rgba(118,214,255,.56);
      background: rgba(118,214,255,.08);
    }
    .scenario-chip.amber {
      border-color: rgba(232,199,126,.56);
      background: rgba(232,199,126,.08);
    }
    .scenario-chip strong {
      color: var(--text);
      font-size: 17px;
      font-weight: 800;
      line-height: 1.2;
    }
    .scenario-chip span {
      flex: 0 0 auto;
      color: var(--muted);
      font-size: 12.4px;
      line-height: 1.2;
    }
    .core-slide .core-lead {
      margin-top: 18px;
      font-size: 21px;
      line-height: 1.36;
    }
    .core-slide .core-system {
      margin-top: 26px;
      grid-template-columns: 318px 66px 326px 66px 1fr;
      min-height: 292px;
      padding: 18px 24px;
      border-radius: 16px;
    }
    .core-slide .core-column {
      gap: 11px;
    }
    .core-slide .column-label {
      font-size: 11px;
    }
    .core-slide .system-module {
      min-height: 92px;
      padding: 15px 18px;
      border-radius: 10px;
    }
    .core-slide .system-module h3 {
      font-size: 20px;
    }
    .core-slide .system-module p {
      margin-top: 7px;
      font-size: 13.4px;
      line-height: 1.3;
    }
    .core-slide .system-module code {
      margin-top: 9px;
      padding: 6px 10px;
      font-size: 12px;
    }
    .core-slide .system-arrow {
      gap: 9px;
      font-size: 10px;
    }
    .core-slide .system-arrow strong {
      width: 38px;
      height: 38px;
      font-size: 21px;
    }
    .core-slide .graph-core-panel {
      gap: 14px;
    }
    .core-slide .core-ring {
      width: 248px;
    }
    .core-slide .core-ring span {
      width: 196px;
      font-size: 26px;
    }
    .core-slide .core-ring p {
      margin-top: 10px;
      width: 202px;
      font-size: 13.1px;
      line-height: 1.3;
    }
    .core-slide .cli-panel {
      padding: 16px 18px;
      border-radius: 10px;
    }
    .core-slide .cli-panel span {
      font-size: 22px;
    }
    .core-slide .cli-panel p {
      margin-top: 8px;
      font-size: 13.6px;
      line-height: 1.32;
    }
    .core-slide .query-strip {
      gap: 7px;
    }
    .core-slide .query-strip span {
      padding: 7px 9px;
      font-size: 11.8px;
    }
    .core-slide .scenario-bridge {
      grid-template-columns: 172px repeat(3, 1fr);
      gap: 11px;
      margin-top: 12px;
      padding: 11px 13px;
      border-radius: 13px;
    }
    .core-slide .bridge-label {
      font-size: 13.6px;
    }
    .core-slide .scenario-chip {
      min-height: 44px;
      padding: 9px 14px;
      border-radius: 10px;
    }
    .core-slide .scenario-chip strong {
      font-size: 15.5px;
    }
    .query-entry strong {
      flex: 0 0 auto;
      color: var(--text);
      font-size: 17px;
      line-height: 1.2;
    }
    .query-entry span {
      min-width: 0;
      color: var(--muted);
      font-size: 13px;
      line-height: 1.25;
      text-align: right;
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
    }
    .metrics {
      display: grid;
      grid-template-columns: repeat(3, 1fr);
      gap: 62px;
      margin-top: 70px;
      margin-bottom: 62px;
      max-width: 980px;
    }
    .metric strong {
      display: block;
      font-size: 51px;
      line-height: 1;
      font-weight: 650;
      color: var(--green);
    }
    .metric.blue strong { color: var(--blue); }
    .metric.amber strong { color: var(--amber); }
    .metric span {
      display: block;
      margin-top: 16px;
      max-width: 250px;
      color: var(--muted);
      font-size: 19px;
      line-height: 1.25;
    }
    .cards {
      display: grid;
      grid-template-columns: repeat(3, 1fr);
      gap: 28px;
      margin-top: 72px;
    }
    .cards.two { grid-template-columns: repeat(2, 1fr); margin-top: 70px; gap: 80px; }
    .cards.five {
      grid-template-columns: repeat(3, 1fr);
      gap: 24px;
      margin-top: 44px;
    }
    .cards.five .card:nth-child(4) { grid-column: 1 / 2; margin-left: 120px; width: calc(100% - 20px); }
    .cards.five .card:nth-child(5) { grid-column: 2 / 3; margin-left: 120px; width: calc(100% - 20px); }
    .card {
      min-height: 152px;
      padding: 24px 26px 26px;
      border-radius: 10px;
      background: var(--panel);
      border: 1px solid var(--line);
      border-left: 4px solid var(--line);
    }
    .card.green { border-left-color: var(--green); }
    .card.blue { border-left-color: var(--blue); }
    .card.amber { border-left-color: var(--amber); }
    .card h3 {
      font-size: 24px;
      line-height: 1.2;
      font-weight: 660;
      color: var(--text);
      margin-bottom: 18px;
    }
    .card p {
      color: var(--muted);
      font-size: 18px;
      line-height: 1.5;
    }
    .orbit {
      display: flex;
      align-items: center;
      justify-content: center;
      margin: 78px auto 44px;
      width: 970px;
    }
    .node {
      display: grid;
      place-items: center;
      width: 122px;
      height: 122px;
      border-radius: 999px;
      border: 1px solid rgba(155,231,197,.65);
      background: rgba(155,231,197,.11);
      color: var(--text);
      font-size: 18px;
      font-weight: 650;
    }
    .node.blue {
      border-color: rgba(118,214,255,.65);
      background: rgba(118,214,255,.11);
    }
    .edge {
      width: 74px;
      height: 1px;
      background: var(--line);
    }
    .note {
      margin: 44px auto 0;
      max-width: 960px;
      color: var(--green);
      font-size: 22px;
      line-height: 1.45;
      text-align: center;
      font-weight: 630;
    }
    .placeholder-row {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 56px;
      margin-top: 70px;
    }
    .placeholder {
      display: grid;
      place-items: center;
      min-height: 168px;
      padding: 28px;
      border: 1.5px solid var(--line);
      border-radius: 10px;
      background: #101316;
      color: var(--blue);
      text-align: center;
      font-size: 22px;
      font-weight: 620;
    }
    .placeholder::before {
      content: "";
      align-self: start;
      width: 100%;
      height: 1px;
      background: var(--line);
      grid-row: 1;
    }
    .placeholder span { grid-row: 2; margin-top: 18px; }
    .placeholder.wide { width: 520px; min-height: 330px; margin-top: 58px; }
    .split {
      display: grid;
      grid-template-columns: 520px 1fr;
      gap: 78px;
      align-items: center;
      margin-top: 52px;
    }
    .text-block h3 {
      font-size: 25px;
      color: var(--green);
      margin-bottom: 26px;
    }
    ul {
      margin: 0;
      padding-left: 22px;
      color: var(--muted);
      font-size: 22px;
      line-height: 1.55;
    }
    .flow {
      display: flex;
      align-items: center;
      justify-content: center;
      margin-top: 76px;
    }
    .flow-item {
      width: 205px;
      min-height: 118px;
      padding: 24px 18px;
      border-radius: 10px;
      border: 1px solid var(--line);
      background: var(--panel);
      text-align: center;
    }
    .flow-item h3 {
      color: var(--green);
      font-size: 21px;
      margin-bottom: 12px;
    }
    .flow-item p {
      color: var(--muted);
      font-size: 16px;
      line-height: 1.35;
    }
    .flow-line {
      width: 54px;
      height: 1px;
      background: var(--line);
    }
    .knowledge-layout,
    .update-layout {
      display: grid;
      grid-template-columns: 560px 1fr;
      gap: 40px;
      align-items: stretch;
      margin-top: 30px;
    }
    .case-left,
    .case-right,
    .update-left,
    .update-right {
      min-width: 0;
    }
    .case-question {
      padding: 22px 24px;
      border-radius: 16px;
      border: 1px solid rgba(155,231,197,.5);
      background:
        radial-gradient(circle at 84% 10%, rgba(155,231,197,.12), transparent 28%),
        rgba(17,20,23,.92);
    }
    .case-question span,
    .schema-title,
    .call-sequence span {
      display: block;
      color: var(--faint);
      font-size: 12px;
      font-weight: 820;
      letter-spacing: .08em;
      text-transform: uppercase;
      margin-bottom: 12px;
    }
    .case-question strong {
      display: block;
      color: var(--text);
      font-size: 22px;
      line-height: 1.34;
      font-weight: 760;
    }
    .case-path {
      display: grid;
      gap: 12px;
      margin-top: 16px;
    }
    .case-path-step {
      display: grid;
      grid-template-columns: 42px 1fr;
      column-gap: 14px;
      min-height: 72px;
      padding: 15px 18px;
      border-radius: 13px;
      border: 1px solid rgba(48,56,61,.92);
      background: rgba(17,20,23,.82);
    }
    .case-path-step em {
      grid-row: 1 / span 2;
      display: grid;
      place-items: center;
      width: 34px;
      height: 34px;
      border-radius: 999px;
      border: 1px solid rgba(155,231,197,.5);
      color: var(--green);
      font-style: normal;
      font-size: 12px;
      font-weight: 820;
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
    }
    .case-path-step h3 {
      color: var(--text);
      font-size: 18px;
      line-height: 1.15;
      margin-bottom: 6px;
    }
    .case-path-step p {
      color: var(--muted);
      font-size: 14.6px;
      line-height: 1.35;
    }
    .case-right,
    .update-right {
      display: grid;
      grid-template-rows: auto auto;
      gap: 16px;
    }
    .knowledge-schema {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px;
      padding: 18px;
      border-radius: 16px;
      border: 1px solid rgba(48,56,61,.92);
      background: rgba(17,20,23,.76);
    }
    .schema-node {
      min-height: 82px;
      padding: 16px 16px;
      border-radius: 12px;
      border: 1px solid rgba(48,56,61,.92);
      background: rgba(11,13,14,.56);
    }
    .schema-node.green { border-color: rgba(155,231,197,.48); }
    .schema-node.blue { border-color: rgba(118,214,255,.48); }
    .schema-node.amber { border-color: rgba(232,199,126,.48); }
    .schema-node strong {
      display: block;
      color: var(--text);
      font-size: 17px;
      line-height: 1.18;
      margin-bottom: 8px;
    }
    .schema-node span {
      display: block;
      color: var(--muted);
      font-size: 13.5px;
      line-height: 1.3;
    }
    .schema-node:nth-child(5) {
      grid-column: 1 / -1;
    }
    .delta-stack {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 12px;
    }
    .delta-item {
      min-height: 128px;
      padding: 18px 16px;
      border-radius: 13px;
      border: 1px solid rgba(48,56,61,.92);
      background: rgba(17,20,23,.86);
    }
    .delta-item span {
      display: grid;
      place-items: center;
      width: 30px;
      height: 30px;
      border-radius: 999px;
      border: 1px solid rgba(155,231,197,.48);
      color: var(--green);
      font-size: 11px;
      font-weight: 820;
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
      margin-bottom: 18px;
    }
    .delta-item strong {
      display: block;
      color: var(--text);
      font-size: 18px;
      line-height: 1.18;
      margin-bottom: 9px;
    }
    .delta-item p {
      color: var(--muted);
      font-size: 13.5px;
      line-height: 1.3;
    }
    .call-sequence {
      margin-top: 16px;
      padding: 20px 22px;
      border-radius: 16px;
      border: 1px solid rgba(118,214,255,.38);
      background: rgba(15,27,33,.68);
    }
    .call-sequence code {
      display: block;
      padding: 9px 12px;
      border-radius: 8px;
      background: rgba(11,13,14,.64);
      color: var(--blue);
      font-size: 13px;
      line-height: 1.25;
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
      margin-top: 8px;
      white-space: nowrap;
    }
    .finding-list {
      display: grid;
      gap: 12px;
    }
    .finding {
      min-height: 98px;
      padding: 18px 20px;
      border-radius: 13px;
      border: 1px solid rgba(48,56,61,.92);
      border-left: 4px solid var(--line);
      background: rgba(17,20,23,.86);
    }
    .finding.green { border-left-color: var(--green); }
    .finding.blue { border-left-color: var(--blue); }
    .finding.amber { border-left-color: var(--amber); }
    .finding h3 {
      color: var(--text);
      font-size: 19px;
      line-height: 1.18;
      margin-bottom: 8px;
    }
    .finding p {
      color: var(--muted);
      font-size: 14.3px;
      line-height: 1.34;
    }
    .knowledge-case-slide .stat-tiles,
    .knowledge-update-slide .stat-tiles {
      grid-template-columns: repeat(3, minmax(0, 1fr));
      grid-template-rows: none;
      gap: 12px;
    }
    .knowledge-case-slide .stat-tile,
    .knowledge-update-slide .stat-tile {
      min-height: 108px;
      padding: 17px 15px;
    }
    .knowledge-case-slide .stat-tile strong,
    .knowledge-update-slide .stat-tile strong {
      font-size: 28px;
    }
    .knowledge-case-slide .stat-tile span,
    .knowledge-update-slide .stat-tile span {
      font-size: 13.2px;
    }
    .knowledge-case-slide .scenario-lead {
      margin-top: 16px;
      max-width: 960px;
      font-size: 20px;
      line-height: 1.32;
    }
    .knowledge-case-slide .knowledge-layout {
      grid-template-columns: 548px 1fr;
      gap: 36px;
      margin-top: 24px;
    }
    .knowledge-case-slide .case-question {
      padding: 17px 20px;
      border-radius: 14px;
    }
    .knowledge-case-slide .case-question span {
      margin-bottom: 8px;
      font-size: 11px;
    }
    .knowledge-case-slide .case-question strong {
      font-size: 20px;
      line-height: 1.24;
    }
    .knowledge-case-slide .case-path {
      gap: 10px;
      margin-top: 12px;
    }
    .knowledge-case-slide .case-path-step {
      grid-template-columns: 38px 1fr;
      column-gap: 12px;
      min-height: 62px;
      padding: 12px 15px;
      border-radius: 11px;
    }
    .knowledge-case-slide .case-path-step em {
      width: 30px;
      height: 30px;
      font-size: 11px;
    }
    .knowledge-case-slide .case-path-step h3 {
      font-size: 16.8px;
      margin-bottom: 4px;
    }
    .knowledge-case-slide .case-path-step p {
      font-size: 13.5px;
      line-height: 1.28;
    }
    .knowledge-case-slide .case-right {
      gap: 12px;
    }
    .knowledge-case-slide .schema-title {
      margin-bottom: 8px;
      font-size: 11px;
    }
    .knowledge-case-slide .knowledge-schema {
      gap: 10px;
      padding: 14px;
      border-radius: 14px;
    }
    .knowledge-case-slide .schema-node {
      min-height: 70px;
      padding: 13px 14px;
      border-radius: 10px;
    }
    .knowledge-case-slide .schema-node strong {
      font-size: 16px;
      margin-bottom: 6px;
    }
    .knowledge-case-slide .schema-node span {
      font-size: 12.8px;
      line-height: 1.24;
    }
    .knowledge-case-slide .stat-tiles {
      gap: 10px;
    }
    .knowledge-case-slide .stat-tile {
      min-height: 88px;
      padding: 13px 13px;
      border-radius: 11px;
    }
    .knowledge-case-slide .stat-tile strong {
      font-size: 25px;
    }
    .knowledge-case-slide .stat-tile span {
      margin-top: 8px;
      font-size: 12.6px;
      line-height: 1.22;
    }
    .knowledge-update-slide .scenario-lead {
      margin-top: 16px;
      max-width: 980px;
      font-size: 20px;
      line-height: 1.32;
    }
    .knowledge-update-slide .update-layout {
      grid-template-columns: 560px 1fr;
      gap: 36px;
      margin-top: 24px;
    }
    .knowledge-update-slide .delta-stack {
      gap: 10px;
    }
    .knowledge-update-slide .delta-item {
      min-height: 102px;
      padding: 14px 14px;
      border-radius: 11px;
    }
    .knowledge-update-slide .delta-item span {
      width: 28px;
      height: 28px;
      margin-bottom: 12px;
      font-size: 10.5px;
    }
    .knowledge-update-slide .delta-item strong {
      font-size: 16.5px;
      margin-bottom: 7px;
    }
    .knowledge-update-slide .delta-item p {
      font-size: 12.6px;
      line-height: 1.24;
    }
    .knowledge-update-slide .call-sequence {
      margin-top: 12px;
      padding: 16px 18px;
      border-radius: 13px;
    }
    .knowledge-update-slide .call-sequence span {
      margin-bottom: 8px;
      font-size: 11px;
    }
    .knowledge-update-slide .call-sequence code {
      padding: 8px 10px;
      margin-top: 7px;
      font-size: 12.3px;
      line-height: 1.18;
      white-space: normal;
    }
    .knowledge-update-slide .update-right {
      gap: 12px;
    }
    .knowledge-update-slide .finding-list {
      gap: 10px;
    }
    .knowledge-update-slide .finding {
      min-height: 82px;
      padding: 14px 16px;
      border-radius: 11px;
    }
    .knowledge-update-slide .finding h3 {
      font-size: 17px;
      margin-bottom: 6px;
    }
    .knowledge-update-slide .finding p {
      font-size: 13px;
      line-height: 1.28;
    }
    .knowledge-update-slide .stat-tiles {
      gap: 10px;
    }
    .knowledge-update-slide .stat-tile {
      min-height: 86px;
      padding: 12px 13px;
      border-radius: 11px;
    }
    .knowledge-update-slide .stat-tile strong {
      font-size: 24px;
    }
    .knowledge-update-slide .stat-tile span {
      margin-top: 7px;
      font-size: 12.4px;
      line-height: 1.2;
    }
    .business-layout,
    .business-loop-layout {
      display: grid;
      grid-template-columns: 560px 1fr;
      gap: 36px;
      align-items: stretch;
      margin-top: 24px;
    }
    .business-case-slide .scenario-lead,
    .business-loop-slide .scenario-lead {
      margin-top: 16px;
      max-width: 1000px;
      font-size: 20px;
      line-height: 1.32;
    }
    .business-case-slide .scenario-lead {
      margin-top: 12px;
      font-size: 19px;
      line-height: 1.28;
    }
    .business-case-slide .business-layout {
      grid-template-columns: 548px 1fr;
      gap: 28px;
      margin-top: 18px;
    }
    .business-left,
    .business-right,
    .loop-left,
    .loop-right {
      min-width: 0;
    }
    .business-case-slide .case-question {
      padding: 16px 20px;
      border-radius: 13px;
    }
    .business-case-slide .case-question span {
      margin-bottom: 7px;
      font-size: 10.8px;
    }
    .business-case-slide .case-question strong {
      font-size: 20px;
      line-height: 1.24;
    }
    .business-paths {
      display: grid;
      gap: 12px;
      margin-top: 14px;
    }
    .business-case-slide .business-paths {
      gap: 9px;
      margin-top: 11px;
    }
    .business-path {
      padding: 17px 18px;
      border-radius: 13px;
      border: 1px solid rgba(48,56,61,.92);
      background: rgba(17,20,23,.82);
    }
    .business-case-slide .business-path {
      padding: 13px 16px;
      border-radius: 11px;
    }
    .business-path.green { border-left: 4px solid var(--green); }
    .business-path.blue { border-left: 4px solid var(--blue); }
    .business-path > span {
      display: block;
      color: var(--muted);
      font-size: 12px;
      font-weight: 820;
      letter-spacing: .06em;
      text-transform: uppercase;
      margin-bottom: 10px;
    }
    .business-case-slide .business-path > span {
      margin-bottom: 8px;
      font-size: 11px;
    }
    .business-case-slide .path-chain {
      gap: 8px;
    }
    .business-case-slide .path-chain strong {
      min-height: 36px;
      padding: 7px 8px;
      font-size: 13.4px;
      line-height: 1.16;
    }
    .business-case-slide .path-chain em {
      font-size: 16px;
    }
    .business-right,
    .loop-right {
      display: grid;
      grid-template-rows: auto auto;
      gap: 12px;
      align-content: start;
    }
    .business-case-slide .business-right {
      gap: 10px;
    }
    .business-case-slide .schema-title {
      margin-bottom: 7px;
      font-size: 10.8px;
    }
    .business-model {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 10px;
      padding: 14px;
      border-radius: 14px;
      border: 1px solid rgba(48,56,61,.92);
      background: rgba(17,20,23,.76);
    }
    .business-case-slide .business-model {
      gap: 8px;
      padding: 12px;
      border-radius: 12px;
    }
    .business-model .schema-node {
      min-height: 68px;
      padding: 12px 14px;
      border-radius: 10px;
    }
    .business-case-slide .business-model .schema-node {
      min-height: 58px;
      padding: 10px 12px;
      border-radius: 9px;
    }
    .business-model .schema-node strong {
      font-size: 15.5px;
      margin-bottom: 6px;
    }
    .business-case-slide .business-model .schema-node strong {
      font-size: 14.8px;
      margin-bottom: 5px;
    }
    .business-model .schema-node span {
      font-size: 12.5px;
      line-height: 1.22;
    }
    .business-case-slide .business-model .schema-node span {
      font-size: 11.8px;
      line-height: 1.18;
    }
    .business-model .schema-node:nth-child(5) {
      grid-column: 1 / -1;
    }
    .business-case-slide .stat-tiles,
    .business-loop-slide .stat-tiles {
      grid-template-columns: repeat(3, minmax(0, 1fr));
      grid-template-rows: none;
      gap: 10px;
    }
    .business-case-slide .stat-tile,
    .business-loop-slide .stat-tile {
      min-height: 88px;
      padding: 13px 13px;
      border-radius: 11px;
    }
    .business-case-slide .stat-tile {
      min-height: 76px;
      padding: 10px 12px;
      border-radius: 10px;
    }
    .business-case-slide .stat-tile strong,
    .business-loop-slide .stat-tile strong {
      font-size: 25px;
    }
    .business-case-slide .stat-tile strong {
      font-size: 23px;
    }
    .business-case-slide .stat-tile span,
    .business-loop-slide .stat-tile span {
      margin-top: 8px;
      font-size: 12.5px;
      line-height: 1.22;
    }
    .business-case-slide .stat-tile span {
      margin-top: 6px;
      font-size: 11.7px;
      line-height: 1.16;
    }
    .risk-list {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 10px;
    }
    .risk-item {
      min-height: 100px;
      padding: 14px 14px;
      border-radius: 11px;
      border: 1px solid rgba(48,56,61,.92);
      background: rgba(17,20,23,.86);
    }
    .risk-item span {
      display: grid;
      place-items: center;
      width: 28px;
      height: 28px;
      border-radius: 999px;
      border: 1px solid rgba(232,199,126,.55);
      color: var(--amber);
      font-size: 10.5px;
      font-weight: 820;
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
      margin-bottom: 12px;
    }
    .risk-item strong {
      display: block;
      color: var(--text);
      font-size: 16.2px;
      line-height: 1.18;
      margin-bottom: 7px;
    }
    .risk-item p {
      color: var(--muted);
      font-size: 12.3px;
      line-height: 1.23;
    }
    .business-steps {
      margin-top: 12px;
      padding: 16px 18px;
      border-radius: 13px;
    }
    .business-steps span {
      margin-bottom: 8px;
      font-size: 11px;
    }
    .business-steps code {
      padding: 8px 10px;
      margin-top: 7px;
      font-size: 12.1px;
      line-height: 1.18;
      white-space: normal;
    }
    .business-steps b {
      color: var(--text);
      font-weight: 820;
    }
    .search-layout {
      display: grid;
      grid-template-columns: 620px 1fr;
      gap: 36px;
      align-items: stretch;
      margin-top: 24px;
    }
    .business-search-slide .scenario-lead {
      margin-top: 16px;
      max-width: 1040px;
      font-size: 20px;
      line-height: 1.32;
    }
    .search-map {
      min-width: 0;
      padding: 22px 24px;
      border-radius: 16px;
      border: 1px solid rgba(48,56,61,.92);
      background:
        radial-gradient(circle at 50% 0%, rgba(155,231,197,.12), transparent 34%),
        rgba(17,20,23,.78);
    }
    .search-root {
      display: grid;
      place-items: center;
      min-height: 68px;
      border-radius: 999px;
      border: 2px solid rgba(155,231,197,.68);
      background: rgba(155,231,197,.10);
      color: var(--text);
      font-size: 22px;
      font-weight: 800;
      line-height: 1.2;
      text-align: center;
      margin-bottom: 18px;
    }
    .frontier-list {
      display: grid;
      gap: 12px;
    }
    .frontier-row {
      position: relative;
      padding: 14px 16px;
      border-radius: 13px;
      border: 1px solid rgba(48,56,61,.92);
      background: rgba(11,13,14,.52);
    }
    .frontier-row::before {
      content: "";
      position: absolute;
      left: 50%;
      top: -13px;
      width: 1px;
      height: 13px;
      background: rgba(155,231,197,.34);
    }
    .frontier-row > span {
      display: block;
      color: var(--muted);
      font-size: 12px;
      font-weight: 820;
      letter-spacing: .06em;
      text-transform: uppercase;
      margin-bottom: 10px;
    }
    .frontier-nodes {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 10px;
    }
    .frontier-nodes strong {
      display: grid;
      place-items: center;
      min-height: 42px;
      padding: 8px 10px;
      border-radius: 999px;
      border: 1px solid rgba(48,56,61,.95);
      background: rgba(17,20,23,.9);
      color: var(--text);
      font-size: 14.3px;
      line-height: 1.16;
      text-align: center;
    }
    .frontier-0 { border-left: 4px solid var(--green); }
    .frontier-1 { border-left: 4px solid var(--blue); }
    .frontier-2 { border-left: 4px solid var(--amber); }
    .search-proof {
      display: grid;
      grid-template-rows: auto auto;
      gap: 14px;
      align-content: start;
      min-width: 0;
    }
    .fit-list {
      display: grid;
      gap: 10px;
    }
    .fit-item {
      min-height: 84px;
      padding: 15px 18px;
      border-radius: 12px;
      border: 1px solid rgba(48,56,61,.92);
      border-left: 4px solid var(--line);
      background: rgba(17,20,23,.86);
    }
    .fit-item.green { border-left-color: var(--green); }
    .fit-item.blue { border-left-color: var(--blue); }
    .fit-item.amber { border-left-color: var(--amber); }
    .fit-item h3 {
      color: var(--text);
      font-size: 18px;
      line-height: 1.18;
      margin-bottom: 7px;
    }
    .fit-item p {
      color: var(--muted);
      font-size: 13.2px;
      line-height: 1.28;
    }
    .business-search-slide .stat-tiles {
      grid-template-columns: repeat(3, minmax(0, 1fr));
      grid-template-rows: none;
      gap: 10px;
    }
    .business-search-slide .stat-tile {
      min-height: 86px;
      padding: 12px 13px;
      border-radius: 11px;
    }
    .business-search-slide .stat-tile strong {
      font-size: 24px;
    }
    .business-search-slide .stat-tile span {
      margin-top: 7px;
      font-size: 12.2px;
      line-height: 1.18;
    }
    .future-entry-slide .scenario-lead {
      margin-top: 16px;
      max-width: 1040px;
      font-size: 20px;
      line-height: 1.32;
    }
    .future-layout {
      display: grid;
      grid-template-columns: 470px 1fr;
      gap: 36px;
      align-items: stretch;
      margin-top: 28px;
    }
    .future-boundary,
    .future-entry-list {
      min-width: 0;
    }
    .future-boundary {
      padding: 22px 24px;
      border-radius: 16px;
      border: 1px solid rgba(232,199,126,.48);
      background:
        radial-gradient(circle at 84% 10%, rgba(232,199,126,.12), transparent 30%),
        rgba(17,20,23,.82);
    }
    .future-boundary > span {
      display: block;
      color: var(--amber);
      font-size: 12px;
      font-weight: 820;
      letter-spacing: .08em;
      text-transform: uppercase;
      margin-bottom: 14px;
    }
    .future-boundary > strong {
      display: block;
      color: var(--text);
      font-size: 24px;
      line-height: 1.22;
      font-weight: 780;
      margin-bottom: 14px;
    }
    .future-boundary > p {
      color: var(--muted);
      font-size: 15.5px;
      line-height: 1.42;
    }
    .future-signals {
      display: grid;
      gap: 10px;
      margin-top: 22px;
    }
    .future-signal {
      padding: 13px 15px;
      border-radius: 11px;
      border: 1px solid rgba(48,56,61,.92);
      border-left: 4px solid var(--line);
      background: rgba(11,13,14,.48);
    }
    .future-signal.green { border-left-color: var(--green); }
    .future-signal.blue { border-left-color: var(--blue); }
    .future-signal.amber { border-left-color: var(--amber); }
    .future-signal h3 {
      color: var(--text);
      font-size: 16.5px;
      line-height: 1.18;
      margin-bottom: 6px;
    }
    .future-signal p {
      color: var(--muted);
      font-size: 12.8px;
      line-height: 1.28;
    }
    .future-entry-list {
      display: grid;
      gap: 14px;
    }
    .future-entry {
      min-height: 132px;
      padding: 18px 20px;
      border-radius: 13px;
      border: 1px solid rgba(48,56,61,.92);
      border-left: 4px solid var(--line);
      background: rgba(17,20,23,.86);
    }
    .future-entry.green { border-left-color: var(--green); }
    .future-entry.blue { border-left-color: var(--blue); }
    .future-entry.amber { border-left-color: var(--amber); }
    .future-entry h3 {
      color: var(--text);
      font-size: 21px;
      line-height: 1.18;
      margin-bottom: 9px;
    }
    .future-entry p {
      color: var(--muted);
      font-size: 15px;
      line-height: 1.32;
      margin-bottom: 10px;
    }
    .future-entry em {
      display: block;
      color: var(--green);
      font-size: 14.2px;
      line-height: 1.28;
      font-style: normal;
      font-weight: 720;
    }
    .future-entry.blue em { color: var(--blue); }
    .future-entry.amber em { color: var(--amber); }
    .scenario-lead {
      margin-top: 20px;
      max-width: 990px;
      color: var(--muted);
      font-size: 21px;
      line-height: 1.38;
    }
    .scenario-layout,
    .analysis-layout {
      display: grid;
      grid-template-columns: 530px 1fr;
      gap: 40px;
      align-items: stretch;
      margin-top: 34px;
    }
    .scenario-flow-panel,
    .analysis-tree,
    .scenario-evidence-panel,
    .analysis-proof {
      min-width: 0;
    }
    .scenario-flow-panel,
    .analysis-tree {
      min-height: 390px;
      padding: 24px;
      border-radius: 18px;
      border: 1px solid rgba(48,56,61,.92);
      background:
        radial-gradient(circle at 72% 14%, rgba(155,231,197,.10), transparent 30%),
        rgba(17,20,23,.86);
    }
    .scenario-steps {
      display: grid;
      grid-template-columns: 1fr;
      gap: 9px;
    }
    .scenario-step {
      display: grid;
      grid-template-columns: 46px 1fr;
      column-gap: 14px;
      min-height: 60px;
      padding: 12px 14px;
      border-radius: 12px;
      border: 1px solid rgba(48,56,61,.92);
      background: rgba(11,13,14,.52);
    }
    .scenario-step span {
      grid-row: 1 / span 2;
      display: grid;
      place-items: center;
      width: 36px;
      height: 36px;
      border-radius: 999px;
      border: 1px solid rgba(155,231,197,.54);
      color: var(--green);
      font-size: 12px;
      font-weight: 820;
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
    }
    .scenario-step h3 {
      color: var(--text);
      font-size: 18px;
      line-height: 1.15;
      font-weight: 760;
    }
    .scenario-step p {
      margin-top: 5px;
      color: var(--muted);
      font-size: 13.8px;
      line-height: 1.32;
    }
    .scenario-step-line {
      justify-self: start;
      width: 1px;
      height: 9px;
      margin-left: 32px;
      background: rgba(155,231,197,.34);
    }
    .scenario-evidence-panel,
    .analysis-proof {
      display: grid;
      grid-template-rows: auto 1fr;
      gap: 16px;
      min-height: 390px;
    }
    .evidence-label {
      color: var(--faint);
      font-size: 12px;
      font-weight: 820;
      letter-spacing: .08em;
      text-transform: uppercase;
    }
    .stat-tiles {
      display: grid;
      grid-template-columns: 1fr;
      grid-template-rows: repeat(3, 1fr);
      gap: 14px;
    }
    .stat-tile {
      min-height: 104px;
      padding: 20px 24px;
      border-radius: 13px;
      border: 1px solid rgba(48,56,61,.92);
      background: rgba(17,20,23,.88);
    }
    .stat-tile.green { border-color: rgba(155,231,197,.52); box-shadow: inset 0 3px 0 var(--green); }
    .stat-tile.blue { border-color: rgba(118,214,255,.52); box-shadow: inset 0 3px 0 var(--blue); }
    .stat-tile.amber { border-color: rgba(232,199,126,.52); box-shadow: inset 0 3px 0 var(--amber); }
    .stat-tile strong {
      display: block;
      color: var(--green);
      font-size: 38px;
      line-height: 1.05;
      font-weight: 820;
      letter-spacing: -0.015em;
    }
    .stat-tile.blue strong { color: var(--blue); }
    .stat-tile.amber strong { color: var(--amber); }
    .stat-tile span {
      display: block;
      margin-top: 10px;
      color: var(--muted);
      font-size: 16px;
      line-height: 1.32;
    }
    .compact-cards {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 12px;
    }
    .compact-card {
      min-height: 276px;
      padding: 20px 18px;
      border-radius: 12px;
      border: 1px solid rgba(48,56,61,.92);
      border-left: 4px solid var(--line);
      background: rgba(23,27,31,.86);
    }
    .compact-card.green { border-left-color: var(--green); }
    .compact-card.blue { border-left-color: var(--blue); }
    .compact-card.amber { border-left-color: var(--amber); }
    .compact-card h3 {
      color: var(--text);
      font-size: 19px;
      line-height: 1.22;
      margin-bottom: 12px;
    }
    .compact-card p {
      color: var(--muted);
      font-size: 14.6px;
      line-height: 1.42;
    }
    .analysis-tree {
      display: grid;
      align-content: center;
      gap: 34px;
      background:
        radial-gradient(circle at 50% 20%, rgba(118,214,255,.12), transparent 28%),
        rgba(17,20,23,.86);
    }
    .analysis-question {
      display: grid;
      place-items: center;
      min-height: 118px;
      padding: 24px 30px;
      border-radius: 999px;
      border: 2px solid rgba(155,231,197,.72);
      background: rgba(155,231,197,.10);
      color: var(--text);
      font-size: 24px;
      line-height: 1.25;
      font-weight: 780;
      text-align: center;
    }
    .analysis-paths {
      display: grid;
      gap: 18px;
    }
    .analysis-path {
      padding: 18px 20px;
      border-radius: 14px;
      border: 1px solid rgba(48,56,61,.92);
      background: rgba(11,13,14,.52);
    }
    .analysis-path.green { border-left: 4px solid var(--green); }
    .analysis-path.blue { border-left: 4px solid var(--blue); }
    .analysis-path span {
      display: block;
      color: var(--muted);
      font-size: 13px;
      font-weight: 820;
      letter-spacing: .05em;
      text-transform: uppercase;
      margin-bottom: 12px;
    }
    .path-chain {
      display: flex;
      align-items: center;
      gap: 10px;
      color: var(--faint);
    }
    .path-chain strong {
      flex: 1 1 0;
      min-height: 40px;
      display: grid;
      place-items: center;
      padding: 8px 10px;
      border-radius: 999px;
      border: 1px solid rgba(48,56,61,.92);
      background: rgba(17,20,23,.86);
      color: var(--text);
      font-size: 14.2px;
      line-height: 1.2;
      text-align: center;
    }
    .path-chain em {
      color: var(--green);
      font-style: normal;
      font-size: 18px;
    }
    .roadmap-lanes {
      display: grid;
      grid-template-columns: repeat(3, 1fr);
      gap: 28px;
      margin-top: 62px;
    }
    .roadmap-lane {
      min-height: 282px;
      padding: 28px 28px 30px;
      border-radius: 16px;
      border: 1px solid rgba(48,56,61,.92);
      background:
        radial-gradient(circle at 82% 12%, rgba(155,231,197,.09), transparent 30%),
        var(--panel);
      border-top: 4px solid var(--line);
    }
    .roadmap-lane.green { border-top-color: var(--green); }
    .roadmap-lane.blue { border-top-color: var(--blue); }
    .roadmap-lane.amber { border-top-color: var(--amber); }
    .roadmap-lane span {
      display: grid;
      place-items: center;
      width: 42px;
      height: 42px;
      border-radius: 999px;
      border: 1px solid rgba(155,231,197,.45);
      color: var(--green);
      font-size: 13px;
      font-weight: 820;
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
      margin-bottom: 24px;
    }
    .roadmap-lane h3 {
      color: var(--text);
      font-size: 25px;
      line-height: 1.2;
      margin-bottom: 18px;
    }
    .roadmap-lane p {
      color: var(--muted);
      font-size: 17px;
      line-height: 1.45;
    }
    .roadmap-takeaway {
      margin: 40px auto 0;
      max-width: 960px;
      color: var(--green);
      font-size: 22px;
      line-height: 1.42;
      text-align: center;
      font-weight: 680;
    }
    .qa-layout {
      display: grid;
      grid-template-columns: 520px 1fr;
      gap: 58px;
      align-items: center;
      min-height: 620px;
    }
    .qa-main h2 {
      max-width: 520px;
      font-size: 104px;
      line-height: 1;
      letter-spacing: -0.02em;
    }
    .qa-main .scenario-lead {
      margin-top: 26px;
      max-width: 470px;
      font-size: 24px;
      line-height: 1.38;
    }
    .qa-github {
      margin-top: 34px;
      color: var(--blue);
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
      font-size: 18px;
      line-height: 1.3;
    }
    .qa-questions {
      display: grid;
      gap: 18px;
    }
    .qa-question {
      min-height: 128px;
      padding: 24px 28px;
      border-radius: 16px;
      border: 1px solid rgba(48,56,61,.92);
      border-left: 4px solid var(--line);
      background:
        radial-gradient(circle at 88% 10%, rgba(155,231,197,.09), transparent 30%),
        rgba(17,20,23,.86);
    }
    .qa-question:nth-child(1) { border-left-color: var(--green); }
    .qa-question:nth-child(2) { border-left-color: var(--blue); }
    .qa-question:nth-child(3) { border-left-color: var(--amber); }
    .qa-question span {
      display: block;
      color: var(--green);
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
      font-size: 12px;
      font-weight: 820;
      letter-spacing: .08em;
      margin-bottom: 14px;
    }
    .qa-question h3 {
      color: var(--text);
      font-size: 24px;
      line-height: 1.2;
      margin-bottom: 10px;
    }
    .qa-question p {
      color: var(--muted);
      font-size: 17px;
      line-height: 1.42;
    }
    .footer {
      position: absolute;
      left: 72px;
      right: 72px;
      bottom: 34px;
      display: flex;
      align-items: center;
      justify-content: flex-end;
      color: var(--faint);
      font-size: 12px;
    }
    .progress {
      position: fixed;
      left: 0;
      bottom: 0;
      height: 3px;
      width: 0;
      background: linear-gradient(90deg, var(--green), var(--blue));
      z-index: 3;
      transition: width .18s ease;
    }
    @media print {
      html, body { overflow: visible; background: white; }
      body::before, .progress { display: none; }
      .deck {
        position: static;
        transform: none;
        width: 100vw;
        height: auto;
        box-shadow: none;
      }
      .slide {
        position: relative;
        display: block !important;
        width: 100vw;
        height: 62.5vw;
        page-break-after: always;
      }
    }
  </style>
</head>
<body>
  <main class="deck" aria-label="NeuG slides">
    ${slides.map(renderSlide).join("\n")}
  </main>
  <div class="progress" aria-hidden="true"></div>
  <script>
    const slides = [...document.querySelectorAll(".slide")];
    const progress = document.querySelector(".progress");
    const DESIGN_WIDTH = 1440;
    const DESIGN_HEIGHT = 900;
    let current = 0;
    function fitDeck() {
      const scale = Math.min(innerWidth / DESIGN_WIDTH, innerHeight / DESIGN_HEIGHT);
      document.documentElement.style.setProperty("--deck-scale", Math.max(0.1, scale).toFixed(4));
    }
    function clamp(n) { return Math.max(0, Math.min(slides.length - 1, n)); }
    function show(n, updateHash = true) {
      current = clamp(n);
      slides.forEach((slide, i) => slide.classList.toggle("active", i === current));
      progress.style.width = ((current + 1) / slides.length * 100) + "%";
      if (updateHash) history.replaceState(null, "", "#/" + (current + 1));
    }
    function fromHash() {
      const match = location.hash.match(/#\\/(\\d+)/);
      return match ? Number(match[1]) - 1 : 0;
    }
    addEventListener("keydown", (event) => {
      if (["ArrowRight", "PageDown", " ", "Enter"].includes(event.key)) show(current + 1);
      if (["ArrowLeft", "PageUp", "Backspace"].includes(event.key)) show(current - 1);
      if (event.key === "Home") show(0);
      if (event.key === "End") show(slides.length - 1);
    });
    addEventListener("click", (event) => {
      if (event.altKey || event.metaKey || event.ctrlKey || event.shiftKey) return;
      show(current + (event.clientX > innerWidth * 0.25 ? 1 : -1));
    });
    addEventListener("resize", fitDeck);
    addEventListener("hashchange", () => show(fromHash(), false));
    fitDeck();
    show(fromHash(), false);
  </script>
</body>
</html>`;

await fs.mkdir(path.dirname(outFile), { recursive: true });
await fs.writeFile(outFile, html, "utf8");
console.log(outFile);
