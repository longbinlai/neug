import fs from "node:fs/promises";
import path from "node:path";

const root = "/Users/robeenly/Documents/neug";
const outFile = path.join(root, "outputs", "neug-llm-agent-graph-engine-slides.html");
const pipInstallImagePath = path.join(root, "outputs", "assets", "neug-pip-install.png");

const slides = [
  {
    kind: "cover",
    kicker: "NeuG",
    title: "面向 LLM 与 Agent 的\n嵌入式图数据引擎",
    subtitle: "支撑企业知识库、自动化分析与未来端侧关联索引",
    footer: "高性能图查询  |  轻量嵌入  |  企业知识库  |  自动化分析  |  端侧关联索引",
  },
  {
    kind: "context",
    kicker: "WHY NOW",
    title: "LLM 与 Agent 需要关联复杂、可维护的上下文",
    lead: "问题不是给模型塞进更多 token，而是维护一层能持续更新、检索和分析的上下文关系网。",
    cards: [
      ["组织知识", "LLM Wiki 说明了一个方向：组织知识不能只靠临时切片召回，需要把原始资料整理成可维护的知识层。企业里还要同时维护文档、代码、会议、项目和人员关系。", "green"],
      ["业务分析", "经营分析需要沿客户、商品、渠道、组织和时间连续追问。难点不是生成一次 SQL，而是维护可复用的分析上下文和中间结果。", "blue"],
      ["个人与设备记忆", "千问眼镜等设备未来需要维护人、地点、事件、对话、任务和时间线的本地关联上下文。重点不是设备形态，而是端上持续积累的关系网络。", "amber"],
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
    title: "NeuG 已把嵌入式形态做到单机顶级图查询性能",
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
      official: "80,510.79",
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
    lead: "NeuG 的轻量不是为了“小”，而是为了让图数据能力可以像一个库一样被嵌入到应用、工具链和未来端侧环境里。",
    image: "assets/neug-pip-install.png",
    cards: [
      ["低部署成本", "无需单独图数据库集群、服务进程和复杂运维，更接近 DuckDB / SQLite 的使用方式。", "green"],
      ["贴近 Agent 执行环境", "Agent 可以在本地工具链、CLI、Skill 和数据分析脚本里直接调用图查询与混合检索能力。", "blue"],
      ["为端侧生态预留入口", "未来可面向千问眼镜、手机、可穿戴等设备做本地关联索引；这里点到为止，不抢当前主线。", "amber"],
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
    kicker: "LLM MEMORY",
    title: "企业知识库需要图、向量和全文统一工作",
    lead: "纯 RAG 容易把知识切碎；NeuG 更适合保留实体、关系、来源和上下文。",
    cards: [
      ["更准召回", "向量负责语义相似，全文负责精确匹配，图关系负责上下文扩展。", "green"],
      ["更可追溯", "文档、代码、会议、人员、项目和决策都可以保留引用链路。", "blue"],
      ["更能沉淀", "组织记忆不再只是 prompt 历史，而是可更新、可查询的数据资产。", "amber"],
    ],
  },
  {
    kicker: "GRAPH DATA SCIENCE",
    title: "GDS 让知识库和 BI 从检索走向结构洞察",
    placeholder: "预留：GDS 算法能力图谱或调用流程图",
    lead: "图算法直接在数据库内运行，避免把数据搬到外部工具。",
    bullets: [
      "PageRank 用于重要节点排序",
      "BFS / DFS 用于自动探索路径",
      "Community detection 用于发现业务群组",
      "SSSP / WCC 用于路径和连通性分析",
    ],
    note: "业务意义：企业数据不只是被查到，而是能产生结构性解释。",
  },
  {
    kicker: "DATA PIPELINE",
    title: "NeuG 可以接入现有数据湖，而不是制造新的数据孤岛",
    lead: "v0.1.2 以来的数据管道能力，让 NeuG 更像企业数据流里的图计算层。",
    flow: [
      ["OSS / S3", "远程 Parquet 直读"],
      ["Schema-on-read", "自动推断列与类型"],
      ["Graph Query", "图查询与图算法"],
      ["Parquet Writeback", "结果写回数据湖"],
    ],
  },
  {
    kicker: "BUSINESS APPLICATION",
    title: "NeuGBI 把 BI 从“问数”推进到“自动分析”",
    lead: "ChatBI 解决自然语言到查询；NeuGBI 解决如何沿业务关系持续下钻。",
    cards: [
      ["ChatBI", "面向报表和 SQL 生成。问题通常被转成一次查询，后续追问依赖人工继续建模。", "muted"],
      ["NeuGBI", "基于图和超图重新建模。Agent 可以自动扩展维度、复用中间结果、沿关系分析原因。", "green"],
    ],
    note: "核心变化：从生成图表，升级为生成分析路径。",
  },
  {
    kicker: "NEUGBI ENGINE",
    title: "NeuGBI 的差异化来自图探索、图分析和端到端采样",
    cards: [
      ["自动下钻", "沿业务图做 BFS / DFS 式探索，自动发现可解释的下一层维度。", "green"],
      ["图分析指标", "把 PageRank、community detection 等结构指标引入 BI，而不只看聚合值。", "blue"],
      ["端到端采样", "让大规模数据分析保持时效性，适合 agent 多轮探索。", "amber"],
    ],
    placeholder: "预留：问题 -> 自动下钻路径 -> 业务洞察流程图",
  },
  {
    kicker: "BUSINESS CASE",
    title: "NeuGBI 的业务表达应贴近经营分析场景",
    lead: "建议使用“招聘与岗位变化分析”一类案例，保留多维下钻价值，避免学术研究案例抢走业务主线。",
    placeholder: "预留：行业 -> 地区 -> 职级 -> 岗位 的自动分析路径",
    bulletsTitle: "可展示的结果形态",
    bullets: [
      "Agent 自动拆解问题",
      "沿业务关系扩展维度",
      "给出趋势、原因和异常群组",
      "支持回溯到数据来源",
    ],
  },
  {
    kicker: "RESEARCH FRONTIER",
    title: "SpecDB 是团队面向未来数据库形态的研究壁垒",
    lead: "核心问题：能否让 LLM 直接生成满足特定工作负载需求的数据库系统？",
    metrics: [
      ["TPC-C", "最新生成版本吞吐超过 PostgreSQL", "green"],
      ["TPC-H", "最新生成版本时延超过 DuckDB", "blue"],
    ],
    note: "这不是当前业务主线，但它会反哺 NeuG 的模块化、测试、优化器和自动生成能力。",
    placeholder: "预留：SpecDB pipeline 图",
  },
  {
    kicker: "FUTURE OPTION",
    title: "端侧关联索引是未来生态延展，不是当前汇报主轴",
    lead: "NeuG 的高性能和轻量体积，为千问眼镜、手机、可穿戴、车载和机器人预留了本地数据引擎入口。",
    cards: [
      ["本地记忆", "人物、地点、事件、对话和任务在端上形成长期关联。", "green"],
      ["低延迟与隐私", "常用上下文不必每次上云，关键个人数据可以本地检索。", "blue"],
      ["生态入口", "千问眼镜等设备需要的不只是模型，也需要可持续积累的关联索引。", "amber"],
    ],
  },
  {
    kicker: "ROADMAP",
    title: "下一步要把底座能力收敛成可展示的业务闭环",
    cards: [
      ["近期", "完成 vector extension 和 full-text，形成企业知识库与组织记忆 demo。", "green"],
      ["中期", "GDS 与 NeuGBI 深度集成，打磨企业经营分析 demo。", "blue"],
      ["长期", "推进 SpecDB 研究，探索千问眼镜等端侧关联索引场景。", "amber"],
    ],
    note: "汇报结论：NeuG 是一套能同时承载高性能图查询、LLM 数据中间层和未来端侧关联索引的嵌入式图数据引擎。",
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
        <div class="kicker">${esc(slide.kicker)}</div>
        <h1>${esc(slide.title).replaceAll("\n", "<br>")}</h1>
        <p class="subtitle">${esc(slide.subtitle)}</p>
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

const html = `<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>NeuG | 面向 LLM 与 Agent 的嵌入式图数据引擎</title>
  <style>
    :root {
      color-scheme: dark;
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
      width: min(100vw, calc(100vh * 16 / 9));
      height: min(100vh, calc(100vw * 9 / 16));
      transform: translate(-50%, -50%);
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
    .rule {
      margin-top: 42px;
      width: 180px;
      height: 2px;
      background: var(--green);
    }
    .cover-footer {
      margin-top: 28px;
      font-size: 17px;
      color: var(--muted);
    }
    .orb {
      position: absolute;
      border-radius: 999px;
      filter: saturate(1.05);
    }
    .orb-a { right: 170px; top: 70px; width: 180px; height: 180px; background: var(--green); opacity: .92; }
    .orb-b { right: 110px; top: 205px; width: 128px; height: 128px; background: var(--blue); opacity: .9; }
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
      margin-top: 24px;
      max-width: 960px;
      color: var(--muted);
      font-size: 23px;
      line-height: 1.45;
    }
    .hybrid-layout {
      display: grid;
      grid-template-columns: 390px 180px 1fr;
      gap: 34px;
      align-items: center;
      margin-top: 52px;
    }
    .hybrid-actions {
      display: grid;
      gap: 18px;
    }
    .action-card {
      min-height: 118px;
      padding: 20px 22px;
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
      font-size: 22px;
      line-height: 1.2;
      margin-bottom: 12px;
    }
    .action-card p {
      color: var(--muted);
      font-size: 16.4px;
      line-height: 1.45;
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
      gap: 13px;
      padding: 54px 24px 22px;
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
      padding: 20px 24px;
      border-radius: 12px;
      border: 1px solid rgba(48,56,61,.92);
      background: rgba(11,13,14,.62);
    }
    .stack-layer h3 {
      margin-bottom: 8px;
      color: var(--text);
      font-size: 22px;
      line-height: 1.15;
    }
    .stack-layer p {
      color: var(--muted);
      font-size: 16.2px;
      line-height: 1.35;
    }
    .stack-layer.layer-0 { border-color: rgba(155,231,197,.6); }
    .stack-layer.layer-1 { border-color: rgba(118,214,255,.52); }
    .stack-layer.layer-2 { border-color: rgba(232,199,126,.48); }
    .stack-layer.layer-3 { border-color: rgba(155,231,197,.44); }
    .benchmark-lead {
      margin-top: 24px;
      max-width: 980px;
      color: var(--muted);
      font-size: 23px;
      line-height: 1.45;
    }
    .benchmark-layout {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 34px;
      align-items: stretch;
      margin-top: 36px;
    }
    .benchmark-panel {
      min-height: 354px;
      border-radius: 18px;
      border: 1px solid var(--line);
      background:
        radial-gradient(circle at 85% 8%, rgba(118,214,255,.1), transparent 26%),
        var(--panel);
      padding: 26px 30px;
    }
    .benchmark-panel.ldbc-panel {
      background:
        radial-gradient(circle at 88% 8%, rgba(155,231,197,.11), transparent 26%),
        var(--panel);
    }
    .panel-head h3 {
      color: var(--text);
      font-size: 25px;
      line-height: 1.2;
      margin-bottom: 10px;
    }
    .panel-head p {
      color: var(--muted);
      font-size: 15.5px;
      line-height: 1.35;
    }
    .qps-bars {
      display: grid;
      gap: 22px;
      margin-top: 32px;
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
      margin-top: 34px;
      padding-top: 22px;
      border-top: 1px solid var(--line);
    }
    .benchmark-callout strong {
      color: var(--green);
      font-size: 56px;
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
      margin-top: 18px;
    }
    .benchmark-facts.three {
      grid-template-columns: repeat(3, 1fr);
      margin-top: 16px;
    }
    .benchmark-facts span {
      display: block;
      padding: 10px 14px;
      border-radius: 9px;
      border: 1px solid rgba(48,56,61,.9);
      color: var(--muted);
      font-size: 14.5px;
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
      margin-top: 28px;
    }
    .duel {
      padding: 22px 20px 20px;
      border-radius: 14px;
      border: 1px solid var(--line);
      background: rgba(11,13,14,.5);
    }
    .duel span {
      display: block;
      color: var(--muted);
      font-size: 15px;
      font-weight: 700;
      margin-bottom: 18px;
    }
    .duel strong {
      display: block;
      color: var(--green);
      font-size: 39px;
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
      margin-top: 18px;
      padding: 18px 22px;
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
      font-size: 38px;
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
    .footer {
      position: absolute;
      left: 72px;
      right: 72px;
      bottom: 34px;
      display: flex;
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
        height: 56.25vw;
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
    let current = 0;
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
    addEventListener("hashchange", () => show(fromHash(), false));
    show(fromHash(), false);
  </script>
</body>
</html>`;

await fs.mkdir(path.dirname(outFile), { recursive: true });
await fs.writeFile(outFile, html, "utf8");
console.log(outFile);
