# BMSSP article: social copy

Article URL: <https://neug.io/blog/bmssp-shortest-path/>

Publish only after the URL resolves publicly.

## Twitter / X

256 characters including the literal URL (the platform may count the URL using its own shortened-link rules):

> Can a theoretically faster shortest-path algorithm win on real graphs?
>
> We implemented BMSSP (STOC 2025 Best Paper) in NeuG. On two public Graphalytics graphs, it beat frontier by 4.5–13.5% and Dijkstra by ~13.5×.
>
> https://neug.io/blog/bmssp-shortest-path/

Suggested image: `public/images/blog/bmssp-shortest-path/banner-v2.png`

## LinkedIn

> Recent work on formally verified shortest-path algorithms reopened a practical question for us: when does a better asymptotic result become a faster graph-database implementation?
>
> We returned to BMSSP, the algorithm from the STOC 2025 Best Paper, implemented it as a NeuG GDS backend, and tested it on public LDBC Graphalytics data.
>
> On the two graphs in this first evaluation, BMSSP reduced median runtime by 4.5–13.5% versus NeuG's frontier backend and was about 13.5× faster than Dijkstra. We also checked the implementation against official reference output, differential tests, fallback-path tests, and more than two million computed distances.
>
> The article covers the algorithmic idea, the engineering decisions that mattered, the build and data pipeline, and the limits of the current benchmark.
>
> Which weighted real-world graph should we test next?
>
> https://neug.io/blog/bmssp-shortest-path/
>
> #GraphAlgorithms #GraphDatabase #SSSP #OpenSource

Suggested image: `public/images/blog/bmssp-shortest-path/banner-v2.png`

## Hacker News

Submit the article as a URL submission.

Suggested title:

> Implementing BMSSP in NeuG and benchmarking it on Graphalytics

Suggested first comment:

> Author here. We implemented BMSSP from the STOC 2025 Best Paper as a NeuG GDS backend and compared it with our existing frontier and Dijkstra backends on two public Graphalytics graphs.
>
> The median latency was 4.5% and 13.5% lower than frontier, and about 13.5x lower than Dijkstra. We checked correctness with the official reference output, differential tests on weighted edge cases, a fallback-path test, and a full comparison covering 2,072,117 vertices.
>
> This is still a deliberately small first benchmark: both large graphs come from the same generator, and the current numbers measure the algorithm after graph projection rather than every possible end-to-end ingestion cost.
>
> I would value feedback on three questions: which real-world weighted graph would be most informative next; whether compact-CSR construction should be included in the primary end-to-end number; and which frontier-style SSSP implementation would make the strongest additional baseline.

Posting notes:

- Use the article's actual title or the neutral title above; avoid performance superlatives.
- Disclose the author relationship in the first comment.
- Do not ask colleagues or social followers to upvote or coordinate comments.
- Answer technical criticism directly and update the article if someone finds a reproducibility issue.
- Do not repost the same link after a weak launch merely to obtain another front-page attempt.
