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

Use the article title:

> From Theory to Real-Graph Benchmarks: Implementing BMSSP in NeuG

Optional opening comment:

> We started this after reading Vals AI's post about a formally verified faster shortest-path algorithm. It made us wonder how much of that theoretical progress survives contact with a graph database. C-HD was not a natural fit for the graphs we had on hand, so we went back to BMSSP, the 2025 STOC Best Paper result, and implemented that in NeuG.
>
> The less tidy result is that both benchmark graphs converged during the parallel frontier probe, before the recursive BMSSP fallback was needed. The adaptive implementation was still 4.5–13.5% faster than our existing frontier backend and about 13.5x faster than Dijkstra, but those numbers describe the hybrid path as a whole, not a clean win for the recursive path by itself.
>
> We forced the fallback in correctness tests, but we do not yet have a large real-world graph where it is the interesting performance path. If you work on weighted SSSP, what graph would you use to test that case?

Posting notes:

- Keep the article's actual title; avoid adding performance claims to it.
- The opening comment is optional. If used, writing "we implemented" already makes the relationship clear without a formulaic disclosure.
- Do not ask colleagues or social followers to upvote or coordinate comments.
- Answer technical criticism directly and update the article if someone finds a reproducibility issue.
- Do not repost the same link after a weak launch merely to obtain another front-page attempt.
