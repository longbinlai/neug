# Pattern Match Extension

Since NeuG **v0.1.3**, we have introduced the Pattern Match extension, which provides subgraph pattern matching over the current NeuG graph.


```cypher
CALL PATTERN_MATCH(Pattern, size, is_sampled)
RETURN *;
```

- **`Pattern`** — the graph pattern to match, e.g. `'(a:Person)-[r:person_knows_person]->(b:Person)'`. It uses Cypher node/relationship syntax with the leading `MATCH` keyword **optional** (added automatically). It is a pattern only, not a full query: every node and relationship must be written out explicitly, though an inline `WHERE` / `RETURN` is allowed (for property filters and ordering).
- **`size`** *(optional)* — a positive integer (`>= 1`). In exact mode it is the early-termination bound (stop after the first `size` matches); in sampled mode it is the sample size.
- **`is_sampled`** *(optional)* — a boolean choosing the algorithm: `false` → exact matching (DAF), `true` → sampled matching (FaSTest). Must be written as `true` / `false` (not `0` / `1`).

`size` and `is_sampled` go together. Omit both for plain exact matching over all matches:

```cypher
CALL PATTERN_MATCH('(a:Person)-[r:person_knows_person]->(b:Person)') RETURN *;
```

**Supported patterns:** directed relationships (`->` / `<-`) written out explicitly; one label per node, one type per relationship; inline property maps with literal values (`{age: 20}`);

**Not supported patterns (rejected at bind time):** variable-length / recursive relationships (`-[:R*3]->`, `-[:R*1..3]->`, `-[*]->`), undirected `(a)-[r]-(b)`, multi-label nodes `(a:A:B)`, multi-type relationships `[:A|:B]`, `OPTIONAL MATCH` / `WITH` / `UNION` / mutations, `OR` / `NOT` / `XOR`, cross-variable comparisons (`a.age = b.age`), and computed projections / `ORDER BY` / `SKIP` / `LIMIT`. Write a fixed-length path out one relationship at a time instead of using `*`.

The pattern can also be a path to a Cypher file, or inline/file JSON in the internal pattern format.


## Install Extension

```cypher
INSTALL pattern_matching;
```

## Load Extension

```cypher
LOAD pattern_matching;
```

When building from source, enable the extension with:

```bash
cmake -S . -B build -DBUILD_EXTENSIONS="pattern_matching" -DBUILD_TEST=ON
cmake --build build --target neug_pattern_matching_extension -j$(sysctl -n hw.ncpu)
```

On Linux, use `-j$(nproc)` instead of `-j$(sysctl -n hw.ncpu)`.

## Exact Matching

Call `PATTERN_MATCH` with only the pattern to enumerate **all** exact embeddings:

```cypher
CALL PATTERN_MATCH('(a:Person)-[r:person_knows_person]->(b:Person)')
RETURN *;
```

Output shape — one row per match, vertices as `Vertex` columns and relationships as `Edge` columns, named by the pattern aliases:

| a | r | b |
| --- | --- | --- |
| `Vertex(Person)` | `Edge(person_knows_person)` | `Vertex(Person)` |

To stop after the first `size` matches (early termination), pass `size` with `is_sampled = false`:

```cypher
-- exact matching, stop after the first 10 matches
CALL PATTERN_MATCH('(a:Person)-[r:person_knows_person]->(b:Person)', 10, false)
RETURN *;
```

## Sampled Matching

Pass `size` with `is_sampled = true` when exact enumeration may be too expensive and sampled embeddings are sufficient:

```cypher
CALL PATTERN_MATCH(
  '(a:Person)-[r:person_knows_person]->(b:Person)',
  1000, true
)
RETURN *;
```

Here `size` is the target sample size. The function returns sampled embeddings using the same vertex/edge column layout as the exact mode.

The sampled implementation is based on the FaSTest filtering-sampling algorithm described in "Cardinality Estimation of Subgraph Matching: A Filtering-Sampling Approach" (VLDB 2024). The implementation computes FaSTest estimates internally, but the public table function returns sampled matches as native `QueryResult` rows rather than `estimated_count`, `result_file`, or `props_file` columns.

## Applying NeuG Operators to the Result

Because the output vertex/edge variables carry the same catalog and property metadata as a `MATCH`-bound node, NeuG's own pipeline clauses can be applied on the trailing `RETURN`: property access, `ORDER BY <var>.<prop>`, `LIMIT`, and aggregates such as `count(<var>)` and `count(DISTINCT <var>.<prop>)`.

```cypher
-- project scalar properties, order and limit
CALL PATTERN_MATCH('(a:Person)-[r:person_knows_person]->(b:Person)')
RETURN a.name AS src, r.weight AS weight
ORDER BY r.weight DESC
LIMIT 10;

-- aggregate over the matches
CALL PATTERN_MATCH('(a:Person)-[r:person_knows_person]->(b:Person)')
RETURN count(a) AS matches, count(DISTINCT a.name) AS distinct_sources;
```

## YIELD

`PATTERN_MATCH` supports an optional `YIELD` clause between the call and the trailing `RETURN`. `YIELD` lists the pattern variables to expose to the rest of the query and may rename them with `AS`:

```cypher
-- expose all matched variables (equivalent to omitting YIELD)
CALL PATTERN_MATCH('(a:Person)-[r:person_knows_person]->(b:Person)')
YIELD a, r, b
RETURN a.name, r.weight, b.name;

-- rename matched variables
CALL PATTERN_MATCH('(a:Person)-[r:person_knows_person]->(b:Person)')
YIELD a AS src, b AS dst
RETURN src.name, dst.name;

-- expose only a subset; unlisted variables are hidden from RETURN
CALL PATTERN_MATCH('(a:Person)-[r:person_knows_person]->(b:Person)')
YIELD b
RETURN b.name;
```

Rules and limitations, consistent with the rest of NeuG's `YIELD`:

- A `YIELD` item is a bare variable name, optionally followed by `AS <alias>`. It refers to a whole pattern variable (a `Vertex` or `Edge`).
- **`YIELD` cannot contain a property access.** `YIELD a.name` is a syntax error; access properties in the trailing `RETURN` (e.g. `YIELD a RETURN a.name`).
- A variable that is not listed in `YIELD` is hidden — referencing it in `RETURN` raises a "not in scope" error.
- Yielding a name that is not a pattern variable raises a bind error.
- `YIELD` must be followed by a `RETURN`; it does not terminate the query on its own.

Ordering by a whole `Vertex`/`Edge` object (for example `ORDER BY a`) is not supported by NeuG; order by a scalar property such as `ORDER BY a.age` instead.
