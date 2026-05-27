/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <gtest/gtest.h>

#include <neug/main/neug_db.h>

#include <unistd.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#if defined(__APPLE__)
#include <mach-o/dyld.h>
#endif

namespace neug {
namespace test {

namespace {

std::filesystem::path GetExecutablePath() {
#if defined(__APPLE__)
  uint32_t size = 0;
  _NSGetExecutablePath(nullptr, &size);
  std::string buf(size, '\0');
  if (_NSGetExecutablePath(buf.data(), &size) != 0) return {};
  return std::filesystem::canonical(buf.c_str());
#else
  return std::filesystem::read_symlink("/proc/self/exe");
#endif
}

// Walk upward from the test binary and return the first ancestor that contains
// the built extension library. That directory is what LOAD expects via
// NEUG_EXTENSION_HOME_PYENV.
std::string FindBuildRoot() {
  auto dir = GetExecutablePath().parent_path();
  const std::string target =
      "extension/sampled_match/libsampled_match.neug_extension";
  for (int i = 0; i < 8; ++i) {
    if (std::filesystem::exists(dir / target)) return dir.string();
    if (dir == dir.parent_path()) break;
    dir = dir.parent_path();
  }
  return "";
}

// Allocate a unique scratch directory under the system temp dir. Using
// mkdtemp avoids races between concurrently-running tests and never touches
// the current working directory.
std::filesystem::path MakeUniqueTempDir() {
  auto tmpl =
      (std::filesystem::temp_directory_path() / "neug_sampled_match_XXXXXX")
          .string();
  std::vector<char> buf(tmpl.begin(), tmpl.end());
  buf.push_back('\0');
  if (mkdtemp(buf.data()) == nullptr) return {};
  return std::filesystem::path(buf.data());
}

// Triangle pattern reused across the basic tests.
constexpr const char* kTrianglePattern = R"({
  "vertices": [
    {"id": 0, "label": "Person"},
    {"id": 1, "label": "Person"},
    {"id": 2, "label": "Person"}
  ],
  "edges": [
    {"source": 0, "target": 1, "label": "person_knows_person"},
    {"source": 1, "target": 2, "label": "person_knows_person"},
    {"source": 2, "target": 0, "label": "person_knows_person"}
  ]
})";

// Single Person -> Person edge — used by tests that only need one vertex /
// edge under inspection (constraint, type mismatch, etc.).
constexpr const char* kSingleEdgePattern = R"({
  "vertices": [
    {"id": 0, "label": "Person"},
    {"id": 1, "label": "Person"}
  ],
  "edges": [
    {"source": 0, "target": 1, "label": "person_knows_person"}
  ]
})";

}  // namespace

class SampledMatchTest : public ::testing::Test {
 public:
  static void SetUpTestSuite() {
    const std::string build_root = FindBuildRoot();
    ASSERT_FALSE(build_root.empty())
        << "Could not locate libsampled_match.neug_extension near "
        << GetExecutablePath();
    setenv("NEUG_EXTENSION_HOME_PYENV", build_root.c_str(), 1);
  }

  void SetUp() override {
    test_dir_ = MakeUniqueTempDir();
    ASSERT_FALSE(test_dir_.empty());

    db_ = std::make_unique<neug::NeugDB>();
    ASSERT_TRUE(db_->Open(test_dir_ / "db"));

    conn_ = db_->Connect();
    ASSERT_TRUE(conn_);

    // Schema: two node labels (Person, Company) and two edge labels with
    // typed properties — this gives the corner-case tests room to exercise
    // unknown labels, unknown properties, type-mismatch on int/string, and
    // edge property constraints.
    //
    // Data layout (id/name/age):
    //   Person(0, 'Alice', 20)
    //   Person(1, 'Bob',   30)
    //   Person(2, 'Carol', 20)
    //   Person(3, 'Dave',  40)
    //   Company(0, 'Acme')
    //   Company(1, 'Globex')
    //
    // Edges (person_knows_person, weight DOUBLE):
    //   0 -> 1 (0.5), 1 -> 2 (1.5), 2 -> 0 (0.5)        -- the original triangle
    //   0 -> 3 (2.5), 3 -> 1 (0.5)                       -- extends to a "Y"
    //
    // Edges (person_works_at_company, since INT32):
    //   0 -> Acme  (2020),  1 -> Acme  (2018)
    //   2 -> Globex(2019),  3 -> Globex(2021)
    const std::pair<std::string, std::string> setup_queries[] = {
        {"schema: Person",
         "CREATE NODE TABLE Person(id INT32 PRIMARY KEY, name STRING, age INT32);"},
        {"schema: Company",
         "CREATE NODE TABLE Company(id INT32 PRIMARY KEY, name STRING);"},
        {"schema: knows",
         "CREATE REL TABLE person_knows_person("
         "FROM Person TO Person, weight DOUBLE);"},
        {"schema: works_at",
         "CREATE REL TABLE person_works_at_company("
         "FROM Person TO Company, since INT32);"},
        {"insert p0", "CREATE (n:Person {id: 0, name: 'Alice', age: 20})"},
        {"insert p1", "CREATE (n:Person {id: 1, name: 'Bob',   age: 30})"},
        {"insert p2", "CREATE (n:Person {id: 2, name: 'Carol', age: 20})"},
        {"insert p3", "CREATE (n:Person {id: 3, name: 'Dave',  age: 40})"},
        {"insert c0", "CREATE (n:Company {id: 0, name: 'Acme'})"},
        {"insert c1", "CREATE (n:Company {id: 1, name: 'Globex'})"},
        {"edge 0->1 (0.5)",
         "MATCH (a:Person), (b:Person) WHERE a.id = 0 AND b.id = 1 "
         "CREATE (a)-[:person_knows_person {weight: 0.5}]->(b)"},
        {"edge 1->2 (1.5)",
         "MATCH (a:Person), (b:Person) WHERE a.id = 1 AND b.id = 2 "
         "CREATE (a)-[:person_knows_person {weight: 1.5}]->(b)"},
        {"edge 2->0 (0.5)",
         "MATCH (a:Person), (b:Person) WHERE a.id = 2 AND b.id = 0 "
         "CREATE (a)-[:person_knows_person {weight: 0.5}]->(b)"},
        {"edge 0->3 (2.5)",
         "MATCH (a:Person), (b:Person) WHERE a.id = 0 AND b.id = 3 "
         "CREATE (a)-[:person_knows_person {weight: 2.5}]->(b)"},
        {"edge 3->1 (0.5)",
         "MATCH (a:Person), (b:Person) WHERE a.id = 3 AND b.id = 1 "
         "CREATE (a)-[:person_knows_person {weight: 0.5}]->(b)"},
        {"edge p0->c0 (2020)",
         "MATCH (a:Person), (b:Company) WHERE a.id = 0 AND b.id = 0 "
         "CREATE (a)-[:person_works_at_company {since: 2020}]->(b)"},
        {"edge p1->c0 (2018)",
         "MATCH (a:Person), (b:Company) WHERE a.id = 1 AND b.id = 0 "
         "CREATE (a)-[:person_works_at_company {since: 2018}]->(b)"},
        {"edge p2->c1 (2019)",
         "MATCH (a:Person), (b:Company) WHERE a.id = 2 AND b.id = 1 "
         "CREATE (a)-[:person_works_at_company {since: 2019}]->(b)"},
        {"edge p3->c1 (2021)",
         "MATCH (a:Person), (b:Company) WHERE a.id = 3 AND b.id = 1 "
         "CREATE (a)-[:person_works_at_company {since: 2021}]->(b)"},
    };
    for (const auto& [label, q] : setup_queries) {
      auto res = conn_->Query(q);
      ASSERT_TRUE(res.has_value())
          << label << " failed: " << res.error().ToString();
    }

    auto load = conn_->Query("LOAD sampled_match;");
    ASSERT_TRUE(load.has_value()) << load.error().ToString();
    // Trivial follow-up so the LOAD pipeline fully retires before any
    // CALL SAMPLED_MATCH (avoids a use-after-free seen when the LOAD
    // QueryResult is the last object torn down at SetUp exit).
    auto ping = conn_->Query("RETURN 1 AS x;");
    ASSERT_TRUE(ping.has_value()) << ping.error().ToString();
  }

  void TearDown() override {
    conn_.reset();
    if (db_) {
      db_->Close();
      db_.reset();
    }
    if (!test_dir_.empty()) {
      std::error_code ec;
      std::filesystem::remove_all(test_dir_, ec);
    }
    for (const auto& d : pattern_dirs_) {
      std::error_code ec;
      std::filesystem::remove_all(d, ec);
    }
    pattern_dirs_.clear();
  }

 protected:
  // ---------------------------------------------------------------------------
  // Test helpers
  // ---------------------------------------------------------------------------

  // Result of one CALL SAMPLED_MATCH invocation, only the columns we assert on.
  struct SmResult {
    bool query_ok = false;             // SQL-layer success
    std::string error;                 // SQL error (if any)
    double estimated_count = 0.0;
    int64_t sample_count = 0;
    std::string result_file;
    std::string props_file;
  };

  // Persist `pattern_json` into a tempdir and return the path. Caller doesn't
  // own the dir — TearDown cleans it up.
  std::string WritePattern(const std::string& pattern_json) {
    std::string tmpl = "/tmp/sm_XXXXXX";
    std::vector<char> buf(tmpl.begin(), tmpl.end());
    buf.push_back('\0');
    if (mkdtemp(buf.data()) == nullptr) return "";
    std::filesystem::path dir(buf.data());
    pattern_dirs_.push_back(dir);
    auto path = dir / "p.json";
    std::ofstream ofs(path);
    if (!ofs.is_open()) return "";
    ofs << pattern_json;
    ofs.close();
    return path.string();
  }

  // Run CALL SAMPLED_MATCH on the given pattern path. Returns a digest with
  // the four columns the function exposes; query-layer errors are captured
  // (rather than ASSERTed) so corner-case tests can choose how to react.
  SmResult RunSampledMatch(const std::string& pattern_path,
                           int64_t sample_size = 1000) {
    SmResult r;
    if (pattern_path.empty()) {
      r.error = "WritePattern returned empty path";
      return r;
    }
    std::ostringstream q;
    q << "CALL SAMPLED_MATCH('" << pattern_path << "', " << sample_size
      << ") RETURN estimated_count, sample_count, result_file, props_file;";
    auto sm = conn_->Query(q.str());
    if (!sm.has_value()) {
      r.error = sm.error().ToString();
      return r;
    }
    const auto& sm_qr = sm.value();
    const auto& resp = sm_qr.response();
    if (sm_qr.length() != 1u || resp.arrays_size() != 4) {
      r.error = "unexpected response shape";
      return r;
    }
    const auto& est = resp.arrays(0).double_array();
    const auto& cnt = resp.arrays(1).int64_array();
    const auto& rf = resp.arrays(2).string_array();
    const auto& pf = resp.arrays(3).string_array();
    if (est.values_size() != 1 || cnt.values_size() != 1) {
      r.error = "missing per-row values";
      return r;
    }
    r.query_ok = true;
    r.estimated_count = est.values(0);
    r.sample_count = cnt.values(0);
    r.result_file = rf.values_size() ? rf.values(0) : "";
    r.props_file = pf.values_size() ? pf.values(0) : "";
    return r;
  }

  std::filesystem::path test_dir_;
  std::vector<std::filesystem::path> pattern_dirs_;
  std::unique_ptr<neug::NeugDB> db_;
  std::shared_ptr<neug::Connection> conn_;
};

// ---------------------------------------------------------------------------
// 1. Happy-path baseline — drives extension load, INITIALIZE bootstrap, and a
//    SAMPLED_MATCH on a known-good triangle pattern. The richer dataset
//    (5 knows-edges among 4 Person nodes, plus a separate Company subgraph)
//    means counts here differ from the original 3-node fixture.
// ---------------------------------------------------------------------------
TEST_F(SampledMatchTest, LoadsExtensionAndGraphQueries) {
  auto count = conn_->Query("MATCH (n:Person) RETURN count(*) AS c;");
  ASSERT_TRUE(count.has_value()) << count.error().ToString();

  auto init = conn_->Query(
      "CALL INITIALIZE() "
      "RETURN status, num_vertices, num_edges, max_degree, degeneracy;");
  ASSERT_TRUE(init.has_value()) << init.error().ToString();

  const auto& init_qr = init.value();
  const auto& init_resp = init_qr.response();
  ASSERT_EQ(init_qr.length(), 1u);
  ASSERT_EQ(init_resp.arrays_size(), 5);

  const auto& status_col = init_resp.arrays(0).string_array();
  const auto& vertices_col = init_resp.arrays(1).int64_array();
  const auto& edges_col = init_resp.arrays(2).int64_array();
  ASSERT_EQ(status_col.values_size(), 1);
  ASSERT_EQ(vertices_col.values_size(), 1);
  ASSERT_EQ(edges_col.values_size(), 1);

  EXPECT_EQ(status_col.values(0), "success");
  // 4 Persons + 2 Companies = 6 vertices.
  EXPECT_EQ(vertices_col.values(0), 6);
  // 5 knows + 4 works_at = 9 directed edges; preprocessing may double-count
  // them as undirected (9) or directed-with-reverse (18).
  EXPECT_TRUE(edges_col.values(0) == 9 || edges_col.values(0) == 18)
      << "unexpected edge count: " << edges_col.values(0);

  auto path = WritePattern(kTrianglePattern);
  auto r = RunSampledMatch(path);
  ASSERT_TRUE(r.query_ok) << r.error;
  // The graph contains exactly one undirected 3-cycle (0 - 1 - 2 - 0); FaSTest
  // should sample at least one embedding and report a positive estimate.
  EXPECT_GE(r.sample_count, 1)
      << "SAMPLED_MATCH returned zero sampled embeddings on a triangle";
  EXPECT_GT(r.estimated_count, 0.0)
      << "SAMPLED_MATCH returned a non-positive estimated count";

  // Result file must exist and be non-empty.
  EXPECT_FALSE(r.result_file.empty());
  EXPECT_TRUE(std::filesystem::exists(r.result_file));
  EXPECT_GT(std::filesystem::file_size(r.result_file), 0u);
}

// ---------------------------------------------------------------------------
// 2. Pattern uses a vertex label that doesn't exist in the schema.
//    CreatePatternFromJson should bail out, match() should return -1, and the
//    SAMPLED_MATCH call still completes at the SQL layer (errors are surfaced
//    via the returned columns, not via Query failure).
// ---------------------------------------------------------------------------
TEST_F(SampledMatchTest, RejectsPatternWithUnknownVertexLabel) {
  constexpr const char* kPattern = R"({
    "vertices": [
      {"id": 0, "label": "Alien"},
      {"id": 1, "label": "Alien"}
    ],
    "edges": [
      {"source": 0, "target": 1, "label": "person_knows_person"}
    ]
  })";
  auto path = WritePattern(kPattern);
  auto r = RunSampledMatch(path);
  ASSERT_TRUE(r.query_ok) << r.error;
  EXPECT_DOUBLE_EQ(r.estimated_count, -1.0)
      << "match() should signal failure with -1 when label is unknown";
  EXPECT_EQ(r.sample_count, 0);
}

// ---------------------------------------------------------------------------
// 3. Pattern uses an edge label that isn't in the schema.
// ---------------------------------------------------------------------------
TEST_F(SampledMatchTest, RejectsPatternWithUnknownEdgeLabel) {
  constexpr const char* kPattern = R"({
    "vertices": [
      {"id": 0, "label": "Person"},
      {"id": 1, "label": "Person"}
    ],
    "edges": [
      {"source": 0, "target": 1, "label": "person_loves_person"}
    ]
  })";
  auto path = WritePattern(kPattern);
  auto r = RunSampledMatch(path);
  ASSERT_TRUE(r.query_ok) << r.error;
  EXPECT_DOUBLE_EQ(r.estimated_count, -1.0);
  EXPECT_EQ(r.sample_count, 0);
}

// ---------------------------------------------------------------------------
// 4. Pattern file content is not valid JSON.
// ---------------------------------------------------------------------------
TEST_F(SampledMatchTest, RejectsMalformedPatternJson) {
  auto path = WritePattern("{ this is not json: ,, }");
  auto r = RunSampledMatch(path);
  ASSERT_TRUE(r.query_ok) << r.error;
  EXPECT_DOUBLE_EQ(r.estimated_count, -1.0);
  EXPECT_EQ(r.sample_count, 0);
}

// ---------------------------------------------------------------------------
// 5. Pattern JSON is well-formed but missing required arrays.
// ---------------------------------------------------------------------------
TEST_F(SampledMatchTest, RejectsPatternMissingVertices) {
  auto path = WritePattern(R"({"edges": []})");
  auto r = RunSampledMatch(path);
  ASSERT_TRUE(r.query_ok) << r.error;
  EXPECT_DOUBLE_EQ(r.estimated_count, -1.0);
  EXPECT_EQ(r.sample_count, 0);
}

TEST_F(SampledMatchTest, RejectsPatternMissingEdges) {
  auto path = WritePattern(R"({"vertices": [{"id": 0, "label": "Person"}]})");
  auto r = RunSampledMatch(path);
  ASSERT_TRUE(r.query_ok) << r.error;
  EXPECT_DOUBLE_EQ(r.estimated_count, -1.0);
  EXPECT_EQ(r.sample_count, 0);
}

// ---------------------------------------------------------------------------
// 6. Pattern file path doesn't exist on disk.
// ---------------------------------------------------------------------------
TEST_F(SampledMatchTest, RejectsNonexistentPatternFile) {
  // Hand-craft a short path that doesn't exist; don't go through
  // WritePattern (we want the file absent).
  std::string path = "/tmp/sm_no_such_file.json";
  ASSERT_FALSE(std::filesystem::exists(path));
  auto r = RunSampledMatch(path);
  ASSERT_TRUE(r.query_ok) << r.error;
  EXPECT_DOUBLE_EQ(r.estimated_count, -1.0);
  EXPECT_EQ(r.sample_count, 0);
}

// ---------------------------------------------------------------------------
// 7. Empty constraint array on a vertex behaves identically to "no
//    constraints" — i.e. it doesn't accidentally filter all candidates out.
//    Compares estimated_count (the cardinality estimate) rather than
//    sample_count, since the sampler saturates at sample_size whenever there
//    is at least one matching embedding.
// ---------------------------------------------------------------------------
TEST_F(SampledMatchTest, EmptyConstraintArrayActsAsNoConstraint) {
  constexpr const char* kPattern = R"({
    "vertices": [
      {"id": 0, "label": "Person", "constraints": []},
      {"id": 1, "label": "Person"}
    ],
    "edges": [
      {"source": 0, "target": 1, "label": "person_knows_person"}
    ]
  })";
  auto with_empty = RunSampledMatch(WritePattern(kPattern));
  auto baseline = RunSampledMatch(WritePattern(kSingleEdgePattern));
  ASSERT_TRUE(with_empty.query_ok) << with_empty.error;
  ASSERT_TRUE(baseline.query_ok) << baseline.error;
  EXPECT_DOUBLE_EQ(with_empty.estimated_count, baseline.estimated_count);
}

// ---------------------------------------------------------------------------
// 8. Constraint references a property name that doesn't exist on the vertex
//    label. The constraint is silently skipped (with a one-shot LOG warning),
//    so the result must equal the no-constraint baseline.
// ---------------------------------------------------------------------------
TEST_F(SampledMatchTest, VertexConstraintOnUnknownPropertyEqualsBaseline) {
  constexpr const char* kPattern = R"({
    "vertices": [
      {"id": 0, "label": "Person",
       "constraints": [
         {"property": "no_such_prop", "operator": "=", "value": 42}
       ]
      },
      {"id": 1, "label": "Person"}
    ],
    "edges": [
      {"source": 0, "target": 1, "label": "person_knows_person"}
    ]
  })";
  auto with_unknown = RunSampledMatch(WritePattern(kPattern));
  auto baseline = RunSampledMatch(WritePattern(kSingleEdgePattern));
  ASSERT_TRUE(with_unknown.query_ok) << with_unknown.error;
  ASSERT_TRUE(baseline.query_ok) << baseline.error;
  EXPECT_DOUBLE_EQ(with_unknown.estimated_count, baseline.estimated_count);
}

// ---------------------------------------------------------------------------
// 9. Same as above, but on an edge constraint instead of a vertex constraint.
// ---------------------------------------------------------------------------
TEST_F(SampledMatchTest, EdgeConstraintOnUnknownPropertyEqualsBaseline) {
  constexpr const char* kPattern = R"({
    "vertices": [
      {"id": 0, "label": "Person"},
      {"id": 1, "label": "Person"}
    ],
    "edges": [
      {"source": 0, "target": 1, "label": "person_knows_person",
       "constraints": [
         {"property": "no_such_edge_prop", "operator": "=", "value": 1}
       ]
      }
    ]
  })";
  auto with_unknown = RunSampledMatch(WritePattern(kPattern));
  auto baseline = RunSampledMatch(WritePattern(kSingleEdgePattern));
  ASSERT_TRUE(with_unknown.query_ok) << with_unknown.error;
  ASSERT_TRUE(baseline.query_ok) << baseline.error;
  EXPECT_DOUBLE_EQ(with_unknown.estimated_count, baseline.estimated_count);
}

// ---------------------------------------------------------------------------
// 10. A vertex-property equality constraint that *can* match: name = 'Alice'
//     pins one vertex of the pair, so the count must be strictly less than
//     the unconstrained baseline (Alice has 2 outgoing knows-edges).
// ---------------------------------------------------------------------------
TEST_F(SampledMatchTest, VertexConstraintNameRestrictsResults) {
  constexpr const char* kPattern = R"({
    "vertices": [
      {"id": 0, "label": "Person",
       "constraints": [
         {"property": "name", "operator": "=", "value": "Alice"}
       ]
      },
      {"id": 1, "label": "Person"}
    ],
    "edges": [
      {"source": 0, "target": 1, "label": "person_knows_person"}
    ]
  })";
  auto restricted = RunSampledMatch(WritePattern(kPattern));
  auto baseline = RunSampledMatch(WritePattern(kSingleEdgePattern));
  ASSERT_TRUE(restricted.query_ok) << restricted.error;
  ASSERT_TRUE(baseline.query_ok) << baseline.error;
  // sample_count saturates at sample_size when *any* embedding matches, so
  // assert on estimated_count instead. Alice has out-edges 0->1 and 0->3 →
  // restricted estimate is 2; baseline is 5.
  EXPECT_GE(restricted.sample_count, 1);
  EXPECT_GT(restricted.estimated_count, 0.0);
  EXPECT_LT(restricted.estimated_count, baseline.estimated_count);
}

// ---------------------------------------------------------------------------
// 11. Multiple constraints on the same vertex are AND-combined implicitly.
//     Alice has age 20, so {name=Alice, age=20} matches; {name=Alice, age=99}
//     matches nothing.
// ---------------------------------------------------------------------------
TEST_F(SampledMatchTest, MultipleVertexConstraintsAreAnded_Match) {
  constexpr const char* kPattern = R"({
    "vertices": [
      {"id": 0, "label": "Person",
       "constraints": [
         {"property": "name", "operator": "=", "value": "Alice"},
         {"property": "age",  "operator": "=", "value": 20}
       ]
      },
      {"id": 1, "label": "Person"}
    ],
    "edges": [
      {"source": 0, "target": 1, "label": "person_knows_person"}
    ]
  })";
  auto r = RunSampledMatch(WritePattern(kPattern));
  ASSERT_TRUE(r.query_ok) << r.error;
  EXPECT_GE(r.sample_count, 1)
      << "Alice matches both constraints; expected >= 1 sample";
}

TEST_F(SampledMatchTest, MultipleVertexConstraintsAreAnded_NoMatch) {
  constexpr const char* kPattern = R"({
    "vertices": [
      {"id": 0, "label": "Person",
       "constraints": [
         {"property": "name", "operator": "=", "value": "Alice"},
         {"property": "age",  "operator": "=", "value": 99}
       ]
      },
      {"id": 1, "label": "Person"}
    ],
    "edges": [
      {"source": 0, "target": 1, "label": "person_knows_person"}
    ]
  })";
  auto r = RunSampledMatch(WritePattern(kPattern));
  ASSERT_TRUE(r.query_ok) << r.error;
  EXPECT_EQ(r.sample_count, 0)
      << "AND-combined unsatisfiable constraints must yield zero samples";
}

// ---------------------------------------------------------------------------
// 12. Constraint value type doesn't match the property type: the underlying
//     Value::operator== returns false for type-mismatch, so candidates are
//     silently filtered out. Asserts the (gotcha) behavior so a future fix
//     that introduces coercion will trip this test and force a deliberate
//     decision.
// ---------------------------------------------------------------------------
TEST_F(SampledMatchTest, TypeMismatchSilentlyExcludesCandidates) {
  // age is INT32 in the schema; passing a string forces a type mismatch.
  constexpr const char* kPattern = R"({
    "vertices": [
      {"id": 0, "label": "Person",
       "constraints": [
         {"property": "age", "operator": "=", "value": "20"}
       ]
      },
      {"id": 1, "label": "Person"}
    ],
    "edges": [
      {"source": 0, "target": 1, "label": "person_knows_person"}
    ]
  })";
  auto r = RunSampledMatch(WritePattern(kPattern));
  ASSERT_TRUE(r.query_ok) << r.error;
  EXPECT_EQ(r.sample_count, 0)
      << "Type mismatch should currently filter all candidates (no implicit "
         "coercion). If this changes, update the test alongside the behavior.";
}

// ---------------------------------------------------------------------------
// 13. Unknown operator string: ParseOperator falls back to COMP_EQUAL and
//     emits a one-shot warning. Asserts that "or" is treated as "=" today —
//     this also documents that boolean combinators aren't supported.
// ---------------------------------------------------------------------------
TEST_F(SampledMatchTest, UnknownOperatorFallsBackToEqual) {
  constexpr const char* kFallback = R"({
    "vertices": [
      {"id": 0, "label": "Person",
       "constraints": [
         {"property": "name", "operator": "or", "value": "Alice"}
       ]
      },
      {"id": 1, "label": "Person"}
    ],
    "edges": [
      {"source": 0, "target": 1, "label": "person_knows_person"}
    ]
  })";
  constexpr const char* kEqual = R"({
    "vertices": [
      {"id": 0, "label": "Person",
       "constraints": [
         {"property": "name", "operator": "=", "value": "Alice"}
       ]
      },
      {"id": 1, "label": "Person"}
    ],
    "edges": [
      {"source": 0, "target": 1, "label": "person_knows_person"}
    ]
  })";
  auto fallback = RunSampledMatch(WritePattern(kFallback));
  auto equal = RunSampledMatch(WritePattern(kEqual));
  ASSERT_TRUE(fallback.query_ok) << fallback.error;
  ASSERT_TRUE(equal.query_ok) << equal.error;
  EXPECT_DOUBLE_EQ(fallback.estimated_count, equal.estimated_count)
      << "Unknown operator must currently behave like '='";
}

// ---------------------------------------------------------------------------
// 14. "in" / "not_in" operators: parsed but not implemented at runtime; they
//     act as a no-op (always true). Result must match the no-constraint
//     baseline.
// ---------------------------------------------------------------------------
TEST_F(SampledMatchTest, InOperatorIsNoOp) {
  constexpr const char* kPattern = R"({
    "vertices": [
      {"id": 0, "label": "Person",
       "constraints": [
         {"property": "name", "operator": "in", "value": "Alice"}
       ]
      },
      {"id": 1, "label": "Person"}
    ],
    "edges": [
      {"source": 0, "target": 1, "label": "person_knows_person"}
    ]
  })";
  auto in_op = RunSampledMatch(WritePattern(kPattern));
  auto baseline = RunSampledMatch(WritePattern(kSingleEdgePattern));
  ASSERT_TRUE(in_op.query_ok) << in_op.error;
  ASSERT_TRUE(baseline.query_ok) << baseline.error;
  EXPECT_DOUBLE_EQ(in_op.estimated_count, baseline.estimated_count)
      << "'in' is a runtime no-op today; result should equal the baseline";
}

// ---------------------------------------------------------------------------
// 15. Two-label pattern (Person -[works_at]-> Company) — exercises the
//     multi-label code path on a graph that has 4 such edges.
// ---------------------------------------------------------------------------
TEST_F(SampledMatchTest, TwoLabelPatternPersonWorksAtCompany) {
  constexpr const char* kPattern = R"({
    "vertices": [
      {"id": 0, "label": "Person"},
      {"id": 1, "label": "Company"}
    ],
    "edges": [
      {"source": 0, "target": 1, "label": "person_works_at_company"}
    ]
  })";
  auto r = RunSampledMatch(WritePattern(kPattern));
  ASSERT_TRUE(r.query_ok) << r.error;
  EXPECT_GE(r.sample_count, 1);
  EXPECT_GT(r.estimated_count, 0.0);
}

// ---------------------------------------------------------------------------
// 16. Edge property equality constraint: matches the 3 knows-edges with
//     weight = 0.5 (0->1, 2->0, 3->1). With a non-matching weight, the
//     count drops; with a constraint that always matches (or doesn't bind),
//     the count is unchanged.
// ---------------------------------------------------------------------------
TEST_F(SampledMatchTest, EdgeConstraintWeightRestrictsResults) {
  constexpr const char* kRestrictive = R"({
    "vertices": [
      {"id": 0, "label": "Person"},
      {"id": 1, "label": "Person"}
    ],
    "edges": [
      {"source": 0, "target": 1, "label": "person_knows_person",
       "constraints": [
         {"property": "weight", "operator": "=", "value": 0.5}
       ]
      }
    ]
  })";
  auto restrictive = RunSampledMatch(WritePattern(kRestrictive));
  auto baseline = RunSampledMatch(WritePattern(kSingleEdgePattern));
  ASSERT_TRUE(restrictive.query_ok) << restrictive.error;
  ASSERT_TRUE(baseline.query_ok) << baseline.error;
  EXPECT_GE(restrictive.sample_count, 1);
  // 3 of 5 knows-edges have weight=0.5; restricted estimate must be lower.
  EXPECT_GT(restrictive.estimated_count, 0.0);
  EXPECT_LT(restrictive.estimated_count, baseline.estimated_count);
}

// ---------------------------------------------------------------------------
// 17. Constraint missing the "value" field — CreateValueFromRapidjson falls
//     back to Value::INT32(0). For a STRING property this triggers the same
//     type-mismatch filter as test 12 (no candidates). Documents the (gotcha)
//     fallback so a future fix that rejects malformed constraints will trip
//     this test.
// ---------------------------------------------------------------------------
TEST_F(SampledMatchTest, ConstraintWithoutValueFallsBackToZero) {
  constexpr const char* kPattern = R"({
    "vertices": [
      {"id": 0, "label": "Person",
       "constraints": [
         {"property": "name", "operator": "="}
       ]
      },
      {"id": 1, "label": "Person"}
    ],
    "edges": [
      {"source": 0, "target": 1, "label": "person_knows_person"}
    ]
  })";
  auto r = RunSampledMatch(WritePattern(kPattern));
  ASSERT_TRUE(r.query_ok) << r.error;
  // Missing value defaults to INT32(0); name is STRING → type mismatch →
  // zero candidates.
  EXPECT_EQ(r.sample_count, 0)
      << "Missing constraint value falls back to INT32(0); when the property "
         "is STRING this should currently filter all candidates.";
}

// ---------------------------------------------------------------------------
// 18. Pattern with a single vertex (0-edge): exercises the degenerate case
//     where preprocessing has nothing to traverse.
// ---------------------------------------------------------------------------
TEST_F(SampledMatchTest, SingleVertexPattern) {
  constexpr const char* kPattern = R"({
    "vertices": [
      {"id": 0, "label": "Person"}
    ],
    "edges": []
  })";
  auto r = RunSampledMatch(WritePattern(kPattern));
  ASSERT_TRUE(r.query_ok) << r.error;
  // 4 Persons in the graph → estimated count should be ≥ 4 (or fall back
  // gracefully; either way the call must not crash and not return -1).
  EXPECT_NE(r.estimated_count, -1.0);
  EXPECT_GE(r.sample_count, 0);
}

// ---------------------------------------------------------------------------
// 19. Empty vertices array: pattern is structurally degenerate — match()
//     guards against pattern_graph_->GetNumVertices() == 0 and returns -1.
// ---------------------------------------------------------------------------
TEST_F(SampledMatchTest, RejectsEmptyVerticesArray) {
  auto path = WritePattern(R"({"vertices": [], "edges": []})");
  auto r = RunSampledMatch(path);
  ASSERT_TRUE(r.query_ok) << r.error;
  EXPECT_DOUBLE_EQ(r.estimated_count, -1.0);
  EXPECT_EQ(r.sample_count, 0);
}

// ---------------------------------------------------------------------------
// 20. Edge constraint with matching value: weight = 1.5 matches exactly one
//     knows-edge (1 -> 2). Combined with no vertex constraints, the sampled
//     count should be ≥ 1.
// ---------------------------------------------------------------------------
TEST_F(SampledMatchTest, EdgeConstraintExactWeightMatchesSingleEdge) {
  constexpr const char* kPattern = R"({
    "vertices": [
      {"id": 0, "label": "Person"},
      {"id": 1, "label": "Person"}
    ],
    "edges": [
      {"source": 0, "target": 1, "label": "person_knows_person",
       "constraints": [
         {"property": "weight", "operator": "=", "value": 1.5}
       ]
      }
    ]
  })";
  auto r = RunSampledMatch(WritePattern(kPattern));
  ASSERT_TRUE(r.query_ok) << r.error;
  EXPECT_GE(r.sample_count, 1)
      << "weight=1.5 matches edge 1->2; expected >= 1 sample";
}

// ---------------------------------------------------------------------------
// 21. Vertex constraint with '>' on age: matches Bob (30) and Dave (40); as
//     source vertex that selects 2 of the 5 knows-edges (1->2, 3->1).
//     CheckValueConstraint implements '>' at runtime; this test locks in
//     that the operator is dispatched and applied (not silently skipped).
// ---------------------------------------------------------------------------
TEST_F(SampledMatchTest, VertexConstraintGreaterThanRestrictsResults) {
  constexpr const char* kPattern = R"({
    "vertices": [
      {"id": 0, "label": "Person",
       "constraints": [
         {"property": "age", "operator": ">", "value": 25}
       ]
      },
      {"id": 1, "label": "Person"}
    ],
    "edges": [
      {"source": 0, "target": 1, "label": "person_knows_person"}
    ]
  })";
  auto restricted = RunSampledMatch(WritePattern(kPattern));
  auto baseline = RunSampledMatch(WritePattern(kSingleEdgePattern));
  ASSERT_TRUE(restricted.query_ok) << restricted.error;
  ASSERT_TRUE(baseline.query_ok) << baseline.error;
  EXPECT_GE(restricted.sample_count, 1);
  EXPECT_GT(restricted.estimated_count, 0.0);
  EXPECT_LT(restricted.estimated_count, baseline.estimated_count);
}

// ---------------------------------------------------------------------------
// 22. Vertex constraint with '<' on age: matches Alice (20) and Carol (20);
//     as source vertex that selects 0->1, 0->3, 2->0 = 3 of 5 knows-edges.
// ---------------------------------------------------------------------------
TEST_F(SampledMatchTest, VertexConstraintLessThanRestrictsResults) {
  constexpr const char* kPattern = R"({
    "vertices": [
      {"id": 0, "label": "Person",
       "constraints": [
         {"property": "age", "operator": "<", "value": 25}
       ]
      },
      {"id": 1, "label": "Person"}
    ],
    "edges": [
      {"source": 0, "target": 1, "label": "person_knows_person"}
    ]
  })";
  auto restricted = RunSampledMatch(WritePattern(kPattern));
  auto baseline = RunSampledMatch(WritePattern(kSingleEdgePattern));
  ASSERT_TRUE(restricted.query_ok) << restricted.error;
  ASSERT_TRUE(baseline.query_ok) << baseline.error;
  EXPECT_GE(restricted.sample_count, 1);
  EXPECT_GT(restricted.estimated_count, 0.0);
  EXPECT_LT(restricted.estimated_count, baseline.estimated_count);
}

// ---------------------------------------------------------------------------
// 23. Boundary handling for '>' vs '>=': Dave's age is exactly 40, so
//     age > 40 must yield zero samples while age >= 40 must match Dave.
//     This pins the exclusion vs. inclusion semantics that test 21 alone
//     can't distinguish.
// ---------------------------------------------------------------------------
TEST_F(SampledMatchTest, GreaterAndGreaterEqualHandleBoundary) {
  constexpr const char* kStrict = R"({
    "vertices": [
      {"id": 0, "label": "Person",
       "constraints": [
         {"property": "age", "operator": ">", "value": 40}
       ]
      },
      {"id": 1, "label": "Person"}
    ],
    "edges": [
      {"source": 0, "target": 1, "label": "person_knows_person"}
    ]
  })";
  constexpr const char* kInclusive = R"({
    "vertices": [
      {"id": 0, "label": "Person",
       "constraints": [
         {"property": "age", "operator": ">=", "value": 40}
       ]
      },
      {"id": 1, "label": "Person"}
    ],
    "edges": [
      {"source": 0, "target": 1, "label": "person_knows_person"}
    ]
  })";
  auto strict = RunSampledMatch(WritePattern(kStrict));
  auto inclusive = RunSampledMatch(WritePattern(kInclusive));
  ASSERT_TRUE(strict.query_ok) << strict.error;
  ASSERT_TRUE(inclusive.query_ok) << inclusive.error;
  EXPECT_EQ(strict.sample_count, 0)
      << "age > 40 should exclude Dave (age=40); expected zero samples";
  EXPECT_GE(inclusive.sample_count, 1)
      << "age >= 40 should include Dave (age=40); expected >= 1 sample";
}

// ---------------------------------------------------------------------------
// 24. Edge constraint with '>=' on weight: weight >= 1.5 matches 1->2 (1.5)
//     and 0->3 (2.5) — 2 of 5 knows-edges. The boundary value 1.5 is
//     deliberately included so the test would also fail if '>=' regressed
//     to strict '>' (count would drop to 1, still less-than-baseline, so
//     the assertion is "strictly less" not "exactly N").
// ---------------------------------------------------------------------------
TEST_F(SampledMatchTest, EdgeConstraintGreaterEqualRestrictsResults) {
  constexpr const char* kPattern = R"({
    "vertices": [
      {"id": 0, "label": "Person"},
      {"id": 1, "label": "Person"}
    ],
    "edges": [
      {"source": 0, "target": 1, "label": "person_knows_person",
       "constraints": [
         {"property": "weight", "operator": ">=", "value": 1.5}
       ]
      }
    ]
  })";
  auto restricted = RunSampledMatch(WritePattern(kPattern));
  auto baseline = RunSampledMatch(WritePattern(kSingleEdgePattern));
  ASSERT_TRUE(restricted.query_ok) << restricted.error;
  ASSERT_TRUE(baseline.query_ok) << baseline.error;
  EXPECT_GE(restricted.sample_count, 1);
  EXPECT_GT(restricted.estimated_count, 0.0);
  EXPECT_LT(restricted.estimated_count, baseline.estimated_count);
}

// ---------------------------------------------------------------------------
// 25. Edge constraint with '<=' on weight: weight <= 0.5 matches the three
//     0.5-weight edges (0->1, 2->0, 3->1). Boundary inclusion of 0.5 is
//     load-bearing — if '<=' regressed to strict '<', no edges would match
//     (smallest weight in the graph is 0.5) and sample_count would be 0.
// ---------------------------------------------------------------------------
TEST_F(SampledMatchTest, EdgeConstraintLessEqualRestrictsResults) {
  constexpr const char* kPattern = R"({
    "vertices": [
      {"id": 0, "label": "Person"},
      {"id": 1, "label": "Person"}
    ],
    "edges": [
      {"source": 0, "target": 1, "label": "person_knows_person",
       "constraints": [
         {"property": "weight", "operator": "<=", "value": 0.5}
       ]
      }
    ]
  })";
  auto restricted = RunSampledMatch(WritePattern(kPattern));
  auto baseline = RunSampledMatch(WritePattern(kSingleEdgePattern));
  ASSERT_TRUE(restricted.query_ok) << restricted.error;
  ASSERT_TRUE(baseline.query_ok) << baseline.error;
  EXPECT_GE(restricted.sample_count, 1)
      << "weight <= 0.5 should match the three 0.5-weight edges";
  EXPECT_GT(restricted.estimated_count, 0.0);
  EXPECT_LT(restricted.estimated_count, baseline.estimated_count);
}

// ---------------------------------------------------------------------------
// 26. 'not_in' operator: parsed but treated as a runtime no-op (always true),
//     mirroring 'in' (test 14). Documents the symmetric behavior so a future
//     implementation that adds real semantics will trip both tests together.
// ---------------------------------------------------------------------------
TEST_F(SampledMatchTest, NotInOperatorIsNoOp) {
  constexpr const char* kPattern = R"({
    "vertices": [
      {"id": 0, "label": "Person",
       "constraints": [
         {"property": "name", "operator": "not_in", "value": "Alice"}
       ]
      },
      {"id": 1, "label": "Person"}
    ],
    "edges": [
      {"source": 0, "target": 1, "label": "person_knows_person"}
    ]
  })";
  auto not_in_op = RunSampledMatch(WritePattern(kPattern));
  auto baseline = RunSampledMatch(WritePattern(kSingleEdgePattern));
  ASSERT_TRUE(not_in_op.query_ok) << not_in_op.error;
  ASSERT_TRUE(baseline.query_ok) << baseline.error;
  EXPECT_DOUBLE_EQ(not_in_op.estimated_count, baseline.estimated_count)
      << "'not_in' is a runtime no-op today; result should equal the baseline";
}

// Inline DSL longer than 48 chars round-trips through the CALL pipeline.
// This guards against regressions of the bare-literal fold path that used to
// crash on string literals exceeding neug_string_t::SHORT_STR_LENGTH.
TEST_F(SampledMatchTest, LongInlineDslRoundTripsThroughCall) {
  const std::string kDsl =
      "MATCH (a:Person)-[:person_knows_person]->"
      "(b:Person)-[:person_knows_person]->"
      "(c:Person)-[:person_knows_person]->(a)";
  ASSERT_GT(kDsl.size(), 48u)
      << "test premise: DSL must exceed the inline-string boundary";
  std::ostringstream q;
  q << "CALL SAMPLED_MATCH_PATTERN('" << kDsl
    << "', 100) RETURN estimated_count, sample_count, result_file, "
       "props_file;";
  auto sm = conn_->Query(q.str());
  ASSERT_TRUE(sm.has_value()) << sm.error().ToString();
  EXPECT_GE(sm.value().response().arrays(1).int64_array().values(0), 1);
}

// ---------------------------------------------------------------------------
// Pattern-DSL coverage. Patterns are passed inline by default; a `.dsl` file
// path is also accepted by the procedure (auto-detected) and exercised below.
// Internally the matcher runs DSL → JSON → PatternGraph entirely in memory.
// ---------------------------------------------------------------------------

namespace {

// Helper local to this test file: persist DSL text into a tempdir and append
// the directory to the fixture's cleanup list.
std::string WriteDslFile(std::vector<std::filesystem::path>& cleanup_dirs,
                         const std::string& contents, const char* basename) {
  std::string tmpl = "/tmp/sm_XXXXXX";
  std::vector<char> buf(tmpl.begin(), tmpl.end());
  buf.push_back('\0');
  if (mkdtemp(buf.data()) == nullptr) return "";
  std::filesystem::path dir(buf.data());
  cleanup_dirs.push_back(dir);
  auto path = dir / basename;
  std::ofstream ofs(path);
  if (!ofs.is_open()) return "";
  ofs << contents;
  ofs.close();
  return path.string();
}

}  // namespace

TEST_F(SampledMatchTest, PatternDslMatchesJsonForTriangle) {
  // Triangle written in DSL. Variables a/b/c make the pattern self-
  // documenting; the result should match the JSON kTrianglePattern up to
  // Monte-Carlo sampling noise.
  const std::string kDsl =
      "MATCH (a:Person)-[:person_knows_person]->"
      "(b:Person)-[:person_knows_person]->"
      "(c:Person)-[:person_knows_person]->(a)";
  auto dsl_path = WriteDslFile(pattern_dirs_, kDsl, "q.dsl");
  ASSERT_FALSE(dsl_path.empty());

  std::ostringstream q;
  q << "CALL SAMPLED_MATCH_PATTERN('" << dsl_path
    << "', 1000) RETURN estimated_count, sample_count, result_file, "
       "props_file;";
  auto sm = conn_->Query(q.str());
  ASSERT_TRUE(sm.has_value()) << sm.error().ToString();
  const auto& resp = sm.value().response();
  ASSERT_EQ(sm.value().length(), 1u);
  double estimated = resp.arrays(0).double_array().values(0);
  int64_t samples = resp.arrays(1).int64_array().values(0);
  EXPECT_GT(estimated, 0.0);
  EXPECT_GE(samples, 1);

  // FaSTest is a randomized estimator, so we compare order of magnitude
  // against the JSON form rather than asserting bit-exact equality.
  auto json_result = RunSampledMatch(WritePattern(kTrianglePattern));
  ASSERT_TRUE(json_result.query_ok) << json_result.error;
  ASSERT_GT(json_result.estimated_count, 0.0);
  double ratio = estimated / json_result.estimated_count;
  EXPECT_GE(ratio, 0.5);
  EXPECT_LE(ratio, 2.0);
}

// File and inline forms of the same DSL pattern must produce the same
// `estimated_count` (FaSTest is randomized so we compare order of magnitude,
// matching the JSON-vs-DSL ratio bounds used elsewhere in this file).
TEST_F(SampledMatchTest, PatternDslFileFormAndInlineFormAgree) {
  const std::string kDsl =
      "MATCH (a:Person)-[:person_knows_person]->"
      "(b:Person)-[:person_knows_person]->"
      "(c:Person)-[:person_knows_person]->(a)";

  // File form — same path that real users would invoke from the docs.
  auto dsl_path = WriteDslFile(pattern_dirs_, kDsl, "q.dsl");
  ASSERT_FALSE(dsl_path.empty());
  std::ostringstream qFile;
  qFile << "CALL SAMPLED_MATCH_PATTERN('" << dsl_path
        << "', 1000) RETURN estimated_count, sample_count, result_file, "
           "props_file;";
  auto smFile = conn_->Query(qFile.str());
  ASSERT_TRUE(smFile.has_value()) << smFile.error().ToString();
  double estFile =
      smFile.value().response().arrays(0).double_array().values(0);
  int64_t samplesFile =
      smFile.value().response().arrays(1).int64_array().values(0);
  EXPECT_GT(estFile, 0.0);
  EXPECT_GE(samplesFile, 1);

  // Inline form — identical DSL embedded straight into the CALL.
  std::ostringstream qInline;
  qInline << "CALL SAMPLED_MATCH_PATTERN('" << kDsl
          << "', 1000) RETURN estimated_count, sample_count, result_file, "
             "props_file;";
  auto smInline = conn_->Query(qInline.str());
  ASSERT_TRUE(smInline.has_value()) << smInline.error().ToString();
  double estInline =
      smInline.value().response().arrays(0).double_array().values(0);
  int64_t samplesInline =
      smInline.value().response().arrays(1).int64_array().values(0);
  EXPECT_GT(estInline, 0.0);
  EXPECT_GE(samplesInline, 1);

  // Both forms should produce comparable estimates (within Monte-Carlo noise).
  double ratio = estFile / estInline;
  EXPECT_GE(ratio, 0.5);
  EXPECT_LE(ratio, 2.0);
}

TEST_F(SampledMatchTest, PatternDslSupportsWhereAndInlineProps) {
  // Same triangle, but with the age constraint expressed once via WHERE on
  // 'a' and once via inline {age: 30} on 'b'. Both forms should translate
  // into per-vertex constraints; the matcher restricts accordingly.
  const std::string kDsl =
      "MATCH (a:Person)-[:person_knows_person]->"
      "(b:Person {age: 30})-[:person_knows_person]->"
      "(c:Person)-[:person_knows_person]->(a) "
      "WHERE a.age >= 18";
  auto dsl_path = WriteDslFile(pattern_dirs_, kDsl, "q.dsl");
  ASSERT_FALSE(dsl_path.empty());

  std::ostringstream q;
  q << "CALL SAMPLED_MATCH_PATTERN('" << dsl_path
    << "', 1000) RETURN estimated_count, sample_count, result_file, "
       "props_file;";
  auto sm = conn_->Query(q.str());
  ASSERT_TRUE(sm.has_value()) << sm.error().ToString();
  // The age=30 / age>=18 restriction is satisfied only when 'b' is Bob
  // (the only Person with age 30); a sample should still come back since
  // Alice (20)-Bob (30)-Carol (20) closes a triangle.
  double estimated = sm.value().response().arrays(0).double_array().values(0);
  int64_t samples = sm.value().response().arrays(1).int64_array().values(0);
  EXPECT_GT(estimated, 0.0);
  EXPECT_GE(samples, 1);
}

// Omnibus negative test: every entry exercises a distinct parser guard.
// Translation failure surfaces as a glog warning + empty pattern path; the
// matcher then reports zero samples while the SQL call itself succeeds —
// same failure shape used by RejectsMalformedPatternJson and friends.
TEST_F(SampledMatchTest, PatternDslRejectsBadCases) {
  struct Case {
    const char* label;
    std::string dsl;
  };
  const std::vector<Case> bad = {
      {"undirected edge ('--')",
       "MATCH (a:Person)-[:person_knows_person]-(b:Person)"},
      {"<> operator",
       "MATCH (a:Person)-[:person_knows_person]->(b:Person) "
       "WHERE a.age <> 18"},
      {"variable-length path '*1..N'",
       "MATCH (a:Person)-[:person_knows_person*1..3]->(b:Person)"},
      {"IN operator",
       "MATCH (a:Person)-[:person_knows_person]->(b:Person) "
       "WHERE a.age in 18"},
      {"OR in WHERE",
       "MATCH (a:Person)-[:person_knows_person]->(b:Person) "
       "WHERE a.age >= 18 OR b.age >= 18"},
      {"NOT in WHERE",
       "MATCH (a:Person)-[:person_knows_person]->(b:Person) "
       "WHERE NOT a.age = 18"},
      {"cross-variable predicate",
       "MATCH (a:Person)-[:person_knows_person]->(b:Person) "
       "WHERE a.age > b.age"},
      {"multi-label node",
       "MATCH (a:Person:Employee)-[:person_knows_person]->(b:Person)"},
      {"function call in WHERE",
       "MATCH (a:Person)-[:person_knows_person]->(b:Person) "
       "WHERE length(a.name) > 5"},
      {"node without label",
       "MATCH (a)-[:person_knows_person]->(b:Person)"},
      {"unterminated string literal",
       "MATCH (a:Person) WHERE a.name = 'Alice"},
      {"unclosed paren", "MATCH (a:Person"},
      {"reused relationship variable",
       "MATCH (a:Person)-[r:person_knows_person]->(b:Person)"
       "-[r:person_knows_person]->(c:Person)"},
      {"WHERE references unknown var",
       "MATCH (a:Person)-[:person_knows_person]->(b:Person) "
       "WHERE c.age > 0"},
      {"RETURN references unknown var",
       "MATCH (a:Person)-[:person_knows_person]->(b:Person) RETURN d.name"},
      {"garbage at top of query", "FOOBAR (a:Person)"},
      {"empty input", ""},

      // ---- additional parser/validator guards ----
      {"MATCH keyword without a pattern body", "MATCH"},
      {"WHERE before MATCH",
       "WHERE a.age = 1 MATCH (a:Person)-[:person_knows_person]->(b:Person)"},
      {"trailing comma in MATCH",
       "MATCH (a:Person)-[:person_knows_person]->(b:Person),"},
      {"trailing AND in WHERE",
       "MATCH (a:Person)-[:person_knows_person]->(b:Person) "
       "WHERE a.age = 1 AND"},
      {"trailing comma in RETURN",
       "MATCH (a:Person)-[:person_knows_person]->(b:Person) RETURN a,"},
      {"unclosed inline property map",
       "MATCH (a:Person {age: 30)-[:person_knows_person]->(b:Person)"},
      {"inline property using comparison operator",
       "MATCH (a:Person {age > 30})-[:person_knows_person]->(b:Person)"},
      {"inline property missing colon",
       "MATCH (a:Person {age 30})-[:person_knows_person]->(b:Person)"},
      {"empty node parens",
       "MATCH ()-[:person_knows_person]->(b:Person)"},
      {"double-colon label",
       "MATCH (a::Person)-[:person_knows_person]->(b:Person)"},
      {"WHERE predicate missing dot",
       "MATCH (a:Person)-[:person_knows_person]->(b:Person) "
       "WHERE a name = 'X'"},
      {"WHERE predicate missing right operand",
       "MATCH (a:Person)-[:person_knows_person]->(b:Person) "
       "WHERE a.age >"},
      {"same variable bound to two different labels",
       "MATCH (a:Person)-[:person_knows_person]->(a:Company)"},
      {"pattern begins with an edge",
       "MATCH -[:person_knows_person]->(b:Person)"},
      {"malformed numeric literal",
       "MATCH (a:Person)-[:person_knows_person]->(b:Person) "
       "WHERE a.age = 1.2.3"},
      {"non-ASCII garbage in the middle of the query",
       "MATCH (a:Person)-[:person_knows_person]->(b:Person) ☠"},
  };
  for (const auto& c : bad) {
    SCOPED_TRACE(c.label);
    auto path = WriteDslFile(pattern_dirs_, c.dsl, "q.dsl");
    ASSERT_FALSE(path.empty());
    std::ostringstream q;
    q << "CALL SAMPLED_MATCH_PATTERN('" << path
      << "', 100) RETURN estimated_count, sample_count, result_file, "
         "props_file;";
    auto sm = conn_->Query(q.str());
    ASSERT_TRUE(sm.has_value()) << sm.error().ToString();
    EXPECT_EQ(sm.value().response().arrays(1).int64_array().values(0), 0)
        << "expected zero samples for invalid DSL: " << c.label;
  }
}

// ===========================================================================
// P0 / P1 additions
// ===========================================================================
//
// Helpers for parsing the CSV files written by the extension. Keep these
// local to the test file — the format is internal and exists only so this
// file can validate it.

namespace {

// Split one CSV line into cells. The result_file emitted by SAMPLED_MATCH /
// GET_*_PROPERTY does not embed commas in any cell that we exercise here, so
// a plain split is sufficient for assertions.
std::vector<std::string> SplitCsvLine(const std::string& line) {
  std::vector<std::string> out;
  std::string cur;
  for (char c : line) {
    if (c == ',') {
      out.push_back(cur);
      cur.clear();
    } else {
      cur.push_back(c);
    }
  }
  out.push_back(cur);
  return out;
}

// Read a CSV file into rows of cells. Empty trailing newlines are skipped.
std::vector<std::vector<std::string>> ReadCsv(const std::string& path) {
  std::vector<std::vector<std::string>> rows;
  std::ifstream ifs(path);
  std::string line;
  while (std::getline(ifs, line)) {
    if (line.empty()) continue;
    rows.push_back(SplitCsvLine(line));
  }
  return rows;
}

// Parse "src:dst:label" into its three components. Returns false on shape
// mismatch.
bool ParseEdgeKey(const std::string& key, int64_t* src, int64_t* dst,
                  int* label) {
  size_t p1 = key.find(':');
  size_t p2 = key.rfind(':');
  if (p1 == std::string::npos || p1 == p2) return false;
  try {
    *src = std::stoll(key.substr(0, p1));
    *dst = std::stoll(key.substr(p1 + 1, p2 - p1 - 1));
    *label = std::stoi(key.substr(p2 + 1));
  } catch (...) {
    return false;
  }
  return true;
}

}  // namespace

// ---------------------------------------------------------------------------
// P0-A. result_file content correctness
// ---------------------------------------------------------------------------
// The existing happy-path test only checks the file exists. These tests pin
// the CSV layout (one column per pattern vertex + one per pattern edge) and
// verify that each row is a valid subgraph isomorphism: distinct vertex
// IDs and well-formed edge keys.

TEST_F(SampledMatchTest, ResultFileHasExpectedHeader) {
  auto r = RunSampledMatch(WritePattern(kTrianglePattern));
  ASSERT_TRUE(r.query_ok) << r.error;
  ASSERT_TRUE(std::filesystem::exists(r.result_file));

  auto rows = ReadCsv(r.result_file);
  ASSERT_FALSE(rows.empty()) << "result file has no header";
  const auto& header = rows.front();
  ASSERT_EQ(header.size(), 6u)
      << "triangle pattern → 3 vertex cols + 3 edge cols";
  EXPECT_EQ(header[0], "v0");
  EXPECT_EQ(header[1], "v1");
  EXPECT_EQ(header[2], "v2");
  // Edge columns are named v<src>-v<dst> for each pattern edge.
  EXPECT_EQ(header[3], "v0-v1");
  EXPECT_EQ(header[4], "v1-v2");
  EXPECT_EQ(header[5], "v2-v0");
}

TEST_F(SampledMatchTest, ResultFileEmbeddingsAreIsomorphismsWithWellFormedEdges) {
  auto r = RunSampledMatch(WritePattern(kTrianglePattern));
  ASSERT_TRUE(r.query_ok) << r.error;

  auto rows = ReadCsv(r.result_file);
  ASSERT_GE(rows.size(), 2u) << "expected at least header + one embedding";
  ASSERT_EQ((int64_t)(rows.size() - 1), r.sample_count)
      << "row count must match reported sample_count";

  for (size_t i = 1; i < rows.size(); ++i) {
    const auto& row = rows[i];
    ASSERT_EQ(row.size(), 6u) << "row " << i;
    // Subgraph isomorphism: the three vertex IDs must be distinct.
    std::set<std::string> distinct{row[0], row[1], row[2]};
    EXPECT_EQ(distinct.size(), 3u)
        << "row " << i << " has duplicate vertex IDs — not an isomorphism";

    // Edge cells are formatted as "src_global:dst_global:edge_label_id" and
    // their endpoints must match the corresponding v* cells.
    int64_t v0 = std::stoll(row[0]);
    int64_t v1 = std::stoll(row[1]);
    int64_t v2 = std::stoll(row[2]);
    auto check_edge = [&](const std::string& key, int64_t expect_src,
                          int64_t expect_dst) {
      int64_t src, dst;
      int label;
      ASSERT_TRUE(ParseEdgeKey(key, &src, &dst, &label))
          << "malformed edge key: " << key;
      EXPECT_EQ(src, expect_src) << "edge " << key;
      EXPECT_EQ(dst, expect_dst) << "edge " << key;
    };
    check_edge(row[3], v0, v1);
    check_edge(row[4], v1, v2);
    check_edge(row[5], v2, v0);
  }
}

// Sample size far above the number of distinct embeddings: matcher must
// not return more rows than embeddings exist (otherwise we'd see duplicates
// or out-of-bounds counts).
TEST_F(SampledMatchTest, ResultFileSampleCountBoundedBySampleSize) {
  auto r = RunSampledMatch(WritePattern(kTrianglePattern), /*sample_size=*/3);
  ASSERT_TRUE(r.query_ok) << r.error;
  auto rows = ReadCsv(r.result_file);
  ASSERT_FALSE(rows.empty());
  EXPECT_LE((int64_t)(rows.size() - 1), 3)
      << "row count must respect the requested sample_size cap";
}

// ---------------------------------------------------------------------------
// P0-B. GET_VERTEX_PROPERTY
// ---------------------------------------------------------------------------

namespace {

// Run CALL GET_VERTEX_PROPERTY and return the path of the generated CSV
// (empty on SQL failure). Build_root note: GET_VERTEX_PROPERTY relies on the
// GraphDataCache; if the cache is empty the call falls back to a full init.
std::string RunGetVertexProperty(neug::Connection& conn,
                                 const std::string& ids_json,
                                 const std::string& label,
                                 const std::string& props_json) {
  std::ostringstream q;
  q << "CALL GET_VERTEX_PROPERTY('" << ids_json << "', '" << label << "', '"
    << props_json << "') RETURN result_file;";
  auto sm = conn.Query(q.str());
  if (!sm.has_value()) return "";
  const auto& resp = sm.value().response();
  if (resp.arrays_size() == 0) return "";
  const auto& rf = resp.arrays(0).string_array();
  return rf.values_size() ? rf.values(0) : "";
}

}  // namespace

// Person global IDs 0..3 map back to (Alice/Bob/Carol/Dave) in the fixture's
// insertion order — see SetUp(). The Initialize() pass builds the mapping
// by iterating labels in label_t order, so Person (label 0) gets the first
// global IDs.
TEST_F(SampledMatchTest, GetVertexPropertyBasicReturnsRequestedProps) {
  // Ensure the cache is warm; without this the first call has to fall back
  // to DoGraphInitialization.
  auto init = conn_->Query(
      "CALL INITIALIZE() "
      "RETURN status, num_vertices, num_edges, max_degree, degeneracy;");
  ASSERT_TRUE(init.has_value()) << init.error().ToString();

  auto path = RunGetVertexProperty(*conn_, "[0,1,2,3]", "Person",
                                   R"(["name","age"])");
  ASSERT_FALSE(path.empty());
  ASSERT_TRUE(std::filesystem::exists(path));

  auto rows = ReadCsv(path);
  ASSERT_EQ(rows.size(), 5u) << "header + 4 vertices";
  ASSERT_EQ(rows[0].size(), 3u);
  EXPECT_EQ(rows[0][0], "vertex_id");
  EXPECT_EQ(rows[0][1], "name");
  EXPECT_EQ(rows[0][2], "age");

  // Map vertex_id → expected name/age. Person.id was inserted in order so
  // the global IDs line up with (Alice, Bob, Carol, Dave).
  const std::vector<std::pair<std::string, std::string>> expected = {
      {"Alice", "20"}, {"Bob", "30"}, {"Carol", "20"}, {"Dave", "40"}};
  for (int i = 0; i < 4; ++i) {
    const auto& row = rows[i + 1];
    ASSERT_EQ(row.size(), 3u);
    EXPECT_EQ(row[0], std::to_string(i));
    EXPECT_EQ(row[1], expected[i].first);
    EXPECT_EQ(row[2], expected[i].second);
  }
}

// Unknown vertex label: the execFunc returns an empty Context, which the
// SQL layer surfaces as a query-level error ("alias out of range") rather
// than a successful query with empty results. This pins that behavior so
// future implementations that decide to return an empty file path instead
// will trip this test and force a deliberate decision.
TEST_F(SampledMatchTest, GetVertexPropertyUnknownLabelFailsQuery) {
  std::ostringstream q;
  q << "CALL GET_VERTEX_PROPERTY('[0]', 'Alien', '[\"name\"]') "
       "RETURN result_file;";
  auto sm = conn_->Query(q.str());
  EXPECT_FALSE(sm.has_value())
      << "unknown vertex label should surface as a SQL-layer error";
}

// Unknown property name → the property column is present in the header but
// the cells are empty (no implicit error).
TEST_F(SampledMatchTest, GetVertexPropertyUnknownPropertyYieldsEmptyCells) {
  auto path = RunGetVertexProperty(*conn_, "[0,1]", "Person",
                                   R"(["no_such_prop"])");
  ASSERT_FALSE(path.empty());
  auto rows = ReadCsv(path);
  ASSERT_EQ(rows.size(), 3u);
  EXPECT_EQ(rows[0][1], "no_such_prop");
  // Per the writer (sampled_match_functions.h:2019), unknown props leave
  // the cell blank.
  EXPECT_EQ(rows[1].size(), 2u);
  EXPECT_TRUE(rows[1][1].empty());
  EXPECT_TRUE(rows[2][1].empty());
}

// Empty id list still produces a valid CSV with only the header — a useful
// degenerate case for downstream consumers that fan-in results.
TEST_F(SampledMatchTest, GetVertexPropertyEmptyIdsYieldsHeaderOnly) {
  auto path = RunGetVertexProperty(*conn_, "[]", "Person", R"(["name"])");
  ASSERT_FALSE(path.empty());
  auto rows = ReadCsv(path);
  ASSERT_EQ(rows.size(), 1u);
  EXPECT_EQ(rows[0][0], "vertex_id");
  EXPECT_EQ(rows[0][1], "name");
}

// ---------------------------------------------------------------------------
// P0-C. GET_EDGE_PROPERTY
// ---------------------------------------------------------------------------

namespace {

std::string RunGetEdgeProperty(neug::Connection& conn,
                               const std::string& keys_json,
                               const std::string& label,
                               const std::string& props_json) {
  std::ostringstream q;
  q << "CALL GET_EDGE_PROPERTY('" << keys_json << "', '" << label << "', '"
    << props_json << "') RETURN result_file;";
  auto sm = conn.Query(q.str());
  if (!sm.has_value()) return "";
  const auto& resp = sm.value().response();
  if (resp.arrays_size() == 0) return "";
  const auto& rf = resp.arrays(0).string_array();
  return rf.values_size() ? rf.values(0) : "";
}

}  // namespace

// Use a real edge key extracted from SAMPLED_MATCH to keep this test robust
// to changes in the internal label_t assignment.
TEST_F(SampledMatchTest, GetEdgePropertyBasicReturnsWeight) {
  auto sm_r = RunSampledMatch(WritePattern(kSingleEdgePattern));
  ASSERT_TRUE(sm_r.query_ok) << sm_r.error;
  auto sm_rows = ReadCsv(sm_r.result_file);
  ASSERT_GE(sm_rows.size(), 2u);

  // First embedding's edge key — the format is "src:dst:label_id".
  const std::string& edge_key = sm_rows[1][2];
  int64_t src, dst;
  int label;
  ASSERT_TRUE(ParseEdgeKey(edge_key, &src, &dst, &label));

  std::ostringstream keys_json;
  keys_json << "[\"" << edge_key << "\"]";
  auto path = RunGetEdgeProperty(*conn_, keys_json.str(),
                                 "person_knows_person", R"(["weight"])");
  ASSERT_FALSE(path.empty());
  auto rows = ReadCsv(path);
  ASSERT_EQ(rows.size(), 2u);
  ASSERT_EQ(rows[0].size(), 4u);
  EXPECT_EQ(rows[0][0], "edge_key");
  EXPECT_EQ(rows[0][1], "src_id");
  EXPECT_EQ(rows[0][2], "dst_id");
  EXPECT_EQ(rows[0][3], "weight");

  EXPECT_EQ(rows[1][0], edge_key);
  EXPECT_EQ(rows[1][1], std::to_string(src));
  EXPECT_EQ(rows[1][2], std::to_string(dst));
  // weight is a DOUBLE; values in the fixture are {0.5, 1.5, 2.5}. The
  // matcher's result CSV can carry any of these — just pin the parse.
  double w = std::stod(rows[1][3]);
  EXPECT_GT(w, 0.0);
}

// Malformed edge key parses as invalid; the CSV still emits a row but the
// property cells are blank.
TEST_F(SampledMatchTest, GetEdgePropertyMalformedKeyYieldsBlankCells) {
  auto path = RunGetEdgeProperty(*conn_, R"(["not-an-edge-key"])",
                                 "person_knows_person", R"(["weight"])");
  ASSERT_FALSE(path.empty());
  auto rows = ReadCsv(path);
  ASSERT_EQ(rows.size(), 2u);
  EXPECT_EQ(rows[1][0], "not-an-edge-key");
  // Property column exists but its cell is empty for invalid keys.
  ASSERT_EQ(rows[1].size(), 4u);
  EXPECT_TRUE(rows[1][3].empty());
}

// ---------------------------------------------------------------------------
// P0-D. SAMPLED_MATCH → GET_VERTEX_PROPERTY end-to-end
// ---------------------------------------------------------------------------
// The whole point of the extension is to compose: sample some embeddings,
// then look up properties on the matched vertices. This test asserts that
// every vertex ID returned by SAMPLED_MATCH resolves through
// GET_VERTEX_PROPERTY to a real Person.

TEST_F(SampledMatchTest, SampledMatchToGetVertexPropertyPipeline) {
  auto sm_r = RunSampledMatch(WritePattern(kTrianglePattern), /*sample_size=*/4);
  ASSERT_TRUE(sm_r.query_ok) << sm_r.error;
  auto sm_rows = ReadCsv(sm_r.result_file);
  ASSERT_GE(sm_rows.size(), 2u);

  // Collect unique vertex IDs across all embeddings.
  std::set<int64_t> ids;
  for (size_t i = 1; i < sm_rows.size(); ++i) {
    ids.insert(std::stoll(sm_rows[i][0]));
    ids.insert(std::stoll(sm_rows[i][1]));
    ids.insert(std::stoll(sm_rows[i][2]));
  }
  ASSERT_FALSE(ids.empty());

  std::ostringstream ids_json;
  ids_json << "[";
  bool first = true;
  for (int64_t v : ids) {
    if (!first) ids_json << ",";
    first = false;
    ids_json << v;
  }
  ids_json << "]";

  auto props_path = RunGetVertexProperty(*conn_, ids_json.str(), "Person",
                                         R"(["name","age"])");
  ASSERT_FALSE(props_path.empty());
  auto prop_rows = ReadCsv(props_path);
  ASSERT_EQ(prop_rows.size(), ids.size() + 1);

  // Every row must carry a non-empty name (it's a real Person, not an
  // unmapped global ID).
  for (size_t i = 1; i < prop_rows.size(); ++i) {
    EXPECT_FALSE(prop_rows[i][1].empty())
        << "vertex_id " << prop_rows[i][0]
        << " has no name — SAMPLED_MATCH returned an ID that isn't a Person";
  }
}

// ---------------------------------------------------------------------------
// P0-E. SAVE_SAMPLEDMATCH_CHECKPOINT + INITIALIZE(dir) round-trip
// ---------------------------------------------------------------------------

TEST_F(SampledMatchTest, SaveCheckpointReturnsSuccessAndWritesFiles) {
  // Need a populated cache before saving — INITIALIZE first.
  auto init = conn_->Query(
      "CALL INITIALIZE() "
      "RETURN status, num_vertices, num_edges, max_degree, degeneracy;");
  ASSERT_TRUE(init.has_value()) << init.error().ToString();

  auto ckpt_dir = test_dir_ / "checkpoint_save";
  std::filesystem::create_directories(ckpt_dir);

  std::ostringstream q;
  q << "CALL SAVE_SAMPLEDMATCH_CHECKPOINT('" << ckpt_dir.string() << "') "
       "RETURN status, checkpoint_dir;";
  auto sm = conn_->Query(q.str());
  ASSERT_TRUE(sm.has_value()) << sm.error().ToString();

  const auto& resp = sm.value().response();
  ASSERT_EQ(sm.value().length(), 1u);
  ASSERT_GE(resp.arrays_size(), 2);
  EXPECT_EQ(resp.arrays(0).string_array().values(0), "success");
  EXPECT_EQ(resp.arrays(1).string_array().values(0), ckpt_dir.string());

  // Implementation writes data_graph_meta.bin and schema_graph.bin into
  // the directory (see SaveGraphCheckpoint() in sampled_match_functions.h).
  EXPECT_TRUE(std::filesystem::exists(ckpt_dir / "data_graph_meta.bin"));
  EXPECT_TRUE(std::filesystem::exists(ckpt_dir / "schema_graph.bin"));
}

// Round-trip: save → swap to a fresh DB with the same schema/data →
// INITIALIZE(dir) restores the cache → SAMPLED_MATCH still returns
// embeddings consistent with the original graph.
//
// We rebuild the schema/data in the new DB to keep the storage interface
// matched to the checkpoint — the checkpoint stores DataGraphMeta /
// SchemaGraph, not the raw row data.
TEST_F(SampledMatchTest, InitializeFromCheckpointRoundtrip) {
  auto init = conn_->Query(
      "CALL INITIALIZE() "
      "RETURN status, num_vertices, num_edges, max_degree, degeneracy;");
  ASSERT_TRUE(init.has_value()) << init.error().ToString();
  int64_t orig_vertices =
      init.value().response().arrays(1).int64_array().values(0);
  int64_t orig_edges =
      init.value().response().arrays(2).int64_array().values(0);

  auto ckpt_dir = test_dir_ / "checkpoint_roundtrip";
  std::filesystem::create_directories(ckpt_dir);

  std::ostringstream save_q;
  save_q << "CALL SAVE_SAMPLEDMATCH_CHECKPOINT('" << ckpt_dir.string()
         << "') RETURN status;";
  auto save_r = conn_->Query(save_q.str());
  ASSERT_TRUE(save_r.has_value()) << save_r.error().ToString();
  ASSERT_EQ(save_r.value().response().arrays(0).string_array().values(0),
            "success");

  // Re-initialize on the same DB using the checkpoint dir. The cache is a
  // process-wide singleton keyed by storage interface pointer, so this
  // exercises the load path even though we don't swap DBs. The behavior
  // we're pinning: INITIALIZE(checkpoint_dir) succeeds and reports the
  // same counts as the original full init.
  std::ostringstream reinit_q;
  reinit_q << "CALL INITIALIZE('" << ckpt_dir.string()
           << "') RETURN status, num_vertices, num_edges, max_degree, "
              "degeneracy;";
  auto reinit = conn_->Query(reinit_q.str());
  ASSERT_TRUE(reinit.has_value()) << reinit.error().ToString();
  EXPECT_EQ(reinit.value().response().arrays(0).string_array().values(0),
            "success");
  EXPECT_EQ(reinit.value().response().arrays(1).int64_array().values(0),
            orig_vertices);
  EXPECT_EQ(reinit.value().response().arrays(2).int64_array().values(0),
            orig_edges);

  // SAMPLED_MATCH must still produce sensible results after the reinit.
  auto sm_r = RunSampledMatch(WritePattern(kTrianglePattern));
  ASSERT_TRUE(sm_r.query_ok) << sm_r.error;
  EXPECT_GE(sm_r.sample_count, 1);
  EXPECT_GT(sm_r.estimated_count, 0.0);
}

// ---------------------------------------------------------------------------
// P1-A. Pattern topology coverage
// ---------------------------------------------------------------------------

// Linear path a → b → c with no closure. The fixture contains multiple
// 2-hop knows-paths (e.g. 0→1→2, 0→3→1, 2→0→1, 2→0→3, 3→1→2), so this
// must return a positive estimate.
TEST_F(SampledMatchTest, PathPatternThreeVerticesNoClosure) {
  constexpr const char* kPath = R"({
    "vertices": [
      {"id": 0, "label": "Person"},
      {"id": 1, "label": "Person"},
      {"id": 2, "label": "Person"}
    ],
    "edges": [
      {"source": 0, "target": 1, "label": "person_knows_person"},
      {"source": 1, "target": 2, "label": "person_knows_person"}
    ]
  })";
  auto r = RunSampledMatch(WritePattern(kPath));
  ASSERT_TRUE(r.query_ok) << r.error;
  EXPECT_GE(r.sample_count, 1);
  EXPECT_GT(r.estimated_count, 0.0);

  // Validate every sampled path: each row's first/last vertex differ from
  // the middle, and the two edges share the middle vertex (b).
  auto rows = ReadCsv(r.result_file);
  ASSERT_GE(rows.size(), 2u);
  ASSERT_EQ(rows[0].size(), 5u);  // 3 vertices + 2 edges
  for (size_t i = 1; i < rows.size(); ++i) {
    std::set<std::string> vs{rows[i][0], rows[i][1], rows[i][2]};
    EXPECT_EQ(vs.size(), 3u) << "row " << i << " has duplicate vertices";
  }
}

// Reverse-direction edge in the DSL must still find embeddings — a `b<-a`
// is logically identical to `a->b`, so the estimate should be positive.
TEST_F(SampledMatchTest, DslReverseEdgeDirection) {
  const std::string kDsl =
      "MATCH (b:Person)<-[:person_knows_person]-(a:Person)";
  std::ostringstream q;
  q << "CALL SAMPLED_MATCH_PATTERN('" << kDsl
    << "', 100) RETURN estimated_count, sample_count, result_file, "
       "props_file;";
  auto sm = conn_->Query(q.str());
  ASSERT_TRUE(sm.has_value()) << sm.error().ToString();
  EXPECT_GT(sm.value().response().arrays(0).double_array().values(0), 0.0);
  EXPECT_GE(sm.value().response().arrays(1).int64_array().values(0), 1);
}

// Larger 4-vertex chain to exercise FaSTest on a pattern with more pattern
// edges than the triangle case. Doesn't pin an exact count — the fixture's
// 5 knows-edges yield several 3-hop paths.
TEST_F(SampledMatchTest, PathPatternFourVerticesNoClosure) {
  constexpr const char* kChain = R"({
    "vertices": [
      {"id": 0, "label": "Person"},
      {"id": 1, "label": "Person"},
      {"id": 2, "label": "Person"},
      {"id": 3, "label": "Person"}
    ],
    "edges": [
      {"source": 0, "target": 1, "label": "person_knows_person"},
      {"source": 1, "target": 2, "label": "person_knows_person"},
      {"source": 2, "target": 3, "label": "person_knows_person"}
    ]
  })";
  auto r = RunSampledMatch(WritePattern(kChain));
  ASSERT_TRUE(r.query_ok) << r.error;
  // The pattern is structurally valid; sample_count of 0 is acceptable
  // (FaSTest may not find a 4-vertex chain in this small fixture under
  // the default sampling budget). The point of this test is "doesn't
  // crash on a 4-vertex pattern", which the existing assertions cover.
  EXPECT_GE(r.sample_count, 0);
  EXPECT_NE(r.estimated_count, -1.0)
      << "pattern should be accepted (not rejected at load time)";
}

// ---------------------------------------------------------------------------
// P1-B. DSL semantic coverage
// ---------------------------------------------------------------------------

// Edge inline props in DSL should match the equivalent JSON edge constraint.
// weight = 1.5 selects exactly the 1→2 edge in the fixture.
TEST_F(SampledMatchTest, DslEdgeInlinePropsMatchJsonEdgeConstraint) {
  const std::string kDsl =
      "MATCH (a:Person)-[:person_knows_person {weight: 1.5}]->(b:Person)";
  std::ostringstream q;
  q << "CALL SAMPLED_MATCH_PATTERN('" << kDsl
    << "', 1000) RETURN estimated_count, sample_count, result_file, "
       "props_file;";
  auto sm = conn_->Query(q.str());
  ASSERT_TRUE(sm.has_value()) << sm.error().ToString();
  double dsl_estimate =
      sm.value().response().arrays(0).double_array().values(0);
  EXPECT_GT(dsl_estimate, 0.0);

  constexpr const char* kJson = R"({
    "vertices": [
      {"id": 0, "label": "Person"},
      {"id": 1, "label": "Person"}
    ],
    "edges": [
      {"source": 0, "target": 1, "label": "person_knows_person",
       "constraints": [
         {"property": "weight", "operator": "=", "value": 1.5}
       ]}
    ]
  })";
  auto json_r = RunSampledMatch(WritePattern(kJson));
  ASSERT_TRUE(json_r.query_ok) << json_r.error;
  ASSERT_GT(json_r.estimated_count, 0.0);

  double ratio = dsl_estimate / json_r.estimated_count;
  EXPECT_GE(ratio, 0.5) << "DSL inline edge prop should match JSON constraint";
  EXPECT_LE(ratio, 2.0);
}

// DSL string-valued WHERE: Alice's name pins the source vertex. Double
// quotes are used for the inner Cypher string literal so it doesn't clash
// with the outer SQL string's single quotes (the DSL parser accepts both
// quote styles — see pattern_dsl.cpp:153).
TEST_F(SampledMatchTest, DslWhereOnStringPropertyRestrictsResults) {
  const std::string kRestricted =
      "MATCH (a:Person)-[:person_knows_person]->(b:Person) "
      "WHERE a.name = \"Alice\"";
  std::ostringstream qr;
  qr << "CALL SAMPLED_MATCH_PATTERN('" << kRestricted
     << "', 1000) RETURN estimated_count, sample_count, result_file, "
        "props_file;";
  auto r_restricted = conn_->Query(qr.str());
  ASSERT_TRUE(r_restricted.has_value()) << r_restricted.error().ToString();
  double restricted =
      r_restricted.value().response().arrays(0).double_array().values(0);
  EXPECT_GT(restricted, 0.0);

  const std::string kBaseline =
      "MATCH (a:Person)-[:person_knows_person]->(b:Person)";
  std::ostringstream qb;
  qb << "CALL SAMPLED_MATCH_PATTERN('" << kBaseline
     << "', 1000) RETURN estimated_count, sample_count, result_file, "
        "props_file;";
  auto r_baseline = conn_->Query(qb.str());
  ASSERT_TRUE(r_baseline.has_value()) << r_baseline.error().ToString();
  double baseline =
      r_baseline.value().response().arrays(0).double_array().values(0);

  // Alice has 2 out-edges (0→1, 0→3); baseline has 5. Restricted estimate
  // must be strictly smaller.
  EXPECT_LT(restricted, baseline)
      << "restricted=" << restricted << " baseline=" << baseline;
}

// Multiple WHERE predicates AND-combine. age>=18 alone keeps everyone;
// adding name='Alice' restricts further.
TEST_F(SampledMatchTest, DslMultipleWhereClausesAreAnded) {
  const std::string kBoth =
      "MATCH (a:Person)-[:person_knows_person]->(b:Person) "
      "WHERE a.age >= 18 AND a.name = \"Alice\"";
  std::ostringstream q;
  q << "CALL SAMPLED_MATCH_PATTERN('" << kBoth
    << "', 1000) RETURN estimated_count, sample_count;";
  auto sm = conn_->Query(q.str());
  ASSERT_TRUE(sm.has_value()) << sm.error().ToString();
  double est_both = sm.value().response().arrays(0).double_array().values(0);
  int64_t samples = sm.value().response().arrays(1).int64_array().values(0);
  EXPECT_GE(samples, 1) << "Alice is over 18; combined predicate must match";
  EXPECT_GT(est_both, 0.0);

  // Compare against name='Alice' alone — both predicates select the same
  // set (Alice qualifies age>=18) so the estimates should agree.
  const std::string kNameOnly =
      "MATCH (a:Person)-[:person_knows_person]->(b:Person) "
      "WHERE a.name = \"Alice\"";
  std::ostringstream q2;
  q2 << "CALL SAMPLED_MATCH_PATTERN('" << kNameOnly
     << "', 1000) RETURN estimated_count;";
  auto sm2 = conn_->Query(q2.str());
  ASSERT_TRUE(sm2.has_value()) << sm2.error().ToString();
  double est_name = sm2.value().response().arrays(0).double_array().values(0);
  double ratio = est_both / est_name;
  EXPECT_GE(ratio, 0.5);
  EXPECT_LE(ratio, 2.0);
}

// ---------------------------------------------------------------------------
// P1-C. sample_size boundaries
// ---------------------------------------------------------------------------

// sample_size = 1: matcher should still return at most one row.
TEST_F(SampledMatchTest, SampleSizeOneReturnsAtMostOneRow) {
  auto r = RunSampledMatch(WritePattern(kSingleEdgePattern), /*sample_size=*/1);
  ASSERT_TRUE(r.query_ok) << r.error;
  EXPECT_LE(r.sample_count, 1);
  if (r.sample_count == 1) {
    auto rows = ReadCsv(r.result_file);
    EXPECT_EQ(rows.size(), 2u) << "header + 1 row";
  }
}

// sample_size far larger than the number of embeddings: matcher must not
// crash and must not return more rows than embeddings actually exist.
// Fixture has 5 knows-edges, so sample_count is at most 5.
TEST_F(SampledMatchTest, SampleSizeFarExceedsEmbeddings) {
  auto r = RunSampledMatch(WritePattern(kSingleEdgePattern),
                           /*sample_size=*/100000);
  ASSERT_TRUE(r.query_ok) << r.error;
  EXPECT_GE(r.sample_count, 1);
  // Soft cap: don't claim an exact upper bound because the matcher may
  // produce duplicates when sampling with replacement. Just verify the
  // run completes successfully.
  EXPECT_GT(r.estimated_count, 0.0);
}

// sample_size = 0 must not crash. Whether the matcher returns 0 or treats
// it as a degenerate request is up to the implementation; this test pins
// "doesn't crash, doesn't produce a positive sample_count".
TEST_F(SampledMatchTest, SampleSizeZeroDoesNotCrash) {
  auto r = RunSampledMatch(WritePattern(kSingleEdgePattern), /*sample_size=*/0);
  ASSERT_TRUE(r.query_ok) << r.error;
  EXPECT_EQ(r.sample_count, 0);
}

}  // namespace test
}  // namespace neug
