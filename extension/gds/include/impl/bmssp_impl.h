/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "neug/common/types/container_types.h"
#include "neug/execution/common/context.h"
#include "neug/storages/graph/graph_interface.h"

namespace neug {
namespace gds {

// Experimental implementation of the BMSSP algorithm from
// "Breaking the Sorting Barrier for Directed Single-Source Shortest Paths"
// (Duan, Mao, Mao, Shu, and Yin, 2025).
//
// This is an adaptive implementation: a bounded sparse/dense frontier probe
// handles low-diameter graphs without materializing another CSR; if the probe
// does not converge, it is discarded and the recursive BMSSP decomposition
// runs on a compact projected CSR. The fallback avoids the paper's explicit
// constant-degree transformation and verifies the labels with a fixed-point
// repair pass, so it does not claim the paper's asymptotic bound.
class BMSSP {
 public:
  BMSSP(const StorageReadInterface& graph, label_t vertex_label,
        label_t edge_label, vid_t source, bool directed,
        const std::string& edge_weight_prop, int concurrency);

  void compute();
  void sink(execution::Context& ctx, int node_alias, int distance_alias);

 private:
  bool try_native_frontier(size_t max_rounds);
  void build_adjacency();
  using virtual_vid_t = vid_t;

  const StorageReadInterface& graph_;
  label_t vertex_label_;
  label_t edge_label_;
  vid_t source_;
  bool directed_;
  bool has_edge_weight_;
  int concurrency_;

  std::unique_ptr<EdgeDataAccessor> edge_weight_accessor_;

  std::vector<uint64_t> adjacency_offsets_;
  std::vector<virtual_vid_t> adjacency_dst_;
  std::vector<double> adjacency_weight_;

  std::vector<double> distances_;
  std::vector<uint64_t> hops_;
  std::vector<virtual_vid_t> predecessor_;
  vector_t<vid_t> vertices_;

  int k_ = 2;
  int t_ = 2;
  double build_ms_ = 0.0;
};

}  // namespace gds
}  // namespace neug
