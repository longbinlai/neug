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

#include "impl/bmssp_impl.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <functional>
#include <limits>
#include <queue>
#include <stdexcept>
#include <utility>
#include <vector>

#include "neug/common/columns/value_columns.h"
#include "neug/common/columns/vertex_columns.h"
#include "utils/parallel_utils.h"

namespace neug {
namespace gds {
namespace {

using virtual_vid_t = vid_t;
constexpr virtual_vid_t kInvalidVid = std::numeric_limits<virtual_vid_t>::max();
constexpr double kInf = std::numeric_limits<double>::infinity();
constexpr uint64_t kInfiniteHops = std::numeric_limits<uint64_t>::max();

struct QueueKey {
  double distance;
  uint64_t hops;
  virtual_vid_t vertex;
  virtual_vid_t predecessor;
};

struct QueueKeyLess {
  bool operator()(const QueueKey& lhs, const QueueKey& rhs) const {
    if (lhs.distance != rhs.distance) {
      return lhs.distance < rhs.distance;
    }
    if (lhs.hops != rhs.hops) {
      return lhs.hops < rhs.hops;
    }
    if (lhs.vertex != rhs.vertex) {
      return lhs.vertex < rhs.vertex;
    }
    return lhs.predecessor < rhs.predecessor;
  }
};

bool key_less(const QueueKey& lhs, const QueueKey& rhs) {
  return QueueKeyLess{}(lhs, rhs);
}

bool key_in_half_open(const QueueKey& value, const QueueKey& lower,
                      const QueueKey& upper) {
  return !key_less(value, lower) && key_less(value, upper);
}

QueueKey infinity_key() {
  return QueueKey{kInf, kInfiniteHops, kInvalidVid, kInvalidVid};
}

size_t saturated_pow2(int exponent, size_t cap) {
  if (exponent <= 0) {
    return 1;
  }
  if (exponent >= static_cast<int>(sizeof(size_t) * 8 - 1)) {
    return cap;
  }
  return std::min<size_t>(size_t{1} << exponent, cap);
}

// Correctness-oriented implementation of the interface from Lemma 3.3.  It
// keeps one value per key and returns the actual M smallest values.  Replacing
// this with the paper's block structure is an optimization and does not change
// the BMSSP control flow.
class OrderedFrontier {
 public:
  explicit OrderedFrontier(size_t vertex_count)
      : values_(vertex_count), stamps_(vertex_count, 0) {}

  void initialize(size_t pull_size, QueueKey upper_bound) {
    pull_size_ = std::max<size_t>(pull_size, 1);
    upper_bound_ = upper_bound;
    heap_.clear();
    active_count_ = 0;
    ++epoch_;
    if (epoch_ == 0) {
      std::fill(stamps_.begin(), stamps_.end(), 0);
      ++epoch_;
    }
  }

  void insert(virtual_vid_t vertex, const QueueKey& value) {
    if (stamps_[vertex] == epoch_) {
      if (!key_less(value, values_[vertex])) {
        return;
      }
      values_[vertex] = value;
    } else {
      stamps_[vertex] = epoch_;
      values_[vertex] = value;
      ++active_count_;
    }
    heap_.push_back(value);
    std::push_heap(heap_.begin(), heap_.end(), Greater{});
  }

  void batch_prepend(
      const std::vector<std::pair<virtual_vid_t, QueueKey>>& xs) {
    for (const auto& [vertex, value] : xs) {
      insert(vertex, value);
    }
  }

  void erase(virtual_vid_t vertex) {
    if (stamps_[vertex] != epoch_) {
      return;
    }
    stamps_[vertex] = 0;
    --active_count_;
  }

  std::pair<QueueKey, std::vector<virtual_vid_t>> pull() {
    std::vector<virtual_vid_t> vertices;
    vertices.reserve(std::min(pull_size_, active_count_));
    discard_stale();
    while (!heap_.empty() && vertices.size() < pull_size_) {
      std::pop_heap(heap_.begin(), heap_.end(), Greater{});
      QueueKey value = heap_.back();
      heap_.pop_back();
      stamps_[value.vertex] = 0;
      --active_count_;
      vertices.push_back(value.vertex);
      discard_stale();
    }
    QueueKey boundary = heap_.empty() ? upper_bound_ : heap_.front();
    return {boundary, std::move(vertices)};
  }

  bool empty() const { return active_count_ == 0; }

 private:
  struct Greater {
    bool operator()(const QueueKey& lhs, const QueueKey& rhs) const {
      return QueueKeyLess{}(rhs, lhs);
    }
  };

  void discard_stale() {
    while (!heap_.empty()) {
      const QueueKey& value = heap_.front();
      if (stamps_[value.vertex] == epoch_ &&
          !key_less(value, values_[value.vertex]) &&
          !key_less(values_[value.vertex], value)) {
        break;
      }
      std::pop_heap(heap_.begin(), heap_.end(), Greater{});
      heap_.pop_back();
    }
  }

  size_t pull_size_ = 1;
  QueueKey upper_bound_ = infinity_key();
  std::vector<QueueKey> heap_;
  std::vector<QueueKey> values_;
  std::vector<uint32_t> stamps_;
  size_t active_count_ = 0;
  uint32_t epoch_ = 0;
};

class BMSSPSolver {
 public:
  BMSSPSolver(const std::vector<uint64_t>& adjacency_offsets,
              const std::vector<virtual_vid_t>& adjacency_dst,
              const std::vector<double>& adjacency_weight,
              std::vector<double>& distances, std::vector<uint64_t>& hops,
              std::vector<virtual_vid_t>& predecessor, int k, int t)
      : adjacency_offsets_(adjacency_offsets),
        adjacency_dst_(adjacency_dst),
        adjacency_weight_(adjacency_weight),
        distances_(distances),
        hops_(hops),
        predecessor_(predecessor),
        pivot_root_(distances.size(), kInvalidVid),
        pivot_tree_size_(distances.size(), 0),
        pivot_seen_(distances.size(), 0),
        last_completed_level_(distances.size(), -1),
        k_(k),
        t_(t) {}

  void run(virtual_vid_t source) {
    distances_[source] = 0.0;
    hops_[source] = 0;
    predecessor_[source] = kInvalidVid;

    const double log_n =
        std::log2(static_cast<double>(std::max<size_t>(distances_.size(), 2)));
    const int level = static_cast<int>(std::ceil(log_n / t_));
    frontiers_.reserve(level);
    for (int i = 0; i < level; ++i) {
      frontiers_.emplace_back(distances_.size());
    }
    const auto recursive_begin = std::chrono::steady_clock::now();
    bmssp(level, infinity_key(), std::vector<virtual_vid_t>{source});
    recursive_ms_ = elapsed_ms(recursive_begin);
    const auto repair_begin = std::chrono::steady_clock::now();
    repair_to_fixed_point();
    repair_ms_ = elapsed_ms(repair_begin);
  }

  double recursive_ms() const { return recursive_ms_; }
  double repair_ms() const { return repair_ms_; }
  uint64_t repair_edges() const { return repair_edges_; }
  uint64_t repair_updates() const { return repair_updates_; }
  uint64_t recursive_calls() const { return recursive_calls_; }
  uint64_t recursive_edges() const { return recursive_edges_; }

 private:
  struct Result {
    QueueKey boundary;
    std::vector<virtual_vid_t> vertices;
  };

  struct PivotResult {
    std::vector<virtual_vid_t> pivots;
    std::vector<virtual_vid_t> visited;
  };

  static double elapsed_ms(std::chrono::steady_clock::time_point begin) {
    return std::chrono::duration<double, std::milli>(
               std::chrono::steady_clock::now() - begin)
        .count();
  }

  QueueKey key(virtual_vid_t vertex) const {
    return QueueKey{distances_[vertex], hops_[vertex], vertex,
                    predecessor_[vertex]};
  }

  bool relax(virtual_vid_t src, virtual_vid_t dst, double weight,
             QueueKey& candidate_key) {
    if (std::isinf(distances_[src])) {
      return false;
    }
    const double candidate_distance = distances_[src] + weight;
    const uint64_t candidate_hops = hops_[src] + 1;
    candidate_key = QueueKey{candidate_distance, candidate_hops, dst, src};

    const QueueKey old_key = key(dst);
    const bool strictly_better = key_less(candidate_key, old_key);
    const bool valid = strictly_better || !key_less(old_key, candidate_key);
    if (strictly_better) {
      distances_[dst] = candidate_distance;
      hops_[dst] = candidate_hops;
      predecessor_[dst] = src;
    }
    return valid;
  }

  template <typename Func>
  void for_each_edge(virtual_vid_t src, Func&& func) {
    for (uint64_t edge = adjacency_offsets_[src];
         edge < adjacency_offsets_[src + 1]; ++edge) {
      func(adjacency_dst_[edge], adjacency_weight_[edge]);
    }
  }

  // The recursive phase only produces path-backed upper bounds.  Until the
  // paper's specialized frontier is implemented exactly, close any remaining
  // violated relaxations so the public algorithm never exposes partial labels.
  void repair_to_fixed_point() {
    struct Greater {
      bool operator()(const QueueKey& lhs, const QueueKey& rhs) const {
        return QueueKeyLess{}(rhs, lhs);
      }
    };
    std::priority_queue<QueueKey, std::vector<QueueKey>, Greater> heap;

    for (virtual_vid_t src = 0; src < distances_.size(); ++src) {
      if (std::isinf(distances_[src])) {
        continue;
      }
      for_each_edge(src, [&](virtual_vid_t dst, double weight) {
        ++repair_edges_;
        const QueueKey before = key(dst);
        QueueKey candidate;
        relax(src, dst, weight, candidate);
        if (key_less(key(dst), before)) {
          ++repair_updates_;
          heap.push(key(dst));
        }
      });
    }

    while (!heap.empty()) {
      const QueueKey current = heap.top();
      heap.pop();
      if (key_less(key(current.vertex), current)) {
        continue;
      }
      for_each_edge(current.vertex, [&](virtual_vid_t dst, double weight) {
        ++repair_edges_;
        const QueueKey before = key(dst);
        QueueKey candidate;
        relax(current.vertex, dst, weight, candidate);
        if (key_less(key(dst), before)) {
          ++repair_updates_;
          heap.push(key(dst));
        }
      });
    }
  }

  PivotResult find_pivots(const QueueKey& bound,
                          const std::vector<virtual_vid_t>& sources) {
    ++pivot_epoch_;
    if (pivot_epoch_ == 0) {
      std::fill(pivot_seen_.begin(), pivot_seen_.end(), 0);
      ++pivot_epoch_;
    }
    std::vector<virtual_vid_t> visited = sources;
    std::vector<virtual_vid_t> previous = sources;
    for (virtual_vid_t source : sources) {
      pivot_seen_[source] = pivot_epoch_;
      pivot_root_[source] = source;
      pivot_tree_size_[source] = 0;
    }

    for (int iteration = 0; iteration < k_; ++iteration) {
      std::vector<virtual_vid_t> next;
      next.reserve(previous.size() * 2);
      for (virtual_vid_t src : previous) {
        for_each_edge(src, [&](virtual_vid_t dst, double weight) {
          ++recursive_edges_;
          QueueKey candidate;
          if (relax(src, dst, weight, candidate) &&
              key_less(candidate, bound)) {
            pivot_root_[dst] = pivot_root_[src];
            next.push_back(dst);
            if (pivot_seen_[dst] != pivot_epoch_) {
              pivot_seen_[dst] = pivot_epoch_;
              visited.push_back(dst);
            }
          }
        });
      }
      if (visited.size() >
          static_cast<size_t>(k_) * std::max<size_t>(sources.size(), 1)) {
        return PivotResult{sources, std::move(visited)};
      }
      previous = std::move(next);
      if (previous.empty()) {
        break;
      }
    }

    for (virtual_vid_t vertex : visited) {
      const virtual_vid_t root = pivot_root_[vertex];
      if (root != kInvalidVid) {
        ++pivot_tree_size_[root];
      }
    }

    std::vector<virtual_vid_t> pivots;
    for (virtual_vid_t source : sources) {
      if (pivot_tree_size_[source] >= static_cast<size_t>(k_)) {
        pivots.push_back(source);
      }
    }
    return PivotResult{std::move(pivots), std::move(visited)};
  }

  Result base_case(const QueueKey& bound,
                   const std::vector<virtual_vid_t>& sources) {
    struct Greater {
      bool operator()(const QueueKey& lhs, const QueueKey& rhs) const {
        return QueueKeyLess{}(rhs, lhs);
      }
    };
    std::priority_queue<QueueKey, std::vector<QueueKey>, Greater> heap;
    std::vector<virtual_vid_t> output;
    output.reserve(static_cast<size_t>(k_) + 1);
    heap.push(key(sources.front()));

    while (!heap.empty() && output.size() < static_cast<size_t>(k_ + 1)) {
      QueueKey current = heap.top();
      heap.pop();
      if (key_less(key(current.vertex), current)) {
        continue;
      }
      output.push_back(current.vertex);
      for_each_edge(current.vertex, [&](virtual_vid_t dst, double weight) {
        ++recursive_edges_;
        QueueKey candidate;
        if (relax(current.vertex, dst, weight, candidate) &&
            key_less(candidate, bound)) {
          heap.push(key(dst));
        }
      });
    }

    if (output.size() <= static_cast<size_t>(k_)) {
      return Result{bound, std::move(output)};
    }

    QueueKey new_bound = key(output.back());
    output.pop_back();
    return Result{new_bound, std::move(output)};
  }

  Result bmssp(int level, const QueueKey& bound,
               const std::vector<virtual_vid_t>& sources) {
    ++recursive_calls_;
    if (level == 0) {
      return base_case(bound, sources);
    }

    PivotResult pivot_result = find_pivots(bound, sources);
    const size_t node_cap = distances_.size() + 1;
    const size_t pull_size = saturated_pow2((level - 1) * t_, node_cap);
    OrderedFrontier& frontier = frontiers_[level - 1];
    frontier.initialize(pull_size, bound);
    for (virtual_vid_t pivot : pivot_result.pivots) {
      frontier.insert(pivot, key(pivot));
    }

    QueueKey last_completed_bound = bound;
    if (!pivot_result.pivots.empty()) {
      last_completed_bound = key(pivot_result.pivots.front());
      for (virtual_vid_t pivot : pivot_result.pivots) {
        if (key_less(key(pivot), last_completed_bound)) {
          last_completed_bound = key(pivot);
        }
      }
    }

    std::vector<virtual_vid_t> output;
    const size_t level_size = saturated_pow2(level * t_, node_cap);
    const size_t work_limit = level_size >= node_cap / static_cast<size_t>(k_)
                                  ? node_cap
                                  : level_size * static_cast<size_t>(k_);

    while (output.size() < work_limit && !frontier.empty()) {
      auto [iteration_bound, next_sources] = frontier.pull();
      Result child = bmssp(level - 1, iteration_bound, next_sources);
      output.insert(output.end(), child.vertices.begin(), child.vertices.end());

      std::vector<std::pair<virtual_vid_t, QueueKey>> prepend;
      for (virtual_vid_t src : child.vertices) {
        frontier.erase(src);
        last_completed_level_[src] = level;
        for_each_edge(src, [&](virtual_vid_t dst, double weight) {
          ++recursive_edges_;
          QueueKey candidate;
          if (!relax(src, dst, weight, candidate)) {
            return;
          }
          if (key_in_half_open(candidate, iteration_bound, bound)) {
            frontier.insert(dst, candidate);
          } else if (key_in_half_open(candidate, child.boundary,
                                      iteration_bound)) {
            prepend.emplace_back(dst, candidate);
          }
        });
      }
      for (virtual_vid_t vertex : next_sources) {
        QueueKey value = key(vertex);
        if (!key_less(value, child.boundary)) {
          prepend.emplace_back(vertex, value);
        }
      }
      frontier.batch_prepend(prepend);
      last_completed_bound = child.boundary;
    }

    QueueKey new_bound = frontier.empty() ? bound : last_completed_bound;
    for (virtual_vid_t vertex : pivot_result.visited) {
      if (last_completed_level_[vertex] != level &&
          key_less(key(vertex), new_bound)) {
        output.push_back(vertex);
      }
    }
    return Result{new_bound,
                  std::vector<virtual_vid_t>(output.begin(), output.end())};
  }

  const std::vector<uint64_t>& adjacency_offsets_;
  const std::vector<virtual_vid_t>& adjacency_dst_;
  const std::vector<double>& adjacency_weight_;
  std::vector<double>& distances_;
  std::vector<uint64_t>& hops_;
  std::vector<virtual_vid_t>& predecessor_;
  std::vector<virtual_vid_t> pivot_root_;
  std::vector<size_t> pivot_tree_size_;
  std::vector<uint32_t> pivot_seen_;
  std::vector<int> last_completed_level_;
  std::vector<OrderedFrontier> frontiers_;
  uint32_t pivot_epoch_ = 0;
  int k_;
  int t_;
  double recursive_ms_ = 0.0;
  double repair_ms_ = 0.0;
  uint64_t repair_edges_ = 0;
  uint64_t repair_updates_ = 0;
  uint64_t recursive_calls_ = 0;
  uint64_t recursive_edges_ = 0;
};

}  // namespace

BMSSP::BMSSP(const StorageReadInterface& graph, label_t vertex_label,
             label_t edge_label, vid_t source, bool directed,
             const std::string& edge_weight_prop, int concurrency)
    : graph_(graph),
      vertex_label_(vertex_label),
      edge_label_(edge_label),
      source_(source),
      directed_(directed),
      has_edge_weight_(!edge_weight_prop.empty()),
      concurrency_(std::max(concurrency, 1)) {
  const auto build_begin = std::chrono::steady_clock::now();
  const auto& vertex_set = graph_.GetVertexSet(vertex_label_);
  const size_t vertex_count = vertex_set.size();
  vertices_.reserve(vertex_count);
  for (vid_t vertex : vertex_set) {
    vertices_.push_back(vertex);
  }

  if (has_edge_weight_) {
    edge_weight_accessor_ =
        std::make_unique<EdgeDataAccessor>(graph_.GetEdgeDataAccessor(
            vertex_label_, vertex_label_, edge_label_, edge_weight_prop));
  }

  distances_.assign(vertex_count, kInf);

  const double log_n =
      std::log2(static_cast<double>(std::max<size_t>(vertex_count, 2)));
  k_ = std::max(1, static_cast<int>(std::floor(std::cbrt(log_n))));
  t_ = std::max(1, static_cast<int>(std::floor(std::pow(log_n, 2.0 / 3.0))));
  build_ms_ = std::chrono::duration<double, std::milli>(
                  std::chrono::steady_clock::now() - build_begin)
                  .count();
}

void BMSSP::build_adjacency() {
  const auto build_begin = std::chrono::steady_clock::now();
  const size_t vertex_count = distances_.size();
  auto outgoing = graph_.GetGenericOutgoingGraphView(
      vertex_label_, vertex_label_, edge_label_);
  std::vector<uint64_t> degree(vertex_count, 0);
  for (vid_t src : vertices_) {
    auto edges = outgoing.get_edges(src);
    for (auto it = edges.begin(); it != edges.end(); ++it) {
      ++degree[src];
      if (!directed_) {
        ++degree[*it];
      }
    }
  }

  adjacency_offsets_.assign(vertex_count + 1, 0);
  for (size_t vertex = 0; vertex < vertex_count; ++vertex) {
    if (adjacency_offsets_[vertex] >
        std::numeric_limits<uint64_t>::max() - degree[vertex]) {
      throw std::runtime_error("BMSSP projected graph is too large");
    }
    adjacency_offsets_[vertex + 1] =
        adjacency_offsets_[vertex] + degree[vertex];
  }

  adjacency_dst_.resize(adjacency_offsets_.back());
  adjacency_weight_.resize(adjacency_offsets_.back());
  std::vector<uint64_t> next_slot = adjacency_offsets_;
  auto add_arc = [&](vid_t src, vid_t dst, double weight) {
    const uint64_t slot = next_slot[src]++;
    adjacency_dst_[slot] = dst;
    adjacency_weight_[slot] = weight;
  };
  for (vid_t src : vertices_) {
    auto edges = outgoing.get_edges(src);
    for (auto it = edges.begin(); it != edges.end(); ++it) {
      const vid_t dst = *it;
      const double weight =
          has_edge_weight_ ? edge_weight_accessor_->get_typed_data<double>(it)
                           : 1.0;
      if (!std::isfinite(weight) || weight < 0.0) {
        throw std::runtime_error(
            "BMSSP requires finite, non-negative edge weights");
      }
      add_arc(src, dst, weight);
      if (!directed_) {
        add_arc(dst, src, weight);
      }
    }
  }
  build_ms_ += std::chrono::duration<double, std::milli>(
                   std::chrono::steady_clock::now() - build_begin)
                   .count();
}

void BMSSP::compute() {
  constexpr size_t kProbeRounds = 32;
  if (try_native_frontier(kProbeRounds)) {
    return;
  }
  std::fill(distances_.begin(), distances_.end(), kInf);
  hops_.assign(distances_.size(), kInfiniteHops);
  predecessor_.assign(distances_.size(), kInvalidVid);
  build_adjacency();
  BMSSPSolver solver(adjacency_offsets_, adjacency_dst_, adjacency_weight_,
                     distances_, hops_, predecessor_, k_, t_);
  solver.run(source_);
  if (std::getenv("NEUG_BMSSP_PROFILE") != nullptr) {
    LOG(INFO) << "BMSSP profile: build_ms=" << build_ms_ << " k=" << k_
              << " t=" << t_ << " recursive_ms=" << solver.recursive_ms()
              << " repair_ms=" << solver.repair_ms()
              << " repair_edges=" << solver.repair_edges()
              << " repair_updates=" << solver.repair_updates();
    LOG(INFO) << "BMSSP recursive counters: calls=" << solver.recursive_calls()
              << " edges=" << solver.recursive_edges();
  }
}

bool BMSSP::try_native_frontier(size_t max_rounds) {
  const auto begin = std::chrono::steady_clock::now();
  auto outgoing = graph_.GetGenericOutgoingGraphView(
      vertex_label_, vertex_label_, edge_label_);
  auto incoming = graph_.GetGenericIncomingGraphView(
      vertex_label_, vertex_label_, edge_label_);
  distances_[source_] = 0.0;
  std::vector<vid_t> current{source_};

  if (concurrency_ > 1) {
    auto atomic_distances =
        std::make_unique<std::atomic<double>[]>(distances_.size());
    ParallelUtils::parallel_for(
        vertices_.data(), vertices_.size(),
        [&](vid_t vertex, int) {
          atomic_distances[vertex].store(kInf, std::memory_order_relaxed);
        },
        concurrency_);
    atomic_distances[source_].store(0.0, std::memory_order_relaxed);
    std::vector<std::vector<vid_t>> local_next(concurrency_);
    std::atomic<bool> invalid_weight{false};
    bool weights_validated = false;
    uint64_t epoch = 0;
    while (!current.empty() && epoch < max_rounds) {
      ++epoch;
      for (auto& bucket : local_next) {
        bucket.clear();
      }
      const bool use_dense = current.size() * 20 > distances_.size();
      const bool validate_weights = !weights_validated;
      if (use_dense) {
        ParallelUtils::parallel_for(
            vertices_.data(), vertices_.size(),
            [&](vid_t dst, int tid) {
              double best =
                  atomic_distances[dst].load(std::memory_order_relaxed);
              auto relax_in = [&](auto edges) {
                for (auto it = edges.begin(); it != edges.end(); ++it) {
                  const vid_t src = *it;
                  const double weight =
                      has_edge_weight_
                          ? edge_weight_accessor_->get_typed_data<double>(it)
                          : 1.0;
                  if (validate_weights &&
                      (!std::isfinite(weight) || weight < 0.0)) {
                    invalid_weight.store(true, std::memory_order_relaxed);
                    continue;
                  }
                  const double src_distance =
                      atomic_distances[src].load(std::memory_order_relaxed);
                  best = std::min(best, src_distance + weight);
                }
              };
              relax_in(incoming.get_edges(dst));
              if (!directed_) {
                relax_in(outgoing.get_edges(dst));
              }
              const double old =
                  atomic_distances[dst].load(std::memory_order_relaxed);
              if (best < old) {
                // Dense mode assigns each destination to exactly one worker,
                // so an atomic store is sufficient; other workers only read.
                atomic_distances[dst].store(best, std::memory_order_relaxed);
                local_next[tid].push_back(dst);
              }
            },
            concurrency_);
      } else {
        ParallelUtils::parallel_for(
            current.data(), current.size(),
            [&](vid_t src, int tid) {
              const double src_distance =
                  atomic_distances[src].load(std::memory_order_relaxed);
              auto relax_edges = [&](auto edges) {
                for (auto it = edges.begin(); it != edges.end(); ++it) {
                  const vid_t dst = *it;
                  const double weight =
                      has_edge_weight_
                          ? edge_weight_accessor_->get_typed_data<double>(it)
                          : 1.0;
                  if (validate_weights &&
                      (!std::isfinite(weight) || weight < 0.0)) {
                    invalid_weight.store(true, std::memory_order_relaxed);
                    continue;
                  }
                  const double candidate = src_distance + weight;
                  double old =
                      atomic_distances[dst].load(std::memory_order_relaxed);
                  bool improved = false;
                  while (candidate < old) {
                    if (atomic_distances[dst].compare_exchange_weak(
                            old, candidate, std::memory_order_relaxed,
                            std::memory_order_relaxed)) {
                      improved = true;
                      break;
                    }
                  }
                  if (improved) {
                    local_next[tid].push_back(dst);
                  }
                }
              };
              relax_edges(outgoing.get_edges(src));
              if (!directed_) {
                relax_edges(incoming.get_edges(src));
              }
            },
            concurrency_);
      }
      if (invalid_weight.load(std::memory_order_relaxed)) {
        throw std::runtime_error(
            "BMSSP requires finite, non-negative edge weights");
      }
      if (use_dense) {
        // Pulling all destinations visits every projected edge.
        weights_validated = true;
      }

      size_t next_size = 0;
      for (const auto& bucket : local_next) {
        next_size += bucket.size();
      }
      std::vector<vid_t> next;
      next.reserve(next_size);
      for (const auto& bucket : local_next) {
        next.insert(next.end(), bucket.begin(), bucket.end());
      }
      current.swap(next);
    }
    if (current.empty()) {
      ParallelUtils::parallel_for(
          vertices_.data(), vertices_.size(),
          [&](vid_t vertex, int) {
            distances_[vertex] =
                atomic_distances[vertex].load(std::memory_order_relaxed);
          },
          concurrency_);
    }
    if (std::getenv("NEUG_BMSSP_PROFILE") != nullptr) {
      LOG(INFO) << "BMSSP probe profile: init_ms=" << build_ms_
                << " rounds=" << epoch << " concurrency=" << concurrency_
                << " compute_ms="
                << std::chrono::duration<double, std::milli>(
                       std::chrono::steady_clock::now() - begin)
                       .count();
    }
    return current.empty();
  }

  std::vector<vid_t> next;
  // Allocate only the smaller predecessor scratch array for a sequential
  // probe. The full BMSSP hop/predecessor state remains lazy until fallback.
  predecessor_.assign(distances_.size(), kInvalidVid);
  uint64_t epoch = 0;
  while (!current.empty() && epoch < max_rounds) {
    ++epoch;
    next.clear();
    const bool use_dense = current.size() * 20 > distances_.size();
    if (use_dense) {
      for (vid_t dst : vertices_) {
        double best = distances_[dst];
        auto relax_in = [&](auto edges) {
          for (auto it = edges.begin(); it != edges.end(); ++it) {
            const vid_t src = *it;
            const double weight =
                has_edge_weight_
                    ? edge_weight_accessor_->get_typed_data<double>(it)
                    : 1.0;
            if (!std::isfinite(weight) || weight < 0.0) {
              throw std::runtime_error(
                  "BMSSP requires finite, non-negative edge weights");
            }
            best = std::min(best, distances_[src] + weight);
          }
        };
        relax_in(incoming.get_edges(dst));
        if (!directed_) {
          relax_in(outgoing.get_edges(dst));
        }
        if (best < distances_[dst]) {
          distances_[dst] = best;
          next.push_back(dst);
        }
      }
    } else {
      for (vid_t src : current) {
        const double src_distance = distances_[src];
        auto relax_edges = [&](auto edges) {
          for (auto it = edges.begin(); it != edges.end(); ++it) {
            const vid_t dst = *it;
            const double weight =
                has_edge_weight_
                    ? edge_weight_accessor_->get_typed_data<double>(it)
                    : 1.0;
            if (!std::isfinite(weight) || weight < 0.0) {
              throw std::runtime_error(
                  "BMSSP requires finite, non-negative edge weights");
            }
            const double candidate = src_distance + weight;
            if (candidate < distances_[dst]) {
              distances_[dst] = candidate;
              if (predecessor_[dst] != static_cast<virtual_vid_t>(epoch)) {
                predecessor_[dst] = static_cast<virtual_vid_t>(epoch);
                next.push_back(dst);
              }
            }
          }
        };
        relax_edges(outgoing.get_edges(src));
        if (!directed_) {
          relax_edges(incoming.get_edges(src));
        }
      }
    }
    current.swap(next);
  }
  if (std::getenv("NEUG_BMSSP_PROFILE") != nullptr) {
    LOG(INFO) << "BMSSP probe profile: init_ms=" << build_ms_
              << " rounds=" << epoch << " compute_ms="
              << std::chrono::duration<double, std::milli>(
                     std::chrono::steady_clock::now() - begin)
                     .count();
  }
  return current.empty();
}

void BMSSP::sink(execution::Context& ctx, int node_alias, int distance_alias) {
  MSVertexColumnBuilder node_builder(vertex_label_);
  ValueColumnBuilder<double> distance_builder;
  distance_builder.reserve(vertices_.size());
  for (vid_t vertex : vertices_) {
    const double distance = distances_[vertex];
    distance_builder.push_back_opt(std::isinf(distance) ? -1.0 : distance);
  }
  node_builder.append(vertex_label_, std::move(vertices_));

  execution::ContextChunk chunk;
  chunk.set(node_alias, node_builder.finish());
  chunk.set(distance_alias, distance_builder.finish());
  ctx.append_chunk(std::move(chunk));
}

}  // namespace gds
}  // namespace neug
