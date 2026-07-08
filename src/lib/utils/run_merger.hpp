#pragma once

#include <algorithm>
#include <memory>
#include <utility>
#include <vector>

#include "loser_tree.hpp"

namespace hyrise {

template <typename T, typename Compare>
class RunMerger {
 public:
  using value_type = T;
  using run = std::span<T>;

  static void merge(std::vector<run>& sorted_runs, std::span<T> merged_output) {
    if (sorted_runs.empty()) {
      return;
    }

    const auto num_runs = sorted_runs.size();
    auto runs = std::vector<std::pair<value_type*, value_type*>>(num_runs);
    std::transform(sorted_runs.begin(), sorted_runs.end(), runs.begin(), [](const auto& run) {
      return std::make_pair(run.begin().base(), run.end().base());
    });

    auto output_begin = merged_output.begin();
    const auto output_end = merged_output.end();
    const auto fan_in = _next_power_of_two(num_runs);

    auto merge_tree = LoserTree<value_type, Compare>(fan_in);
    merge_tree.reset();
    for (auto run_idx = size_t{0}; run_idx < num_runs; ++run_idx) {
      auto& [current_it, end] = runs[run_idx];
      if (current_it < end) {
        merge_tree.push_to_leaf(*current_it, run_idx);
        ++current_it;
      } else {
        merge_tree.invalidate(run_idx);
      }
    }
    for (auto run_idx = num_runs; run_idx < fan_in; ++run_idx) {
      merge_tree.invalidate(run_idx);
    }
    while (output_begin < output_end && !merge_tree.empty()) {
      auto element = merge_tree.peek();
      auto run_idx = merge_tree.champion_node_index();
      auto& [current_it, end] = runs[run_idx];
      *(output_begin++) = element;
      if (current_it < end) {
        merge_tree.push_to_leaf(*current_it, run_idx);
        ++current_it;
      } else {
        merge_tree.invalidate(run_idx);
      }
    }

    DebugAssert(merge_tree.empty(), "Merge tree is not empty.");
  }

 private:
  [[nodiscard]] static auto _next_power_of_two(const uint64_t value) {
    return std::bit_ceil(value);
  }
};
}  // namespace hyrise
