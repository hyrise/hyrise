#pragma once

#include <algorithm>
#include <functional>
#include <memory>
#include <utility>
#include <vector>

#include "loser_tree.hpp"
#include "uninitialized_vector.hpp"

namespace hyrise {

template <typename T, typename Compare = std::greater<T>>
class RunMerger {
 public:
  using value_type = T;
  using input_run = std::span<T>;
  using output_run = uninitialized_vector<T>;

  static output_run merge(std::vector<input_run>& sorted_runs) {
    if (sorted_runs.empty()) {
      return output_run{};
    }

    const auto num_runs = sorted_runs.size();
    auto runs = std::vector<std::pair<value_type*, value_type*>>(num_runs);
    std::transform(sorted_runs.begin(), sorted_runs.end(), runs.begin(), [](const auto& run) {
      return std::make_pair(run.begin().base(), run.end().base());
    });

    const auto output_size = std::accumulate(sorted_runs.begin(), sorted_runs.end(), 0, [](auto acc, auto &run) {
      return acc + run.size();
    });
    auto merged_output = output_run(output_size);
    auto output_begin = merged_output.begin();
    const auto output_end = merged_output.end();
    // Compute next power of two.
    const auto fan_in = std::bit_ceil(num_runs);

    auto merge_tree = LoserTree<value_type, Compare>(fan_in);
    for (auto run_index = size_t{0}; run_index < num_runs; ++run_index) {
      auto& [current_it, end] = runs[run_index];
      if (current_it < end) {
        merge_tree.push_to_leaf(*current_it, run_index);
        ++current_it;
      } else {
        merge_tree.invalidate(run_index);
      }
    }
    for (auto run_index = num_runs; run_index < fan_in; ++run_index) {
      merge_tree.invalidate(run_index);
    }
    while (output_begin < output_end && !merge_tree.empty()) {
      const auto& element = merge_tree.peek();
      auto run_index = merge_tree.champion_node_index();
      auto& [current_it, end] = runs[run_index];
      *(output_begin++) = element;
      if (current_it < end) {
        merge_tree.push_to_leaf(*current_it, run_index);
        ++current_it;
      } else {
        merge_tree.invalidate(run_index);
      }
    }

    DebugAssert(merge_tree.empty(), "Merge tree is not empty.");
    return merged_output;
  }
};
}  // namespace hyrise
