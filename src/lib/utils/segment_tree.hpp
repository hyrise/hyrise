#pragma once

#include <algorithm>
#include <bit>
#include <cstdint>
#include <span>
#include <vector>

namespace hyrise {

template <typename T, typename Combine>
class SegmentTree {
  friend class SegmentTreeTest;

 public:
  using RangeBound = uint64_t;

  struct Range {
    RangeBound start;
    RangeBound end;
  };

  explicit SegmentTree(std::span<const T> leaf_values, T init_neutral_element = T(), Combine&& init_combine = Combine())
      : leaf_count(std::bit_ceil(leaf_values.size())),
        neutral_element(std::move(init_neutral_element)),
        combine(init_combine),
        tree(2 * leaf_count + 1, neutral_element) {
    const auto first_leaf_index = leaf_count;
    std::ranges::copy(leaf_values, &tree[first_leaf_index]);

    for (auto node = first_leaf_index - 1; root <= node; --node) {
      tree[node] = combine(tree[left_child(node)], tree[right_child(node)]);
    }
  }

  T range_query(Range range) const {
    auto agg = neutral_element;
    for (auto left_node = leaf_count + range.start, right_node = leaf_count + range.end; left_node < right_node;
         left_node = parent(left_node), right_node = parent(right_node)) {
      if (left_node & 1u) {
        agg = combine(tree[left_node++], agg);
      }
      if (right_node & 1u) {
        agg = combine(agg, tree[--right_node]);
      }
    }
    return agg;
  }

 private:
  using NodeIndex = RangeBound;

  static constexpr NodeIndex root = 1;

  // clang-format off

  static NodeIndex parent(NodeIndex node) { return node / 2; }
  static NodeIndex left_child(NodeIndex node) { return 2 * node; }
  static NodeIndex right_child(NodeIndex node) { return 2 * node + 1; }

  // clang-format on

  size_t leaf_count;
  T neutral_element;
  Combine combine;
  std::vector<T> tree;
};

};  // namespace hyrise
