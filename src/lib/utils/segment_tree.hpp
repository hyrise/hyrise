#pragma once

#include <algorithm>
#include <bit>  // NOLINT(build/include_order)
#include <cstdint>
#include <span>  // NOLINT(build/include_order)
#include <vector>

namespace hyrise {

struct QueryRange {
  using Bound = uint64_t;

  Bound start;
  Bound end;
};

template <typename T>
struct ElementFactory {
  constexpr T operator()() const {
    return element;
  }

  T element;
};

template <typename T>
constexpr ElementFactory<T> element_factory(T element) {
  return {element};
}

// A segment tree is a data structure that supports efficiently computing aggregates over arbitrary connected intervals
// (`Range`s) of the underlying array of values. The aggregate function, `combine`, must
//
// 1. be associative,
// 2. have neutral element `neutral_element`, and
// 3. be commutative.
//
// Additionally, `combine` should be cheap to compute, usually in constant time. The commutativity requirement can be
// avoided by changing the implementation of `range_query` accordingly. The resulting complexities are
//
// - O(n) for construction,
// - O(log(n)) for a range query, and
// - O(log(n)) for a range update with lazy propagation (currently not implemented).
//
// Note that the current implementation is a read-only segment tree.
//
// In general, segment trees work as follows: Assume that n is a power of two, otherwise round up to the next one. Each
// slot of the input array forms a leaf in a completely filled binary tree of height log(n). Each inner node has the
// `combine`d value of its two children. The nodes of the tree are not allocated individually on the heap, but are
// stored in one contiguous vector that is allocated during construction. To navigate the tree, the common 1-based
// binary tree indexing method is used (see `parent`, `left_child`, `right_child`), that is, the root is at index 1.
// The first leaf is at index n.
//
// Refer to the documentation of `range_query` for details on its implementation.
template <typename T, typename Combine, typename MakeNeutralElement = ElementFactory<T>>
class SegmentTree {
  friend class SegmentTreeTest;

 public:
  explicit SegmentTree(std::span<const T> leaf_values, Combine init_combine,
                       MakeNeutralElement init_make_neutral_element)
      : leaf_count(std::bit_ceil(leaf_values.size())),
        combine(std::move(init_combine)),
        make_neutral_element(std::move(init_make_neutral_element)),
        tree(2 * leaf_count + 1, make_neutral_element()) {
    const auto first_leaf_index = leaf_count;
    std::ranges::copy(leaf_values, &tree[first_leaf_index]);

    for (auto node = first_leaf_index - 1; root <= node; --node) {
      tree[node] = combine(tree[left_child(node)], tree[right_child(node)]);
    }
  }

  // Returns the aggregate of the leaf values in the queried range.
  T range_query(QueryRange range) const {
    // The algorithm works like this: There are two pointers, left_node and right_node, that span the range that still
    // needs to be included in the query aggregate. Initially, they start at the corresponding leaf node values. As with
    // the query interface, the left_node is inclusive and the right_node is exclusive (that is, right_node could point
    // outside of the tree array).
    // In each iteration, the pointers update one level towards the root of the tree. Whenever the next parent of one of
    // the pointers would not be fully contained in `range`, the current node's value is `combine`d into the aggregate
    // result and the pointer moves inward. After moving inward, the new node's parent must be fully included in the
    // range again. A node must be included exactly if its index is odd, because each level (except for the root) starts
    // with an even index and the leftmost node is always a left child.
    //
    // This process continues until both pointers point to the same node. This final node is explicitly not included in
    // the query aggregate, because by definition, the right pointer is exclusive, so the interval of the two pointers
    // is now empty.
    //
    // Tiny example: sum query for the range [1, 4):
    // (read algorithm steps from bottom to top; l and r are short for left_node and right_node respectively)
    //
    //                  6        l and r are both after 6 (index 2) ; query is done
    //                1   5      l is on 5, r is after 5 (index 4)  ; 5 is included from l in the aggregate
    //               0 1 2 3     l is on 1, r is after 3 (index 8)  ; 1 is included from l in the aggregate
    //
    // Leaf index:   4 5 6 7

    DebugAssert(range.start <= range.end, "Got malformed Range in SegmentTree::range_query.");
    DebugAssert(range.end <= leaf_count, "Query range out of bounds.");

    // Note: To avoid the commutativity assumption, two separate aggregates are needed.
    auto agg = make_neutral_element();
    auto left_node = leaf_count + range.start;
    auto right_node = leaf_count + range.end;
    while (left_node < right_node) {
      if (left_node & 1u)
        agg = combine(tree[left_node++], agg);
      if (right_node & 1u)
        agg = combine(agg, tree[--right_node]);
      left_node = parent(left_node);
      right_node = parent(right_node);
    }
    return agg;
  }

 private:
  using NodeIndex = QueryRange::Bound;

  static constexpr NodeIndex root = 1;

  // clang-format off

  static NodeIndex parent(NodeIndex node) { return node / 2; }
  static NodeIndex left_child(NodeIndex node) { return 2 * node; }
  static NodeIndex right_child(NodeIndex node) { return 2 * node + 1; }

  // clang-format on

  size_t leaf_count;
  Combine combine;
  MakeNeutralElement make_neutral_element;
  std::vector<T> tree;
};

};  // namespace hyrise
