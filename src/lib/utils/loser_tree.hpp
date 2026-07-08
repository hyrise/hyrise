#pragma once

#include <ranges>
#include <vector>

namespace hyrise {

template <typename T, typename CompareValues>
class LoserTree {
 public:
  using value_type = T;
  using node_index_type = std::size_t;

  explicit LoserTree(std::size_t capacity) : _nodes(capacity) {}

  [[nodiscard]] node_index_type champion_node_index() const {
    return _nodes[0].slot;
  }

  [[nodiscard]] value_type peek() const {
    return _nodes[0].element;
  }

  [[nodiscard]] bool empty() const {
    return _nodes[0].is_high_sentinel;
  }

  void push_to_leaf(const value_type& value, const node_index_type slot) {
    _update(value, slot, false, false, CompareOnElements{});
  }

  void invalidate(const node_index_type slot) {
    _update({}, slot, false, true, CompareOnElements{});
  }

  void reset() {
    std::ranges::fill(_nodes, Node{});
    for (auto node_idx = size_t{0}; node_idx < _nodes.size(); ++node_idx) {
      _update({}, node_idx, true, false, CompareOnSlots{});
    }
  }

 private:
  struct Node {
    value_type element;
    node_index_type slot;
    bool is_low_sentinel;
    bool is_high_sentinel;
  };

  struct CompareOnElements {
    bool operator()(const Node& lhs, const Node& rhs) const {
      return CompareValues{}(lhs.element, rhs.element);
    }
  };

  struct CompareOnSlots {
    bool operator()(const Node& lhs, const Node& rhs) const {
      return lhs.slot > rhs.slot;
    }
  };

  template <typename Compare>
  void _update(const value_type& value, const node_index_type& slot, bool is_low_sentinel, bool is_high_sentinel,
               Compare comp) {
    auto path_winner_node =
        Node{.element = value, .slot = slot, .is_low_sentinel = is_low_sentinel, .is_high_sentinel = is_high_sentinel};
    auto parent = (_nodes.size() + slot) / 2;
    while (parent > 0) {
      auto loser_node = _nodes[parent];
      const auto must_swap_high_sen = path_winner_node.is_high_sentinel;
      const auto must_swap_low_sen = loser_node.is_low_sentinel;
      const auto must_swap_comp = !path_winner_node.is_low_sentinel && !path_winner_node.is_high_sentinel &&
                                  !loser_node.is_low_sentinel && !loser_node.is_high_sentinel &&
                                  comp(path_winner_node, loser_node);
      if (must_swap_high_sen || must_swap_low_sen || must_swap_comp) {
        _nodes[parent] = path_winner_node;
        path_winner_node = loser_node;
      }
      parent /= 2;
    }
    _nodes[0] = path_winner_node;
  }

  std::vector<Node> _nodes;
};

}  // namespace hyrise
