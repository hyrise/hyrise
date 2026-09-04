#pragma once

#include <cstddef>
#include <functional>
#include <ranges>
#include <vector>

namespace hyrise {

template <typename T, typename CompareValues = std::greater<T>>
class LoserTree {
 public:
  explicit LoserTree(std::size_t capacity) : _nodes(capacity) {}

  size_t champion_node_index() const {
    return _nodes[0]._slot;
  }

  const T& peek() const {
    return _nodes[0]._element;
  }

  bool empty() const {
    return _nodes[0]._tag == Tag::HighSentinel;
  }

  void push_to_leaf(const T& value, const size_t slot) {
    _update(Tag::Regular, value, slot, CompareOnElements{});
  }

  void invalidate(const size_t slot) {
    _update(Tag::HighSentinel, {}, slot, CompareOnElements{});
  }

  void reset() {
    std::ranges::fill(_nodes, Node{});
    for (auto node_idx = size_t{0}; node_idx < _nodes.size(); ++node_idx) {
      _update(Tag::LowSentinel, {}, node_idx, CompareOnSlots{});
    }
  }

 private:
  enum class Tag : uint8_t {
    None = 0b00,
    LowSentinel = 0b01,
    Regular = 0b10,
    HighSentinel = 0b11,
  };

  struct Node {
    Tag _tag{Tag::None};
    T _element{};
    size_t _slot{};

    Node() = default;

    Node(const Tag tag, const T& element, const size_t slot) : _tag(tag), _element(element), _slot(slot) {}
  };

  struct CompareOnElements {
    bool operator()(const Node& lhs, const Node& rhs) const {
      return CompareValues{}(lhs._element, rhs._element);
    }
  };

  struct CompareOnSlots {
    bool operator()(const Node& lhs, const Node& rhs) const {
      return lhs._slot > rhs._slot;
    }
  };

  template <typename Compare>
  void _update(const Tag tag, const T& value, const size_t& slot, Compare comp) {
    auto path_winner_node = Node(tag, value, slot);
    auto parent = (_nodes.size() + slot) / 2;
    while (parent > 0) {
      auto loser_node = _nodes[parent];
      auto winner_tag = std::to_underlying(path_winner_node._tag);
      auto loser_tag = std::to_underlying(loser_node._tag);
      if (winner_tag > loser_tag || (winner_tag == loser_tag ? comp(path_winner_node, loser_node) : false)) {
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
