#pragma once

#include <array>
#include <optional>
#include <vector>

namespace hyrise {

template <typename T, int8_t Size, typename Iterator>
class FixedSizeHeap : public Noncopyable {
 public:
  FixedSizeHeap(std::vector<Iterator> begins,
                std::vector<Iterator> ends) {

    DebugAssert(begins.size() == Size, "Unepected number of passed iterators.");
    DebugAssert(ends.size() == Size, "Unepected number of passed iterators.");

    for (auto index = int8_t{0}; index < Size; ++index) {
      _begins[index] = begins[index];
      _ends[index] = ends[index];

      if (_begins[index] != _ends[index]) {
        _heap[index] = {*_begins[index], index};
        ++_begins[index];
        ++_active_leaves_count;
      } else {
        _heap[index] = {T{}, -1};
      }
    }

    std::ranges::make_heap(_heap, Comp{});
  }

  struct Comp {
    bool operator() (const std::pair<T, int8_t>& lhs, const std::pair<T, int8_t>& rhs) {
        if (lhs.second < 0) {
          return true;
        }

        if (rhs.second < 0) {
          return false;
        }

        return lhs.first > rhs.first;
    }
  };

  std::optional<T> next() {
    auto heap_end = _heap.begin() + std::max(size_t{1}, _active_leaves_count);
    auto heap_back = heap_end - 1;

    std::ranges::pop_heap(_heap.begin(), heap_end, Comp{});
    const auto [value, leaf_index] = *heap_back;

    if (leaf_index < 0) {
      return std::nullopt;
    }

    if (_begins[leaf_index] != _ends[leaf_index]) {
      *heap_back = {*_begins[leaf_index], leaf_index};
      ++_begins[leaf_index];
      std::ranges::push_heap(_heap.begin(), heap_end, Comp{});
    } else {
      *heap_back = {T{}, -1};
      --_active_leaves_count;
      std::ranges::push_heap(_heap.begin(), heap_back, Comp{});
    }

    return value;
  }

 private:
  // Store the number of leaves we can still pull from. When several leaves are already emptied, we can avoid 
  size_t _active_leaves_count{0};

  std::array<Iterator, Size> _begins;
  std::array<Iterator, Size> _ends;
  std::array<std::pair<T, int8_t>, Size> _heap;
};

}  // namespace hyrise
