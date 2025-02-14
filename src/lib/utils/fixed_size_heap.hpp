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
    std::pop_heap(_heap.begin(), _heap.end(), Comp{});
    const auto [value, leaf_index] = _heap.back();

    if (leaf_index < 0) {
      return std::nullopt;
    }

    if (_begins[leaf_index] != _ends[leaf_index]) {
      _heap.back() = {*_begins[leaf_index], leaf_index};
      ++_begins[leaf_index];
    } else {
      _heap.back() = {T{}, -1};
    }
    std::push_heap(_heap.begin(), _heap.end(), Comp{});

    return value;
  }

 private:
  std::array<Iterator, Size> _begins;
  std::array<Iterator, Size> _ends;
  std::array<std::pair<T, int8_t>, Size> _heap;
};

}  // namespace hyrise
