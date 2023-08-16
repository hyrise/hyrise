#pragma once

#include <array>
#include <functional>
#include <ranges>

#include "assert.hpp"
#include "comparator_concepts.hpp"

namespace hyrise {

// Min-Heap optimized for small number of elements that are cheap to move around. Instead of using a complicated layout,
// the elements of the heap are stored in order in an array and moved back and forth in linear time.
template <uint8_t max_size, typename T, BooleanComparator<T> Compare = std::less<T>>
class SmallMinHeap {
 public:
  // Constructs a `SmallMinHeap` from a range of initial elements.
  template <typename R>
    requires std::ranges::input_range<R> && std::ranges::sized_range<R>
  explicit SmallMinHeap(R&& initial_elements, Compare init_compare = {}) : _compare(std::move(init_compare)) {
    const auto size = std::ranges::size(initial_elements);
    Assert(size <= max_size, "SmallMinHeap got more than max_size initial elements.");
    _size = static_cast<uint8_t>(size);
    std::ranges::move(initial_elements, _elements.begin());
    std::ranges::sort(std::span(_elements).subspan(0, _size), _compare);
  }

  // Constructs an empty `SmallMinHeap`.
  explicit SmallMinHeap(Compare init_compare = {}) : SmallMinHeap(std::array<T, 0>{}, std::move(init_compare)) {}

  // Returns the number of elements. Runs in constant time.
  uint8_t size() const {
    return _size;
  }

  // Returns true, if the heap contains any elements. Runs in constant time.
  bool empty() const {
    return size() == 0;
  }

  // Adds an element to the heap. Uses `O(size())` many moves of `T` and `O(log(size()))` many calls to `compare`.
  void push(T element) {
    DebugAssert(size() < max_size, "Pushed into already full SmallMinHeap");

    const auto insertion_point =
        std::ranges::partition_point(_elements.begin(), _elements.begin() + _size,
                                     [&](const auto& contained) { return !_compare(element, contained); });
    std::ranges::move_backward(insertion_point, _elements.begin() + _size, _elements.begin() + _size + 1);
    *insertion_point = std::move(element);
    ++_size;
  }

  // Returns the minimal element according to `compare`. Runs in constant time.
  const T& top() const {
    return _elements.front();
  }

  // Removes and returns the top element (see `top()`). Uses `O(size())` many moves of `T`.
  T pop() {
    DebugAssert(size() > 0, "Popped from empty SmallMinHeap");
    auto result = std::move(_elements.front());
    std::ranges::move(_elements.begin() + 1, _elements.begin() + _size, _elements.begin());
    --_size;
    return result;
  }

 private:
  std::array<T, max_size> _elements;
  Compare _compare;
  uint8_t _size;
};

}  // namespace hyrise
