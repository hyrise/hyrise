#pragma once

#include <array>
#include <memory>

#include "storage/vector_compression/base_compressed_vector.hpp"

#include "bitpacking_vector.hpp"

#include "vector_types.hpp"
#include "utils/performance_warning.hpp"

namespace opossum {


class BitpackingIterator : public BaseCompressedVectorIterator<BitpackingIterator> {

 public:
  explicit BitpackingIterator(const BitpackingVector& vector, const size_t absolute_index = 0u) : _vector{vector}, _absolute_index{absolute_index}
  {
    
  }

  BitpackingIterator(const BitpackingIterator& other) = default;
  BitpackingIterator(BitpackingIterator&& other) = default;

  BitpackingIterator& operator=(const BitpackingIterator& other) {
    if (this == &other) return *this;

    DebugAssert(&_vector == &other._vector, "Cannot reassign BitpackingIterator");
    _absolute_index = other._absolute_index;
    return *this;
  }

  BitpackingIterator& operator=(BitpackingIterator&& other) {
    if (this == &other) return *this;

    DebugAssert(&_vector == &other._vector, "Cannot reassign BitpackingIterator");
    _absolute_index = other._absolute_index;
    return *this;
  }

  ~BitpackingIterator() = default;

 private:
  friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

  void increment() {
     ++_absolute_index;
  }
  void decrement() {
    --_absolute_index;
  }

  void advance(std::ptrdiff_t n) {
    _absolute_index += n;
  }

  bool equal(const BitpackingIterator& other) const {
    return _absolute_index == other._absolute_index;
  }

  std::ptrdiff_t distance_to(const BitpackingIterator& other) const {
    return other._absolute_index - _absolute_index;
  }

  uint32_t dereference() const {
    return _vector.get(_absolute_index);
  };

 private:
  const BitpackingVector& _vector;
  size_t _absolute_index;
};

}  // namespace opossum
