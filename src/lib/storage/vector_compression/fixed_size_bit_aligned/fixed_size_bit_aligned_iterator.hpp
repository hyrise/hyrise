#pragma once

#include <array>
#include <memory>

#include "fixed_size_bit_aligned_decompressor.hpp"
#include "fixed_size_bit_aligned_vector_type.hpp"
#include "storage/vector_compression/base_compressed_vector.hpp"

namespace opossum {

class FixedSizeBitAlignedIterator : public BaseCompressedVectorIterator<FixedSizeBitAlignedIterator> {
 public:
  explicit FixedSizeBitAlignedIterator(const pmr_compact_vector<uint32_t>& data, const size_t absolute_index = 0u)
      : _data{data}, _absolute_index{absolute_index} {}

  FixedSizeBitAlignedIterator(const FixedSizeBitAlignedIterator& other) = default;
  FixedSizeBitAlignedIterator(FixedSizeBitAlignedIterator&& other) = default;

  FixedSizeBitAlignedIterator& operator=(const FixedSizeBitAlignedIterator& other) {
    if (this == &other) return *this;

    DebugAssert(&_data == &other._data, "Cannot reassign FixedSizeBitAlignedIterator");
    _absolute_index = other._absolute_index;
    return *this;
  }

  FixedSizeBitAlignedIterator& operator=(FixedSizeBitAlignedIterator&& other) {
    if (this == &other) return *this;

    DebugAssert(&_data == &other._data, "Cannot reassign FixedSizeBitAlignedIterator");
    _absolute_index = other._absolute_index;
    return *this;
  }

  ~FixedSizeBitAlignedIterator() = default;

 private:
  friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

  void increment() { ++_absolute_index; }
  void decrement() { --_absolute_index; }

  void advance(std::ptrdiff_t n) { _absolute_index += n; }

  bool equal(const FixedSizeBitAlignedIterator& other) const { return _absolute_index == other._absolute_index; }

  std::ptrdiff_t distance_to(const FixedSizeBitAlignedIterator& other) const { return other._absolute_index - _absolute_index; }

  uint32_t dereference() const { return _data[_absolute_index]; }

 private:
  const pmr_compact_vector<uint32_t>& _data;
  size_t _absolute_index;
};

}  // namespace opossum
