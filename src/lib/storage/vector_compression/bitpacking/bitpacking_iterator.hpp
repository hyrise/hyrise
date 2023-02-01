#pragma once

#include <array>
#include <memory>

#include "bitpacking_decompressor.hpp"
#include "bitpacking_vector_type.hpp"
#include "storage/vector_compression/base_compressed_vector.hpp"

namespace hyrise {

class BitPackingIterator : public BaseCompressedVectorIterator<BitPackingIterator> {
 public:
  explicit BitPackingIterator(const pmr_compact_vector& data, const size_t absolute_index = 0u)
      : _data{data}, _absolute_index{absolute_index} {}

  BitPackingIterator(const BitPackingIterator& other) = default;
  BitPackingIterator(BitPackingIterator&& other) = default;

  BitPackingIterator& operator=(const BitPackingIterator& other) {
    if (this == &other) {
      return *this;
    }

    DebugAssert(&_data == &other._data, "Cannot reassign BitPackingIterator");
    _absolute_index = other._absolute_index;
    return *this;
  }

  BitPackingIterator& operator=(BitPackingIterator&& other) {
    if (this == &other) {
      return *this;
    }

    DebugAssert(&_data == &other._data, "Cannot reassign BitPackingIterator");
    _absolute_index = other._absolute_index;
    return *this;
  }

  ~BitPackingIterator() = default;

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

  bool equal(const BitPackingIterator& other) const {
    return _absolute_index == other._absolute_index;
  }

  std::ptrdiff_t distance_to(const BitPackingIterator& other) const {
    return other._absolute_index - _absolute_index;
  }

  uint32_t dereference() const {
    return _data[_absolute_index];
  }

 private:
  const pmr_compact_vector& _data;
  size_t _absolute_index = 0u;
};

}  // namespace hyrise
