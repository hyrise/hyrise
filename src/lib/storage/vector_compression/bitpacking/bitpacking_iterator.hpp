#pragma once

#include <array>
#include <memory>

#include "storage/vector_compression/base_compressed_vector.hpp"

#include "bitpacking_decompressor.hpp"

#include "vector_types.hpp"
#include "utils/performance_warning.hpp"

namespace opossum {

class BitpackingIterator : public BaseCompressedVectorIterator<BitpackingIterator> {

 public:
  explicit BitpackingIterator(BitpackingDecompressor&& decompressor, const size_t absolute_index = 0u);
  BitpackingIterator(const BitpackingIterator& other);
  BitpackingIterator(BitpackingIterator&& other) noexcept;

  BitpackingIterator& operator=(const BitpackingIterator& other);
  BitpackingIterator& operator=(BitpackingIterator&& other) = default;

  ~BitpackingIterator() = default;

 private:
  friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

  void increment();
  void decrement();
  void advance(std::ptrdiff_t n);
  bool equal(const BitpackingIterator& other) const;
  std::ptrdiff_t distance_to(const BitpackingIterator& other) const;
  uint32_t dereference() const;

 private:
  mutable BitpackingDecompressor _decompressor;
  size_t _absolute_index;
};

}  // namespace opossum
