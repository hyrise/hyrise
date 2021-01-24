#pragma once

#include <array>
#include <memory>

#include "storage/vector_compression/base_compressed_vector.hpp"

#include "turboPFor_bitpacking_decompressor.hpp"

#include "types.hpp"
#include "utils/performance_warning.hpp"

namespace opossum {

class TurboPForBitpackingIterator : public BaseCompressedVectorIterator<TurboPForBitpackingIterator> {

 public:
  explicit TurboPForBitpackingIterator(TurboPForBitpackingDecompressor&& decompressor, const size_t absolute_index = 0u);
  TurboPForBitpackingIterator(const TurboPForBitpackingIterator& other);
  TurboPForBitpackingIterator(TurboPForBitpackingIterator&& other) noexcept;

  TurboPForBitpackingIterator& operator=(const TurboPForBitpackingIterator& other);
  TurboPForBitpackingIterator& operator=(TurboPForBitpackingIterator&& other) = default;

  ~TurboPForBitpackingIterator() = default;

 private:
  friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

  void increment();
  void decrement();
  void advance(std::ptrdiff_t n);
  bool equal(const TurboPForBitpackingIterator& other) const;
  std::ptrdiff_t distance_to(const TurboPForBitpackingIterator& other) const;
  uint32_t dereference() const;

 private:
  mutable TurboPForBitpackingDecompressor _decompressor;
  size_t _absolute_index;
};

}  // namespace opossum
