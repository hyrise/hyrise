#pragma once

#include <array>
#include <memory>

#include "storage/vector_compression/base_compressed_vector.hpp"

#include "oversized_types.hpp"
#include "simd_bp128_decompressor.hpp"
#include "simd_bp128_packing.hpp"

#include "types.hpp"
#include "utils/performance_warning.hpp"

namespace opossum {

class SimdBp128Iterator : public BaseCompressedVectorIterator<SimdBp128Iterator> {
 public:
  using Packing = SimdBp128Packing;

 public:
  // The SimdBp128Iterator simply wraps a SimdBp128Decompressor (which implements the logic to cache and extract
  // blocks/meta blocks) to allow STL algorithms (e.g., std::lower_bound) to be used on a compressed vector. However,
  // please be aware of #1531. The fixed-size-byte-aligned vector compression uses std::vector for the underlying data
  // and does thus not require an additional iterator.
  explicit SimdBp128Iterator(SimdBp128Decompressor&& decompressor, const size_t absolute_index = 0u);
  SimdBp128Iterator(const SimdBp128Iterator& other);
  SimdBp128Iterator(SimdBp128Iterator&& other) noexcept;

  SimdBp128Iterator& operator=(const SimdBp128Iterator& other);
  SimdBp128Iterator& operator=(SimdBp128Iterator&& other) = default;

  ~SimdBp128Iterator() = default;

 private:
  friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

  void increment();
  void decrement();
  void advance(std::ptrdiff_t n);
  bool equal(const SimdBp128Iterator& other) const;
  std::ptrdiff_t distance_to(const SimdBp128Iterator& other) const;
  uint32_t dereference() const;

 private:
  mutable SimdBp128Decompressor _decompressor;
  size_t _absolute_index;
};

}  // namespace opossum
