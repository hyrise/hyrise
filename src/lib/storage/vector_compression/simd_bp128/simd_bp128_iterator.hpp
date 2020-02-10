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
  SimdBp128Iterator(std::unique_ptr<SimdBp128Decompressor>&& decompressor, const size_t absolute_index = 0u)
      : _decompressor{std::move(decompressor)}, _absolute_index{absolute_index} {}

  explicit SimdBp128Iterator(const SimdBp128Iterator& other)
      : _decompressor{std::make_unique<SimdBp128Decompressor>(SimdBp128Decompressor(*other._decompressor))},
        _absolute_index{other._absolute_index} {}

  SimdBp128Iterator(SimdBp128Iterator&& other)
      : _decompressor{std::move(other._decompressor)}, _absolute_index{std::move(other._absolute_index)} {}

  SimdBp128Iterator& operator=(const SimdBp128Iterator& other) {
    if (this == &other) return *this;

    _decompressor = std::make_unique<SimdBp128Decompressor>(SimdBp128Decompressor(*other._decompressor));
    _absolute_index = other._absolute_index;
    return *this;
  }

  SimdBp128Iterator& operator=(SimdBp128Iterator&& other) = default;

  ~SimdBp128Iterator() = default;

 private:
  friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

  void increment() { ++_absolute_index; }

  void decrement() { --_absolute_index; }

  void advance(std::ptrdiff_t n) { _absolute_index += n; }

  bool equal(const SimdBp128Iterator& other) const { return _absolute_index == other._absolute_index; }

  std::ptrdiff_t distance_to(const SimdBp128Iterator& other) const { return other._absolute_index - _absolute_index; }

  uint32_t dereference() const { return _decompressor->get(_absolute_index); }

 private:
  std::unique_ptr<SimdBp128Decompressor> _decompressor;
  size_t _absolute_index;
};

}  // namespace opossum
