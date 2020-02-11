#include "simd_bp128_iterator.hpp"

namespace opossum {

SimdBp128Iterator::SimdBp128Iterator(SimdBp128Decompressor&& decompressor,
		                     const size_t absolute_index)
    : _decompressor{std::move(decompressor)}, _absolute_index{absolute_index} {}

SimdBp128Iterator::SimdBp128Iterator(const SimdBp128Iterator& other)
    : _decompressor{SimdBp128Decompressor(other._decompressor)},
      _absolute_index{other._absolute_index} {}

SimdBp128Iterator::SimdBp128Iterator(SimdBp128Iterator&& other)
    : _decompressor{std::move(other._decompressor)}, _absolute_index{std::move(other._absolute_index)} {}

SimdBp128Iterator& SimdBp128Iterator::operator=(const SimdBp128Iterator& other) {
  if (this == &other) return *this;

  _decompressor = SimdBp128Decompressor(other._decompressor);
  _absolute_index = other._absolute_index;
  return *this;
}

void SimdBp128Iterator::increment() { ++_absolute_index; }

void SimdBp128Iterator::decrement() { --_absolute_index; }

void SimdBp128Iterator::advance(std::ptrdiff_t n) { _absolute_index += n; }

bool SimdBp128Iterator::equal(const SimdBp128Iterator& other) const { return _absolute_index == other._absolute_index; }

std::ptrdiff_t SimdBp128Iterator::distance_to(const SimdBp128Iterator& other) const {
  return other._absolute_index - _absolute_index;
}

uint32_t SimdBp128Iterator::dereference() const { return _decompressor.get(_absolute_index); }

}  // namespace opossum
