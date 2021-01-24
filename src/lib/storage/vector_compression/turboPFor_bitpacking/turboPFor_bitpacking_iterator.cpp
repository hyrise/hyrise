#include "turboPFor_bitpacking_iterator.hpp"

namespace opossum {

TurboPForBitpackingIterator::TurboPForBitpackingIterator(TurboPForBitpackingDecompressor&& decompressor, const size_t absolute_index)
    : _decompressor{std::move(decompressor)}, _absolute_index{absolute_index} {}

TurboPForBitpackingIterator::TurboPForBitpackingIterator(const TurboPForBitpackingIterator& other)
    : _decompressor{TurboPForBitpackingDecompressor(other._decompressor)}, _absolute_index{other._absolute_index} {}

TurboPForBitpackingIterator::TurboPForBitpackingIterator(TurboPForBitpackingIterator&& other) noexcept
    : _decompressor{std::move(other._decompressor)}, _absolute_index{other._absolute_index} {}

TurboPForBitpackingIterator& TurboPForBitpackingIterator::operator=(const TurboPForBitpackingIterator& other) {
  if (this == &other) return *this;

  _decompressor = TurboPForBitpackingDecompressor(other._decompressor);
  _absolute_index = other._absolute_index;
  return *this;
}

// Our code style would want this to be _increment() as it is a private method, but we need to implement boost’s
// interface. Same for the methods below.
void TurboPForBitpackingIterator::increment() { ++_absolute_index; }  // NOLINT

void TurboPForBitpackingIterator::decrement() { --_absolute_index; }  // NOLINT

void TurboPForBitpackingIterator::advance(std::ptrdiff_t n) { _absolute_index += n; }  // NOLINT

bool TurboPForBitpackingIterator::equal(const TurboPForBitpackingIterator& other) const {  // NOLINT
  return _absolute_index == other._absolute_index;
}

std::ptrdiff_t TurboPForBitpackingIterator::distance_to(const TurboPForBitpackingIterator& other) const {  // NOLINT
  return other._absolute_index - _absolute_index;
}

uint32_t TurboPForBitpackingIterator::dereference() const { 
  return _decompressor.get(_absolute_index); 
}  // NOLINT

}  // namespace opossum
