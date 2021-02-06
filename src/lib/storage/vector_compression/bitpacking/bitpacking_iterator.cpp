#include "bitpacking_iterator.hpp"

namespace opossum {

  BitpackingIterator::BitpackingIterator(const pmr_bitpacking_vector<uint32_t>& data, const size_t absolute_index) : _data{data}, _absolute_index{absolute_index}
  {
    
  }

BitpackingIterator& BitpackingIterator::operator=(const BitpackingIterator& other) {
  if (this == &other) return *this;

  DebugAssert(&_data == &other._data, "Cannot reassign BitpackingIterator");
  _absolute_index = other._absolute_index;
  return *this;
}

BitpackingIterator& BitpackingIterator::operator=(BitpackingIterator&& other) {
  if (this == &other) return *this;

  DebugAssert(&_data == &other._data, "Cannot reassign BitpackingIterator");
  _absolute_index = other._absolute_index;
  return *this;
}

// Our code style would want this to be _increment() as it is a private method, but we need to implement boost’s
// interface. Same for the methods below.
void BitpackingIterator::increment() { ++_absolute_index; }  // NOLINT

void BitpackingIterator::decrement() { --_absolute_index; }  // NOLINT

void BitpackingIterator::advance(std::ptrdiff_t n) { _absolute_index += n; }  // NOLINT

bool BitpackingIterator::equal(const BitpackingIterator& other) const {  // NOLINT
  return _absolute_index == other._absolute_index;
}

std::ptrdiff_t BitpackingIterator::distance_to(const BitpackingIterator& other) const {  // NOLINT
  return other._absolute_index - _absolute_index;
}

uint32_t BitpackingIterator::dereference() const { 
  return _data[_absolute_index]; 
}  // NOLINT

}  // namespace opossum
