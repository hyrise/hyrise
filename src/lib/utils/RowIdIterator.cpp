#include "RowIdIterator.h"

namespace hyrise {
RowIdIterator::RowIdIterator(std::vector<unsigned char>& buffer, const uint64_t tuple_key_size, const bool is_end)
    : _buffer(buffer),
      _tuple_key_size(tuple_key_size),
      _current_offset(is_end ? buffer.size() + tuple_key_size - sizeof(RowID) : tuple_key_size - sizeof(RowID)) {}

RowIdIterator::RowIdIterator(std::vector<unsigned char>& buffer, const uint64_t tuple_key_size,
                             const uint64_t current_offset)
    : _buffer(buffer), _tuple_key_size(tuple_key_size), _current_offset(current_offset) {}

 RowIdIterator::RowIdIterator(const RowIdIterator& other) = default;

RowIdIterator& RowIdIterator::operator=(const RowIdIterator& other) = default;

RowID RowIdIterator::operator*() const {
  auto result = RowID{};
  std::memcpy(&result, &_buffer[_current_offset], sizeof(RowID));
  return result;
}

RowIdIterator& RowIdIterator::operator++() {
  _current_offset += _tuple_key_size;
  return *this;
}

// This is probably not a good idea to use from a performance standpoint. Consider using operator++().
RowIdIterator RowIdIterator::operator++(int) {
  const auto old_value = RowIdIterator(_buffer, _tuple_key_size, _current_offset);
  _current_offset += _tuple_key_size;
  return old_value;
}

const RowID* RowIdIterator::operator->() const {
  return reinterpret_cast<const RowID*>(&_buffer[_current_offset]);
}

// This implementation is inherently unsafe, but should be fine as a RowIdIterator should only ever be compared to its
// end() counterpart.
bool operator==(const RowIdIterator& lhs, const RowIdIterator& rhs) {
  return lhs._current_offset >= rhs._current_offset;
}

// This implementation is inherently unsafe, but should be fine as a RowIdIterator should only ever be compared to its
// end() counterpart.
bool operator!=(const RowIdIterator& lhs, const RowIdIterator& rhs) {
  return !(lhs == rhs);
}

}  // namespace hyrise
