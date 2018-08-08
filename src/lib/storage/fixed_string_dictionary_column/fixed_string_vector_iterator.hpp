#pragma once

#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "fixed_string.hpp"
#include "type_cast.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

// We need a custom iterator for this vector, since we have to perform jumps when iterating over the vector.
// Depending on OnConstStorage, it either returns a (mutable) FixedString or an (immutable) std::string_view
template <bool OnConstStorage,
          typename Storage = std::conditional_t<OnConstStorage, const pmr_vector<char>, pmr_vector<char>>,
          typename DereferenceValue = std::conditional_t<OnConstStorage, const std::string_view, FixedString>>
class FixedStringIterator : public boost::iterator_facade<FixedStringIterator<OnConstStorage>, DereferenceValue,
                                                          std::random_access_iterator_tag, DereferenceValue> {
 public:
  FixedStringIterator(size_t string_length, Storage& vector, size_t pos = 0)
      : _string_length(string_length), _chars(vector), _pos(pos) {}

  FixedStringIterator& operator=(const FixedStringIterator& other) {
    DebugAssert(_string_length == other._string_length && &_chars == &other._chars,
                "can't convert pointers from different vectors");
    _pos = other._pos;
    return *this;
  }

 private:
  friend class boost::iterator_core_access;

  // We have a couple of NOLINTs here becaues the facade expects these method names:

  bool equal(FixedStringIterator const& other) const {  // NOLINT
    return _chars == other._chars && _pos == other._pos;
  }

  size_t distance_to(FixedStringIterator const& other) const {  // NOLINT
    if (_string_length == 0) return 0;
    return (std::intptr_t(other._pos) - std::intptr_t(this->_pos)) / std::intptr_t(_string_length);
  }

  void advance(size_t n) {  // NOLINT
    _pos += n * _string_length;
  }

  void increment() {  // NOLINT
    _pos += _string_length;
  }

  void decrement() {  // NOLINT
    _pos -= _string_length;
  }

  template <bool OnConstStorageLocal = OnConstStorage>
  std::enable_if_t<OnConstStorageLocal, const std::string_view> dereference() const {  // NOLINT
    return std::string_view{&_chars[_pos], strnlen(&_chars[_pos], _string_length)};
  }

  template <bool OnConstStorageLocal = OnConstStorage>
  std::enable_if_t<!OnConstStorageLocal, FixedString> dereference() const {  // NOLINT
    return FixedString{&_chars[_pos], _string_length};
  }

  const size_t _string_length;
  Storage& _chars;
  size_t _pos;
};

}  // namespace opossum
