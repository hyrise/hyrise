#pragma once

#include <cstddef>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "fixed_string.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

// We need a custom iterator for this vector, since we have to perform jumps when iterating over the vector.
// Depending on OnConstStorage, it either returns a (mutable) FixedString or an (immutable) std::string_view
template <bool on_const_storage,
          typename Storage = std::conditional_t<on_const_storage, const pmr_vector<char>, pmr_vector<char>>,
          typename DereferenceValue = std::conditional_t<on_const_storage, const std::string_view, FixedString>>
class FixedStringIterator : public boost::iterator_facade<FixedStringIterator<on_const_storage>, DereferenceValue,
                                                          std::random_access_iterator_tag, DereferenceValue> {
  using ValueType = std::string_view;

 public:
  FixedStringIterator(size_t string_length, Storage& vector, ptrdiff_t pos = 0)
      : _string_length(string_length), _chars(vector), _pos(pos) {}

  FixedStringIterator(const FixedStringIterator&) = default;
  FixedStringIterator(FixedStringIterator&&) = default;

  FixedStringIterator& operator=(const FixedStringIterator& other) {
    if (this == &other) {
      return *this;
    }
    DebugAssert(_string_length == other._string_length && &_chars == &other._chars,
                "can't convert pointers from different vectors");
    _pos = other._pos;
    return *this;
  }

  // NOLINTNEXTLINE(cppcoreguidelines-noexcept-move-operations,hicpp-noexcept-move,performance-noexcept-move-constructor)
  FixedStringIterator& operator=(FixedStringIterator&& other) {
    if (this == &other) {
      return *this;
    }
    *this = const_cast<const FixedStringIterator&>(other);
    return *this;
  }

  ~FixedStringIterator() = default;

 private:
  friend class boost::iterator_core_access;

  // We have a couple of NOLINTs here becaues the facade expects these method names:

  bool equal(FixedStringIterator const& other) const {  // NOLINT(readability-identifier-naming)
    return &_chars == &other._chars && _pos == other._pos;
  }

  ptrdiff_t distance_to(FixedStringIterator const& other) const {  // NOLINT(readability-identifier-naming)
    if (_string_length == 0) {
      return 0;
    }
    return (static_cast<intptr_t>(other._pos) - static_cast<intptr_t>(this->_pos)) /
           static_cast<intptr_t>(_string_length);
  }

  void advance(size_t n) {  // NOLINT(readability-identifier-naming)
    _pos += n * _string_length;
  }

  void increment() {  // NOLINT(readability-identifier-naming)
    _pos += _string_length;
  }

  void decrement() {  // NOLINT(readability-identifier-naming)
    _pos -= _string_length;
  }

  template <bool on_const_storage_local = on_const_storage>
  // NOLINTNEXTLINE(readability-identifier-naming)
  std::enable_if_t<on_const_storage_local, const std::string_view> dereference() const {
    return std::string_view{&_chars[_pos], strnlen(&_chars[_pos], _string_length)};
  }

  template <bool on_const_storage_local = on_const_storage>
  std::enable_if_t<!on_const_storage_local, FixedString> dereference() const {  // NOLINT(readability-identifier-naming)
    return FixedString{&_chars[_pos], _string_length};
  }

  const size_t _string_length;
  Storage& _chars;
  ptrdiff_t _pos;
};

}  // namespace hyrise
