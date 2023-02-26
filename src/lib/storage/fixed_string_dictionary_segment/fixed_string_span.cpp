#include "fixed_string_span.hpp"

#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "utils/performance_warning.hpp"

namespace hyrise {

FixedStringSpan::FixedStringSpan(const FixedStringVector& vector)
    : _string_length(vector.string_length()), _chars{vector.data(), vector.chars().size()}, _size(vector.size()) {}

FixedStringSpan::FixedStringSpan(const char* start_address, const uint32_t string_length, const uint32_t size)
  : _string_length(string_length), _chars{start_address, size}, _size(size) {}

FixedStringSpanIterator<false> FixedStringSpan::begin() noexcept {
  return {_string_length, _chars, 0};
}

FixedStringSpanIterator<false> FixedStringSpan::end() noexcept {
  return {_string_length, _chars, _string_length == 0 ? 0 : _chars.size()};
}

FixedStringSpanIterator<true> FixedStringSpan::begin() const noexcept {
  return {_string_length, _chars, 0};
}

FixedStringSpanIterator<true> FixedStringSpan::end() const noexcept {
  return {_string_length, _chars, _string_length == 0 ? 0 : _chars.size()};
}

FixedStringSpanIterator<true> FixedStringSpan::cbegin() const noexcept {
  return {_string_length, _chars, 0};
}

FixedStringSpanIterator<true> FixedStringSpan::cend() const noexcept {
  return {_string_length, _chars, _string_length == 0 ? 0 : _chars.size()};
}

using ReverseIterator = boost::reverse_iterator<FixedStringSpanIterator<false>>;

ReverseIterator FixedStringSpan::rbegin() noexcept {
  return ReverseIterator(end());
}

ReverseIterator FixedStringSpan::rend() noexcept {
  return ReverseIterator(begin());
}

// FixedString FixedStringSpan::operator[](const size_t pos) {
//   PerformanceWarning("operator[] used");
//   return {&_chars[pos * _string_length], _string_length};
// }

pmr_string FixedStringSpan::get_string_at(const size_t pos) const {
  const auto* const string_start = &_chars[pos * _string_length];
  // String end checks if the string length is zero to avoid reading the data directly "in front" of `chars`.
  // If the string length is > 0, it is the position of the last char.
  const auto string_end = _string_length == 0 ? 0 : _string_length - 1;

  if (*(string_start + string_end) == '\0') {
    // The string is zero-padded - the pmr_string constructor takes care of finding the correct length
    return {string_start};
  }

  return {string_start, _string_length};
}

const char* FixedStringSpan::data() const {
  return _chars.data();
}

size_t FixedStringSpan::size() const {
  return _size;
}

size_t FixedStringSpan::string_length() const {
  return _string_length;
}

// size_t FixedStringSpan::data_size() const {
//   return sizeof(*this) + _chars.capacity();
// }

}  // namespace hyrise
