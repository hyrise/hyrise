#include "fixed_string_vector.hpp"

#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "utils/performance_warning.hpp"

namespace hyrise {

FixedStringVector::FixedStringVector(const FixedStringVector& other, const PolymorphicAllocator<char>& allocator)
    : _string_length(other._string_length), _chars(other._chars, allocator), _size(other._size) {
  // For pmr_vectors, operator= does not change the allocator. As such, we need to set _chars in the initializer list.
  // Otherwise, it would be created using the default allocator and ignore the passed-in allocator.
}

void FixedStringVector::push_back(const pmr_string& string) {
  Assert(string.size() <= _string_length, "Inserted string is too long to insert in FixedStringVector");
  const auto pos = _chars.size();
  // Default value of inserted elements using resize is null terminator ('\0')
  _chars.resize(_chars.size() + _string_length);
  string.copy(&_chars[pos], string.size());

  ++_size;
}

FixedStringIterator<false> FixedStringVector::begin() noexcept {
  return {_string_length, _chars, 0};
}

FixedStringIterator<false> FixedStringVector::end() noexcept {
  return {_string_length, _chars, _string_length == 0 ? 0 : _chars.size()};
}

FixedStringIterator<true> FixedStringVector::begin() const noexcept {
  return {_string_length, _chars, 0};
}

FixedStringIterator<true> FixedStringVector::end() const noexcept {
  return {_string_length, _chars, _string_length == 0 ? 0 : _chars.size()};
}

FixedStringIterator<true> FixedStringVector::cbegin() const noexcept {
  return {_string_length, _chars, 0};
}

FixedStringIterator<true> FixedStringVector::cend() const noexcept {
  return {_string_length, _chars, _string_length == 0 ? 0 : _chars.size()};
}

using ReverseIterator = boost::reverse_iterator<FixedStringIterator<false>>;

ReverseIterator FixedStringVector::rbegin() noexcept {
  return ReverseIterator(end());
}

ReverseIterator FixedStringVector::rend() noexcept {
  return ReverseIterator(begin());
}

FixedString FixedStringVector::operator[](const size_t pos) {
  PerformanceWarning("operator[] used");
  return {&_chars[pos * _string_length], _string_length};
}

FixedString FixedStringVector::at(const size_t pos) {
  return {&_chars.at(pos * _string_length), _string_length};
}

pmr_string FixedStringVector::get_string_at(const size_t pos) const {
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

const char* FixedStringVector::data() const {
  return _chars.data();
}

size_t FixedStringVector::size() const {
  return _size;
}

size_t FixedStringVector::capacity() const {
  return _chars.capacity();
}

size_t FixedStringVector::string_length() const {
  return _string_length;
}

void FixedStringVector::erase(const FixedStringIterator<false> start, const FixedStringIterator<false> end) {
  const auto count = std::distance(start, end);

  if (_string_length == 0) {
    _size -= count;
    return;
  }

  auto iter = _chars.begin();
  std::advance(iter, _chars.size() - count * _string_length);
  _chars.erase(iter, _chars.end());
  _size -= count;
}

void FixedStringVector::shrink_to_fit() {
  _chars.shrink_to_fit();
}

PolymorphicAllocator<FixedString> FixedStringVector::get_allocator() {
  return _chars.get_allocator();
}

void FixedStringVector::reserve(const size_t size) {
  _chars.reserve(size * _string_length);
}

size_t FixedStringVector::data_size() const {
  return sizeof(*this) + _chars.capacity();
}

}  // namespace hyrise
