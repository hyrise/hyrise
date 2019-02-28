#include "fixed_string_vector.hpp"

#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "utils/performance_warning.hpp"

namespace opossum {

void FixedStringVector::push_back(const pmr_string& string) {
  DebugAssert(string.size() <= _string_length, "Inserted string is too long to insert in FixedStringVector");
  const auto pos = _chars.size();
  // Default value of inserted elements using resize is null terminator ('\0')
  _chars.resize(_chars.size() + _string_length);
  string.copy(&_chars[pos], string.size());
}

FixedStringIterator<false> FixedStringVector::begin() noexcept {
  return FixedStringIterator<false>(_string_length, _chars, 0);
}

FixedStringIterator<false> FixedStringVector::end() noexcept {
  return FixedStringIterator<false>(_string_length, _chars, _string_length == 0 ? 0 : _chars.size());
}

FixedStringIterator<true> FixedStringVector::begin() const noexcept {
  return FixedStringIterator<true>(_string_length, _chars, 0);
}

FixedStringIterator<true> FixedStringVector::end() const noexcept {
  return FixedStringIterator<true>(_string_length, _chars, _string_length == 0 ? 0 : _chars.size());
}

FixedStringIterator<true> FixedStringVector::cbegin() const noexcept {
  return FixedStringIterator<true>(_string_length, _chars, 0);
}

FixedStringIterator<true> FixedStringVector::cend() const noexcept {
  return FixedStringIterator<true>(_string_length, _chars, _string_length == 0 ? 0 : _chars.size());
}

using ReverseIterator = boost::reverse_iterator<FixedStringIterator<false>>;
ReverseIterator FixedStringVector::rbegin() noexcept { return ReverseIterator(end()); }

ReverseIterator FixedStringVector::rend() noexcept { return ReverseIterator(begin()); }

FixedString FixedStringVector::operator[](const size_t pos) {
  PerformanceWarning("operator[] used");
  return FixedString(&_chars[pos * _string_length], _string_length);
}

FixedString FixedStringVector::at(const size_t pos) {
  return FixedString(&_chars.at(pos * _string_length), _string_length);
}

const pmr_string FixedStringVector::get_string_at(const size_t pos) const {
  const auto string_start = &_chars[pos * _string_length];
  if (*(string_start + _string_length - 1) == '\0') {
    // The string is zero-padded - the pmr_string constructor takes care of finding the correct length
    return pmr_string(string_start);
  } else {
    return pmr_string(string_start, _string_length);
  }
}

char* FixedStringVector::data() { return _chars.data(); }

size_t FixedStringVector::size() const {
  // If the string length is zero, `_chars` has always the size 0. Thus, we don't know
  // how many empty strings were added to the FixedStringVector. So the FixedStringVector size is
  // always 1 and it returns an empty string when the first element is accessed.
  return _string_length == 0u ? 1u : _chars.size() / _string_length;
}

size_t FixedStringVector::capacity() const { return _chars.capacity(); }

void FixedStringVector::erase(const FixedStringIterator<false> start, const FixedStringIterator<false> end) {
  if (_string_length == 0) return;
  auto it = _chars.begin();
  std::advance(it, _chars.size() - std::distance(start, end) * _string_length);
  _chars.erase(it, _chars.end());
}

void FixedStringVector::shrink_to_fit() { _chars.shrink_to_fit(); }

PolymorphicAllocator<FixedString> FixedStringVector::get_allocator() { return _chars.get_allocator(); }

void FixedStringVector::reserve(const size_t n) { _chars.reserve(n * _string_length); }

size_t FixedStringVector::data_size() const { return sizeof(*this) + _chars.size(); }

std::shared_ptr<const pmr_vector<pmr_string>> FixedStringVector::dictionary() const {
  pmr_vector<pmr_string> string_values;
  for (auto it = cbegin(); it != cend(); ++it) {
    string_values.emplace_back(*it);
  }
  return std::make_shared<pmr_vector<pmr_string>>(std::move(string_values));
}

}  // namespace opossum
