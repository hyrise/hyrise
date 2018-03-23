#include "value_vector.hpp"

#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "utils/performance_warning.hpp"

namespace opossum {

void FixedStringVector::push_back(const std::string& string) {
  DebugAssert(string.size() <= _string_length, "Inserted string is too long to insert in FixedStringVector");
  const auto pos = _chars.size();
  // Default value of inserted elements using resize is null terminator ('\0')
  _chars.resize(_chars.size() + _string_length);
  string.copy(&_chars[pos], string.size());
}

FixedString FixedStringVector::at(const ChunkOffset chunk_offset) {
  return FixedString(&_chars.at(chunk_offset * _string_length), _string_length);
}

FixedStringVector::iterator FixedStringVector::begin() noexcept { return iterator(_string_length, _chars, 0); }

FixedStringVector::iterator FixedStringVector::end() noexcept {
  return iterator(_string_length, _chars, _chars.size());
}

FixedStringVector::iterator FixedStringVector::begin() const noexcept { return iterator(_string_length, _chars, 0); }

FixedStringVector::iterator FixedStringVector::end() const noexcept {
  return iterator(_string_length, _chars, _chars.size());
}

FixedStringVector::iterator FixedStringVector::cbegin() const noexcept { return iterator(_string_length, _chars, 0); }

FixedStringVector::iterator FixedStringVector::cend() const noexcept {
  return iterator(_string_length, _chars, _chars.size());
}

typedef boost::reverse_iterator<FixedStringVector::iterator> reverse_iterator;
reverse_iterator FixedStringVector::rbegin() noexcept { return reverse_iterator(end()); }

reverse_iterator FixedStringVector::rend() noexcept { return reverse_iterator(begin()); }

const std::string FixedStringVector::operator[](const size_t n) const {
  PerformanceWarning("operator[] used");
  const auto string_value = std::string(&_chars[n * _string_length], _string_length);
  const auto pos = string_value.find('\0');

  if (pos == std::string::npos) {
    return string_value;
  } else {
    return string_value.substr(0, pos);
  }
}

size_t FixedStringVector::size() const { return _chars.size() / _string_length; }

size_t FixedStringVector::capacity() const { return _chars.capacity(); }

void FixedStringVector::erase(const iterator start, const iterator end) {
  auto it = _chars.begin();
  std::advance(it, _chars.size() - std::distance(start, end) * _string_length);
  _chars.erase(it, _chars.end());
}

void FixedStringVector::shrink_to_fit() { _chars.shrink_to_fit(); }

PolymorphicAllocator<FixedString> FixedStringVector::get_allocator() { return _chars.get_allocator(); }

void FixedStringVector::reserve(const size_t n) { _chars.reserve(n * _string_length); }

const char* FixedStringVector::data() const { return &_chars[0]; }

size_t FixedStringVector::data_size() const { return sizeof(*this) + _chars.size(); }

}  // namespace opossum
