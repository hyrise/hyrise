#include "fixed_string_vector.hpp"

#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "utils/performance_warning.hpp"

namespace opossum {

FixedStringVector::FixedStringVector(size_t string_length) : _string_length(string_length) {}

FixedStringVector::FixedStringVector(const FixedStringVector&& other)
    : _string_length(std::move(other._string_length)), _chars(std::move(other._chars)) {}

FixedStringVector::FixedStringVector(const FixedStringVector& other)
    : _string_length(other._string_length), _chars(other._chars) {}

FixedStringVector::FixedStringVector(const FixedStringVector& other, const PolymorphicAllocator<size_t>& alloc)
    : _string_length(other._string_length), _chars(other._chars, alloc) {}

void FixedStringVector::push_back(const std::string& string) {
  DebugAssert(string.size() <= _string_length, "Inserted string is too long to insert in FixedStringVector");
  const auto pos = _chars.size();
  // Default value of inserted elements using resize is null terminator ('\0')
  _chars.resize(_chars.size() + _string_length);
  string.copy(&_chars[pos], string.size());
}

FixedStringIterator FixedStringVector::begin() noexcept { return FixedStringIterator(_string_length, _chars, 0); }

FixedStringIterator FixedStringVector::end() noexcept {
  return FixedStringIterator(_string_length, _chars, _chars.size());
}

FixedStringIterator FixedStringVector::begin() const noexcept { return FixedStringIterator(_string_length, _chars, 0); }

FixedStringIterator FixedStringVector::end() const noexcept {
  return FixedStringIterator(_string_length, _chars, _chars.size());
}

FixedStringIterator FixedStringVector::cbegin() const noexcept {
  return FixedStringIterator(_string_length, _chars, 0);
}

FixedStringIterator FixedStringVector::cend() const noexcept {
  return FixedStringIterator(_string_length, _chars, _chars.size());
}

typedef boost::reverse_iterator<FixedStringIterator> reverse_iterator;
reverse_iterator FixedStringVector::rbegin() noexcept { return reverse_iterator(end()); }

reverse_iterator FixedStringVector::rend() noexcept { return reverse_iterator(begin()); }

FixedString FixedStringVector::operator[](const size_t value_id) {
  PerformanceWarning("operator[] used");
  return FixedString(&_chars[value_id * _string_length], _string_length);
}

FixedString FixedStringVector::at(const size_t value_id) {
  return FixedString(&_chars.at(value_id * _string_length), _string_length);
}

const std::string FixedStringVector::get_string_at(const size_t value_id) const {
  const auto string_value = std::string(&_chars[value_id * _string_length], _string_length);
  const auto pos = string_value.find('\0');

  if (pos == std::string::npos) {
    return string_value;
  } else {
    return string_value.substr(0, pos);
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

void FixedStringVector::erase(const FixedStringIterator start, const FixedStringIterator end) {
  if (_string_length == 0) return;
  auto it = _chars.begin();
  std::advance(it, _chars.size() - std::distance(start, end) * _string_length);
  _chars.erase(it, _chars.end());
}

void FixedStringVector::shrink_to_fit() { _chars.shrink_to_fit(); }

PolymorphicAllocator<FixedString> FixedStringVector::get_allocator() { return _chars.get_allocator(); }

void FixedStringVector::reserve(const size_t n) { _chars.reserve(n * _string_length); }

size_t FixedStringVector::data_size() const { return sizeof(*this) + _chars.size(); }

std::shared_ptr<const pmr_vector<std::string>> FixedStringVector::dictionary() const {
  pmr_vector<std::string> string_values;
  for (auto it = cbegin(); it != cend(); ++it) {
    string_values.push_back(it->string());
  }
  return std::make_shared<pmr_vector<std::string>>(std::move(string_values));
}

}  // namespace opossum
