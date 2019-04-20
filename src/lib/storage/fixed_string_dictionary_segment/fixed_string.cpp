#include "fixed_string.hpp"

#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "all_type_variant.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

FixedString::FixedString(char* mem, size_t string_length)
    : _mem(mem), _maximum_length(string_length), _owns_memory(false) {}

FixedString::FixedString(const FixedString& other)
    : _mem(new char[other._maximum_length]{}), _maximum_length(other._maximum_length) {
  std::memcpy(_mem, other._mem, _maximum_length);
}

FixedString::~FixedString() {
  if (_owns_memory) delete[] _mem;
}

FixedString& FixedString::operator=(const FixedString& other) {
  DebugAssert(other.maximum_length() <= _maximum_length,
              "Other FixedString is longer than current maximum string length");
  DebugAssert(other._mem + _maximum_length < _mem + 1 || _mem + _maximum_length < other._mem + 1,
              "This and the other's FixedString memory can't overlap");

  const auto copied_length = std::min(other.maximum_length(), _maximum_length);
  std::memcpy(_mem, other._mem, copied_length);

  // Fill unused fields of char array with null terminator, in order to overwrite the content of
  // the old FixedString. This is especially important if the old FixedString was longer than the other FixedString.
  if (copied_length < _maximum_length) {
    memset(_mem + copied_length, '\0', _maximum_length - copied_length);
  }
  return *this;
}

size_t FixedString::size() const {
  const auto position = std::find(_mem, _mem + _maximum_length, '\0');
  return std::distance(_mem, position);
}

size_t FixedString::maximum_length() const { return _maximum_length; }

std::string FixedString::string() const { return std::string(_mem, strnlen(_mem, _maximum_length)); }

std::string_view FixedString::string_view() const { return std::string_view(_mem, strnlen(_mem, _maximum_length)); }

bool FixedString::operator<(const FixedString& other) const {
  const auto smallest_length = std::min(size(), other.size());
  const auto result = memcmp(_mem, other._mem, smallest_length);
  if (result == 0) return size() < other.size();
  return result < 0;
}

bool operator<(const FixedString& lhs, const std::string& rhs) { return lhs.string() < rhs; }

bool operator<(const std::string& lhs, const FixedString& rhs) { return lhs < rhs.string(); }

bool operator<(const FixedString& lhs, const std::string_view& rhs) { return lhs.string_view() < rhs; }

bool operator<(const std::string_view& lhs, const FixedString& rhs) { return lhs < rhs.string_view(); }

bool operator<(const FixedString& lhs, const char* rhs) { return lhs.string_view() < rhs; }

bool operator<(const char* lhs, const FixedString& rhs) { return lhs < rhs.string_view(); }

bool FixedString::operator==(const FixedString& other) const {
  if (size() != other.size()) return false;
  return memcmp(_mem, other._mem, size()) == 0;
}

void FixedString::swap(FixedString& other) {
  DebugAssert(_maximum_length == other.maximum_length(),
              "FixedStrings must have the same maximum_length in order to swap them");
  std::swap_ranges(_mem, _mem + _maximum_length, other._mem);
}

std::ostream& operator<<(std::ostream& os, const FixedString& obj) { return os << obj.string(); }

void swap(FixedString lhs, FixedString rhs) { lhs.swap(rhs); }

bool operator==(const FixedString& lhs, const std::string& rhs) { return lhs.string() == rhs; }

bool operator==(const std::string& lhs, const FixedString& rhs) { return lhs == rhs.string(); }

bool operator==(const FixedString& lhs, const std::string_view& rhs) { return lhs.string_view() == rhs; }

bool operator==(const std::string_view& lhs, const FixedString& rhs) { return lhs == rhs.string_view(); }

bool operator==(const FixedString& lhs, const char* rhs) { return lhs.string_view() == rhs; }

bool operator==(const char* lhs, const FixedString& rhs) { return lhs == rhs.string_view(); }

}  // namespace opossum
