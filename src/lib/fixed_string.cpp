#include "fixed_string.hpp"

#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "all_type_variant.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

FixedString::FixedString(const std::string& string) : _mem(new char[string.size()]{}), _maximum_length(string.size()) {
std::memcpy(_mem, string.c_str(), _maximum_length);
}

FixedString::FixedString(char* mem, size_t string_length) : _mem(mem), _maximum_length(string_length), _owns_memory(false) {}

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
const auto copied_length = other.maximum_length() < _maximum_length ? other.maximum_length() : _maximum_length;
other._copy_to(_mem, copied_length);
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

std::string FixedString::string() const {
const auto string_value = std::string(_mem, _maximum_length);
const auto pos = string_value.find('\0');

if (pos == std::string::npos) {
  return string_value;
} else {
  return string_value.substr(0, pos);
}
}

bool FixedString::operator<(const FixedString& other) const {
const auto smallest_length = size() < other.size() ? size() : other.size();
const auto result = memcmp(_mem, other._mem, smallest_length);
if (result == 0) return size() < other.size();
return result < 0;
}

bool FixedString::operator==(const FixedString& other) const {
if (size() != other.size()) return false;
return memcmp(_mem, other._mem, size()) == 0;
}

void FixedString::swap(FixedString other) {
DebugAssert(_maximum_length == other.maximum_length(),
            "FixedStrings must have the same maximum_length in order to swap them");
std::swap_ranges(_mem, _mem + _maximum_length, other._mem);
}

std::ostream& operator<<(std::ostream& os, const FixedString& obj) { return os << obj.string(); }

void swap(FixedString lhs, FixedString rhs) { lhs.swap(rhs); }

size_t FixedString::_copy_to(char* destination, size_t len, size_t pos) const {
DebugAssert(&destination + _maximum_length < &_mem || &destination > &_mem + _maximum_length,
  "Can't copy FixedString to same location");
const auto copied_length = len < _maximum_length - pos ? len : _maximum_length - pos;
std::memcpy(destination, _mem + pos, copied_length);
return copied_length;
}

}  // namespace opossum
