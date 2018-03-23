#pragma once

#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "all_type_variant.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

// FixedString is a data type, which stores a string in an array of chars in order to
// save memory space by avoiding small string optimization (SSO)
class FixedString {
 public:
  // Create a FixedString from a std::string
  // Currently, there is no solution to create a char array on the stack. One possible solution, called dynarray,
  // (see http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2013/n3662.html) was rejected.
  // (see https://stackoverflow.com/questions/20777623/what-is-the-status-on-dynarrays/20777801#20777801)
  explicit FixedString(const std::string& string) : _mem(new char[string.size()]{}), _maximum_length(string.size()) {
    std::memcpy(_mem, string.c_str(), _maximum_length);
  }

  // Create a FixedString from a memory address
  FixedString(char* mem, size_t string_length) : _mem(mem), _maximum_length(string_length), _owns_memory(false) {}

  // Create a FixedString with an existing one
  FixedString(const FixedString& other)
      : _mem(new char[other._maximum_length]{}), _maximum_length(other._maximum_length) {
    std::memcpy(_mem, other._mem, _maximum_length);
  }

  ~FixedString() {
    if (_owns_memory) delete[] _mem;
  }

  // Copy assign
  FixedString& operator=(const FixedString& other) {
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

  // Returns the length of the string
  size_t size() const { return string().size(); }

  // Returns the maixmum possible size of storable strings
  size_t maximum_length() const { return _maximum_length; }

  // Creates a string object from FixedString
  std::string string() const {
    const auto string_value = std::string(_mem, _maximum_length);
    const auto pos = string_value.find('\0');

    if (pos == std::string::npos) {
      return string_value;
    } else {
      return string_value.substr(0, pos);
    }
  }

  // Compare FixedStrings by comparing the underlying char arrays.
  // If one FixedString is longer than the other FixedString and the beginning of the longer FixedString
  // is equal to the other FixedString, the shorter FixedString is smaller.
  // Example: "defg" < "defghi"
  bool operator<(const FixedString& other) const {
    const auto smallest_length = size() < other.size() ? size() : other.size();
    const auto result = memcmp(_mem, other._mem, smallest_length);
    if (result == 0) return size() < other.size();
    return result < 0;
  }

  // The FixedStrings must have the same length to be equal
  bool operator==(const FixedString& other) const {
    if (size() != other.size()) return false;
    return memcmp(_mem, other._mem, size()) == 0;
  }

  // Prints FixedString as string
  friend std::ostream& operator<<(std::ostream& os, const FixedString& obj) { return os << obj.string(); }

  // Support swappable concept needed for sorting values. See: http://en.cppreference.com/w/cpp/concept/Swappable
  friend void swap(const FixedString& lhs, const FixedString& rhs) { lhs.swap(rhs); }

  // Swap two FixedStrings by exchanging the underlying memory's content
  void swap(const FixedString& other) const { std::swap_ranges(_mem, _mem + _maximum_length, other._mem); }

 protected:
  char* const _mem;
  const size_t _maximum_length;
  const bool _owns_memory = true;

  // Copy chars of current FixedString to a new destination
  size_t _copy_to(char* s, size_t len, size_t pos = 0) const {
    const auto copied_length = len < _maximum_length - pos ? len : _maximum_length - pos;
    std::memcpy(s, _mem + pos, copied_length);
    return copied_length;
  }
};

}  // namespace opossum
