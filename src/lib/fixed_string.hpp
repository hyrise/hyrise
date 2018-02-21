#pragma once

#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "all_type_variant.hpp"
#include "types.hpp"

namespace opossum {

// FixedString is a data type, which stores a string in an array of chars in order to
// save memory space by avoiding SSO 
class FixedString {
 public:
  // Create a FixedString from a std::string
  // We did not find any good solution for creating a char array on stack
  explicit FixedString(const std::string& string) : _mem(new char[string.size()]{}), _string_length(string.size()) {
    std::memcpy(_mem, string.c_str(), _string_length);
  }

  // Create a FixedString from a memory address
  FixedString(char* mem, size_t string_length) : _mem(mem), _string_length(string_length), _delete(false) {}

  FixedString(FixedString& other) : _mem(new char[other._string_length]{}), _string_length(other._string_length) {
    std::memcpy(_mem, other._mem, _string_length);
  }

  // Create a FixedString with an existing one
  FixedString(const FixedString& other) : _mem(new char[other._string_length]{}), _string_length(other._string_length) {
    std::memcpy(_mem, other._mem, _string_length);
  }

  FixedString(const FixedString&& other)
      : _mem(new char[other._string_length]{}), _string_length(other._string_length) {
    std::memcpy(_mem, other._mem, _string_length);
  }

  ~FixedString() {
    if (_delete) delete[] _mem;
  }

  // Copy chars of current FixedString to a new destination
  size_t copys(char* s, size_t len, size_t pos = 0) const {
    const auto copied_length = len < _string_length - pos ? len : _string_length - pos;
    std::memcpy(s, _mem + pos, copied_length);
    return copied_length;
  }

  // Returns the length of the string
  size_t size() const { return _string_length; }

  // Creates a string object from FixedString
  std::string string() const { return std::string(_mem, _string_length); }

  
  FixedString& operator=(const FixedString& other) {
    const auto copied_length = other.size() < _string_length ? other.size() : _string_length;
    other.copys(_mem, copied_length);
    // Fill unused fields of char array with null terminator
    if (copied_length < _string_length) {
      memset(_mem + copied_length, '\0', _string_length - copied_length);
    }
    return *this;
  }

  // Compare FixedStrings by comparing the underlying char arrays
  bool operator<(const FixedString& other) const { return memcmp(_mem, other._mem, _string_length) < 0; }
  bool operator==(const FixedString& other) const { return memcmp(_mem, other._mem, _string_length) == 0; }

  // Prints FixedString as string
  friend std::ostream& operator<<(std::ostream& os, const FixedString& obj) { return os << obj.string(); }

  // Support swappable concept needed for sorting values. See: http://en.cppreference.com/w/cpp/concept/Swappable
  friend void swap(const FixedString lhs, const FixedString rhs) { lhs.swap(rhs); }

  // Swap two FixedStrings by exchanging the underlying memory's content
  void swap(const FixedString& other) const { std::swap_ranges(_mem, _mem + _string_length, other._mem); }

 private:
  char* const _mem;
  const size_t _string_length;
  const bool _delete = true;
};

}  // namespace opossum
