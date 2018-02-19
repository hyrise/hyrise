#pragma once

#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "all_type_variant.hpp"
#include "types.hpp"

namespace opossum {

class FixedString {
 public:
  explicit FixedString(const std::string& string) : _mem(new char[string.size()]{}), _string_length(string.size()) {
    std::memcpy(_mem, string.c_str(), _string_length);
  }

  FixedString(char* mem, size_t string_length) : _mem(mem), _string_length(string_length), _delete(false) {}

  ~FixedString() {
    if (_delete) delete[] _mem;
  }

  FixedString(FixedString& other) : _mem(new char[other._string_length]{}), _string_length(other._string_length) {
    std::memcpy(_mem, other._mem, _string_length);
  }

  FixedString(const FixedString& other) : _mem(new char[other._string_length]{}), _string_length(other._string_length) {
    std::memcpy(_mem, other._mem, _string_length);
  }

  FixedString(const FixedString&& other)
      : _mem(new char[other._string_length]{}), _string_length(other._string_length) {
    std::memcpy(_mem, other._mem, _string_length);
  }

  size_t copy(char* s, size_t len, size_t pos = 0) const {
    const auto copied_length = len < _string_length - pos ? len : _string_length - pos;
    std::memcpy(s, _mem + pos, copied_length);
    return copied_length;
  }

  size_t size() const { return _string_length; }

  std::string string() const { return std::string(_mem, _string_length); }

  FixedString& operator=(const FixedString& other) {
    const auto copied_length = other.size() < _string_length ? other.size() : _string_length;
    other.copy(_mem, copied_length, 0);
    if (copied_length < _string_length) {
      memset(_mem + copied_length, '\0', _string_length - copied_length);
    }
    return *this;
  }

  bool operator<(const FixedString& other) const { return memcmp(_mem, other._mem, _string_length) < 0; }

  bool operator==(const FixedString& other) const { return memcmp(_mem, other._mem, _string_length) == 0; }

  friend std::ostream& operator<<(std::ostream& os, const FixedString& obj) { return os << obj.string(); }

  friend void swap(const FixedString lha, const FixedString rhs) { lha.swap(rhs); }

  void swap(const FixedString& other) const { std::swap_ranges(_mem, _mem + _string_length, other._mem); }

 private:
  char* const _mem;
  const size_t _string_length;
  const bool _delete = true;
};

}  // namespace opossum
