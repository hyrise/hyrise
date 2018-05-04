#pragma once

#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "all_type_variant.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

// FixedString is a data type, in order to access the elements of a `fixedstring_vector` and interact with them.
// It has two different functionalities:
//     1. Represent an object in the `fixedstring_vector` (some sort of view) not owning the memory itself
//     2. Standalone object owning the memory
// It stores a string in an array of chars in order to save memory space by avoiding small string optimization (SSO).
class FixedString {
 public:
  // Create a FixedString from a std::string
  // Currently, there is no solution to create a char array on the stack. One possible solution, called dynarray,
  // (see http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2013/n3662.html) was rejected.
  // (see https://stackoverflow.com/questions/20777623/what-is-the-status-on-dynarrays/20777801#20777801)
  explicit FixedString(const std::string& string);

  // Create a FixedString from a memory address
  FixedString(char* mem, size_t string_length);

  // Create a FixedString with an existing one
  FixedString(const FixedString& other);

  ~FixedString();

  // Copy assign
  FixedString& operator=(const FixedString& other);

  // Returns the length of the string
  size_t size() const;

  // Returns the maixmum possible size of storable strings
  size_t maximum_length() const;

  // Creates a string object from FixedString
  std::string string() const;

  // Compare FixedStrings by comparing the underlying char arrays.
  // If one FixedString is longer than the other FixedString and the beginning of the longer FixedString
  // is equal to the other FixedString, the shorter FixedString is smaller.
  // Example: "defg" < "defghi"
  bool operator<(const FixedString& other) const;

  // The FixedStrings must have the same length to be equal
  bool operator==(const FixedString& other) const;

  // Prints FixedString as string
  friend std::ostream& operator<<(std::ostream& os, const FixedString& obj);

  // Support swappable concept needed for sorting values. See: http://en.cppreference.com/w/cpp/concept/Swappable
  friend void swap(FixedString lhs, FixedString rhs);

  // Swap two FixedStrings by exchanging the underlying memory's content
  void swap(FixedString& other);

 protected:
  char* const _mem;
  const size_t _maximum_length;
  const bool _owns_memory = true;

  // Copy chars of current FixedString to a new destination
  size_t _copy_to(char* destination, size_t len, size_t pos = 0) const;
};

}  // namespace opossum
