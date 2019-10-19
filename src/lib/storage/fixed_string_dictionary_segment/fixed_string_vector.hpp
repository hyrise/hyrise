#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "fixed_string.hpp"
#include "fixed_string_vector_iterator.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

// FixedStringVector is a data type, which stores all its values in a vector and
// is capable of storing FixedStrings.

class FixedStringVector {
 public:
  // Create a FixedStringVector of FixedStrings with given values
  FixedStringVector(const FixedStringVector& other) = default;

  // Create a FixedStringVector of FixedStrings with given values by iterating over other container
  template <class Iter>
  FixedStringVector(Iter first, Iter last, const size_t string_length) : _string_length(string_length) {
    const auto value_count = std::distance(first, last);
    // If string_length equals 0 we would not have any elements in the vector. Hence, we would have to deal with null
    // pointers. In order to avoid this, we insert a null terminator to the vector by using resize.
    if (_string_length == 0) {
      _chars.resize(1u);
      _size = value_count;
    } else {
      _chars.reserve(_string_length * value_count);
      _iterator_push_back(first, last);
    }
  }

  // Add a string to the end of the vector
  void push_back(const pmr_string& string);

  // Return the value at a certain position.
  FixedString operator[](const size_t pos);

  FixedString at(const size_t pos);

  pmr_string get_string_at(const size_t pos) const;

  // Make the FixedStringVector of FixedStrings iterable in different ways
  FixedStringIterator<false> begin() noexcept;
  FixedStringIterator<false> end() noexcept;
  FixedStringIterator<true> begin() const noexcept;
  FixedStringIterator<true> end() const noexcept;
  FixedStringIterator<true> cbegin() const noexcept;
  FixedStringIterator<true> cend() const noexcept;

  using ReverseIterator = boost::reverse_iterator<FixedStringIterator<false>>;
  ReverseIterator rbegin() noexcept;
  ReverseIterator rend() noexcept;

  // Return a pointer to the underlying memory
  char* data();

  // Return the number of entries in the vector.
  size_t size() const;

  // Return the amount of allocated memory
  size_t capacity() const;

  // Request the vector capacity to be at least enough to contain n elements
  void reserve(const size_t n);

  // Remove elements from the vector
  void erase(const FixedStringIterator<false> start, const FixedStringIterator<false> end);

  // Reduce capacity to fit its size
  void shrink_to_fit();

  // Return a copy of the allocator object associated with the vector of values
  PolymorphicAllocator<FixedString> get_allocator();

  // Return the calculated size of FixedStringVector in main memory
  size_t data_size() const;

  // Return the underlying dictionary as a vector of string
  std::shared_ptr<const pmr_vector<pmr_string>> dictionary() const;

 protected:
  const size_t _string_length;
  pmr_vector<char> _chars;
  size_t _size = 0;

  template <class Iter>
  void _iterator_push_back(Iter first, Iter last) {
    while (first != last) {
      push_back(*first);
      ++first;
    }
  }
};

}  // namespace opossum
