#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "fixed_string.hpp"
#include "fixed_string_vector_iterator.hpp"
#include "type_cast.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

// FixedStringVector is a data type, which stores all its values in a vector and
// is capable of storing FixedStrings.

class FixedStringVector {
 public:
  explicit FixedStringVector(size_t string_length);

  // Create a FixedStringVector of FixedStrings with given values
  FixedStringVector(const FixedStringVector&& other);
  FixedStringVector(const FixedStringVector& other);
  FixedStringVector(const FixedStringVector& other, const PolymorphicAllocator<size_t>& alloc);

  // Create a FixedStringVector of FixedStrings with given values by iterating over other container
  template <class Iter>
  FixedStringVector(Iter first, Iter last, size_t string_length) : _string_length(string_length) {
    _iterator_push_back(first, last);
  }

  // Add a string to the end of the vector
  void push_back(const std::string& string);

  // Return the value at a certain position.
  FixedString operator[](const size_t value_id);

  FixedString at(const size_t value_id);

  const std::string get_string_at(const size_t value_id) const;

  // Make the FixedStringVector of FixedStrings iterable in different ways
  FixedStringIterator begin() noexcept;
  FixedStringIterator end() noexcept;
  FixedStringIterator begin() const noexcept;
  FixedStringIterator end() const noexcept;

  FixedStringIterator cbegin() const noexcept;
  FixedStringIterator cend() const noexcept;

  typedef boost::reverse_iterator<FixedStringIterator> reverse_iterator;
  reverse_iterator rbegin() noexcept;
  reverse_iterator rend() noexcept;

  // Return a pointer to the underlying memory
  char* data();

  // Return the number of entries in the column.
  size_t size() const;

  // Return the amount of allocated memory
  size_t capacity() const;

  // Request the vector capacity to be at least enough to contain n elements
  void reserve(const size_t n);

  // Remove elements from the vector
  void erase(const FixedStringIterator start, const FixedStringIterator end);

  // Reduce capacity to fit its size
  void shrink_to_fit();

  // Return a copy of the allocator object associated with the vector of values
  PolymorphicAllocator<FixedString> get_allocator();

  // Return the calculated size of FixedStringVector in main memory
  size_t data_size() const;

  // Return the underlying dictionary as a vector of string
  std::shared_ptr<const pmr_vector<std::string>> dictionary() const;

 protected:
  const size_t _string_length;
  pmr_vector<char> _chars;

  template <class Iter>
  void _iterator_push_back(Iter first, Iter last) {
    while (first != last) {
      push_back(*first);
      ++first;
    }
  }
};

}  // namespace opossum
