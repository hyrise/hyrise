#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "fixed_string.hpp"
#include "type_cast.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

// FixedStringVector is a data type, which stores all its values in a vector and
// is capable of storing FixedStrings.

class FixedStringVector {
 public:
  explicit FixedStringVector(size_t string_length) : _string_length(string_length) {}

  // Create a FixedStringVector of FixedStrings with given values by iterating over other container
  template <class Iter>
  FixedStringVector(Iter first, Iter last, size_t string_length) : _string_length(string_length) {
    _iterator_push_back(first, last);
  }

  // Create a FixedStringVector of FixedStrings with given values
  FixedStringVector(const FixedStringVector&& other)
      : _string_length(std::move(other._string_length)), _chars(std::move(other._chars)) {}
  FixedStringVector(const FixedStringVector& other) : _string_length(other._string_length), _chars(other._chars) {}
  FixedStringVector(const FixedStringVector& other, const PolymorphicAllocator<size_t>& alloc)
      : _string_length(other._string_length), _chars(other._chars, alloc) {}

  // Add a string to the end of the vector
  void push_back(const std::string& string);

  // Return the value at a certain position.
  const std::string operator[](const size_t n) const;

  FixedString at(const ChunkOffset chunk_offset);

  class iterator : public boost::iterator_facade<iterator, FixedString, std::random_access_iterator_tag, FixedString> {
   public:
    iterator(size_t string_length, const pmr_vector<char>& vector, size_t pos = 0)
        : _string_length(string_length), _chars(vector), _pos(pos) {}
    iterator& operator=(const iterator& other) {
      DebugAssert(_string_length == other._string_length && &_chars == &other._chars,
                  "can't convert pointers from different vectors");
      _pos = other._pos;
      return *this;
    }

   private:
    using facade = boost::iterator_facade<iterator, FixedString, std::random_access_iterator_tag, FixedString>;
    friend class boost::iterator_core_access;
    bool equal(iterator const& other) const { return this->_pos == other._pos; }
    typename facade::difference_type distance_to(iterator const& other) const {
      if (_string_length == 0) return 0;
      return (std::intptr_t(other._pos) - std::intptr_t(this->_pos)) / std::intptr_t(_string_length);
    }
    void advance(typename facade::difference_type n) { _pos += n * _string_length; }
    void increment() { _pos += _string_length; }
    void decrement() { _pos -= _string_length; }
    FixedString dereference() const { return FixedString(const_cast<char*>(&_chars[_pos]), _string_length); }

    const size_t _string_length;
    const pmr_vector<char>& _chars;
    size_t _pos;
  };

  // Make the FixedStringVector of FixedStrings iterable in different ways
  iterator begin() noexcept;
  iterator end() noexcept;
  iterator begin() const noexcept;
  iterator end() const noexcept;

  iterator cbegin() const noexcept;
  iterator cend() const noexcept;

  typedef boost::reverse_iterator<iterator> reverse_iterator;
  reverse_iterator rbegin() noexcept;
  reverse_iterator rend() noexcept;

  // Return the number of entries in the column.
  size_t size() const;

  // Return the amount of allocated memory
  size_t capacity() const;

  // Request the vector capacity to be at least enough to contain n elements
  void reserve(const size_t n);

  // Remove elements from the vector
  void erase(const iterator start, const iterator end);

  // Reduce capacity to fit its size
  void shrink_to_fit();

  // Return a copy of the allocator object associated with the vector of values
  PolymorphicAllocator<FixedString> get_allocator();

  // Return a pointer to the first element of the char vector
  const char* data() const;

  // Return the calculated size of FixedStringVector in main memory
  size_t data_size() const;

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
