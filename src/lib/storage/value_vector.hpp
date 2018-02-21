#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "fixed_string.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

// ValueVector is a data type, which stores all its values in a vector and
// is capable of storing FixedStrings.
template <typename T>
class ValueVector {
 public:
  using iterator = typename pmr_vector<T>::iterator;
  using const_iterator = typename pmr_vector<T>::const_iterator;
  using reverse_iterator = typename pmr_vector<T>::reverse_iterator;

  ValueVector();

  // Create a ValueVector with given values by iterating over other container
  template <class Iter>
  ValueVector(Iter first, Iter last) {
    _iterator_push_back(first, last);
  }

  template <class Iter>
  ValueVector(Iter first, Iter last, const PolymorphicAllocator<size_t>& alloc) : _values(alloc) {
    _iterator_push_back(first, last);
  }

  // Create a ValueVector with given values
  ValueVector(const ValueVector<T>&& other) : _values(other._values) {}
  ValueVector(const ValueVector<T>& other) : _values(other._values) {}
  ValueVector(const ValueVector<T>& other, const PolymorphicAllocator<size_t>& alloc) : _values(other._values, alloc) {}

  // Add a value to the end of the vector
  void push_back(const T& value);
  void push_back(T&& value);

  // Make the ValueVector iterable in different ways
  iterator begin() noexcept;
  iterator end() noexcept;
  const_iterator begin() const noexcept;
  const_iterator end() const noexcept;

  reverse_iterator rbegin() noexcept;
  reverse_iterator rend() noexcept;

  const_iterator cbegin() const noexcept;
  const_iterator cend() const noexcept;

  void erase(iterator start, iterator end);

  // Return the value at a certain position.
  T& operator[](const size_t n);
  const T& operator[](const size_t n) const;

  T& at(const ChunkOffset chunk_offset);

  // Return the number of entries in the column.
  size_t size() const;

  // Return the amount of allocated memory
  size_t capacity() const;

  // Request the vector capacity to be at least enough to contain n elements
  void reserve(const size_t n);

  // Reduce capacity to fit its size
  void shrink_to_fit();

  // Return a copy of the allocator object associated with the vector of values
  PolymorphicAllocator<T> get_allocator();

  // Return the calculated size of ValueVector in main memory
  size_t data_size() const;

 protected:
  pmr_vector<T> _values;

  template <class Iter>
  void _iterator_push_back(Iter first, Iter last) {
    while (first != last) {
      push_back(*first);
      ++first;
    }
  }
};

template <>
class ValueVector<FixedString> {
 public:
  explicit ValueVector(size_t string_length) : _string_length(string_length) {}

  // Create a ValueVector of FixedStrings with given values by iterating over other container
  template <class Iter>
  ValueVector(Iter first, Iter last, size_t string_length) : _string_length(string_length) {
    _iterator_push_back(first, last);
  }

  template <class Iter>
  ValueVector(Iter first, Iter last) : _string_length(first->size()) {
    _iterator_push_back(first, last);
  }

  // Create a ValueVector of FixedStrings with given values
  ValueVector(const ValueVector<FixedString>&& other) : _string_length(other._string_length), _chars(other._chars) {}
  ValueVector(const ValueVector<FixedString>& other) : _string_length(other._string_length), _chars(other._chars) {}
  ValueVector(const ValueVector<FixedString>& other, const PolymorphicAllocator<size_t>& alloc)
      : _string_length(other._string_length), _chars(other._chars, alloc) {}

  // Add a string to the end of the vector
  void push_back(const std::string& string);

  // Return the value at a certain position.
  FixedString operator[](const size_t n);
  const FixedString operator[](const size_t n) const;

  FixedString at(const ChunkOffset chunk_offset);

  // TODO(team_btm): move??
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
      return (int64_t(other._pos) - int64_t(this->_pos)) / int64_t(_string_length);
    }
    void advance(typename facade::difference_type n) { _pos += n * _string_length; }
    void increment() { _pos += _string_length; }
    void decrement() { _pos -= _string_length; }
    FixedString dereference() const { return FixedString(const_cast<char*>(&_chars[_pos]), _string_length); }

    const size_t _string_length;
    const pmr_vector<char>& _chars;
    size_t _pos;
  };

  // Make the ValueVector of FixedStrings iterable in different ways
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

  // Return the calculated size of ValueVector in main memory
  size_t data_size() const;

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
