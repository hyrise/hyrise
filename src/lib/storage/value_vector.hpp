#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "fixed_string.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

template <typename T>
class ValueVector {
 public:
  using iterator = typename pmr_vector<T>::iterator;
  using const_iterator = typename pmr_vector<T>::const_iterator;
  using reverse_iterator = typename pmr_vector<T>::reverse_iterator;

  ValueVector();

  // TODO(team_btm): move to cpp
  template <class Iter>
  ValueVector(Iter first, Iter last) {
    while (first != last) {
      push_back(*first);
      ++first;
    }
  }

  ValueVector(const ValueVector&& other) : _values(other._values) {}

  ValueVector(const ValueVector& other) : _values(other._values) {}

  void push_back(const T& value);

  void push_back(T&& value);

  T& at(const ChunkOffset chunk_offset);

  iterator begin() noexcept;

  iterator end() noexcept;

  iterator begin() const noexcept;

  iterator end() const noexcept;

  reverse_iterator rbegin() noexcept;

  reverse_iterator rend() noexcept;

  const_iterator cbegin() noexcept;

  const_iterator cend() noexcept;

  T& operator[](const size_t n);

  const T& operator[](const size_t n) const;

  size_t size() const;

  void reserve(const size_t n);

  void shrink_to_fit();

  PolymorphicAllocator<T> get_allocator();

 protected:
  pmr_vector<T> _values;
};

template <>
class ValueVector<FixedString> {
 public:
  explicit ValueVector(size_t string_length) : _string_length(string_length) {}

  // TODO(team_btm): move to cpp
  template <class Iter>
  ValueVector(Iter first, Iter last, size_t string_length) : _string_length(string_length) {
    _iterator_push_back(first, last);
  }

  // TODO(team_btm): move to cpp
  template <class Iter>
  ValueVector(Iter first, Iter last) : _string_length(first->size()) {
    _iterator_push_back(first, last);
  }

  ValueVector(const ValueVector&& other) : _string_length(other._string_length), _chars(other._chars) {}

  ValueVector(const ValueVector& other) : _string_length(other._string_length), _chars(other._chars) {}

  void push_back(const std::string& string);

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

  iterator begin() noexcept;

  iterator end() noexcept;

  iterator begin() const noexcept;

  iterator end() const noexcept;

  iterator cbegin() const noexcept;

  iterator cend() const noexcept;

  typedef boost::reverse_iterator<iterator> reverse_iterator;
  reverse_iterator rbegin() noexcept;

  reverse_iterator rend() noexcept;

  FixedString operator[](const size_t n);

  const FixedString operator[](const size_t n) const;

  size_t size() const;

  void erase(const iterator start, const iterator end);

  void shrink_to_fit();

  void reserve(const size_t n);

  PolymorphicAllocator<FixedString> get_allocator();

 private:
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
