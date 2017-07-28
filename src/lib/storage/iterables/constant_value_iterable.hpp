#pragma once

#include <iterator>

#include "all_type_variant.hpp"


namespace opossum {


template <typename T>
class ConstantValueIterable
{
 public:
  class ColumnValue {
   public:
    ColumnValue(const T & value) : _value{value} {}

    const T & value() const { return _value; }
    bool is_null() const { return false; }
    ChunkOffset chunk_offset() const { return 0u; }

   private:
    const T & _value;
  };

  class Iterator : public std::iterator<std::input_iterator_tag, ColumnValue, std::ptrdiff_t, ColumnValue *, ColumnValue> {
   public:
    explicit Iterator(const T & value) : _value{value} {}

    Iterator& operator++() { return *this; }
    Iterator operator++(int) { auto retval = *this; ++(*this); return retval; }
    bool operator==(Iterator other) const { return _value == other._value; }
    bool operator!=(Iterator other) const { return !(*this == other); }
    auto operator*() const { return ColumnValue{_value}; }

   private:
    const T _value;
  };

  ConstantValueIterable(const T & value) : _value{value} {}
  ConstantValueIterable(const AllTypeVariant & value) : _value{type_cast<T>(value)} {}

  template <typename Functor>
  auto execute_for_all(const Functor & func) const {
    auto begin = Iterator{_value};
    return func(begin);
  }

 private:
  const T _value;
};

}  // namespace opossum
