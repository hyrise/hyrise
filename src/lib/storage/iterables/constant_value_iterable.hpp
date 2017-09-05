#pragma once

#include "iterables.hpp"

#include "all_type_variant.hpp"
#include "type_cast.hpp"

namespace opossum {

template <typename T>
class ConstantValueIterable : public Iterable<ConstantValueIterable<T>> {
 public:
  explicit ConstantValueIterable(const T& value) : _value{value} {}
  explicit ConstantValueIterable(const AllTypeVariant& value) : _value{type_cast<T>(value)} {}

  template <typename Functor>
  void _on_with_iterators(const Functor& f) const {
    auto it = Iterator{_value};
    f(it, it);
  }

 private:
  const T _value;

 private:
  class Iterator : public BaseIterator<Iterator, ColumnValue<T>> {
   public:
    explicit Iterator(const T& value) : _value{value} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    void increment() {}
    bool equal(const Iterator& other) const { return _value == other._value; }
    ColumnValue<T> dereference() const { return ColumnValue<T>{_value, 0u}; }

   private:
    const T _value;
  };
};

}  // namespace opossum
