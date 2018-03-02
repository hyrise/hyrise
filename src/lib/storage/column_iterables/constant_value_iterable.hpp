#pragma once

#include "all_type_variant.hpp"
#include "storage/column_iterables.hpp"
#include "type_cast.hpp"

namespace opossum {

/**
 * @brief A column iterable returning an iterator with a constant value.
 */
template <typename T>
class ConstantValueIterable : public ColumnIterable<ConstantValueIterable<T>> {
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
  class Iterator : public BaseColumnIterator<Iterator, NonNullColumnIteratorValue<T>> {
   public:
    explicit Iterator(const T& value) : _value{value} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    void increment() {}
    bool equal(const Iterator& other) const { return _value == other._value; }
    NonNullColumnIteratorValue<T> dereference() const { return NonNullColumnIteratorValue<T>{_value, 0u}; }

   private:
    const T _value;
  };
};

}  // namespace opossum
