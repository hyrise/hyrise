#pragma once

#include "column_value.hpp"
#include "iterator_utils.hpp"

#include "all_type_variant.hpp"

namespace opossum {

template <typename T>
class ConstantValueIterable {
 public:
  explicit ConstantValueIterable(const T& value) : _value{value} {}
  explicit ConstantValueIterable(const AllTypeVariant& value) : _value{type_cast<T>(value)} {}

  template <typename Functor>
  void execute_for_all_no_mapping(const Functor& func) const {
    auto it = Iterator{_value};
    func(it, it);
  }

  template <typename Functor>
  void execute_for_all(const Functor& func) const {
    execute_for_all_no_mapping(func);
  }

 private:
  const T _value;

 private:
  class Iterator : public BaseIterator<Iterator, ColumnValue<T>>  {
   public:
    explicit Iterator(const T& value) : _value{value} {}

   private:
    friend class boost::iterator_core_access;

    void increment() {}
    bool equal(const Iterator &other) const { return _value == other._value; }
    ColumnValue<T> dereference() const { return ColumnValue<T>{_value, 0u}; }

   private:
    const T _value;
  };
};

}  // namespace opossum
