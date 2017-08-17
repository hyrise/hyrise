#pragma once

#include "all_type_variant.hpp"
#include "iterator_utils.hpp"

namespace opossum {

template <typename T>
class ConstantValueIterable {
 public:
  class ColumnValue {
   public:
    explicit ColumnValue(const T& value) : _value{value} {}

    const T& value() const { return _value; }
    bool is_null() const { return false; }

   private:
    const T& _value;
  };

  class Iterator : public BaseIterator<Iterator, ColumnValue>  {
   public:
    explicit Iterator(const T& value) : _value{value} {}

   private:
    friend class boost::iterator_core_access;

    void increment() {}
    bool equal(const Iterator &other) const { return _value == other._value; }
    ColumnValue dereference() const { return ColumnValue{_value}; }

   private:
    const T _value;
  };

  explicit ConstantValueIterable(const T& value) : _value{value} {}
  explicit ConstantValueIterable(const AllTypeVariant& value) : _value{type_cast<T>(value)} {}

  template <typename Functor>
  void execute_for_all_no_mapping(const Functor& func) const {
    auto begin = Iterator{_value};
    // TODO(mjendruk): Find a better solution here.
    func(begin, begin);
  }

  template <typename Functor>
  void execute_for_all(const Functor& func) const {
    execute_for_all_no_mapping(func);
  }

 private:
  const T _value;
};

}  // namespace opossum
