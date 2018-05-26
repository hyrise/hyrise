#pragma once

#include "expression_result.hpp"
#include "storage/column_iterables/base_column_iterators.hpp"

namespace opossum {

class NullValueIterator : public BaseColumnIterator<NullValueIterator, ColumnIteratorValue<NullValue>> {
 public:
  static constexpr bool Nullable = true;

 private:
  friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

  void increment() { }
  bool equal(const NullValueIterator& other) const { return true; }
  ColumnIteratorValue<NullValue> dereference() const { return ColumnIteratorValue<NullValue>(NullValue{}, true, ChunkOffset{0}); }
};

template<typename T>
class NullableValueIterator : public BaseColumnIterator<NullableValueIterator<T>, ColumnIteratorValue<T>> {
 public:
  static constexpr bool Nullable = true;

  explicit NullableValueIterator(const NullableValue<T>& nullable_value): _nullable_value(nullable_value) {}

 private:
  friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

  void increment() {  }
  bool equal(const NullableValueIterator<T>& other) const { return _nullable_value == other._nullable_value; }
  ColumnIteratorValue<T> dereference() const { return ColumnIteratorValue<T>(_nullable_value.value_or(T{}), !_nullable_value.has_value(), ChunkOffset{0}); }

  const NullableValue<T> _nullable_value;
};

template<typename T>
class NullableValuesIterator : public BaseColumnIterator<NullableValuesIterator<T>, ColumnIteratorValue<T>> {
 public:
  static constexpr bool Nullable = true;

  using ValuesIterator = typename NullableValues<T>::first_type::const_iterator;
  using NullsIterator = typename NullableValues<T>::second_type::const_iterator;

  NullableValuesIterator(const ValuesIterator& values_iter, const NullsIterator& null_iter):
  _values_iter(values_iter), _null_iter(null_iter) {}

 private:
  friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

  void increment() {
    ++_values_iter;
    ++_null_iter;
  }

  bool equal(const NullableValuesIterator<T>& other) const {
    return _values_iter == other._values_iter;
  }

  ColumnIteratorValue<T> dereference() const {
    // Intentionally don't pass in a ChunkOffset - the ExpressionEvaluator will never use it
    // TODO(moritz) investigate whether passing it it makes any perf difference
    return ColumnIteratorValue<T>(*_values_iter, *_null_iter, ChunkOffset{0});
  }

  ValuesIterator _values_iter;
  NullsIterator _null_iter;
};

template<typename T>
class NonNullableValuesIterator : public BaseColumnIterator<NonNullableValuesIterator<T>, ColumnIteratorValue<T>> {
 public:
  static constexpr bool Nullable = false;

  using ValuesIterator = typename NullableValues<T>::first_type::const_iterator;

  NonNullableValuesIterator(const ValuesIterator& values_iter):
  _values_iter(values_iter) {}

 private:
  friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

  void increment() {
    ++_values_iter;
  }

  bool equal(const NonNullableValuesIterator<T>& other) const {
    return _values_iter == other._values_iter;
  }

  ColumnIteratorValue<T> dereference() const {
    // Intentionally don't pass in a ChunkOffset - the ExpressionEvaluator will never use it
    // TODO(moritz) investigate whether passing it it makes any perf difference
    return ColumnIteratorValue<T>(*_values_iter, false, ChunkOffset{0});
  }

  ValuesIterator _values_iter;
};

template<typename T>
class NullableArraysIterator : public BaseColumnIterator<NullableArraysIterator<T>, ColumnIteratorValue<typename NullableArrays<T>::value_type>> {
 public:
  static constexpr bool Nullable = false;

  using ValuesIterator = typename NullableArrays<T>::const_iterator;

  explicit NullableArraysIterator(const ValuesIterator& values_iter):
  _values_iter(values_iter) {}

 private:
  friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

  void increment() {
    ++_values_iter;
  }

  bool equal(const NullableArraysIterator<T>& other) const {
    return _values_iter == other._values_iter;
  }

  ColumnIteratorValue<typename NullableArrays<T>::value_type> dereference() const {
    return {*_values_iter, false, ChunkOffset{0}};
  }

  ValuesIterator _values_iter;
};

template<typename T>
class NonNullableArraysIterator : public BaseColumnIterator<NonNullableArraysIterator<T>, ColumnIteratorValue<typename NonNullableArrays<T>::value_type>> {
 public:
  static constexpr bool Nullable = false;

  using ValuesIterator = typename NonNullableArrays<T>::const_iterator;

  explicit NonNullableArraysIterator(const ValuesIterator& values_iter):
  _values_iter(values_iter) {}

 private:
  friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

  void increment() {
    ++_values_iter;
  }

  bool equal(const NonNullableArraysIterator<T>& other) const {
    return _values_iter == other._values_iter;
  }

  ColumnIteratorValue<typename NonNullableArrays<T>::value_type> dereference() const {
    return {*_values_iter, false, ChunkOffset{0}};
  }

  ValuesIterator _values_iter;
};

template<typename T>
NullableValueIterator<T> create_iterator_for_expression_result(const NullableValue<T>& nullable_value) {
  return NullableValueIterator<T>(nullable_value);
}

template<typename T>
NullableValuesIterator<T> create_iterator_for_expression_result(const NullableValues<T>& nullable_values) {
  return NullableValuesIterator<T>(nullable_values.first.begin(), nullable_values.second.begin());
}

template<typename T>
NonNullableValuesIterator<T> create_iterator_for_expression_result(const NonNullableValues<T>& non_nullable_values) {
  return NonNullableValuesIterator<T>(non_nullable_values.begin());
}

template<typename T>
NullableArraysIterator<T> create_iterator_for_expression_result(const NullableArrays<T>& nullable_arrays) {
  return NullableArraysIterator<T>(nullable_arrays.begin());
}

template<typename T>
NonNullableArraysIterator<T> create_iterator_for_expression_result(const NonNullableArrays<T>& non_nullable_arrays) {
  return NonNullableArraysIterator<T>(non_nullable_arrays.begin());
}


}  // namespace opossum

