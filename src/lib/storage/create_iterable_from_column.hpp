#pragma once

#include "deprecated_dictionary_column/deprecated_dictionary_column_iterable.hpp"
#include "dictionary_column/dictionary_column_iterable.hpp"
#include "reference_column/reference_column_iterable.hpp"
#include "run_length_column/run_length_column_iterable.hpp"
#include "value_column/value_column_iterable.hpp"

namespace opossum {

/**
 * @defgroup Uniform interface to create an iterable from a column
 *
 * These methods cannot be part of the columns’ interfaces because
 * reference column are not templated and thus don’t know their type.
 *
 * All iterables implement the same interface using static polymorphism
 * (i.e. the CRTP pattern, see column_iterables.hpp).
 *
 * @{
 */

template <typename T>
auto create_iterable_from_column(const ValueColumn<T>& column) {
  return ValueColumnIterable<T>{column};
}

template <typename T>
auto create_iterable_from_column(const DeprecatedDictionaryColumn<T>& column) {
  return DeprecatedDictionaryColumnIterable<T>{column};
}

template <typename T>
auto create_iterable_from_column(const ReferenceColumn& column) {
  return ReferenceColumnIterable<T>{column};
}

template <typename T>
auto create_iterable_from_column(const DictionaryColumn<T>& column) {
  return DictionaryColumnIterable<T>{column};
}

template <typename T>
auto create_iterable_from_column(const RunLengthColumn<T>& column) {
  return RunLengthColumnIterable<T>{column};
}

/**@}*/

}  // namespace opossum
