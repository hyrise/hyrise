#pragma once

#include "storage/deprecated_dictionary_column/deprecated_dictionary_column_iterable.hpp"
#include "storage/dictionary_column/dictionary_column_iterable.hpp"
#include "storage/reference_column.hpp"
#include "storage/run_length_column/run_length_column_iterable.hpp"
#include "storage/value_column/value_column_iterable.hpp"

#if IS_DEBUG
#include "storage/column_iterables/any_column_iterable.hpp"
#endif

namespace opossum {

template <typename T>
class ReferenceColumnIterable;

namespace detail {

/**
 * In debug mode, this function returns a type erased version
 * of the passed iterable, i.e., all iterators have the same type,
 * which greatly reduces compile times.
 */
template <typename Iterable>
decltype(auto) may_erase_type(const Iterable& iterable) {
#if IS_DEBUG
  return erase_type_from_iterable(iterable);
#else
  return iterable;
#endif
}

}  // namespace detail

/**
 * @defgroup Uniform interface to create an iterable from a column
 *
 * These methods cannot be part of the columns’ interfaces because
 * reference column are not templated and thus don’t know their type.
 *
 * All iterables implement the same interface using static polymorphism
 * (i.e. the CRTP pattern, see column_iterables.hpp).
 *
 * In debug mode, create_iterable_from_column returns a type erased
 * iterable, i.e., all iterators have the same type
 *
 * @{
 */

template <typename T>
auto create_iterable_from_column(const ValueColumn<T>& column) {
  return detail::may_erase_type(ValueColumnIterable<T>{column});
}

template <typename T>
auto create_iterable_from_column(const DeprecatedDictionaryColumn<T>& column) {
  return detail::may_erase_type(DeprecatedDictionaryColumnIterable<T>{column});
}

template <typename T>
auto create_iterable_from_column(const DictionaryColumn<T>& column) {
  return detail::may_erase_type(DictionaryColumnIterable<T>{column});
}

template <typename T>
auto create_iterable_from_column(const RunLengthColumn<T>& column) {
  return detail::may_erase_type(RunLengthColumnIterable<T>{column});
}

/**
 * This function must be forward-declared because ReferenceColumnIterable
 * includes this file leading to a circular dependency
 */
template <typename T>
auto create_iterable_from_column(const ReferenceColumn& column);

/**@}*/

}  // namespace opossum

#include "create_iterable_from_column.ipp"
