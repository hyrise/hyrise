#pragma once

#include "storage/column_iterables/any_column_iterable.hpp"
#include "storage/dictionary_column/dictionary_column_iterable.hpp"
#include "storage/encoding_type.hpp"
#include "storage/frame_of_reference/frame_of_reference_iterable.hpp"
#include "storage/reference_column.hpp"
#include "storage/run_length_column/run_length_column_iterable.hpp"
#include "storage/value_column/value_column_iterable.hpp"

namespace opossum {

template <typename T>
class ReferenceColumnIterable;

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
  return erase_type_from_iterable_if_debug(ValueColumnIterable<T>{column});
}

template <typename T>
auto create_iterable_from_column(const DictionaryColumn<T>& column) {
  return erase_type_from_iterable_if_debug(DictionaryColumnIterable<T>{column});
}

template <typename T>
auto create_iterable_from_column(const RunLengthColumn<T>& column) {
  return erase_type_from_iterable_if_debug(RunLengthColumnIterable<T>{column});
}

template <typename T>
auto create_iterable_from_column(const FrameOfReferenceColumn<T>& column) {
  return erase_type_from_iterable_if_debug(FrameOfReferenceIterable<T>{column});
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
