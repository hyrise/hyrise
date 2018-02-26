#pragma once

#include "storage/column_iterables/any_column_iterable.hpp"
#include "storage/deprecated_dictionary_column.hpp"
#include "storage/deprecated_dictionary_column/deprecated_attribute_vector_iterable.hpp"
#include "storage/dictionary_column.hpp"
#include "storage/dictionary_column/attribute_vector_iterable.hpp"

namespace opossum {

/**
 * @defgroup Uniform interface to create an attribute vector iterable from a dictionary column
 *
 * All iterables implement the same interface using static polymorphism
 * (i.e. the CRTP pattern, see column_iterables.hpp).
 *
 * In debug mode, create_iterable_from_column returns a type erased
 * iterable, i.e., all iterators have the same type
 *
 * These functions exist as long as their are two implementations of dictionary encoding
 *
 * @{
 */

inline auto create_iterable_from_attribute_vector(const BaseDictionaryColumn& column) {
  return erase_type_from_iterable_if_debug(AttributeVectorIterable{*column.attribute_vector(), column.null_value_id()});
}

inline auto create_iterable_from_attribute_vector(const BaseDeprecatedDictionaryColumn& column) {
  return erase_type_from_iterable_if_debug(DeprecatedAttributeVectorIterable{*column.attribute_vector()});
}

/**@}*/

}  // namespace opossum
