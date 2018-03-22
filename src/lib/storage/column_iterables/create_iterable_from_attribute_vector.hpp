#pragma once

#include "storage/column_iterables/any_column_iterable.hpp"
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
 * @{
 */

inline auto create_iterable_from_attribute_vector(const BaseDictionaryColumn& column) {
  return erase_type_from_iterable_if_debug(AttributeVectorIterable{*column.attribute_vector(), column.null_value_id()});
}

/**@}*/

}  // namespace opossum
