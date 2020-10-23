#pragma once

#include "storage/dictionary_segment.hpp"
#include "storage/dictionary_segment/attribute_vector_iterable.hpp"
#include "storage/segment_iterables/any_segment_iterable.hpp"

namespace opossum {

/**
 * @defgroup Uniform interface to create an attribute vector iterable from a dictionary segment
 *
 * All iterables implement the same interface using static polymorphism
 * (i.e. the CRTP pattern, see segment_iterables/.hpp).
 *
 * In debug mode, create_iterable_from_segment returns a type erased
 * iterable, i.e., all iterators have the same type
 *
 * @{
 */

inline auto create_iterable_from_attribute_vector(const BaseDictionarySegment& segment) {
  return erase_type_from_iterable_if_debug(AttributeVectorIterable{segment, segment.null_value_id()});
}

/**@}*/

}  // namespace opossum
