#pragma once

#include "reference_column/reference_column_iterable.hpp"

namespace opossum {

template <typename T>
auto create_iterable_from_column(const ReferenceColumn& column) {
  return erase_type_from_iterable_if_debug(ReferenceColumnIterable<T>{column});
}

}  // namespace opossum
