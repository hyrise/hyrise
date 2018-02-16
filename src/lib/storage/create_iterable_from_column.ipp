#pragma once

#include "reference_column/reference_column_iterable.hpp"

namespace opossum {

template <typename T>
auto create_iterable_from_column(const ReferenceColumn& column) {
  return detail::may_erase_type(ReferenceColumnIterable<T>{column});
}

}  // namespace opossum
