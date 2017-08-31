#pragma once

#include "dictionary_column_iterable.hpp"
#include "reference_column_iterable.hpp"
#include "value_column_iterable.hpp"

namespace opossum {

template <typename T>
auto create_iterable_from_column(const ValueColumn<T>& column) {
  return ValueColumnIterable<T>{column};
}

template <typename T>
auto create_iterable_from_column(const DictionaryColumn<T>& column) {
  return DictionaryColumnIterable<T>{column};
}

template <typename T>
auto create_iterable_from_column(const ReferenceColumn& column) {
  return ReferenceColumnIterable<T>{column};
}

}  // namespace opossum
