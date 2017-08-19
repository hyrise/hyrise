#pragma once

#include <tbb/tbb.h>

#include <memory>
#include <utility>

namespace opossum {

template <typename>
class ValueColumn;

template <typename T>
std::shared_ptr<ValueColumn<T>> create_single_value_column(T value) {
  tbb::concurrent_vector<T> column;
  column.push_back(value);

  return std::make_shared<ValueColumn<T>>(std::move(column));
}

}  // namespace opossum
