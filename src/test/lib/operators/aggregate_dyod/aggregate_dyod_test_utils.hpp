#pragma once

#include <cstddef>
#include <cstring>
#include <memory>
#include <vector>

#include "expression/expression_functional.hpp"
#include "expression/window_function_expression.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace hyrise {

inline std::shared_ptr<WindowFunctionExpression> make_aggregate(const WindowFunction function, const Table& table,
                                                                const ColumnID column_id) {
  if (column_id == INVALID_COLUMN_ID) {
    return std::make_shared<WindowFunctionExpression>(
        function, expression_functional::pqp_column_(column_id, DataType::Long, "*"));
  }
  return std::make_shared<WindowFunctionExpression>(
      function,
      expression_functional::pqp_column_(column_id, table.column_data_type(column_id), table.column_name(column_id)));
}

template <typename T>
std::vector<std::byte> pack_values(const std::vector<T>& values) {
  auto bytes = std::vector<std::byte>(values.size() * sizeof(T));
  std::memcpy(bytes.data(), values.data(), bytes.size());
  return bytes;
}

}  // namespace hyrise
