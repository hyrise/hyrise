#include "materialize.hpp"

#include <type_traits>

namespace opossum {

std::shared_ptr<BaseColumn> materialize_as_value_column(const BaseColumn& base_column) {
  std::shared_ptr<BaseColumn> value_column;
  resolve_data_and_column_type(base_column, [&](auto type, const auto& column) {
    using DataType = typename decltype(type)::type;
    using ColumnType = std::remove_reference_t<decltype(column)>;

    pmr_concurrent_vector<DataType> values;
    values.reserve(column.size());
    materialize_values(column, values);

    // If the column is a ValueColumn, check if materializing the nulls is necessary
    if
      constexpr(std::is_base_of_v<BaseValueColumn, ColumnType>) {
        if (!column.is_nullable()) {
          value_column = std::make_shared<ValueColumn<DataType>>(std::move(values));
          return;
        }
      }

    pmr_concurrent_vector<bool> nulls(column.size());
    nulls.reserve(column.size());
    materialize_nulls<DataType>(column, nulls);

    value_column = std::make_shared<ValueColumn<DataType>>(std::move(values), std::move(nulls));
  });
  return value_column;
}

}  // namespace opossum
