#include "generate_table_statistics.hpp"

#include <set>

#include "storage/table.hpp"
#include "abstract_column_statistics2.hpp"
#include "storage/create_iterable_from_column.hpp"
#include "storage/table.hpp"
#include "column_statistics2.hpp"
#include "table_statistics2.hpp"
#include "resolve_type.hpp"

namespace opossum {

TableStatistics2 generate_table_statistics(const Table& table) {
  std::vector<std::shared_ptr<const AbstractColumnStatistics2>> column_statistics;
  column_statistics.reserve(table.column_count());

  for (ColumnID column_id{0}; column_id < table.column_count(); ++column_id) {
    const auto column_data_type = table.column_data_types()[column_id];

    resolve_data_type(column_data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;

      std::set<ColumnDataType> min_max_set;
      auto null_value_count = size_t{0};

      for (ChunkID chunk_id{0}; chunk_id < table.chunk_count(); ++chunk_id) {
        const auto base_column = table.get_chunk(chunk_id)->get_column(column_id);

        resolve_column_type<ColumnDataType>(*base_column, [&](auto& column) {
          auto iterable = create_iterable_from_column<ColumnDataType>(column);
          iterable.for_each([&](const auto& column_value) {
            if (column_value.is_null()) {
              ++null_value_count;
            } else {
              min_max_set.insert(column_value.value());
            }
          });
        });
      }

      const auto null_value_ratio = static_cast<float>(null_value_count) / static_cast<float>(table.row_count());
      const auto distinct_count = static_cast<float>(min_max_set.size());

      auto min = ColumnDataType{};
      auto max = ColumnDataType{};

      if constexpr (std::is_arithmetic_v<ColumnDataType>) {
        min = min_max_set.empty() ? std::numeric_limits<float>::min() : *min_max_set.begin();
        max = min_max_set.empty() ? std::numeric_limits<float>::max() : *min_max_set.rbegin();
      } else {
        min = min_max_set.empty() ? min : *min_max_set.begin();
        max = min_max_set.empty() ? max : *min_max_set.rbegin();
      }

      column_statistics.emplace_back(std::make_shared<ColumnStatistics2<ColumnDataType>>(null_value_ratio, distinct_count, min, max));
    });
  }

  return {static_cast<float>(table.row_count()), std::move(column_statistics)};
}

}  // namespace opossum