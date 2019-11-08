#include "table_statistics.hpp"

#include <numeric>
#include <thread>

#include "attribute_statistics.hpp"
#include "resolve_type.hpp"
#include "statistics/statistics_objects/abstract_histogram.hpp"
#include "storage/table.hpp"
#include "utils/assert.hpp"

namespace opossum {

std::shared_ptr<TableStatistics> TableStatistics::from_table(const Table& table) {
  std::vector<std::shared_ptr<BaseAttributeStatistics>> column_statistics(table.column_count());

  /**
   * Determine bin count, within mostly arbitrarily chosen bounds: 5 (for tables with <=2k rows) up to 100 bins
   * (for tables with >= 200m rows) are created.
   */
  const auto histogram_bin_count = std::min<size_t>(100, std::max<size_t>(5, table.row_count() / 2'000));

  auto next_column_id = std::atomic<size_t>{0u};
  auto threads = std::vector<std::thread>{};

  /**
   * Parallely create statistics objects for the Table's columns
   */
  for (auto thread_id = 0u;
       thread_id < std::min(static_cast<uint>(table.column_count()), std::thread::hardware_concurrency() + 1);
       ++thread_id) {
    threads.emplace_back([&] {
      while (true) {
        auto my_column_id = ColumnID{static_cast<ColumnID::base_type>(next_column_id++)};
        if (static_cast<ColumnCount>(my_column_id) >= table.column_count()) return;

        const auto column_data_type = table.column_data_type(my_column_id);

        resolve_data_type(column_data_type, [&](auto type) {
          using ColumnDataType = typename decltype(type)::type;

          const auto output_column_statistics = std::make_shared<AttributeStatistics<ColumnDataType>>();

          const auto histogram =
              EqualDistinctCountHistogram<ColumnDataType>::from_column(table, my_column_id, histogram_bin_count);

          if (histogram) {
            output_column_statistics->set_statistics_object(histogram);

            // Use the insight that the histogram will only contain non-null values to generate the NullValueRatio
            // property
            const auto null_value_ratio =
                table.row_count() == 0 ? 0.0f
                                       : 1.0f - (static_cast<float>(histogram->total_count()) / table.row_count());
            output_column_statistics->set_statistics_object(
                std::make_shared<NullValueRatioStatistics>(null_value_ratio));
          } else {
            // Failure to generate a histogram currently only stems from all-null segments.
            // TODO(anybody) this is a slippery assumption. But the alternative would be a full segment scan...
            output_column_statistics->set_statistics_object(std::make_shared<NullValueRatioStatistics>(1.0f));
          }

          column_statistics[my_column_id] = output_column_statistics;
        });
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  return std::make_shared<TableStatistics>(std::move(column_statistics), table.row_count());
}

TableStatistics::TableStatistics(std::vector<std::shared_ptr<BaseAttributeStatistics>>&& column_statistics,
                                 const Cardinality row_count)
    : column_statistics(std::move(column_statistics)), row_count(row_count) {}

DataType TableStatistics::column_data_type(const ColumnID column_id) const {
  DebugAssert(column_id < column_statistics.size(), "ColumnID out of bounds");
  return column_statistics[column_id]->data_type;
}

std::ostream& operator<<(std::ostream& stream, const TableStatistics& table_statistics) {
  stream << "TableStatistics {" << std::endl;
  stream << "  RowCount: " << table_statistics.row_count << "; " << std::endl;

  for (const auto& column_statistics : table_statistics.column_statistics) {
    resolve_data_type(column_statistics->data_type, [&](const auto data_type_t) {
      using ColumnDataType = typename decltype(data_type_t)::type;
      stream << *std::dynamic_pointer_cast<AttributeStatistics<ColumnDataType>>(column_statistics) << std::endl;
    });
  }

  stream << "}" << std::endl;

  return stream;
}

}  // namespace opossum
