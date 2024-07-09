#include "table_statistics.hpp"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <ostream>
#include <utility>
#include <vector>

#include "all_type_variant.hpp"
#include "attribute_statistics.hpp"
#include "hyrise.hpp"
#include "resolve_type.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "statistics/statistics_objects/equal_distinct_count_histogram.hpp"
#include "statistics/statistics_objects/null_value_ratio_statistics.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

std::shared_ptr<TableStatistics> TableStatistics::from_table(const Table& table) {
  const auto column_count = table.column_count();
  auto column_statistics = std::vector<std::shared_ptr<const BaseAttributeStatistics>>{column_count};

  /**
   * Determine bin count, within mostly arbitrarily chosen bounds: 5 (for tables with <=2k rows) up to 100 bins
   * (for tables with >= 200m rows) are created.
   */
  const auto histogram_bin_count = std::min<size_t>(100, std::max<size_t>(5, table.row_count() / 2'000));

  /**
   * We highly recommend setting up a multithreaded scheduler before the following procedure is executed to parallelly
   * create statistics objects for the table's columns.
   */

  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  jobs.reserve(column_count);

  for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
    const auto generate_column_statistics = [&, column_id]() {
      const auto column_data_type = table.column_data_type(column_id);
      resolve_data_type(column_data_type, [&](auto type) {
        using ColumnDataType = typename decltype(type)::type;

        const auto output_column_statistics = std::make_shared<AttributeStatistics<ColumnDataType>>();

        const auto histogram =
            EqualDistinctCountHistogram<ColumnDataType>::from_column(table, column_id, histogram_bin_count);

        if (histogram) {
          output_column_statistics->set_statistics_object(histogram);

          // Use the insight that the histogram will only contain non-null values to generate the NullValueRatio
          // property.
          const auto null_value_ratio =
              table.row_count() == 0
                  ? 0.0f
                  : 1.0f - (static_cast<float>(histogram->total_count()) / static_cast<float>(table.row_count()));
          output_column_statistics->set_statistics_object(std::make_shared<NullValueRatioStatistics>(null_value_ratio));
        } else {
          // Failure to generate a histogram currently only stems from all-null segments.
          // TODO(anybody) this is a slippery assumption. But the alternative would be a full segment scan...
          output_column_statistics->set_statistics_object(std::make_shared<NullValueRatioStatistics>(1.0f));
        }

        column_statistics[column_id] = output_column_statistics;
      });
    };
    jobs.emplace_back(std::make_shared<JobTask>(generate_column_statistics));
  }
  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);

  return std::make_shared<TableStatistics>(std::move(column_statistics), table.row_count());
}

TableStatistics::TableStatistics(std::vector<std::shared_ptr<const BaseAttributeStatistics>>&& init_column_statistics,
                                 const Cardinality init_row_count)
    : column_statistics(std::move(init_column_statistics)), row_count(init_row_count) {}

DataType TableStatistics::column_data_type(const ColumnID column_id) const {
  DebugAssert(column_id < column_statistics.size(), "ColumnID out of bounds");
  return column_statistics[column_id]->data_type;
}

std::ostream& operator<<(std::ostream& stream, const TableStatistics& table_statistics) {
  stream << "TableStatistics {\n  RowCount: " << table_statistics.row_count << ";\n";

  for (const auto& column_statistics : table_statistics.column_statistics) {
    resolve_data_type(column_statistics->data_type, [&](const auto data_type_t) {
      using ColumnDataType = typename decltype(data_type_t)::type;
      stream << *std::dynamic_pointer_cast<const AttributeStatistics<ColumnDataType>>(column_statistics) << '\n';
    });
  }

  stream << "}\n";

  return stream;
}

}  // namespace hyrise
