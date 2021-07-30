#include "table_statistics.hpp"

#include <iostream>
#include <numeric>
#include <thread>

#include "resolve_type.hpp"
#include "statistics/statistics_objects/abstract_histogram.hpp"
#include "storage/table.hpp"
#include "utils/assert.hpp"

namespace opossum {

template <typename ColumnDataType>
void TableStatistics::_add_statistics_from_histogram(
    const std::shared_ptr<AttributeStatistics<ColumnDataType>> attribute_statistics,
    const std::shared_ptr<EqualDistinctCountHistogram<ColumnDataType>> histogram, const Table& table) {
  if (histogram) {
    attribute_statistics->set_statistics_object(histogram);

    // Use the insight that the histogram will only contain non-null values to generate the NullValueRatio
    // property
    const auto null_value_ratio =
        table.row_count() == 0
            ? 0.0f
            : 1.0f - (static_cast<float>(histogram->total_count()) / static_cast<float>(table.row_count()));
    attribute_statistics->set_statistics_object(std::make_shared<NullValueRatioStatistics>(null_value_ratio));
  } else {
    // Failure to generate a histogram currently only stems from all-null segments.
    // TODO(anybody) this is a slippery assumption. But the alternative would be a full segment scan...
    attribute_statistics->set_statistics_object(std::make_shared<NullValueRatioStatistics>(1.0f));
  }
}

std::shared_ptr<TableStatistics> TableStatistics::from_table(const Table& table) {
  std::vector<std::shared_ptr<BaseAttributeStatistics>> column_statistics(table.column_count());
  std::vector<std::vector<std::shared_ptr<BaseAttributeStatistics>>> segment_statistics(table.column_count());
  /**
   * Determine bin count, within mostly arbitrarily chosen bounds: 5 (for tables with <=2k rows) up to 100 bins
   * (for tables with >= 200m rows) are created.
   */
  const auto histogram_bin_count = std::min<size_t>(100, std::max<size_t>(5, table.row_count() / 2'000));

  auto next_column_id = std::atomic_size_t{0u};
  auto threads = std::vector<std::thread>{};

  /**
   * Parallely create statistics objects for the Table's columns. Do not use JobTask as we want this to be parallel
   * even if Hyrise is running without a scheduler.
   */
  for (auto thread_id = 0u;
       thread_id < std::min(static_cast<uint>(table.column_count()), std::thread::hardware_concurrency() + 1);
       ++thread_id) {
    threads.emplace_back([&] {
      while (true) {
        auto my_column_id = ColumnID{static_cast<ColumnID::base_type>(next_column_id++)};
        if (static_cast<ColumnCount>(my_column_id) >= table.column_count()) {
          return;
        }

        const auto column_data_type = table.column_data_type(my_column_id);

        resolve_data_type(column_data_type, [&](auto type) {
          using ColumnDataType = typename decltype(type)::type;

          const auto output_column_statistics = std::make_shared<AttributeStatistics<ColumnDataType>>();

          // Use some magic oracle to find out if we want to merge or use the full histogram.

          const auto chunk_count = table.chunk_count();

          const auto should_generate_small_histograms = (chunk_count > 1) && column_data_type != DataType::String;
          std::shared_ptr<EqualDistinctCountHistogram<ColumnDataType>> histogram = nullptr;
          if (should_generate_small_histograms) {
            // Generate the small histograms.
            // Per Segment statistics will be twice as detailed.
            std::vector<std::shared_ptr<EqualDistinctCountHistogram<ColumnDataType>>> histograms;
            histograms.reserve(chunk_count);

            for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
              const auto output_chunk_column_statistics = std::make_shared<AttributeStatistics<ColumnDataType>>();
              const auto segment_histogram = EqualDistinctCountHistogram<ColumnDataType>::from_segment(
                  table, my_column_id, chunk_id, histogram_bin_count * 2);
              _add_statistics_from_histogram(output_chunk_column_statistics, segment_histogram, table);
              segment_statistics[my_column_id].push_back(output_chunk_column_statistics);
              histograms.push_back(segment_histogram);
            }

            const auto merge_result =
                EqualDistinctCountHistogram<ColumnDataType>::merge(histograms, histogram_bin_count);
            histogram = merge_result.first;
            // This gives us the maximum deviation of the total distinct count when compared to the full histogram.
            // The error might be different when measured with respect to the individual unique values.
            const auto max_estimation_error = merge_result.second;
            // Using the max_estimation_error, we can detect if the merged histogram is significantly too bad.
            // In that case we generate the full histogram.
            // Wasting time is better than using bad histograms.
            const auto error_percent_threshold =
                0.05;  // Some more elaborate experiments might be necessary to find a good value for this.
            if (max_estimation_error > histogram->total_distinct_count() * error_percent_threshold) {
              histogram =
                  EqualDistinctCountHistogram<ColumnDataType>::from_column(table, my_column_id, histogram_bin_count);
            }
          } else {
            histogram =
                EqualDistinctCountHistogram<ColumnDataType>::from_column(table, my_column_id, histogram_bin_count);
          }

          _add_statistics_from_histogram(output_column_statistics, histogram, table);

          column_statistics[my_column_id] = output_column_statistics;
        });
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  return std::make_shared<TableStatistics>(std::move(column_statistics), std::move(segment_statistics),
                                           table.row_count());
}

TableStatistics::TableStatistics(std::vector<std::shared_ptr<BaseAttributeStatistics>>&& init_column_statistics,
                                 const Cardinality init_row_count)
    : column_statistics(std::move(init_column_statistics)), row_count(init_row_count) {}

TableStatistics::TableStatistics(
    std::vector<std::shared_ptr<BaseAttributeStatistics>>&& init_column_statistics,
    std::vector<std::vector<std::shared_ptr<BaseAttributeStatistics>>>&& init_segment_statistics,
    const Cardinality init_row_count)
    : column_statistics(std::move(init_column_statistics)),
      segment_statistics(std::move(init_segment_statistics)),
      row_count(init_row_count) {}

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
