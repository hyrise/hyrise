#include "table_statistics.hpp"

#include <numeric>
#include <iostream>
#include <thread>

#include "attribute_statistics.hpp"
#include "resolve_type.hpp"
#include "statistics/statistics_objects/abstract_histogram.hpp"
#include "storage/table.hpp"
#include "utils/assert.hpp"

namespace opossum {

std::shared_ptr<TableStatistics> TableStatistics::from_table(const Table& table) {
  std::vector<std::shared_ptr<BaseAttributeStatistics>> column_statistics(table.column_count());
  std::vector<std::shared_ptr<BaseAttributeStatistics>> new_column_statistics(table.column_count());
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
                table.row_count() == 0
                    ? 0.0f
                    : 1.0f - (static_cast<float>(histogram->total_count()) / static_cast<float>(table.row_count()));
            output_column_statistics->set_statistics_object(
                std::make_shared<NullValueRatioStatistics>(null_value_ratio));
          } else {
            // Failure to generate a histogram currently only stems from all-null segments.
            // TODO(anybody) this is a slippery assumption. But the alternative would be a full segment scan...
            output_column_statistics->set_statistics_object(std::make_shared<NullValueRatioStatistics>(1.0f));
          }

          column_statistics[my_column_id] = output_column_statistics;

          // For now, also create the per segment statistics here. A bit redundant...
          // Per Segment statistics will be twice as detailed.
          // std::cout << "Start chunk based histograms for column" << std::endl;
          const auto chunk_count = table.chunk_count();
          for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
            const auto output_chunk_column_statistics = std::make_shared<AttributeStatistics<ColumnDataType>>();
            const auto histogram = EqualDistinctCountHistogram<ColumnDataType>::from_segment(
                table, my_column_id, chunk_id, histogram_bin_count * 2);
            if (histogram) {
              output_chunk_column_statistics->set_statistics_object(histogram);

              // Use the insight that the histogram will only contain non-null values to generate the NullValueRatio
              // property
              const auto null_value_ratio =
                  table.row_count() == 0
                      ? 0.0f
                      : 1.0f - (static_cast<float>(histogram->total_count()) / static_cast<float>(table.row_count()));
              output_chunk_column_statistics->set_statistics_object(
                  std::make_shared<NullValueRatioStatistics>(null_value_ratio));
            } else {
              // Failure to generate a histogram currently only stems from all-null segments.
              // TODO(anybody) this is a slippery assumption. But the alternative would be a full segment scan...
              output_chunk_column_statistics->set_statistics_object(std::make_shared<NullValueRatioStatistics>(1.0f));
            }
            segment_statistics[my_column_id].push_back(output_chunk_column_statistics);
          }
          if (column_data_type != DataType::String) {
            std::cout << column_data_type << " in column " << my_column_id<< std::endl;

            // std::cout << "Merging chunk based histograms for column, chunk count = " << chunk_count << ", " << my_column_id << std::endl;
            //Merge small histograms
            // std::vector<std::shared_ptr<AbstractTask>> jobs;
            // jobs.reserve(static_cast<size_t>(num_chunks));
            // jobs.emplace_back(std::make_shared<JobTask>([&, column_index]() {}));
            // jobs.back()->schedule();
            // Hyrise::get().scheduler()->wait_for_tasks(jobs);
            std::shared_ptr<EqualDistinctCountHistogram<ColumnDataType>> merged_histogram = nullptr;
            for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
              if (!std::dynamic_pointer_cast<AttributeStatistics<ColumnDataType>>(segment_statistics[my_column_id][chunk_id])) continue;
              const auto attribute_statistics = std::dynamic_pointer_cast<AttributeStatistics<ColumnDataType>>(segment_statistics[my_column_id][chunk_id]);
              const auto histogram = std::dynamic_pointer_cast<EqualDistinctCountHistogram<ColumnDataType>>(attribute_statistics->histogram);
              // std::cout << "Start Merging (col)" << my_column_id << std::endl;
              merged_histogram = EqualDistinctCountHistogram<ColumnDataType>::merge(merged_histogram, histogram, histogram_bin_count);
              // std::cout << "End Merging (col)" << my_column_id << std::endl;
            }
            const auto new_output_column_statistics = std::make_shared<AttributeStatistics<ColumnDataType>>();
            if (merged_histogram) {
              new_output_column_statistics->set_statistics_object(merged_histogram);

              // Use the insight that the histogram will only contain non-null values to generate the NullValueRatio
              // property
              const auto null_value_ratio =
                  table.row_count() == 0
                      ? 0.0f
                      : 1.0f - (static_cast<float>(merged_histogram->total_count()) / static_cast<float>(table.row_count()));
              new_output_column_statistics->set_statistics_object(
                  std::make_shared<NullValueRatioStatistics>(null_value_ratio));
            } else {
              // Failure to generate a histogram currently only stems from all-null segments.
              // TODO(anybody) this is a slippery assumption. But the alternative would be a full segment scan...
              new_output_column_statistics->set_statistics_object(std::make_shared<NullValueRatioStatistics>(1.0f));
            }
            new_column_statistics[my_column_id] = new_output_column_statistics;
          }
          // std::cout << "Finished chunk based histograms for column" << my_column_id << std::endl;
        });
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  return std::make_shared<TableStatistics>(std::move(column_statistics), std::move(new_column_statistics), std::move(segment_statistics),
                                           table.row_count());
}

TableStatistics::TableStatistics(std::vector<std::shared_ptr<BaseAttributeStatistics>>&& init_column_statistics,
                                 const Cardinality init_row_count)
    : column_statistics(std::move(init_column_statistics)), row_count(init_row_count) {}

TableStatistics::TableStatistics(
    std::vector<std::shared_ptr<BaseAttributeStatistics>>&& init_column_statistics,
    std::vector<std::shared_ptr<BaseAttributeStatistics>>&& init_previous_statistics,
    std::vector<std::vector<std::shared_ptr<BaseAttributeStatistics>>>&& init_segment_statistics,
    const Cardinality init_row_count)
    : column_statistics(std::move(init_column_statistics)),
      previous_column_statistics(std::move(init_previous_statistics)),
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
