#include "table_cardinality_estimation_statistics.hpp"

#include <numeric>
#include <thread>

#include "horizontal_statistics_slice.hpp"
#include "statistics/histograms/abstract_histogram.hpp"
#include "statistics/histograms/histogram_utils.hpp"
#include "storage/table.hpp"
#include "utils/assert.hpp"
#include "vertical_statistics_slice.hpp"

namespace opossum {

std::shared_ptr<TableCardinalityEstimationStatistics> TableCardinalityEstimationStatistics::from_table(
    const Table& table) {
  const auto table_statistics = std::make_shared<TableCardinalityEstimationStatistics>(table.column_data_types());

  const auto histogram_bin_count = std::min<size_t>(100, std::max<size_t>(5, table.row_count() / 2'000));

  const auto statistics_slice = std::make_shared<HorizontalStatisticsSlice>(table.row_count());
  statistics_slice->vertical_slices.resize(table.column_count());

  auto next_column_id = std::atomic<size_t>{0u};
  auto threads = std::vector<std::thread>{};

  for (auto thread_id = 0u;
       thread_id < std::min(static_cast<uint>(table.column_count()), std::thread::hardware_concurrency() + 1);
       ++thread_id) {
    threads.emplace_back([&] {
      while (true) {
        auto my_column_id = ColumnID{static_cast<ColumnID::base_type>(next_column_id++)};
        if (static_cast<size_t>(my_column_id) >= table.column_count()) return;

        const auto column_data_type = table.column_data_type(my_column_id);

        resolve_data_type(column_data_type, [&](auto type) {
          using ColumnDataType = typename decltype(type)::type;

          const auto vertical_slices = std::make_shared<VerticalStatisticsSlice<ColumnDataType>>();

          auto histogram = std::shared_ptr<AbstractHistogram<ColumnDataType>>{};

          const auto value_distribution =
              histogram::value_distribution_from_column<ColumnDataType>(table, my_column_id);
          histogram =
              EqualDistinctCountHistogram<ColumnDataType>::from_distribution(value_distribution, histogram_bin_count);

          if (histogram) {
            vertical_slices->set_statistics_object(histogram);

            // Use the insight the the histogram will only contain non-null values to generate the NullValueRatio property
            const auto null_value_ratio =
                table.row_count() == 0 ? 0.0f
                                       : 1.0f - (static_cast<float>(histogram->total_count()) / table.row_count());
            vertical_slices->set_statistics_object(std::make_shared<NullValueRatio>(null_value_ratio));
          } else {
            // Failure to generate a histogram currently only stems from all-null segments.
            // TODO(anybody) this is a slippery assumption. But the alternative would be a full segment scan...
            vertical_slices->set_statistics_object(std::make_shared<NullValueRatio>(1.0f));
          }

          statistics_slice->vertical_slices[my_column_id] = vertical_slices;
        });
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  table_statistics->horizontal_slices.emplace_back(statistics_slice);

  return table_statistics;
}

TableCardinalityEstimationStatistics::TableCardinalityEstimationStatistics(
    const std::vector<DataType>& column_data_types)
    : column_data_types(column_data_types) {}

Cardinality TableCardinalityEstimationStatistics::row_count() const {
  return std::accumulate(horizontal_slices.begin(), horizontal_slices.end(), Cardinality{0},
                         [](const auto& a, const auto& statistics_slice) { return a + statistics_slice->row_count; });
}

size_t TableCardinalityEstimationStatistics::column_count() const { return column_data_types.size(); }

std::ostream& operator<<(std::ostream& stream, const TableCardinalityEstimationStatistics& table_statistics) {
  stream << "TableCardinalityEstimationStatistics {" << std::endl;
  stream << "ApproxInvalidRowCount: " << table_statistics.approx_invalid_row_count.load() << "; " << std::endl;

  if (!table_statistics.horizontal_slices.empty()) {
    stream << "Horizontal Slices {" << std::endl;
    for (auto slice_idx = size_t{0}; slice_idx < table_statistics.horizontal_slices.size(); ++slice_idx) {
      stream << "Cardinality Estimation Slice " << slice_idx << ": {" << std::endl;
      stream << (*table_statistics.horizontal_slices[slice_idx]);
      stream << "} // Cardinality Estimation Slice " << slice_idx << std::endl;
    }
    stream << "} // Cardinality Estimation Statistics" << std::endl;
  }

  return stream;
}

}  // namespace opossum
