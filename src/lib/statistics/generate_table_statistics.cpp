#include "generate_table_statistics.hpp"

#include <atomic>
#include <iostream>
#include <thread>
#include <unordered_set>

#include "base_column_statistics.hpp"
#include "column_statistics.hpp"
#include "generate_column_statistics.hpp"
#include "operators/table_wrapper.hpp"
#include "resolve_type.hpp"
#include "statistics/histograms/equal_distinct_count_histogram.hpp"
#include "statistics/histograms/generic_histogram_builder.hpp"
#include "statistics/histograms/histogram_utils.hpp"
#include "statistics/segment_statistics2.hpp"
#include "statistics/statistics_objects/min_max_filter.hpp"
#include "statistics/statistics_objects/null_value_ratio.hpp"
#include "statistics/statistics_objects/range_filter.hpp"
#include "statistics/table_statistics2.hpp"
#include "statistics/table_statistics_slice.hpp"
#include "storage/table.hpp"
#include "table_statistics.hpp"

namespace {

using namespace opossum;  // NOLINT

template <typename T>
void create_pruning_filter_for_segment(SegmentStatistics2<T>& segment_statistics, const pmr_vector<T>& dictionary) {
  std::shared_ptr<AbstractStatisticsObject> pruning_filter;
  if constexpr (std::is_arithmetic_v<T>) {
    if (!segment_statistics.range_filter || !segment_statistics.range_filter->is_derived_from_complete_chunk) {
      pruning_filter = RangeFilter<T>::build_filter(dictionary);
    }
  } else {
    if (!segment_statistics.min_max_filter || !segment_statistics.min_max_filter->is_derived_from_complete_chunk) {
      if (!dictionary.empty()) {
        pruning_filter = std::make_shared<MinMaxFilter<T>>(dictionary.front(), dictionary.back());
      }
    }
  }

  if (pruning_filter) {
    pruning_filter->is_derived_from_complete_chunk = true;
    segment_statistics.set_statistics_object(pruning_filter);
  }
}

}  // namespace

namespace opossum {

TableStatistics generate_table_statistics(const Table& table) {
  std::vector<std::shared_ptr<const BaseColumnStatistics>> column_statistics;
  column_statistics.reserve(table.column_count());

  for (ColumnID column_id{0}; column_id < table.column_count(); ++column_id) {
    const auto column_data_type = table.column_data_types()[column_id];

    resolve_data_type(column_data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      column_statistics.emplace_back(generate_column_statistics<ColumnDataType>(table, column_id));
    });
  }

  return {table.type(), static_cast<float>(table.row_count()), column_statistics};
}

void generate_cardinality_estimation_statistics(const std::shared_ptr<Table>& table) {
  std::cout << "generate_cardinality_estimation_statistics(): Table with " << table->chunk_count() << " chunks; "
            << table->row_count() << " rows;" << std::endl;

  table->table_statistics2()->cardinality_estimation_slices.clear();

  const auto histogram_bin_count = std::min<size_t>(100, std::max<size_t>(5, table->row_count() / 2'000));

  const auto statistics_slice = std::make_shared<TableStatisticsSlice>(table->row_count());
  statistics_slice->segment_statistics.resize(table->column_count());

  auto next_column_id = std::atomic<size_t>{0u};
  auto threads = std::vector<std::thread>{};

  for (auto thread_id = 0u;
       thread_id < std::min(static_cast<uint>(table->column_count()), std::thread::hardware_concurrency() + 1);
       ++thread_id) {
    threads.emplace_back([&] {
      while (true) {
        auto my_column_id = ColumnID{static_cast<ColumnID::base_type>(next_column_id++)};
        if (static_cast<size_t>(my_column_id) >= table->column_count()) return;

        const auto column_data_type = table->column_data_type(my_column_id);

        resolve_data_type(column_data_type, [&](auto type) {
          using ColumnDataType = typename decltype(type)::type;

          const auto segment_statistics = std::make_shared<SegmentStatistics2<ColumnDataType>>();

          auto histogram = std::shared_ptr<AbstractHistogram<ColumnDataType>>{};

          if (std::is_same_v<ColumnDataType, std::string>) {
            const auto string_histogram_domain = StringHistogramDomain{};
            const auto value_distribution =
            histogram::value_distribution_from_column<ColumnDataType>(*table, my_column_id, string_histogram_domain);
            histogram = EqualDistinctCountHistogram<ColumnDataType>::from_distribution(
            value_distribution, histogram_bin_count, string_histogram_domain);
          } else {
            const auto value_distribution = histogram::value_distribution_from_column<ColumnDataType>(*table, my_column_id);
            histogram =
            EqualDistinctCountHistogram<ColumnDataType>::from_distribution(value_distribution, histogram_bin_count);
          }

          if (histogram) {
            std::cout << "  " << table->column_name(my_column_id) << " bin count: " << histogram->bin_count() << std::endl;

            segment_statistics->set_statistics_object(histogram);

            // Use the insight the the histogram will only contain non-null values to generate the NullValueRatio property
            const auto null_value_ratio =
            table->row_count() == 0 ? 0.0f : 1.0f - (static_cast<float>(histogram->total_count()) / table->row_count());
            segment_statistics->set_statistics_object(std::make_shared<NullValueRatio>(null_value_ratio));
          } else {
            // Failure to generate a histogram currently only stems from all-null segments.
            // TODO(anybody) this is a slippery assumption. But the alternative would be a full segment scan...
            segment_statistics->set_statistics_object(std::make_shared<NullValueRatio>(1.0f));
          }

          statistics_slice->segment_statistics[my_column_id] = segment_statistics;
        });
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  table->table_statistics2()->cardinality_estimation_slices.emplace_back(statistics_slice);
}

void generate_chunk_pruning_statistics(const std::shared_ptr<Table>& table) {
  for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
    const auto chunk = table->get_chunk(chunk_id);

    if (chunk->is_mutable()) {
      continue;
    }

    const auto chunk_statistics = std::make_shared<TableStatisticsSlice>(chunk->size());

    for (auto column_id = ColumnID{0}; column_id < chunk->column_count(); ++column_id) {
      const auto segment = chunk->get_segment(column_id);

      resolve_data_and_segment_type(*segment, [&](auto type, auto& typed_segment) {
        using SegmentType = std::decay_t<decltype(typed_segment)>;
        using ColumnDataType = typename decltype(type)::type;

        const auto segment_statistics = std::make_shared<SegmentStatistics2<ColumnDataType>>();

        if constexpr (std::is_same_v<SegmentType, DictionarySegment<ColumnDataType>>) {
          // we can use the fact that dictionary segments have an accessor for the dictionary
          const auto& dictionary = *typed_segment.dictionary();
          create_pruning_filter_for_segment(*segment_statistics, dictionary);
        } else {
          // if we have a generic segment we create the dictionary ourselves
          auto iterable = create_iterable_from_segment<ColumnDataType>(typed_segment);
          std::unordered_set<ColumnDataType> values;
          iterable.for_each([&](const auto& value) {
            // we are only interested in non-null values
            if (!value.is_null()) {
              values.insert(value.value());
            }
          });
          pmr_vector<ColumnDataType> dictionary{values.cbegin(), values.cend()};
          std::sort(dictionary.begin(), dictionary.end());
          create_pruning_filter_for_segment(*segment_statistics, dictionary);
        }

        chunk_statistics->segment_statistics.emplace_back(segment_statistics);
      });
    }

    table->table_statistics2()->chunk_pruning_statistics.emplace(chunk_id, chunk_statistics);
  }
}

}  // namespace opossum
