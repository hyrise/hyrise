#include "generate_table_statistics.hpp"

#include <iostream>
#include <unordered_set>

#include "base_column_statistics.hpp"
#include "column_statistics.hpp"
#include "generate_column_statistics.hpp"
#include "resolve_type.hpp"
#include "operators/table_sample.hpp"
#include "operators/table_materialize.hpp"
#include "operators/table_wrapper.hpp"
#include "statistics/statistics_objects/null_value_ratio.hpp"
#include "statistics/histograms/generic_histogram_builder.hpp"
#include "statistics/histograms/equal_distinct_count_histogram.hpp"
#include "statistics/histograms/histogram_utils.hpp"
#include "statistics/table_statistics_slice.hpp"
#include "statistics/segment_statistics2.hpp"
#include "statistics/table_statistics2.hpp"
#include "storage/table.hpp"
#include "table_statistics.hpp"

namespace {

using namespace opossum;  // NOLINT

template<typename T>
std::shared_ptr<AbstractHistogram<T>> rescale_histogram(const AbstractHistogram<T>& input_histogram, const float rescale_factor) {
  GenericHistogramBuilder<T> builder{input_histogram.bin_count(), input_histogram.string_domain()};

  for (auto bin_id = BinID{0}; bin_id < input_histogram.bin_count(); ++bin_id) {
    const auto input_bin = input_histogram.bin(bin_id);

    builder.add_bin(input_bin.min, input_bin.max, input_bin.height * rescale_factor, input_bin.distinct_count * rescale_factor);
  }

  return builder.build();
}

}  // namespace

namespace opossum {

TableStatistics generate_table_statistics(const Table &table) {
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

void generate_table_statistics2(const std::shared_ptr<Table>& table) {
  std::cout << "generate_table_statistics2(): Table with " << table->chunk_count() << " chunks; " << table->row_count()
            << " rows;" << std::endl;

  const auto histogram_bin_count = std::min<size_t>(100, std::max<size_t>(5, table->row_count() / 2'000));

  const auto chunk_statistics = std::make_shared<TableStatisticsSlice>(table->row_count());
  chunk_statistics->segment_statistics.resize(table->column_count());

  for (auto column_id = ColumnID{0}; column_id < table->column_count(); ++column_id) {
    const auto column_data_type = table->column_data_type(column_id);

    resolve_data_type(column_data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;

      const auto segment_statistics = std::make_shared<SegmentStatistics2<ColumnDataType>>();

      auto histogram = std::shared_ptr<AbstractHistogram<ColumnDataType>>{};

      if (std::is_same_v<ColumnDataType, std::string>) {
        const auto string_histogram_domain = StringHistogramDomain{};
        const auto value_distribution = histogram::value_distribution_from_column<ColumnDataType>(*table, column_id, string_histogram_domain);
        histogram = EqualDistinctCountHistogram<ColumnDataType>::from_distribution(value_distribution, histogram_bin_count, string_histogram_domain);
      } else {
        const auto value_distribution = histogram::value_distribution_from_column<ColumnDataType>(*table, column_id);
        histogram = EqualDistinctCountHistogram<ColumnDataType>::from_distribution(value_distribution, histogram_bin_count);
      }

      if (histogram) {
        segment_statistics->set_statistics_object(histogram);

        // Use the insight the the histogram will only contain non-null values to generate the NullValueRatio property
        const auto null_value_ratio = table->row_count() == 0 ? 0.0f : 1.0f - (static_cast<float>(histogram->total_count()) / table->row_count());
        segment_statistics->set_statistics_object(std::make_shared<NullValueRatio>(null_value_ratio));
      } else {
        // Failure to generate a histogram currently only stems from all-null segments.
        // TODO(anybody) this is a slippery assumption. But the alternative would be a full segment scan...
        segment_statistics->set_statistics_object(std::make_shared<NullValueRatio>(1.0f));
      }

      chunk_statistics->segment_statistics[column_id] = segment_statistics;
    });
  }

  table->table_statistics2()->table_statistics_slice_sets.emplace_back(TableStatisticsSliceSet{chunk_statistics});
}

void generate_compact_table_statistics(const std::shared_ptr<Table>& table) {
  auto& table_statistics = *table->table_statistics2();

  if (table_statistics.table_statistics_slice_sets.empty() || table_statistics.table_statistics_slice_sets.front().empty()) {
    return;
  }

  if (table_statistics.table_statistics_slice_sets.size() == 2) {
    return;
  }

  // TODO(moritz) revise
  Assert(table->table_statistics2()->table_statistics_slice_sets.size() <= 2, "Unexpected amount of ChunkStatisticsSets");

  std::cout << "generate_compact_table_statistics():    Compacting..." << std::endl;

  constexpr auto TARGET_SAMPLING_RATIO = 0.001f;
  constexpr auto MIN_NUM_ROWS = 100;
  constexpr auto MAX_NUM_ROWS = 10'000;
  constexpr auto TARGET_VALUES_PER_BIN = 60;
  constexpr auto MIN_HISTOGRAM_BIN_COUNT = 5;
  constexpr auto MAX_HISTOGRAM_BIN_COUNT = 50;

  const auto sample_num_rows = std::min<float>(MAX_NUM_ROWS, std::max<float>(MIN_NUM_ROWS, table->row_count() * TARGET_SAMPLING_RATIO));

  std::cout << "generate_compact_table_statistics():      Sampling table to " << sample_num_rows << " rows" << std::endl;

  const auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();
  const auto table_sample = std::make_shared<TableSample>(table_wrapper, static_cast<size_t>(sample_num_rows));
  table_sample->execute();
  const auto table_materialize = std::make_shared<TableMaterialize>(table_sample);
  table_materialize->execute();
  const auto sampled_table = table_materialize->get_output();

  const auto chunk_statistics_compact = std::make_shared<TableStatisticsSlice>(table->row_count());
  chunk_statistics_compact->segment_statistics.resize(table->column_count());

  for (auto column_id = ColumnID{0}; column_id < table->column_count(); ++column_id) {
    std::cout << "generate_compact_table_statistics():      Column " << column_id << " '" << table->column_name(column_id) << "'" << std::flush;
    const auto column_data_type = table->column_data_type(column_id);

    resolve_data_type(column_data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;

      /**
       * Determine the number of NULL values in the column across all segments. This value is used to inflate the
       * sampled histogram later
       */
      auto null_value_count = 0.0f;
      for (const auto& chunk_statistics : table_statistics.table_statistics_slice_sets.front()) {
        const auto chunk_null_value_ratio = chunk_statistics->estimate_column_null_value_ratio(column_id);
        if (chunk_null_value_ratio) {
          null_value_count += chunk_statistics->row_count * *chunk_null_value_ratio;
        }
      }

      chunk_statistics_compact->segment_statistics[column_id] = std::make_shared<SegmentStatistics2<ColumnDataType>>();

      const auto histogram_bin_count = std::min<size_t>(MAX_HISTOGRAM_BIN_COUNT, std::max<size_t>(MIN_HISTOGRAM_BIN_COUNT, sampled_table->row_count() / TARGET_VALUES_PER_BIN));
      std::cout << "; target bin count: " << histogram_bin_count << std::flush;

      auto histogram_compact = std::shared_ptr<EqualDistinctCountHistogram<ColumnDataType>>{};

      if (std::is_same_v<ColumnDataType, std::string>) {
        histogram_compact =
        EqualDistinctCountHistogram<ColumnDataType>::from_segment(sampled_table->get_chunk(ChunkID{0})->get_segment(column_id), histogram_bin_count, StringHistogramDomain{});

      } else {
        histogram_compact =
        EqualDistinctCountHistogram<ColumnDataType>::from_segment(sampled_table->get_chunk(ChunkID{0})->get_segment(column_id), histogram_bin_count);

      }


      if (histogram_compact) {
        const auto rescale_factor = histogram_compact->total_count() > 0 ? static_cast<float>(table->row_count() - null_value_count) / histogram_compact->total_count() : 1.0f;
        const auto rescaled_histogram = rescale_histogram(*histogram_compact, rescale_factor);
        std::cout << "; sampled histogram: " << histogram_compact->bin_count() << " bins" << std::endl;
        chunk_statistics_compact->segment_statistics[column_id]->set_statistics_object(rescaled_histogram);
      } else {
        std::cout << "; failed to created histogram" << std::endl;
      }
    });
  }

  table_statistics.table_statistics_slice_sets.emplace_back(TableStatisticsSliceSet{chunk_statistics_compact});
}

}  // namespace opossum
