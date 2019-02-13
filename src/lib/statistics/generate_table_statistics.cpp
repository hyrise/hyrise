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
#include "statistics/histograms/equal_distinct_count_histogram.hpp"
#include "statistics/histograms/histogram_utils.hpp"
#include "statistics/chunk_statistics2.hpp"
#include "statistics/segment_statistics2.hpp"
#include "statistics/table_statistics2.hpp"
#include "storage/table.hpp"
#include "table_statistics.hpp"

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

  for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
    const auto chunk = table->get_chunk(chunk_id);
    const auto &chunk_statistics = table->table_statistics2()->chunk_statistics_sets.front()[chunk_id];

    const auto histogram_bin_count = std::min<size_t>(100, std::max<size_t>(5, chunk->size() / 2'000));

    std::cout << "generate_table_statistics2():   Chunk " << chunk_id << ": ~" << histogram_bin_count << " bins"
              << std::endl;

    for (auto column_id = ColumnID{0}; column_id < table->column_count(); ++column_id) {
      const auto column_data_type = table->column_data_type(column_id);

      resolve_data_type(column_data_type, [&](auto type) {
        using ColumnDataType = typename decltype(type)::type;

        const auto segment_statistics = std::static_pointer_cast<SegmentStatistics2<ColumnDataType>>(
        chunk_statistics->segment_statistics[column_id]);
        if (segment_statistics->histogram) return;

        auto histogram = std::shared_ptr<AbstractHistogram<ColumnDataType>>{};

        if (std::is_same_v<ColumnDataType, std::string>) {
          histogram =
          EqualDistinctCountHistogram<ColumnDataType>::from_segment(chunk->get_segment(column_id), histogram_bin_count, StringHistogramDomain{});

        } else {
          histogram =
          EqualDistinctCountHistogram<ColumnDataType>::from_segment(chunk->get_segment(column_id), histogram_bin_count);

        }

        if (!histogram) {
          std::cout << "generate_table_statistics2():     Column " << table->column_name(column_id)
                    << ": Failed to generate histogram" << std::endl;
          return;
        }

        segment_statistics->set_statistics_object(histogram);
//
//        std::cout << "generate_table_statistics2():     Column " << table->column_name(column_id) << ": "
//                  << histogram->bin_count() << " bins" << std::endl;
      });
    }
  }

  /**
   * Compact statistics
   */
  generate_compact_table_statistics(table);
}

void generate_compact_table_statistics(const std::shared_ptr<Table>& table) {
  auto& table_statistics = *table->table_statistics2();

  if (table->table_statistics2()->chunk_statistics_sets.empty() || table->table_statistics2()->chunk_statistics_sets.front().empty()) {
    return;
  }

  if (table->table_statistics2()->chunk_statistics_sets.size() == 2) {
    return;
  }

  // TODO(moritz) revise
  Assert(table->table_statistics2()->chunk_statistics_sets.size() <= 2, "Unexpected amount of ChunkStatisticsSets");

  std::cout << "generate_compact_table_statistics():    Compacting..." << std::endl;

  constexpr auto TARGET_SAMPLING_RATIO = 0.001f;
  constexpr auto MIN_NUM_ROWS = 100;
  constexpr auto MAX_NUM_ROWS = 10'000;
  constexpr auto TARGET_VALUES_PER_BIN = 60;
  constexpr auto MIN_HISTOGRAM_BIN_COUNT = 5;
  constexpr auto MAX_HISTOGRAM_BIN_COUNT = 50;

  const auto sample_num_rows = std::min<float>(MAX_NUM_ROWS, std::max<float>(MIN_NUM_ROWS, table->row_count() * TARGET_SAMPLING_RATIO));
  const auto rescale_factor = static_cast<float>(table->row_count()) / sample_num_rows;

  std::cout << "generate_compact_table_statistics():      Sampling table to " << sample_num_rows << " rows" << std::endl;

  const auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();
  const auto table_sample = std::make_shared<TableSample>(table_wrapper, static_cast<size_t>(sample_num_rows));
  table_sample->execute();
  const auto table_materialize = std::make_shared<TableMaterialize>(table_sample);
  table_materialize->execute();
  const auto sampled_table = table_materialize->get_output();

  const auto chunk_statistics_compact = std::make_shared<ChunkStatistics2>(table->row_count());
  chunk_statistics_compact->segment_statistics.resize(table->column_count());

  for (auto column_id = ColumnID{0}; column_id < table->column_count(); ++column_id) {
    std::cout << "generate_compact_table_statistics():      Column " << column_id << " '" << table->column_name(column_id) << "'" << std::flush;
    const auto column_data_type = table->column_data_type(column_id);

    resolve_data_type(column_data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;

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
        const auto rescaled_histogram = histogram_compact->scaled(rescale_factor);
        std::cout << "; sampled histogram: " << histogram_compact->bin_count() << " bins" << std::endl;
        std::cout << histogram_compact->description(true) << std::endl;
        chunk_statistics_compact->segment_statistics[column_id]->set_statistics_object(histogram_compact);
      } else {
        std::cout << "; failed to created histogram" << std::endl;
      }
    });
  }

  table_statistics.chunk_statistics_sets.emplace_back(ChunkStatistics2Set{chunk_statistics_compact});
}

}  // namespace opossum
