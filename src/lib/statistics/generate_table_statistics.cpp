#include "generate_table_statistics.hpp"

#include <iostream>
#include <unordered_set>

#include "base_column_statistics.hpp"
#include "column_statistics.hpp"
#include "generate_column_statistics.hpp"
#include "resolve_type.hpp"
#include "statistics/chunk_statistics/histograms/equal_distinct_count_histogram.hpp"
#include "statistics/chunk_statistics/histograms/histogram_utils.hpp"
#include "statistics/chunk_statistics2.hpp"
#include "statistics/segment_statistics2.hpp"
#include "statistics/table_statistics2.hpp"
#include "storage/table.hpp"
#include "table_statistics.hpp"

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

void generate_table_statistics2(Table& table) {
  std::cout << "generate_table_statistics2(): Table with " << table.chunk_count() << " chunks; " << table.row_count()
            << " rows;" << std::endl;

  for (auto chunk_id = ChunkID{0}; chunk_id < table.chunk_count(); ++chunk_id) {
    const auto chunk = table.get_chunk(chunk_id);
    const auto& chunk_statistics = table.table_statistics2()->chunk_statistics_sets.front()[chunk_id];

    const auto bin_count = std::min<size_t>(10, std::max<size_t>(2, chunk->size() / 10));

    std::cout << "generate_table_statistics2():   Chunk " << chunk_id << ": ~" << bin_count << " bins" << std::endl;

    for (auto column_id = ColumnID{0}; column_id < table.column_count(); ++column_id) {
      const auto column_data_type = table.column_data_type(column_id);

      resolve_data_type(column_data_type, [&](auto type) {
        using ColumnDataType = typename decltype(type)::type;

        const auto segment_statistics = std::static_pointer_cast<SegmentStatistics2<ColumnDataType>>(
            chunk_statistics->segment_statistics[column_id]);
        if (segment_statistics->equal_distinct_count_histogram) return;

        const auto histogram =
            EqualDistinctCountHistogram<ColumnDataType>::from_segment(chunk->get_segment(column_id), bin_count);
        if (!histogram) {
          std::cout << "generate_table_statistics2():     Column " << table.column_name(column_id)
                    << ": Failed to generate histogram" << std::endl;
          return;
        }

        segment_statistics->set_statistics_object(histogram);

        std::cout << "generate_table_statistics2():     Column " << table.column_name(column_id) << ": "
                  << histogram->bin_count() << " bins" << std::endl;
      });
    }
  }

  /**
   * Compact statistics
   */
  generate_compact_table_statistics(*table.table_statistics2());
}

void generate_compact_table_statistics(TableStatistics2& table_statistics) {
  if (table_statistics.chunk_statistics_sets.empty() || table_statistics.chunk_statistics_sets.front().empty()) {
    return;
  }

  if (table_statistics.chunk_statistics_sets.size() == 2) {
    return;
  }

  // TODO(moritz) revise
  Assert(table_statistics.chunk_statistics_sets.size() <= 2, "Unexpected amount of ChunkStatisticsSets");

  std::cout << "generate_table_statistics2():    Compacting..." << std::endl;

  const auto chunk_statistics_compact = std::make_shared<ChunkStatistics2>(table_statistics.row_count());
  chunk_statistics_compact->segment_statistics.resize(table_statistics.column_count());

  for (auto column_id = ColumnID{0}; column_id < table_statistics.column_count(); ++column_id) {
    const auto column_data_type = table_statistics.column_data_type(column_id);

    resolve_data_type(column_data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;

      chunk_statistics_compact->segment_statistics[column_id] = std::make_shared<SegmentStatistics2<ColumnDataType>>();

      if constexpr (!std::is_same_v<ColumnDataType, std::string>) {
        auto histogram_compact = std::shared_ptr<AbstractHistogram<ColumnDataType>>();

        for (auto chunk_id = ChunkID{0}; chunk_id < table_statistics.chunk_statistics_sets.front().size(); ++chunk_id) {
          const auto base_segment_statistics =
              table_statistics.chunk_statistics_sets.front()[chunk_id]->segment_statistics[column_id];
          const auto segment_statistics =
              std::dynamic_pointer_cast<SegmentStatistics2<ColumnDataType>>(base_segment_statistics);

          const auto segment_histogram = segment_statistics->get_best_available_histogram();

          if (!segment_histogram) {
            continue;
          }

          if (histogram_compact) {
            histogram_compact = histogram::merge_histograms(*histogram_compact, *segment_histogram);
          } else {
            histogram_compact = segment_histogram;
          }
        }

        if (!histogram_compact) {
          return;
        }

        histogram_compact = histogram::reduce_histogram(*histogram_compact, 2);

        if (histogram_compact) {
          //std::cout << "generate_table_statistics2():     Column " << column_id << " compacted to " << histogram_compact->bin_count() << " bins" << std::endl;

          chunk_statistics_compact->segment_statistics[column_id]->set_statistics_object(histogram_compact);
        }
      } else {
        //std::cout << "generate_table_statistics2():     Skipping string column " << column_id << std::endl;
      }
    });
  }

  table_statistics.chunk_statistics_sets.emplace_back(ChunkStatistics2Set{chunk_statistics_compact});
}

}  // namespace opossum
