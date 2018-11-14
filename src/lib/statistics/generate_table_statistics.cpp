#include "generate_table_statistics.hpp"

#include <iostream>
#include <unordered_set>

#include "base_column_statistics.hpp"
#include "column_statistics.hpp"
#include "generate_column_statistics.hpp"
#include "resolve_type.hpp"
#include "statistics/chunk_statistics/histograms/equal_distinct_count_histogram.hpp"
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
    const auto& chunk_statistics = table.table_statistics2()->chunk_statistics[chunk_id];

    const auto bin_count = std::min<size_t>(100, std::max<size_t>(2, chunk->size() / 10));

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
        segment_statistics->set_statistics_object(histogram);

        std::cout << "generate_table_statistics2():     Column " << table.column_name(column_id) << ": "
                  << histogram->bin_count() << " bins" << std::endl;
      });
    }
  }
}

}  // namespace opossum
