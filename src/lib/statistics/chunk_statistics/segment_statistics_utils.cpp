#include "segment_statistics_utils.hpp"

#include <unordered_set>

#include "resolve_type.hpp"
#include "statistics/abstract_statistics_object.hpp"
#include "statistics/chunk_statistics/min_max_filter.hpp"
#include "statistics/chunk_statistics/range_filter.hpp"
#include "statistics/chunk_statistics2.hpp"
#include "statistics/segment_statistics2.hpp"
#include "statistics/table_statistics2.hpp"
#include "storage/chunk.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/table.hpp"
#include "utils/assert.hpp"

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
      pruning_filter = std::make_shared<MinMaxFilter<T>>(dictionary.front(), dictionary.back());
    }
  }

  pruning_filter->is_derived_from_complete_chunk = true;
  segment_statistics.set_statistics_object(pruning_filter);
}

}  // namespace

namespace opossum {

void create_pruning_filter_for_chunk(Table& table, const ChunkID chunk_id) {
  const auto chunk = table.get_chunk(chunk_id);
  Assert(!chunk->is_mutable(), "Pruning is only safe for immutable Chunks.");

  for (auto column_id = ColumnID{0}; column_id < chunk->column_count(); ++column_id) {
    const auto segment = chunk->get_segment(column_id);
    resolve_data_and_segment_type(*segment, [&](auto type, auto& typed_segment) {
      using SegmentType = std::decay_t<decltype(typed_segment)>;
      using ColumnDataType = typename decltype(type)::type;

      const auto segment_statistics = std::dynamic_pointer_cast<SegmentStatistics2<ColumnDataType>>(
          table.table_statistics2()->chunk_statistics[chunk_id]->segment_statistics[column_id]);

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
    });
  }
}

void create_pruning_filter_for_immutable_chunks(Table& table) {
  for (auto chunk_id = ChunkID{0}; chunk_id < table.chunk_count(); ++chunk_id) {
    if (!table.get_chunk(chunk_id)->is_mutable()) {
      create_pruning_filter_for_chunk(table, chunk_id);
    }
  }
}

}  // namespace opossum