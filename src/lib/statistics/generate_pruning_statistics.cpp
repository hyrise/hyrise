#include "generate_pruning_statistics.hpp"

#include <atomic>
#include <iostream>
#include <thread>
#include <unordered_set>

#include "operators/table_wrapper.hpp"
#include "resolve_type.hpp"
#include "statistics/attribute_statistics.hpp"
#include "statistics/statistics_objects/equal_distinct_count_histogram.hpp"
#include "statistics/statistics_objects/generic_histogram_builder.hpp"
#include "statistics/statistics_objects/min_max_filter.hpp"
#include "statistics/statistics_objects/null_value_ratio_statistics.hpp"
#include "statistics/statistics_objects/range_filter.hpp"
#include "statistics/table_statistics.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "storage/table.hpp"

namespace {

using namespace opossum;  // NOLINT

template <typename T>
void create_pruning_statistics_for_segment(AttributeStatistics<T>& segment_statistics,
                                           const pmr_vector<T>& dictionary) {
  std::shared_ptr<AbstractStatisticsObject> pruning_statistics;
  if constexpr (std::is_arithmetic_v<T>) {
    pruning_statistics = RangeFilter<T>::build_filter(dictionary);
  } else {
    if (!dictionary.empty()) {
      pruning_statistics = std::make_shared<MinMaxFilter<T>>(dictionary.front(), dictionary.back());
    }
  }

  if (pruning_statistics) {
    segment_statistics.set_statistics_object(pruning_statistics);
  }
}

}  // namespace

namespace opossum {

void generate_chunk_pruning_statistics(const std::shared_ptr<Chunk>& chunk) {
  auto chunk_statistics = ChunkPruningStatistics{chunk->column_count()};

  for (auto column_id = ColumnID{0}; column_id < chunk->column_count(); ++column_id) {
    const auto segment = chunk->get_segment(column_id);

    resolve_data_and_segment_type(*segment, [&](auto type, auto& typed_segment) {
      using SegmentType = std::decay_t<decltype(typed_segment)>;
      using ColumnDataType = typename decltype(type)::type;

      const auto segment_statistics = std::make_shared<AttributeStatistics<ColumnDataType>>();

      if constexpr (std::is_same_v<SegmentType, DictionarySegment<ColumnDataType>>) {
        // we can use the fact that dictionary segments have an accessor for the dictionary
        const auto& dictionary = *typed_segment.dictionary();
        create_pruning_statistics_for_segment(*segment_statistics, dictionary);
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
        create_pruning_statistics_for_segment(*segment_statistics, dictionary);
      }

      chunk_statistics[column_id] = segment_statistics;
    });
  }

  chunk->set_pruning_statistics(chunk_statistics);
}

void generate_chunk_pruning_statistics(const std::shared_ptr<Table>& table) {
  const auto chunk_count = table->chunk_count();
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto chunk = table->get_chunk(chunk_id);

    if (!chunk || chunk->is_mutable()) {
      continue;
    }

    generate_chunk_pruning_statistics(chunk);
  }
}

}  // namespace opossum
