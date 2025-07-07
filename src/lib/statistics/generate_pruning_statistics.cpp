#include "generate_pruning_statistics.hpp"

#include <memory>
#include <type_traits>
#include <unordered_set>
#include <vector>

#include <boost/sort/pdqsort/pdqsort.hpp>
#include <boost/unordered/unordered_flat_set.hpp>

#include "hyrise.hpp"
#include "resolve_type.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "statistics/attribute_statistics.hpp"
#include "statistics/statistics_objects/distinct_value_count.hpp"
#include "statistics/statistics_objects/equal_distinct_count_histogram.hpp"
#include "statistics/statistics_objects/min_max_filter.hpp"
#include "statistics/statistics_objects/range_filter.hpp"
#include "statistics/table_statistics.hpp"
#include "storage/chunk.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace {

using namespace hyrise;  // NOLINT (build/namespaces)

template <typename T>
void create_pruning_statistics_for_segment(AttributeStatistics<T>& segment_statistics,
                                           const pmr_vector<T>& dictionary) {
  if constexpr (std::is_arithmetic_v<T>) {
    segment_statistics.set_statistics_object(RangeFilter<T>::build_filter(dictionary));
  } else {
    if (!dictionary.empty()) {
      segment_statistics.set_statistics_object(
          std::make_shared<MinMaxFilter<T>>(dictionary.front(), dictionary.back()));
    }
  }

  segment_statistics.set_statistics_object(std::make_shared<DistinctValueCount>(dictionary.size()));
}

}  // namespace

namespace hyrise {
bool is_immutable_chunk_without_pruning_statistics(const std::shared_ptr<Chunk>& chunk) {
  // We do not generate statistics for chunks as long as they are mutable.
  // Also, pruning statistics should be stable no matter what encoding or sort order is used.
  // Hence, when they are present they are up to date, and we can skip the recreation.
  return chunk && !chunk->is_mutable() && !chunk->pruning_statistics();
}

void generate_chunk_pruning_statistics(const std::shared_ptr<Chunk>& chunk) {
  DebugAssert(is_immutable_chunk_without_pruning_statistics(chunk),
              "Method should only be called for qualifying chunks.");

  auto chunk_statistics = ChunkPruningStatistics{chunk->column_count()};

  for (auto column_id = ColumnID{0}; column_id < chunk->column_count(); ++column_id) {
    const auto segment = chunk->get_segment(column_id);

    resolve_data_and_segment_type(*segment, [&](auto type, auto& typed_segment) {
      using SegmentType = std::decay_t<decltype(typed_segment)>;
      using ColumnDataType = typename decltype(type)::type;

      const auto segment_statistics = std::make_shared<AttributeStatistics<ColumnDataType>>();

      // TODO(anyone): use dictionary-optimized path for FixedStringDictionarySegments as well.
      if constexpr (std::is_same_v<SegmentType, DictionarySegment<ColumnDataType>>) {
        // We can use the fact that dictionary segments have an accessor for the dictionary.
        const auto& dictionary = typed_segment.dictionary();
        create_pruning_statistics_for_segment(*segment_statistics, dictionary);
      } else {
        // If we have a generic segment, we create the dictionary ourselves. We use two approaches:
        //   (i) std::set: The set is usually faster for small distinct value counts.
        //   (ii) std::sort and std::unique(): This approach is faster when the set is so large that it no longer fits
        //        into the local CPU caches (we measured roughly 10,000 thousand strings).
        //   Both approaches run roughly in O(n*log(n)) -- (i) has n times an n*log(n) search, (ii) sorts in n*log(n).
        //   However, modern optimized sort algorithms are usually faster with many distinct values (see DYOD 2025,
        //   weeks 7 and 8).
        //   We sample the first 64 elements to recognize many repeated values.
        auto values = pmr_vector<ColumnDataType>{};
        values.reserve(segment->size());
        auto value_set = boost::unordered_flat_set<ColumnDataType>{};
        value_set.reserve(64);

        segment_with_iterators<ColumnDataType>(typed_segment, [&](const auto& begin, const auto& end) {
          auto iter = begin;
          for (auto counter = uint8_t{0}; counter < 64 && iter != end; ++iter) {
            if (!iter->is_null()) {
              value_set.insert(iter->value());
            }
          }

          if (value_set.size() < 32) {
            // Continue with set.
            for (; iter != end; ++iter) {
              if (!iter->is_null()) {
                value_set.insert(iter->value());
              }
            }

            values = pmr_vector<ColumnDataType>{value_set.cbegin(), value_set.cend()};
            boost::sort::pdqsort(values.begin(), values.end());
            return;
          }

          // Expecting many distinct values: using sort-based alternative.
          iter = begin;
          for (; iter != end; ++iter) {
            if (!iter->is_null()) {
              values.emplace_back(iter->value());
            }
          }
          boost::sort::pdqsort(values.begin(), values.end());
          values.erase(std::unique(values.begin(), values.end()), values.cend());
          values.shrink_to_fit();
        });

        create_pruning_statistics_for_segment(*segment_statistics, values);
      }

      chunk_statistics[column_id] = segment_statistics;
    });
  }

  chunk->set_pruning_statistics(chunk_statistics);
}

void generate_chunk_pruning_statistics(const std::shared_ptr<Table>& table) {
  const auto chunk_count = table->chunk_count();
  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  jobs.reserve(chunk_count);

  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto chunk = table->get_chunk(chunk_id);

    if (!is_immutable_chunk_without_pruning_statistics(chunk)) {
      continue;
    }

    jobs.emplace_back(std::make_shared<JobTask>([&, chunk]() {
      generate_chunk_pruning_statistics(chunk);
    }));
  }
  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);
}

}  // namespace hyrise
