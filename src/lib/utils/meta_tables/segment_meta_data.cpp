#include "segment_meta_data.hpp"

#include "magic_enum.hpp"

#include "hyrise.hpp"
#include "resolve_type.hpp"
#include "statistics/attribute_statistics.hpp"
#include "storage/abstract_encoded_segment.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/fixed_string_dictionary_segment.hpp"

namespace hyrise {

void gather_segment_meta_data(const std::shared_ptr<Table>& meta_table, const MemoryUsageCalculationMode mode) {
  for (const auto& [table_name, table] : Hyrise::get().storage_manager.tables()) {
    const auto chunk_count = table->chunk_count();
    for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
      const auto& chunk = table->get_chunk(chunk_id);
      // Skip physically deleted chunks
      if (!chunk) {
        continue;
      }

      const auto column_count = table->column_count();
      for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
        const auto& segment = chunk->get_segment(column_id);

        const auto data_type = table->column_data_type(column_id);
        const auto data_type_str = pmr_string{data_type_to_string.left.at(data_type)};

        const auto estimated_size = segment->memory_usage(mode);
        auto encoding = NULL_VALUE;
        auto vector_compression = NULL_VALUE;
        if (const auto& encoded_segment = std::dynamic_pointer_cast<AbstractEncodedSegment>(segment)) {
          encoding = pmr_string{magic_enum::enum_name(encoded_segment->encoding_type())};

          if (encoded_segment->compressed_vector_type()) {
            std::stringstream sstream;
            sstream << *encoded_segment->compressed_vector_type();
            vector_compression = pmr_string{sstream.str()};
          }
        }

        auto distinct_value_count = NULL_VALUE;
        const auto& pruning_statistics = chunk->pruning_statistics();
        if (pruning_statistics) {
          Assert(pruning_statistics->size() > column_id, "Malformed pruning statistics");
          resolve_data_type(data_type, [&](auto type) {
            using ColumnDataType = typename decltype(type)::type;

            if (const auto attribute_statistics =
                    std::dynamic_pointer_cast<AttributeStatistics<ColumnDataType>>((*pruning_statistics)[column_id])) {
              const auto& distinct_value_count_object = attribute_statistics->distinct_value_count;
              if (distinct_value_count_object) {
                distinct_value_count = static_cast<int64_t>(distinct_value_count_object->count);
              }
            }
          });
        }

        if (mode == MemoryUsageCalculationMode::Full && variant_is_null(distinct_value_count)) {
          distinct_value_count = static_cast<int64_t>(get_distinct_value_count(segment));
        }

        const auto& access_counter = segment->access_counter;
        meta_table->append({pmr_string{table_name}, static_cast<int32_t>(chunk_id), static_cast<int32_t>(column_id),
                            pmr_string{table->column_name(column_id)}, data_type_str, distinct_value_count, encoding,
                            vector_compression, static_cast<int64_t>(estimated_size),
                            static_cast<int64_t>(access_counter[SegmentAccessCounter::AccessType::Point]),
                            static_cast<int64_t>(access_counter[SegmentAccessCounter::AccessType::Sequential]),
                            static_cast<int64_t>(access_counter[SegmentAccessCounter::AccessType::Monotonic]),
                            static_cast<int64_t>(access_counter[SegmentAccessCounter::AccessType::Random]),
                            static_cast<int64_t>(access_counter[SegmentAccessCounter::AccessType::Dictionary])});
      }
    }
  }
}

size_t get_distinct_value_count(const std::shared_ptr<AbstractSegment>& segment) {
  auto distinct_value_count = size_t{0};
  resolve_data_type(segment->data_type(), [&](auto type) {
    using ColumnDataType = typename decltype(type)::type;

    // For dictionary segments, an early (and much faster) exit is possible by using the dictionary size
    if (const auto dictionary_segment = std::dynamic_pointer_cast<const DictionarySegment<ColumnDataType>>(segment)) {
      distinct_value_count = dictionary_segment->dictionary()->size();
      return;
    }

    if (const auto fs_dictionary_segment =
            std::dynamic_pointer_cast<const FixedStringDictionarySegment<pmr_string>>(segment)) {
      distinct_value_count = fs_dictionary_segment->fixed_string_dictionary()->size();
      return;
    }

    auto distinct_values = std::unordered_set<ColumnDataType>{};
    auto iterable = create_any_segment_iterable<ColumnDataType>(*segment);
    iterable.with_iterators([&](auto it, const auto end) {
      for (; it != end; ++it) {
        const auto segment_item = *it;
        if (!segment_item.is_null()) {
          distinct_values.insert(segment_item.value());
        }
      }
    });
    distinct_value_count = distinct_values.size();
  });
  return distinct_value_count;
}

}  // namespace hyrise
