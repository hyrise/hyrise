#include "segment_meta_data.hpp"

#include "hyrise.hpp"
#include "resolve_type.hpp"
#include "storage/abstract_encoded_segment.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/fixed_string_dictionary_segment.hpp"

namespace opossum {

void gather_segment_meta_data(const std::shared_ptr<Table>& meta_table, const MemoryUsageCalculationMode mode) {
  for (const auto& [table_name, table] : Hyrise::get().storage_manager.tables()) {
    for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
      for (auto column_id = ColumnID{0}; column_id < table->column_count(); ++column_id) {
        const auto& chunk = table->get_chunk(chunk_id);
        const auto& segment = chunk->get_segment(column_id);

        const auto data_type = pmr_string{data_type_to_string.left.at(table->column_data_type(column_id))};

        std::vector<size_t> string_lengths(chunk->size());
        auto value_switches = size_t{0};
        resolve_data_and_segment_type(*segment, [&](auto segment_data_type, auto& typed_segment) {
          using ColumnDataType = typename decltype(segment_data_type)::type;

          auto write_index = size_t{0};
          ColumnDataType previous_value = {};

          auto iterable = create_iterable_from_segment<ColumnDataType>(typed_segment);
          iterable.for_each([&](const auto& value) {
            if constexpr (std::is_same_v<ColumnDataType, pmr_string>) {
              string_lengths[write_index] = value.value().length();
            }

            // Execute if first iterat
            if (write_index == 0) {
              previous_value = value.value();
              ++write_index;
              return;
            }

            if (previous_value != value.value()) {
              ++value_switches;
              previous_value = value.value();
            }

            ++write_index;
          });
        });

        const auto estimated_size = segment->memory_usage(mode);

        AllTypeVariant avg_string_length = NULL_VALUE;
        AllTypeVariant max_string_length = NULL_VALUE;
        if (data_type == "string") {
          avg_string_length = static_cast<int64_t>(std::accumulate(string_lengths.cbegin(), string_lengths.cend(), 0ul) / string_lengths.size());
          max_string_length = static_cast<int64_t>(*std::max_element(string_lengths.cbegin(), string_lengths.cend()));
        }

        AllTypeVariant encoding = NULL_VALUE;
        AllTypeVariant vector_compression = NULL_VALUE;
        if (const auto& encoded_segment = std::dynamic_pointer_cast<AbstractEncodedSegment>(segment)) {
          encoding = pmr_string{encoding_type_to_string.left.at(encoded_segment->encoding_type())};

          if (encoded_segment->compressed_vector_type()) {
            std::stringstream ss;
            ss << *encoded_segment->compressed_vector_type();
            vector_compression = pmr_string{ss.str()};
          }
        }

        const auto& access_counter = segment->access_counter;

        if (mode == MemoryUsageCalculationMode::Full) {
          const auto distinct_value_count = static_cast<int64_t>(get_distinct_value_count(segment));
          meta_table->append({pmr_string{table_name}, static_cast<int32_t>(chunk_id), static_cast<int32_t>(column_id),
                              pmr_string{table->column_name(column_id)}, data_type, distinct_value_count, encoding,
                              vector_compression, static_cast<int64_t>(estimated_size),
                              static_cast<int64_t>(value_switches),
                              avg_string_length,
                              max_string_length,
                              static_cast<int64_t>(access_counter[SegmentAccessCounter::AccessType::Point]),
                              static_cast<int64_t>(access_counter[SegmentAccessCounter::AccessType::Sequential]),
                              static_cast<int64_t>(access_counter[SegmentAccessCounter::AccessType::Monotonic]),
                              static_cast<int64_t>(access_counter[SegmentAccessCounter::AccessType::Random])});
        } else {
          meta_table->append({pmr_string{table_name}, static_cast<int32_t>(chunk_id), static_cast<int32_t>(column_id),
                              pmr_string{table->column_name(column_id)}, data_type, encoding, vector_compression,
                              static_cast<int64_t>(estimated_size),
                              static_cast<int64_t>(value_switches),
                              avg_string_length,
                              max_string_length,
                              static_cast<int64_t>(access_counter[SegmentAccessCounter::AccessType::Point]),
                              static_cast<int64_t>(access_counter[SegmentAccessCounter::AccessType::Sequential]),
                              static_cast<int64_t>(access_counter[SegmentAccessCounter::AccessType::Monotonic]),
                              static_cast<int64_t>(access_counter[SegmentAccessCounter::AccessType::Random])});
        }
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
    } else if (const auto fs_dictionary_segment =
                   std::dynamic_pointer_cast<const FixedStringDictionarySegment<pmr_string>>(segment)) {
      distinct_value_count = fs_dictionary_segment->fixed_string_dictionary()->size();
      return;
    }

    std::unordered_set<ColumnDataType> distinct_values;
    auto iterable = create_any_segment_iterable<ColumnDataType>(*segment);
    iterable.with_iterators([&](auto it, auto end) {
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

}  // namespace opossum
