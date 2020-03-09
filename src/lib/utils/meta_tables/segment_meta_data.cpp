#include "segment_meta_data.hpp"

#include "hyrise.hpp"
#include "resolve_type.hpp"
#include "storage/base_encoded_segment.hpp"
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

        const auto estimated_size = segment->memory_usage(mode);
        AllTypeVariant encoding = NULL_VALUE;
        AllTypeVariant vector_compression = NULL_VALUE;
        if (const auto& encoded_segment = std::dynamic_pointer_cast<BaseEncodedSegment>(segment)) {
          encoding = pmr_string{encoding_type_to_string.left.at(encoded_segment->encoding_type())};

          if (encoded_segment->compressed_vector_type()) {
            std::stringstream ss;
            ss << *encoded_segment->compressed_vector_type();
            vector_compression = pmr_string{ss.str()};
          }
        }

        if (mode == MemoryUsageCalculationMode::Full) {
          const auto distinct_value_count = static_cast<int64_t>(get_distinct_value_count(segment));
          meta_table->append({pmr_string{table_name}, static_cast<int32_t>(chunk_id), static_cast<int32_t>(column_id),
                              pmr_string{table->column_name(column_id)}, data_type, distinct_value_count, encoding,
                              vector_compression, static_cast<int64_t>(estimated_size)});
        } else {
          meta_table->append({pmr_string{table_name}, static_cast<int32_t>(chunk_id), static_cast<int32_t>(column_id),
                              pmr_string{table->column_name(column_id)}, data_type, encoding, vector_compression,
                              static_cast<int64_t>(estimated_size)});
        }
      }
    }
  }
}

size_t get_distinct_value_count(const std::shared_ptr<BaseSegment>& segment) {
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
