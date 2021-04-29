#include "segment_meta_data.hpp"

#include "hyrise.hpp"
#include "resolve_type.hpp"
#include "storage/abstract_encoded_segment.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/fixed_string_dictionary_segment.hpp"

namespace {
 
using namespace opossum;  // NOLINT

// Only store selected segment data to keep the size of these maps small.
//                         table                   chunk_id column_id encoding_str                          
static std::map<std::tuple<std::shared_ptr<Table>, ChunkID, ColumnID, AllTypeVariant>, int64_t> sizes_cache{};
static std::map<std::tuple<std::shared_ptr<Table>, ChunkID, ColumnID>, int64_t> distinct_values_count_cache{};

int64_t get_distinct_value_count(const std::shared_ptr<AbstractSegment>& segment) {
  auto distinct_value_count = int64_t{0};
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

int64_t segment_size(
              const std::shared_ptr<Table>& table,
  const std::shared_ptr<AbstractSegment>& segment, const MemoryUsageCalculationMode mode,  const ChunkID chunk_id, const ColumnID column_id,
                        const pmr_string& data_type, const AllTypeVariant encoding_str) {
  if (data_type != "string") {
    return segment->memory_usage(mode);
  }

  if (sizes_cache.contains({table, chunk_id, column_id, encoding_str})) {
    // std::cout << "got " << sizes_cache[{table, chunk_id, column_id, encoding_str}] << " from cache for " << table << "." << table->column_name(column_id) << std::endl;
    Assert((static_cast<int64_t>(segment->memory_usage(mode)) == sizes_cache[{table, chunk_id, column_id, encoding_str}]), "Size mismatch.");
    return sizes_cache[{table, chunk_id, column_id, encoding_str}];
  }
  
  const auto size = segment->memory_usage(mode);
  // std::cout << "added " << size << " to cache for " << table << "." << table->column_name(column_id) << std::endl;
  sizes_cache.emplace(std::tuple<std::shared_ptr<Table>, ChunkID, ColumnID, AllTypeVariant>{table, chunk_id, column_id, encoding_str}, size);
  return size;
}

int64_t segment_distinct_values_count(
              const std::shared_ptr<Table>& table,
  const std::shared_ptr<AbstractSegment>& segment, const MemoryUsageCalculationMode mode,  const ChunkID chunk_id, const ColumnID column_id) {

  if (distinct_values_count_cache.contains({table, chunk_id, column_id})) {
    // std::cout << "got " << distinct_values_count_cache[{table, chunk_id, column_id}] << " from cache for " << table << "." << table->column_name(column_id) << std::endl;
    Assert((get_distinct_value_count(segment) == distinct_values_count_cache[{table, chunk_id, column_id}]), "Distinct values count mismatch.");
    return distinct_values_count_cache[{table, chunk_id, column_id}];
  }
  
  const auto distinct_values_count = get_distinct_value_count(segment);
  // std::cout << "added " << distinct_values_count << " to cache for " << table << "." << table->column_name(column_id) << std::endl;
  distinct_values_count_cache.emplace(std::tuple<std::shared_ptr<Table>, ChunkID, ColumnID>{table, chunk_id, column_id}, distinct_values_count);
  return distinct_values_count;
}

}  // namespace

namespace opossum {

void gather_segment_meta_data(const std::shared_ptr<Table>& meta_table, const MemoryUsageCalculationMode mode) {
  for (const auto& [table_name, table] : Hyrise::get().storage_manager.tables()) {
    for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
      const auto& chunk = table->get_chunk(chunk_id);
      if (!chunk) continue;  // Skip physically deleted chunks

      for (auto column_id = ColumnID{0}; column_id < table->column_count(); ++column_id) {
        const auto& segment = chunk->get_segment(column_id);

        const auto data_type = pmr_string{data_type_to_string.left.at(table->column_data_type(column_id))};

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

        const auto estimated_size = segment_size(table, segment, mode, chunk_id, column_id, data_type, encoding);
        const auto& access_counter = segment->access_counter;
        if (mode == MemoryUsageCalculationMode::Full) {
          const auto distinct_values_count = segment_distinct_values_count(table, segment, mode, chunk_id, column_id);
          meta_table->append({pmr_string{table_name}, static_cast<int32_t>(chunk_id), static_cast<int32_t>(column_id),
                              pmr_string{table->column_name(column_id)}, data_type, distinct_values_count, encoding,
                              vector_compression, estimated_size,
                              static_cast<int64_t>(access_counter[SegmentAccessCounter::AccessType::Point]),
                              static_cast<int64_t>(access_counter[SegmentAccessCounter::AccessType::Sequential]),
                              static_cast<int64_t>(access_counter[SegmentAccessCounter::AccessType::Monotonic]),
                              static_cast<int64_t>(access_counter[SegmentAccessCounter::AccessType::Random]),
                              static_cast<int64_t>(access_counter[SegmentAccessCounter::AccessType::Dictionary])});
        } else {
          meta_table->append({pmr_string{table_name}, static_cast<int32_t>(chunk_id), static_cast<int32_t>(column_id),
                              pmr_string{table->column_name(column_id)}, data_type, encoding, vector_compression,
                              static_cast<int64_t>(estimated_size),
                              static_cast<int64_t>(access_counter[SegmentAccessCounter::AccessType::Point]),
                              static_cast<int64_t>(access_counter[SegmentAccessCounter::AccessType::Sequential]),
                              static_cast<int64_t>(access_counter[SegmentAccessCounter::AccessType::Monotonic]),
                              static_cast<int64_t>(access_counter[SegmentAccessCounter::AccessType::Random]),
                              static_cast<int64_t>(access_counter[SegmentAccessCounter::AccessType::Dictionary])});
        }
      }
    }
  }
}

}  // namespace opossum
