#include <algorithm>
#include <fstream>
#include <iostream>
#include <iterator>

#include "../benchmarklib/tpcds/tpcds_table_generator.hpp"
#include "hyrise.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/dictionary_segment.hpp"
#include "synthetic_table_generator.hpp"
#include "types.hpp"
#include "lossless_cast.hpp"

using namespace opossum;  // NOLINT

// IDEA: Reuse v_intersection for next iteration if jaccard index is over threshold.
// IDEA: Not only compare with previous segment but also with already created dictionaries. This would also allow cross column comparisons.
// IDEA: set_intersection can be optimized if v_intersection is not needed.
// IDEA: set_intersection can be optimized if jaccard index threshold is not reachable by exiting earlier.
// IDEA: set_intersection can be optimized if jaccard index threshold is already reached by exiting earlier.

template <typename T>
bool canBeMerged(const std::shared_ptr<DictionarySegment<T>> segment1,
                 const std::shared_ptr<DictionarySegment<T>> segment2) {
  // TODO
  return true;
}

/**
* This will replace the dictionaries inside of the given segments by a shared (merged) dictionary.
* All attribute vectors in the dictionary segments will be updated to point to the correct values inside the shared dictionary.
*/
template <typename T>
void merge_dictionary_segments(std::vector<std::shared_ptr<DictionarySegment<T>>> segments,
                               std::vector<T> merged_dictionary) {
  // create new dictionary segments with shared dictionaries and replace the old ones
  const auto allocator = PolymorphicAllocator<T>{};
  auto shared_dictionary =
      std::make_shared<pmr_vector<T>>(merged_dictionary.cbegin(), merged_dictionary.cend(), allocator);
  for (auto segment : segments) {
    auto uncompressed_attribute_vector = pmr_vector<uint32_t>{allocator};
    const auto old_attribute_vector = segment->attribute_vector();
    const auto old_attribute_vector_decompressor = old_attribute_vector->create_base_decompressor();
    const auto old_attribute_vector_size = old_attribute_vector_decompressor->size();
    uncompressed_attribute_vector.reserve(old_attribute_vector_size);

    for (auto decompressor_index = 0ul; decompressor_index < old_attribute_vector_size; ++decompressor_index) {
      // TODO(hig): Ask how to get all value ids from dictionary segment
      const auto value_id = old_attribute_vector_decompressor->get(decompressor_index);
      const auto search_value_variant = segment->value_of_value_id(ValueID{value_id});
      // TODO(hig): Ask how to convert AllTypeVariant to typed value
      const auto search_value = boost::get<T>(search_value_variant);
      const auto search_iter = std::lower_bound(merged_dictionary.begin(), merged_dictionary.end(), search_value);
      const auto found_index = std::distance(merged_dictionary.begin(), search_iter);
      uncompressed_attribute_vector.emplace_back(found_index);
    }

    const auto max_value_id = static_cast<uint32_t>(shared_dictionary->size());
    const auto compressed_attribute_vector = std::shared_ptr<const BaseCompressedVector>(compress_vector(
        uncompressed_attribute_vector, VectorCompressionType::FixedWidthInteger, allocator, {max_value_id}));

    const auto new_dictionary_segment =
        std::make_shared<DictionarySegment<T>>(shared_dictionary, compressed_attribute_vector);

    // TODO(hig): Ask how to replace dictionary segment
    segment = std::move(new_dictionary_segment);
  }
}

int main() {
  std::cout << "Playground: Jaccard-Index" << std::endl;

  // Generate benchmark data
  const auto jaccard_index_threshold = 0.95;
  const auto scale_factor = 1u;
  const auto chunk_size = Chunk::DEFAULT_SIZE;
  const auto table_generator = std::make_unique<TPCDSTableGenerator>(scale_factor, chunk_size);
  table_generator->generate_and_store();

  // Create output file
  auto output_file_stream = std::ofstream("jaccard_index_log.txt", std::ofstream::out | std::ofstream::trunc);

  // Get tables using storage manager
  const auto& sm = Hyrise::get().storage_manager;
  auto table_names = sm.table_names();
  std::sort(table_names.begin(), table_names.end());

  // Calculate jaccard index for each column in each table
  // The jaccard index is calculated between a dictionary segment and its preceding dictionary segment
  for (const auto table_name : table_names) {
    const auto table = sm.get_table(table_name);
    const auto column_count = table->column_count();
    const auto chunk_count = table->chunk_count();

    for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
      const auto column_data_type = table->column_definitions()[column_id].data_type;
      const auto column_name = table->column_definitions()[column_id].name;
      resolve_data_type(column_data_type, [&](const auto type) {
        using ColumnDataType = typename decltype(type)::type;

        auto current_merged_dictionary = std::vector<ColumnDataType>{};
        auto dictionary_segments_to_merge = std::vector<std::shared_ptr<DictionarySegment<ColumnDataType>>>{};
        std::shared_ptr<DictionarySegment<ColumnDataType>> previous_dictionary_segment = nullptr;
        std::shared_ptr<DictionarySegment<ColumnDataType>> current_dictionary_segment = nullptr;

        for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
          const auto chunk = table->get_chunk(chunk_id);
          const auto segment = chunk->get_segment(column_id);
          current_dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<ColumnDataType>>(segment);

          if (previous_dictionary_segment && current_dictionary_segment) {
            Assert(previous_dictionary_segment != current_dictionary_segment, "Comparison of the same segment.");
            const auto previous_dictionary = previous_dictionary_segment->dictionary();
            const auto current_dictionary = current_dictionary_segment->dictionary();

            auto potential_new_merged_dictionary = std::vector<ColumnDataType>{};
            auto intersection_size = 0ul;
            auto union_size = 0ul;
            if (current_merged_dictionary.empty()) {
              std::set_union(previous_dictionary->begin(), previous_dictionary->end(), current_dictionary->begin(),
                             current_dictionary->end(), std::back_inserter(potential_new_merged_dictionary));
              union_size = potential_new_merged_dictionary.size();
              intersection_size = previous_dictionary->size() + current_dictionary->size() - union_size;
            } else {
              std::set_union(current_merged_dictionary.begin(), current_merged_dictionary.end(),
                             current_dictionary->begin(), current_dictionary->end(),
                             std::back_inserter(potential_new_merged_dictionary));
              union_size = potential_new_merged_dictionary.size();
              intersection_size = current_merged_dictionary.size() + previous_dictionary->size() - union_size;
            }

            const auto jaccard_index = intersection_size * 1.0 / union_size;
            if (jaccard_index >= jaccard_index_threshold) {
              // add to dictionary sharing queue because dictionarys are similar enough (jaccard index is over threshold)
              if (current_merged_dictionary.empty()) {
                dictionary_segments_to_merge.push_back(previous_dictionary_segment);
              }
              dictionary_segments_to_merge.push_back(current_dictionary_segment);
              current_merged_dictionary = potential_new_merged_dictionary;
            }
            
            bool is_last_chunk = chunk_id == chunk_count - 1;
            if (jaccard_index < jaccard_index_threshold || is_last_chunk) {
              if (!dictionary_segments_to_merge.empty()) {
                // make enqueued dictionaries shared

                std::cout << "Merging " << dictionary_segments_to_merge.size()
                          << " segments with dictionary sizes of: " << std::endl;
                for (const auto segment : dictionary_segments_to_merge) {
                  std::cout << segment->dictionary()->size() << ", ";
                }
                std::cout << std::endl;
                merge_dictionary_segments<ColumnDataType>(dictionary_segments_to_merge, current_merged_dictionary);
                std::cout << "Merge completed, new dictionary size: " << std::endl;
                for (const auto segment : dictionary_segments_to_merge) {
                  std::cout << segment->dictionary()->size() << ", ";
                }
                std::cout << std::endl;
                // TODO: output dictionary_segments_to_merge

                dictionary_segments_to_merge.clear();
              }
              current_merged_dictionary.clear();
            }

            output_file_stream << "Jaccard index = " << jaccard_index << " (Table=" << table_name
                               << ", Column=" << column_name << ", DataType=" << column_data_type
                               << ", Chunk=" << chunk_id << "\n";
            std::cout << "Jaccard index = " << jaccard_index << " (Table=" << table_name << ", Column=" << column_name
                      << ", DataType=" << column_data_type << ", Chunk=" << chunk_id << std::endl;
          }

          previous_dictionary_segment = current_dictionary_segment;
        }
      });
    }
  }

  output_file_stream.close();
  return 0;
}
