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
* Replaces the dictionary segments of the given segments by new dictionary segments that have a shared dictionary.
*/
template <typename T>
void apply_shared_dictionary_to_segments(const std::vector<std::shared_ptr<DictionarySegment<T>>>& segments,
                                         const std::shared_ptr<pmr_vector<T>> shared_dictionary,
                                         const PolymorphicAllocator<T>& allocator) {
  for (auto segment : segments) {
    const auto max_value_id = static_cast<uint32_t>(shared_dictionary->size());
    const auto chunk_size = segment->size();
    
    // Construct new attribute vector that maps to the shared dictionary
    auto uncompressed_attribute_vector = pmr_vector<uint32_t>{allocator};
    uncompressed_attribute_vector.reserve(chunk_size);

    for (auto chunk_index = ChunkOffset{0}; chunk_index < chunk_size; ++chunk_index) {
      const auto search_value_opt = segment->get_typed_value(chunk_index);
      if (search_value_opt){
        // Find and add new value id using binary search
        const auto search_iter = std::lower_bound(shared_dictionary->cbegin(), shared_dictionary->cend(), search_value_opt.value());
        DebugAssert(search_iter != shared_dictionary->end(), "Merged dictionary does not contain value.");
        const auto found_index = std::distance(shared_dictionary->cbegin(), search_iter);
        uncompressed_attribute_vector.emplace_back(found_index);
      }
      else{
        // Assume that search value is NULL
        uncompressed_attribute_vector.emplace_back(max_value_id);
      }
    }

    const auto compressed_attribute_vector = std::shared_ptr<const BaseCompressedVector>(
      compress_vector(uncompressed_attribute_vector, VectorCompressionType::FixedWidthInteger, allocator, {max_value_id}));

    // Create new dictionary segment
    const auto new_dictionary_segment =
        std::make_shared<DictionarySegment<T>>(shared_dictionary, compressed_attribute_vector);

    // Replace dictionary segment with new one that has a shared dictionary
    // TODO(hig): Consider using Chunk::replace_segment
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
        
        const auto allocator = PolymorphicAllocator<ColumnDataType>{};
        
        auto current_shared_dictionary = std::make_shared<pmr_vector<ColumnDataType>>(allocator);
        auto dictionary_segments_to_merge = std::vector<std::shared_ptr<DictionarySegment<ColumnDataType>>>{};
        
        std::shared_ptr<DictionarySegment<ColumnDataType>> previous_dictionary_segment = nullptr;
        std::shared_ptr<DictionarySegment<ColumnDataType>> current_dictionary_segment = nullptr;

        for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
          const auto chunk = table->get_chunk(chunk_id);
          const auto segment = chunk->get_segment(column_id);
          current_dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<ColumnDataType>>(segment);

          if (previous_dictionary_segment && current_dictionary_segment) {
            DebugAssert(previous_dictionary_segment != current_dictionary_segment, "Comparison of the same segment.");
            const auto previous_dictionary = previous_dictionary_segment->dictionary();
            const auto current_dictionary = current_dictionary_segment->dictionary();

            auto potential_new_shared_dictionary = std::make_shared<pmr_vector<ColumnDataType>>(allocator);
            auto intersection_size = 0ul;
            auto union_size = 0ul;
            if (current_shared_dictionary->empty()) {
              std::set_union(previous_dictionary->cbegin(), previous_dictionary->cend(), current_dictionary->cbegin(),
                             current_dictionary->cend(), std::back_inserter(*potential_new_shared_dictionary));
              union_size = potential_new_shared_dictionary->size();
              intersection_size = previous_dictionary->size() + current_dictionary->size() - union_size;
            } else {
              std::set_union(current_shared_dictionary->cbegin(), current_shared_dictionary->cend(),
                             current_dictionary->cbegin(), current_dictionary->cend(),
                             std::back_inserter(*potential_new_shared_dictionary));
              union_size = potential_new_shared_dictionary->size();
              intersection_size = current_shared_dictionary->size() + previous_dictionary->size() - union_size;
            }

            const auto jaccard_index = intersection_size * 1.0 / union_size;
            if (jaccard_index >= jaccard_index_threshold) {
              // add to dictionary sharing queue because dictionarys are similar enough (jaccard index is over threshold)
              if (current_shared_dictionary->empty()) {
                dictionary_segments_to_merge.push_back(previous_dictionary_segment);
              }
              dictionary_segments_to_merge.push_back(current_dictionary_segment);
              current_shared_dictionary = potential_new_shared_dictionary;
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
                apply_shared_dictionary_to_segments<ColumnDataType>(dictionary_segments_to_merge, current_shared_dictionary, allocator);
                std::cout << "Merge completed, new dictionary size: " << std::endl;
                for (const auto segment : dictionary_segments_to_merge) {
                  std::cout << segment->dictionary()->size() << ", ";
                }
                std::cout << std::endl;
                // TODO: output dictionary_segments_to_merge

                dictionary_segments_to_merge.clear();
              }
              current_shared_dictionary->clear();
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
