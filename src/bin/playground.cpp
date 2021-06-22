#include <algorithm>
#include <fstream>
#include <iostream>
#include <iterator>

#include "../benchmarklib/tpcds/tpcds_table_generator.hpp"
#include "hyrise.hpp"
#include "lossless_cast.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/dictionary_segment.hpp"
#include "synthetic_table_generator.hpp"
#include "types.hpp"

using namespace opossum;  // NOLINT

// IDEA: Not only compare with previous segment but also with already created dictionaries. This would also allow cross column comparisons.
// IDEA: set_union can be optimized if jaccard index threshold is not reachable by exiting earlier.

/**
* Replaces the dictionary segments of the given segments by new dictionary segments that have a shared dictionary.
*/
template <typename T>
void apply_shared_dictionary_to_segments(const std::vector<std::shared_ptr<DictionarySegment<T>>>& segments,
                                         const std::shared_ptr<pmr_vector<T>> shared_dictionary,
                                         const PolymorphicAllocator<T>& allocator) {
  const auto max_value_id = static_cast<uint32_t>(shared_dictionary->size());
  for (auto segment : segments) {
    const auto chunk_size = segment->size();

    // Construct new attribute vector that maps to the shared dictionary
    auto uncompressed_attribute_vector = pmr_vector<uint32_t>{allocator};
    uncompressed_attribute_vector.reserve(chunk_size);

    for (auto chunk_index = ChunkOffset{0}; chunk_index < chunk_size; ++chunk_index) {
      const auto search_value_opt = segment->get_typed_value(chunk_index);
      if (search_value_opt) {
        // Find and add new value id using binary search
        const auto search_iter =
            std::lower_bound(shared_dictionary->cbegin(), shared_dictionary->cend(), search_value_opt.value());
        DebugAssert(search_iter != shared_dictionary->end(), "Merged dictionary does not contain value.");
        const auto found_index = std::distance(shared_dictionary->cbegin(), search_iter);
        uncompressed_attribute_vector.emplace_back(found_index);
      } else {
        // Assume that search value is NULL
        uncompressed_attribute_vector.emplace_back(max_value_id);
      }
    }

    const auto compressed_attribute_vector = std::shared_ptr<const BaseCompressedVector>(compress_vector(
        uncompressed_attribute_vector, VectorCompressionType::FixedWidthInteger, allocator, {max_value_id}));

    // Create new dictionary segment
    const auto new_dictionary_segment =
        std::make_shared<DictionarySegment<T>>(shared_dictionary, compressed_attribute_vector);

    // Replace dictionary segment with new one that has a shared dictionary
    // TODO(hig): Consider using Chunk::replace_segment
    segment = std::move(new_dictionary_segment);
  }
}

/**
* Calculates and returns Jaccard index.
*/
double calc_jaccard_index(size_t union_size, size_t intersection_size) { return intersection_size * 1.0 / union_size; }

void log_jaccard_index(const double jaccard_index, const std::string& table_name, const std::string& column_name,
                       const std::string& compare_type, const DataType& column_data_type, const ChunkID chunk_id,
                       std::ofstream& output_file_stream) {
  const auto data_type_name = data_type_to_string.left.at(column_data_type);
  std::cout << "Jaccard index = " << jaccard_index << " (" << compare_type << ") (Table=" << table_name
            << ", Column=" << column_name << ", DataType=" << column_data_type << ", Chunk=" << chunk_id << "\n";
  output_file_stream << table_name << ";" << column_name << ";" << data_type_name << ";" << chunk_id << ";"
                     << compare_type << ";" << jaccard_index << std::endl;
}

template <typename T>
void debug_output_segment_counts(const std::vector<std::shared_ptr<DictionarySegment<T>>>& dictionary_segments_to_merge,
                                 const std::string& text) {
  std::cout << "Dictionary segments to merge " << text << " " << dictionary_segments_to_merge.size()
            << " segments with dictionary sizes of: " << std::endl;
  for (const auto segmentPtr : dictionary_segments_to_merge) {
    std::cout << segmentPtr->dictionary()->size() << ", ";
  }
  std::cout << std::endl;
}

int main() {
  std::cout << "Playground: Jaccard-Index" << std::endl;

  // Generate benchmark data
  const auto jaccard_index_threshold = 0.95;
  const auto scale_factor = 1u;
  const auto chunk_size = Chunk::DEFAULT_SIZE;
  // TODO: generate joinorderbenchmark tables
  const auto table_generator = std::make_unique<TPCDSTableGenerator>(scale_factor, chunk_size);
  table_generator->generate_and_store();

  // Create output file
  auto output_file_stream = std::ofstream("jaccard_index_log.csv", std::ofstream::out | std::ofstream::trunc);
  output_file_stream << "Table;Column;DataType;Chunk;CompareType;JaccardIndex";

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

        auto previous_shared_dictionary = std::make_shared<pmr_vector<ColumnDataType>>(allocator);
        auto current_shared_dictionary = std::make_shared<pmr_vector<ColumnDataType>>(allocator);
        auto dictionary_segments_to_merge = std::vector<std::shared_ptr<DictionarySegment<ColumnDataType>>>{};

        std::shared_ptr<DictionarySegment<ColumnDataType>> previous_dictionary_segment = nullptr;
        std::shared_ptr<DictionarySegment<ColumnDataType>> current_dictionary_segment = nullptr;

        for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
          const auto chunk = table->get_chunk(chunk_id);
          const auto segment = chunk->get_segment(column_id);

          current_dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<ColumnDataType>>(segment);
          const auto current_dictionary = current_dictionary_segment->dictionary();
          auto current_jaccard_index = 0.0;
          auto current_compare_type = std::string{};

          auto potential_new_shared_dictionary = std::make_shared<pmr_vector<ColumnDataType>>(allocator);

          if (current_shared_dictionary->empty()) {
            // The current shared dictionary is empty.
            // This means that we want to create a new shared dictionary using the previous and current segment
            // or merge the current segment with the last shared dictionary.

            if (!previous_shared_dictionary->empty()) {
              // First try to merge with previous shared dictionary
              std::set_union(current_dictionary->cbegin(), current_dictionary->cend(),
                             previous_shared_dictionary->cbegin(), previous_shared_dictionary->cend(),
                             std::back_inserter(*potential_new_shared_dictionary));
              const auto total_size = current_dictionary->size() + previous_shared_dictionary->size();
              const auto union_size = potential_new_shared_dictionary->size();
              current_jaccard_index = calc_jaccard_index(union_size, total_size - union_size);
              current_compare_type = "PreviousShared";
            }

            if (current_jaccard_index < jaccard_index_threshold && previous_dictionary_segment) {
              // If the merge with the last shared dictionary was not successful, try to merge the neighboring segments.
              const auto previous_dictionary = previous_dictionary_segment->dictionary();
              std::set_union(current_dictionary->cbegin(), current_dictionary->cend(), previous_dictionary->cbegin(),
                             previous_dictionary->cend(), std::back_inserter(*potential_new_shared_dictionary));
              const auto total_size = current_dictionary->size() + previous_dictionary->size();
              const auto union_size = potential_new_shared_dictionary->size();
              current_jaccard_index = calc_jaccard_index(union_size, total_size - union_size);
              current_compare_type = "NeighboringSegments";
            }
          } else {
            // The current shared dictionary is not empty.
            // This means that we want to merge the current segment to this already existing dictionary.
            std::set_union(current_dictionary->cbegin(), current_dictionary->cend(),
                           current_shared_dictionary->cbegin(), current_shared_dictionary->cend(),
                           std::back_inserter(*potential_new_shared_dictionary));
            const auto total_size = current_dictionary->size() + current_shared_dictionary->size();
            const auto union_size = potential_new_shared_dictionary->size();
            current_jaccard_index = calc_jaccard_index(union_size, total_size - union_size);
            current_compare_type = "CurrentShared";
          }

          if (current_jaccard_index >= jaccard_index_threshold) {
            // The jaccard index matches the threshold, so we add the segment to the collection
            dictionary_segments_to_merge.push_back(current_dictionary_segment);
            current_shared_dictionary = potential_new_shared_dictionary;
          }

          bool is_last_chunk = chunk_id == chunk_count - 1;
          if (current_jaccard_index < jaccard_index_threshold || is_last_chunk) {
            // The jaccard index is below the threshold or we processed the last chunk.
            // This means that we apply the the shared dictionary to the collected segments.
            if (!dictionary_segments_to_merge.empty()) {
              debug_output_segment_counts(dictionary_segments_to_merge, "before");
              apply_shared_dictionary_to_segments<ColumnDataType>(dictionary_segments_to_merge,
                                                                  current_shared_dictionary, allocator);
              debug_output_segment_counts(dictionary_segments_to_merge, "after");

              // Save current_shared_dictionary to previous_shared_dictionary and reset
              std::swap(previous_shared_dictionary, current_shared_dictionary);
              current_shared_dictionary->clear();
              dictionary_segments_to_merge.clear();
            } else {
              current_shared_dictionary->clear();
            }
          }

          log_jaccard_index(current_jaccard_index, table_name, column_name, current_compare_type, column_data_type,
                            chunk_id, output_file_stream);

          previous_dictionary_segment = current_dictionary_segment;
        }
      });
    }
  }

  output_file_stream.close();
  return 0;
}
