#include <algorithm>
#include <fstream>
#include <iostream>
#include <iterator>

#include "../benchmarklib/tpcds/tpcds_table_generator.hpp"
#include "../lib/import_export/binary/binary_parser.hpp"
#include "../lib/import_export/binary/binary_writer.hpp"
#include "benchmark_config.hpp"
#include "file_based_table_generator.hpp"
#include "hyrise.hpp"
#include "lossless_cast.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/dictionary_segment.hpp"
#include "synthetic_table_generator.hpp"
#include "types.hpp"
#include "utils/size_estimation_utils.hpp"

using namespace opossum;  // NOLINT

// IDEA: Not only compare with previous segment but also with already created dictionaries. This would also allow cross column comparisons.
// IDEA: set_union can be optimized if jaccard index threshold is not reachable by exiting earlier.

template <typename T>
struct SegmentChunkColumn {
  std::shared_ptr<DictionarySegment<T>> segment;
  std::shared_ptr<Chunk> chunk;
  ColumnID column_id;
};

template <typename T>
size_t calc_dictionary_memory_usage(const std::shared_ptr<const pmr_vector<T>> dictionary) {
  // TODO(hig): It can happen that the new dictionary is bigger than the old one, although the dictionaries have the same values
  // Is there maybe a string compression?

  if constexpr (std::is_same_v<T, pmr_string>) {
    return string_vector_memory_usage(*dictionary, MemoryUsageCalculationMode::Sampled);
  }
  return dictionary->size() * sizeof(typename decltype(dictionary)::element_type::value_type);
}

/**
* Replaces the dictionary segments of the given segments by new dictionary segments that have a shared dictionary.
* Returns the difference of the estimated memory usage.
**/
template <typename T>
int64_t apply_shared_dictionary_to_segments(const std::vector<SegmentChunkColumn<T>>& segment_chunk_columns,
                                            const std::shared_ptr<const pmr_vector<T>> shared_dictionary,
                                            const PolymorphicAllocator<T>& allocator) {
  auto previous_attribute_vector_memory_usage = 0l;
  auto new_attribute_vector_memory_usage = 0l;
  auto previous_dictionary_memory_usage = 0l;
  auto new_dictionary_memory_usage = static_cast<int64_t>(calc_dictionary_memory_usage(shared_dictionary));

  const auto max_value_id = static_cast<uint32_t>(shared_dictionary->size());
  for (auto segment_chunk_column : segment_chunk_columns) {
    const auto segment = segment_chunk_column.segment;
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
    previous_attribute_vector_memory_usage += segment->attribute_vector()->data_size();
    previous_dictionary_memory_usage += calc_dictionary_memory_usage(segment->dictionary());

    segment_chunk_column.chunk->replace_segment(segment_chunk_column.column_id, new_dictionary_segment);

    new_attribute_vector_memory_usage += new_dictionary_segment->attribute_vector()->data_size();
  }

  

  const auto attribute_vector_memory_usage_diff =
      new_attribute_vector_memory_usage - previous_attribute_vector_memory_usage;
  const auto dictionary_memory_usage_diff = new_dictionary_memory_usage - previous_dictionary_memory_usage;
  
  std::cout << "Merged " << segment_chunk_columns.size()
            << " dictionaries. (dict_mem_diff = " << dictionary_memory_usage_diff
            << ", attr_vec_diff = " << attribute_vector_memory_usage_diff << ")" << std::endl;

  return attribute_vector_memory_usage_diff + dictionary_memory_usage_diff;
}

double calc_jaccard_index(size_t union_size, size_t intersection_size) { return intersection_size * 1.0 / union_size; }

void log_jaccard_index(const double jaccard_index, const std::string& table_name, const std::string& column_name,
                       const std::string& compare_type, const DataType& column_data_type, const ChunkID chunk_id,
                       std::ofstream& output_file_stream) {
  const auto data_type_name = data_type_to_string.left.at(column_data_type);
  // std::cout << "Jaccard index = " << jaccard_index << " (" << compare_type << ") (Table=" << table_name
  //           << ", Column=" << column_name << ", DataType=" << column_data_type << ", Chunk=" << chunk_id << "\n";
  output_file_stream << table_name << ";" << column_name << ";" << data_type_name << ";" << chunk_id << ";"
                     << compare_type << ";" << jaccard_index << std::endl;
}

int main() {
  std::cout << "Playground: Jaccard-Index" << std::endl;
  auto total_merged_dictionaries = 0ul;
  auto total_new_shared_dictionaries = 0ul;
  auto memory_usage_difference = 0l;

  // Generate benchmark data
  const auto jaccard_index_threshold = 0.6;
  const auto table_path = std::string{"/home/Halil.Goecer/imdb_bin/"};

  const auto table_generator = std::make_unique<FileBasedTableGenerator>(
      std::make_shared<BenchmarkConfig>(BenchmarkConfig::get_default_config()), table_path);
  table_generator->generate_and_store();

  // Get tables using storage manager
  auto& sm = Hyrise::get().storage_manager;

  //const auto table = BinaryParser::parse(table_path + "company_name.bin");
  //sm.add_table("company_name", table);

  // Create output file
  auto output_file_stream = std::ofstream("jaccard_index_log.csv", std::ofstream::out | std::ofstream::trunc);
  output_file_stream << "Table;Column;DataType;Chunk;CompareType;JaccardIndex\n";

  auto table_names = sm.table_names();
  std::sort(table_names.begin(), table_names.end());

  // Calculate jaccard index for each column in each table
  // The jaccard index is calculated between a dictionary segment and its preceding dictionary segment
  for (const auto table_name : table_names) {
    const auto table = sm.get_table(table_name);
    //BinaryWriter::write(*table, table_path + table_name + ".bin");
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
        auto dictionary_segments_to_merge = std::vector<SegmentChunkColumn<ColumnDataType>>{};

        std::shared_ptr<DictionarySegment<ColumnDataType>> previous_dictionary_segment = nullptr;
        std::shared_ptr<DictionarySegment<ColumnDataType>> current_dictionary_segment = nullptr;

        for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
          const auto chunk = table->get_chunk(chunk_id);
          const auto segment = chunk->get_segment(column_id);

          current_dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<ColumnDataType>>(segment);
          if (!current_dictionary_segment) {
            std::cerr << "Not a dictionary segment! " << chunk_id << "/" << column_id << std::endl;
            continue;
          }
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
            const auto segmentLocation =
                SegmentChunkColumn<ColumnDataType>{current_dictionary_segment, chunk, column_id};
            dictionary_segments_to_merge.push_back(segmentLocation);
            current_shared_dictionary = potential_new_shared_dictionary;
          }

          bool is_last_chunk = chunk_id == chunk_count - 1;
          if (current_jaccard_index < jaccard_index_threshold || is_last_chunk) {
            // The jaccard index is below the threshold or we processed the last chunk.
            // This means that we apply the the shared dictionary to the collected segments.
            if (dictionary_segments_to_merge.size() >= 2) {
              total_merged_dictionaries += dictionary_segments_to_merge.size();
              total_new_shared_dictionaries++;

              memory_usage_difference += apply_shared_dictionary_to_segments<ColumnDataType>(
                  dictionary_segments_to_merge, current_shared_dictionary, allocator);

              // Save current_shared_dictionary to previous_shared_dictionary
              std::swap(previous_shared_dictionary, current_shared_dictionary);
            }

            // Reset
            current_shared_dictionary->clear();
            dictionary_segments_to_merge.clear();
          }

          log_jaccard_index(current_jaccard_index, table_name, column_name, current_compare_type, column_data_type,
                            chunk_id, output_file_stream);

          previous_dictionary_segment = current_dictionary_segment;
        }
      });
    }
  }

  std::cout << "Merged " << total_merged_dictionaries << " dictionaries to " << total_new_shared_dictionaries
            << " shared dictionaries.\n";
  std::cout << "The estimated memory change is: " << memory_usage_difference << " bytes.\n";
  output_file_stream.close();
  return 0;
}
