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
  const auto jaccard_index_threshold = 0.5;
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

        auto shared_dictionaries = std::vector<std::shared_ptr<pmr_vector<ColumnDataType>>>{};
        auto segments_to_merge_at = std::vector<std::vector<SegmentChunkColumn<ColumnDataType>>>{};
        auto last_merged_index = -1;

        std::optional<SegmentChunkColumn<ColumnDataType>> previous_segment_info_opt = std::nullopt;

        for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
          const auto chunk = table->get_chunk(chunk_id);
          const auto segment = chunk->get_segment(column_id);

          auto current_dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<ColumnDataType>>(segment);
          if (!current_dictionary_segment) {
            std::cerr << "Not a dictionary segment! " << chunk_id << "/" << column_id << std::endl;
            continue;
          }
          const auto current_dictionary = current_dictionary_segment->dictionary();
          auto current_jaccard_index = 0.0;
          auto current_compare_type = std::string{};

          auto potential_new_shared_dictionary = std::make_shared<pmr_vector<ColumnDataType>>(allocator);
          auto potential_new_shared_dictionary_index = -1;

          // Try to merge with last shared dictionary
          if (last_merged_index >= 0) {
            //TODO(hig): Fix code duplication
            auto shared_dictionary = shared_dictionaries[last_merged_index];
            std::set_union(current_dictionary->cbegin(), current_dictionary->cend(), shared_dictionary->cbegin(),
                           shared_dictionary->cend(), std::back_inserter(*potential_new_shared_dictionary));
            const auto total_size = current_dictionary->size() + shared_dictionary->size();
            const auto union_size = potential_new_shared_dictionary->size();
            current_jaccard_index = calc_jaccard_index(union_size, total_size - union_size);
            current_compare_type = "ExistingShared @ " + std::to_string(last_merged_index);
            if (current_jaccard_index >= jaccard_index_threshold) {
              potential_new_shared_dictionary_index = last_merged_index;
            }
            else{
              potential_new_shared_dictionary->clear();
            }
          }

          // Try to merge with other existing shared dictionaries.
          // Traverse in reverse order to first check the most recent created shared dictioanries.
          if (potential_new_shared_dictionary_index < 0) {
            for (auto shared_dictionary_index = static_cast<int>(shared_dictionaries.size()) - 1;
                 shared_dictionary_index >= 0; --shared_dictionary_index) {
              if (shared_dictionary_index != last_merged_index) {
                auto shared_dictionary = shared_dictionaries[shared_dictionary_index];
                std::set_union(current_dictionary->cbegin(), current_dictionary->cend(), shared_dictionary->cbegin(),
                               shared_dictionary->cend(), std::back_inserter(*potential_new_shared_dictionary));
                const auto total_size = current_dictionary->size() + shared_dictionary->size();
                const auto union_size = potential_new_shared_dictionary->size();
                current_jaccard_index = calc_jaccard_index(union_size, total_size - union_size);
                current_compare_type = "ExistingShared @ " + std::to_string(shared_dictionary_index);
                if (current_jaccard_index >= jaccard_index_threshold) {
                  potential_new_shared_dictionary_index = shared_dictionary_index;
                  break;
                }
                else{
                  potential_new_shared_dictionary->clear();
                }
              }
            }
          }

          // Try to merge with neighboring segments if we didn't find a good existing shared dictionary
          auto merged_with_previous = false;
          if (potential_new_shared_dictionary_index < 0 && previous_segment_info_opt) {
            // If the merge with the last shared dictionary was not successful, try to merge the neighboring segments.
            const auto previous_dictionary = previous_segment_info_opt->segment->dictionary();
            std::set_union(current_dictionary->cbegin(), current_dictionary->cend(), previous_dictionary->cbegin(),
                           previous_dictionary->cend(), std::back_inserter(*potential_new_shared_dictionary));
            const auto total_size = current_dictionary->size() + previous_dictionary->size();
            const auto union_size = potential_new_shared_dictionary->size();
            current_jaccard_index = calc_jaccard_index(union_size, total_size - union_size);
            current_compare_type = "NeighboringSegments";
            if (current_jaccard_index >= jaccard_index_threshold) {
              merged_with_previous = true;
            }
            else{
              potential_new_shared_dictionary->clear();
            }
          }

          const auto current_segment_info =
              SegmentChunkColumn<ColumnDataType>{current_dictionary_segment, chunk, column_id};
          if (current_jaccard_index >= jaccard_index_threshold) {
            // The jaccard index matches the threshold, so we add the segment to the collection

            if (potential_new_shared_dictionary_index < 0) {
              shared_dictionaries.emplace_back(potential_new_shared_dictionary);
              segments_to_merge_at.emplace_back(std::vector<SegmentChunkColumn<ColumnDataType>>{current_segment_info});
              if (merged_with_previous) {
                segments_to_merge_at.back().push_back(*previous_segment_info_opt);
              }
              last_merged_index = shared_dictionaries.size() - 1;

            } else {
              shared_dictionaries[potential_new_shared_dictionary_index] = potential_new_shared_dictionary;
              segments_to_merge_at[potential_new_shared_dictionary_index].push_back(current_segment_info);
              if (merged_with_previous) {
                segments_to_merge_at[potential_new_shared_dictionary_index].push_back(*previous_segment_info_opt);
              }
              last_merged_index = potential_new_shared_dictionary_index;
            }
          }

          log_jaccard_index(current_jaccard_index, table_name, column_name, current_compare_type, column_data_type,
                            chunk_id, output_file_stream);

          previous_segment_info_opt = current_segment_info;
        }

        // Merge the collected shared dictioaries with the segments
        Assert(shared_dictionaries.size() == segments_to_merge_at.size(),
               "The two vectors used for the merging must have the same size.");
        const auto merge_size = shared_dictionaries.size();
        for (auto merge_index = 0u; merge_index < merge_size; ++merge_index) {
          const auto shared_dictionary = shared_dictionaries[merge_index];
          const auto segments_to_merge = segments_to_merge_at[merge_index];

          total_merged_dictionaries += segments_to_merge.size();
          total_new_shared_dictionaries++;

          memory_usage_difference +=
              apply_shared_dictionary_to_segments<ColumnDataType>(segments_to_merge, shared_dictionary, allocator);
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
