#include <iostream>
#include <fstream>
#include <iterator>
#include <algorithm>


#include "hyrise.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/dictionary_segment.hpp"
#include "synthetic_table_generator.hpp"
#include "../benchmarklib/tpcds/tpcds_table_generator.hpp"
#include "types.hpp"

using namespace opossum;  // NOLINT

// IDEA: Reuse v_intersection for next iteration if jaccard index is over threshold.
// IDEA: Not only compare with previous segment but also with already created dictionaries. This would also allow cross column comparisons.
// IDEA: set_intersection can be optimized if v_intersection is not needed.
// IDEA: set_intersection can be optimized if jaccard index threshold is not reachable by exiting earlier.
// IDEA: set_intersection can be optimized if jaccard index threshold is already reached by exiting earlier.

int main() {
  std::cout << "Playground: Jaccard-Index" << std::endl;
  
  // Generate benchmark data
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
  for (const auto table_name : table_names){
     const auto table = sm.get_table(table_name);
     const auto column_count = table->column_count();
     const auto chunk_count = table->chunk_count();
     
     for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id){
        const auto column_data_type = table->column_definitions()[column_id].data_type;
        const auto column_name = table->column_definitions()[column_id].name; 
        resolve_data_type(column_data_type, [&](const auto type) {
          using ColumnDataType = typename decltype(type)::type;

          std::shared_ptr<DictionarySegment<ColumnDataType>> dictionary_segment_a = nullptr;
          std::shared_ptr<DictionarySegment<ColumnDataType>> dictionary_segment_b = nullptr;

          for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
            const auto chunk = table->get_chunk(chunk_id);
            const auto segment = chunk->get_segment(column_id);
            dictionary_segment_b = std::dynamic_pointer_cast<DictionarySegment<ColumnDataType>>(segment);

            if (dictionary_segment_a && dictionary_segment_b){
              Assert(dictionary_segment_a != dictionary_segment_b, "Comparison of the same segment.");
              const auto dictionary_a = dictionary_segment_a->dictionary();
              const auto dictionary_b = dictionary_segment_b->dictionary();

              std::vector<ColumnDataType> v_intersection;
              std::set_intersection(dictionary_a->begin(), dictionary_a->end(),
                                    dictionary_b->begin(), dictionary_b->end(),
                                    std::back_inserter(v_intersection));

              const auto intersection_size = v_intersection.size();
              const auto union_size = dictionary_a->size() + dictionary_b->size() - intersection_size;
              const auto jaccard_index = intersection_size * 1.0 / union_size; 
      
              output_file_stream << "Jaccard index = " << jaccard_index << " (Table=" << table_name << ", Column=" << column_name << ", DataType=" << column_data_type << ", Chunk=" << chunk_id << "\n";
              std::cout << "Jaccard index = " << jaccard_index << " (Table=" << table_name << ", Column=" << column_name << ", DataType=" << column_data_type << ", Chunk=" << chunk_id << std::endl;
            } 

            dictionary_segment_a = dictionary_segment_b;
          }
        });
     }
  }

  output_file_stream.close();
  return 0;
}
