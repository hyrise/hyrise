#include "abstract_clustering_algo.hpp"

#include <algorithm>
#include <memory>
#include <utility>
#include <vector>

#include "hyrise.hpp"
#include "storage/chunk.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/segment_encoding_utils.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"

#define VERBOSE false

namespace opossum {

void AbstractClusteringAlgo::_run_assertions() const {
  for (const auto& table_name : Hyrise::get().storage_manager.table_names()) {
    if (VERBOSE) std::cout << "[" << description() << "] " << "- Running assertions for table " << table_name << std::endl;

    const auto table = Hyrise::get().storage_manager.get_table(table_name);
    Assert(table, "table named \"" + table_name + "\" disappeared");

    // Make sure the table size did not change, i.e., no rows got lost or duplicated in the clustering process
    const auto original_row_count = _original_table_sizes.at(table_name);
    const auto row_count = table->row_count();
    Assert(row_count == original_row_count, "Table " + table_name + " had " 
      + std::to_string(original_row_count) + " rows when the clustering began, "
      + "but has now " + std::to_string(row_count) + " rows");

    // Clustering config for this table
    std::vector<std::pair<std::string, uint32_t>> clustering_columns {};
    if (clustering_by_table.find(table_name) != clustering_by_table.end()) {
      clustering_columns = clustering_by_table.at(table_name);
    }

    // Determine whether and by which column the table should be ordered
    std::optional<ColumnID> ordered_by_column_id;
    if (!clustering_columns.empty()) {
      uint64_t expected_final_chunk_count = 1;
      for (const auto& clustering_entry : clustering_columns) {
        expected_final_chunk_count *= clustering_entry.second;
      }
      // TODO uncomment this
      //Assert(table->chunk_count() == expected_final_chunk_count, "expected " + std::to_string(expected_final_chunk_count)
      //  + " chunks, but " + table_name + " got " + std::to_string(table->chunk_count()));

      auto sorted_column_name = clustering_columns.back().first;
      ordered_by_column_id = table->column_id_by_name(sorted_column_name);
    }
    if (VERBOSE) std::cout << "[" << description() << "] " << "-  Chunk count is correct" << " for table " << table_name << std::endl;

    // Determine the target chunk size.
    // TODO this is hacky, but this class does not know about benchmark_config->chunk_size, and not sure if it should
    ChunkOffset target_chunk_size;
    if (Hyrise::get().storage_manager.has_table("lineitem")) {
      target_chunk_size = 25'100;
    } else if (Hyrise::get().storage_manager.has_table("customer_demographics")) {
      target_chunk_size = Chunk::DEFAULT_SIZE;
    } else {
      Fail("Please provide a default chunk size for this benchmark");
    }


    // Iterate over all chunks, and check that ...
    for (ChunkID chunk_id{0};chunk_id < table->chunk_count();chunk_id++) {
      const auto chunk = table->get_chunk(chunk_id);
      Assert(chunk, "could not get chunk with id " + std::to_string(chunk_id));

      // ... ordering information is as expected
      Assert(!chunk->is_mutable(), "chunk is still mutable");
      if (ordered_by_column_id) {
        Assert(chunk->ordered_by(), "chunk should be ordered by " + table->column_name(*ordered_by_column_id) + ", but is unordered");
        Assert((*chunk->ordered_by()).first == *ordered_by_column_id, "chunk should be ordered by " + table->column_name(*ordered_by_column_id) 
          + ", but is ordered by " + table->column_name((*chunk->ordered_by()).first));
      } else {
        Assert(!chunk->ordered_by(), "chunk should be unordered, but is ordered by " + table->column_name((*chunk->ordered_by()).first));
      }
      if (VERBOSE) std::cout << "[" << description() << "] " << "-  Ordering information is correct" << " for table " << table_name << std::endl;

      // ... chunks are actually ordered according to the ordering information
      if (ordered_by_column_id) {
        auto is_sorted = true;
        const auto sort_column_id = *ordered_by_column_id;
        resolve_data_type(table->column_data_type(sort_column_id), [&](auto type) {
          using ColumnDataType = typename decltype(type)::type;

          auto last_value = std::optional<ColumnDataType>{};
          const auto& segment = table->get_chunk(chunk_id)->get_segment(sort_column_id);
          segment_with_iterators<ColumnDataType>(*segment, [&](auto it, const auto end) {
            while (it != end) {
              if (it->is_null()) {
                if (last_value) {
                  // NULLs should come before all values
                  is_sorted = false;
                  break;
                } else {
                  it++;
                  continue;
                }
              }

              if (!last_value || it->value() >= *last_value) {
                last_value = it->value();
              } else {
                is_sorted = false;
                break;
              }

              ++it;
            }
          });
        });

        Assert(is_sorted, "segment should be sorted by " + table->column_name(sort_column_id) + ", but it isn't");
        if (VERBOSE) std::cout << "[" << description() << "] " << "-  Segments are actually sorted" << " for table " << table_name << std::endl;
      }

      // ... all segments should have DictionaryEncoding
      for (ColumnID col_id{0}; col_id < table->column_count();col_id++) {
        const auto& segment = chunk->get_segment(col_id);
        Assert(segment, "could not get segment with column id " + std::to_string(col_id));
        const auto encoding_spec = get_segment_encoding_spec(segment);
        Assert(encoding_spec.encoding_type == EncodingType::Dictionary, "segment is not dictionary-encoded");
      }
      if (VERBOSE) std::cout << "[" << description() << "] " << "-  All segments are DictionarySegments" << " for table " << table_name << std::endl;

      // ... chunks have at most the target chunk size
      Assert(chunk->size() <= target_chunk_size, "chunk size should be <= " + std::to_string(target_chunk_size)
        + ", but is " + std::to_string(chunk->size()));
      if (VERBOSE) std::cout << "[" << description() << "] " << "-  All chunks have about the expected size" << " for table " << table_name << std::endl;
    }
  }
}

void AbstractClusteringAlgo::run() {
  const auto& table_names = Hyrise::get().storage_manager.table_names();
  for (const auto& table_name : table_names) {
    _original_table_sizes[table_name] = Hyrise::get().storage_manager.get_table(table_name)->row_count();
  }

  _perform_clustering();

  _run_assertions();
}

} // namespace opossum