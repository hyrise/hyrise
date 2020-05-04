#include "abstract_clustering_algo.hpp"

#include <algorithm>
#include <memory>
#include <utility>
#include <vector>

#include "hyrise.hpp"
#include "operators/sort.hpp"
#include "operators/table_wrapper.hpp"

#include "statistics/attribute_statistics.hpp"
#include "statistics/base_attribute_statistics.hpp"
#include "statistics/table_statistics.hpp"
#include "statistics/statistics_objects/abstract_histogram.hpp"

#include "storage/chunk.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/segment_encoding_utils.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "storage/value_segment.hpp"

#define VERBOSE false

namespace opossum {

namespace detail {

template <typename ColumnDataType>
std::shared_ptr<const AbstractHistogram<ColumnDataType>> HistogramGetter<ColumnDataType>::get_histogram(const std::shared_ptr<const Table>& table, const std::string& column_name) {
  const auto table_statistics = table->table_statistics();
  const auto column_id = table->column_id_by_name(column_name);
  const auto base_attribute_statistics = table_statistics->column_statistics[column_id];

  const auto attribute_statistics = std::dynamic_pointer_cast<AttributeStatistics<ColumnDataType>>(base_attribute_statistics);
  Assert(attribute_statistics, "could not cast to AttributeStatistics");
  const auto histogram = attribute_statistics->histogram;
  Assert(histogram, "no histogram available for column "  + column_name);
  return histogram;
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(HistogramGetter);
} // namespace detail

void AbstractClusteringAlgo::_run_assertions() const {
  for (const auto& table_name : Hyrise::get().storage_manager.table_names()) {
    if (VERBOSE) std::cout << "[" << description() << "] " << "- Running assertions for table " << table_name << std::endl;

    const auto table = Hyrise::get().storage_manager.get_table(table_name);
    Assert(table, "table named \"" + table_name + "\" disappeared");

    // Make sure the table size did not change, i.e., no rows got lost or duplicated in the clustering process
    // We have to filter out invalidated rows
    const auto original_row_count = _original_table_sizes.at(table_name);
    size_t invalidated_rows{0};
    for (ChunkID chunk_id{0}; chunk_id < table->chunk_count(); chunk_id++) {
      const auto& table_chunk = table->get_chunk(chunk_id);
      if (table_chunk) {
        invalidated_rows += table_chunk->invalid_row_count();
      }
    }
    const auto row_count = table->row_count() - invalidated_rows;

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
      if (chunk) {
        // Clustering algos might generate invalidated chunks as temporary results. Ignore them.
        bool chunk_is_invalidated = chunk->invalid_row_count() == chunk->size();
        if (chunk_is_invalidated) {
          continue;
        }


        Assert(!chunk->is_mutable(), "Chunk " + std::to_string(chunk_id) + "/" + std::to_string(table->chunk_count()) + " of table \"" + table_name + "\" is still mutable.\nSize: " + std::to_string(chunk->size()));
        // ... ordering information is as expected
        if (ordered_by_column_id) {
          Assert(chunk->ordered_by(), "chunk " + std::to_string(chunk_id) + " should be ordered by " + table->column_name(*ordered_by_column_id) + ", but is unordered");
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
}

void AbstractClusteringAlgo::run() {
  const auto& table_names = Hyrise::get().storage_manager.table_names();
  for (const auto& table_name : table_names) {
    _original_table_sizes[table_name] = Hyrise::get().storage_manager.get_table(table_name)->row_count();
  }

  _perform_clustering();

  _run_assertions();
}

std::shared_ptr<Chunk> AbstractClusteringAlgo::_create_empty_chunk(const std::shared_ptr<const Table>& table, const size_t rows_per_chunk) const {
  Segments segments;
  const auto column_definitions = table->column_definitions();
  for (const auto& column_definition : column_definitions) {
    resolve_data_type(column_definition.data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      segments.push_back(
          std::make_shared<ValueSegment<ColumnDataType>>(column_definition.nullable, rows_per_chunk));
    });
  }

  std::shared_ptr<MvccData> mvcc_data;
  if (table->uses_mvcc() == UseMvcc::Yes) {
    mvcc_data = std::make_shared<MvccData>(rows_per_chunk, MvccData::MAX_COMMIT_ID);
  }
  return std::make_shared<Chunk>(segments, mvcc_data);
}

Segments AbstractClusteringAlgo::_get_segments(const std::shared_ptr<const Chunk> chunk) const {
  Segments segments;
  for (ColumnID column_id{0}; column_id < chunk->column_count(); column_id++) {
    segments.push_back(chunk->get_segment(column_id));
  }
  return segments;
}

//TODO threadsafe?
void AbstractClusteringAlgo::_append_chunk_to_table(const std::shared_ptr<Chunk> chunk, const std::shared_ptr<Table> table, bool allow_mutable) const {
  Assert(chunk, "chunk is nullptr");
  Assert(chunk->mvcc_data(), "chunk has no mvcc data");
  if (!allow_mutable) {
    Assert(!chunk->is_mutable(), "chunk must not be mutable");
  }

  Segments segments;
  for (ColumnID column_id{0}; column_id < table->column_count(); column_id++) {
    segments.push_back(chunk->get_segment(column_id));
  }
  table->append_chunk(segments, chunk->mvcc_data());
  Assert(table->last_chunk()->mvcc_data(), "MVCC data disappeared");

  if (chunk->ordered_by()) {
    const auto ordered_by = *chunk->ordered_by();
    table->last_chunk()->set_ordered_by(ordered_by);
  }

  if (!chunk->is_mutable()) {
    table->last_chunk()->finalize();
  }
}


void AbstractClusteringAlgo::_append_sorted_chunk_to_table(const std::shared_ptr<Chunk> chunk, const std::shared_ptr<Table> table, bool allow_mutable) const {
  Assert(chunk, "chunk is nullptr");
  Assert(chunk->ordered_by(), "chunk has no ordering information");
  _append_chunk_to_table(chunk, table, allow_mutable);
}

void AbstractClusteringAlgo::_append_chunks_to_table(const std::vector<std::shared_ptr<Chunk>>& chunks, const std::shared_ptr<Table> table, bool allow_mutable) const {
  for (const auto& chunk : chunks) {
    _append_chunk_to_table(chunk, table, allow_mutable);
  }
}

void AbstractClusteringAlgo::_append_sorted_chunks_to_table(const std::vector<std::shared_ptr<Chunk>>& chunks, const std::shared_ptr<Table> table, bool allow_mutable) const {
  for (const auto& chunk : chunks) {
    _append_sorted_chunk_to_table(chunk, table, allow_mutable);
  }
}

std::shared_ptr<Chunk> AbstractClusteringAlgo::_sort_chunk(std::shared_ptr<Chunk> chunk, const ColumnID sort_column, const TableColumnDefinitions& column_definitions) const {
  Assert(chunk, "chunk is nullptr");
  Assert(chunk->mvcc_data(), "chunk has no mvcc data");

  const auto size_before_sort = chunk->size();

  auto table = std::make_shared<Table>(column_definitions, TableType::Data, chunk->size(), UseMvcc::Yes);
  Assert(chunk->mvcc_data(), "chunk has no mvcc data");
  _append_chunk_to_table(chunk, table);
  Assert(chunk->mvcc_data(), "chunk has no mvcc data");
  auto wrapper = std::make_shared<TableWrapper>(table);
  wrapper->execute();
  Assert(wrapper->get_output()->get_chunk(ChunkID{0})->mvcc_data(), "wrapper result has no mvcc data");
  const std::vector<SortColumnDefinition> sort_column_definitions = { SortColumnDefinition(sort_column, OrderByMode::Ascending) };
  auto sort = std::make_shared<Sort>(wrapper, sort_column_definitions, size_before_sort);
  sort->execute();

  const auto& sort_result = sort->get_output();
  const auto size_after_sort = sort_result->row_count();
  Assert(size_before_sort == size_after_sort, "chunk size changed during sorting");
  Assert(sort_result->chunk_count() == 1, "expected exactly 1 chunk");

  const auto sorted_const_chunk = sort_result->get_chunk(ChunkID{0});
  Assert(!sorted_const_chunk->mvcc_data(), "sort result has mvcc data now - what changed?");
  const auto mvcc_data = std::make_shared<MvccData>(chunk->size(), CommitID{0});

  auto sorted_chunk_with_mvcc = std::make_shared<Chunk>(_get_segments(sorted_const_chunk), mvcc_data);
  sorted_chunk_with_mvcc->set_ordered_by(*sorted_const_chunk->ordered_by());
  return sorted_chunk_with_mvcc;
}

} // namespace opossum