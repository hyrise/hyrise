#include "chunkwise_clustering_algo.hpp"

#include <algorithm>
#include <chrono>
#include <memory>
#include <utility>

#include "abstract_clustering_algo.hpp"
#include "hyrise.hpp"
#include "operators/sort.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/chunk.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/segment_encoding_utils.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/value_segment.hpp"
#include "utils/format_duration.hpp"
#include "utils/timer.hpp"

#include "statistics/statistics_objects/abstract_histogram.hpp"


namespace opossum {

ChunkwiseClusteringAlgo::ChunkwiseClusteringAlgo(ClusteringByTable clustering) : AbstractClusteringAlgo(clustering) {}

const std::string ChunkwiseClusteringAlgo::description() const {
  return "ChunkwiseClusteringAlgo";
}

template <typename ColumnDataType>
std::vector<std::pair<ColumnDataType, ColumnDataType>> ChunkwiseClusteringAlgo::_get_boundaries(const std::shared_ptr<const AbstractHistogram<ColumnDataType>>& histogram, const size_t row_count, const size_t split_factor, const size_t rows_per_chunk) const {
  Assert(histogram->total_count() <= row_count, "histogram has more entries than the table rows");
  const auto num_null_values = row_count - histogram->total_count();

  std::vector<std::pair<ColumnDataType, ColumnDataType>> boundaries(split_factor);

  // TODO null values should appear first  
  BinID current_bin_id{0};
  size_t current_bin_rows_processed = 0;  
  size_t total_rows_processed = 0;
  bool start_value_set;
  std::optional<ColumnDataType> old_min_value; // TODO also track old_max_value
  for (auto& boundary : boundaries) {
    start_value_set = false;
    size_t start_value_rows_processed = 0;
    while (start_value_rows_processed < rows_per_chunk && total_rows_processed < row_count) {
      Assert(current_bin_id < histogram->bin_count(), "invalid bin id: " + std::to_string(current_bin_id));
      const auto bin_size = static_cast<size_t>(histogram->bin_height(current_bin_id));
      auto bin_unprocessed_rows = bin_size - current_bin_rows_processed;
      if (!start_value_set && bin_unprocessed_rows > 0) {
        const auto& current_min_value = histogram->bin_minimum(current_bin_id);
        if (old_min_value) {
          Assert(*old_min_value <= current_min_value, "bins appear to be unsorted");
        }
        old_min_value = current_min_value;
        boundary.first = current_min_value;
        start_value_set = true;
      }      
      const auto rows_taken = std::min(rows_per_chunk - start_value_rows_processed, bin_unprocessed_rows);
      start_value_rows_processed += rows_taken;
      current_bin_rows_processed += rows_taken;
      total_rows_processed += rows_taken;

      boundary.second = histogram->bin_maximum(current_bin_id);
      if (current_bin_rows_processed == bin_size) {
        current_bin_id++;
        current_bin_rows_processed = 0;
      }
    }

  }
  Assert(total_rows_processed == row_count, "did not process the correct number of rows: " + std::to_string(total_rows_processed) + " vs " + std::to_string(row_count));
  Assert(current_bin_id == histogram->bin_count(), "did not process all bins");

  if (num_null_values > 0) {}
  return boundaries;
}

template <typename ColumnDataType>
size_t _insertion_index(const std::vector<std::pair<ColumnDataType, ColumnDataType>>& boundaries, const ColumnDataType& value) {
  for (size_t index{0};index < boundaries.size();index++) {
    const auto& boundary = boundaries[index];
    if (boundary.first <= value && value <= boundary.second) {
      return index;
    }
  }
  Fail("No boundary matched");
}

template <typename ColumnDataType>
void _distribute_chunk(const std::shared_ptr<Chunk>& chunk, std::vector<std::shared_ptr<Chunk>>& target_chunks, const std::vector<std::pair<ColumnDataType, ColumnDataType>>& boundaries, const size_t rows_per_chunk, const ColumnID clustering_column, std::shared_ptr<Table>& trash_table) {
  Assert(target_chunks.size() == boundaries.size(), "mismatching input sizes");
  std::vector<size_t> chunk_sizes(target_chunks.size());
  for (size_t index{0};index < chunk_sizes.size();index++) {
    Assert(target_chunks[index], "got nullptr instead of chunk");
    chunk_sizes[index] = target_chunks[index]->size();
  }
  std::cout << "chunk sizes determined" << std::endl;

  // calculate insertion points
  std::vector<size_t> insertion_indices(chunk->size());
  const auto& segment = chunk->get_segment(clustering_column);
  const auto& dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<ColumnDataType>>(segment);
  Assert(dictionary_segment, "segment is not a dictionary segment");
  auto trash = 0;
  for (size_t index{0};index < insertion_indices.size();index++) {
    const auto& value = dictionary_segment->get_typed_value(index);
    Assert(value, "not supporting NULL yet");
    auto insertion_index = _insertion_index<ColumnDataType>(boundaries, *value);
    auto shift = 0;
    while (chunk_sizes[insertion_index] == rows_per_chunk) {
      shift++;
      insertion_index++;
      Assert(insertion_index < chunk_sizes.size(), "that became too big");
    }
    
    const auto boundary = boundaries[insertion_index];
    if (!(boundary.first <= value && value <= boundary.second)) {
      trash++;
      insertion_indices[index] = std::numeric_limits<size_t>::max();
    } else {      
      Assert(boundary.first <= value && value <= boundary.second, "could not fit in a value");
      insertion_indices[index] = insertion_index;
      chunk_sizes[insertion_index]++;
    }
  }
  std::cout << "insertion points calculated, trash rows: " << trash << std::endl;

  // perform actual inserts
  // TODO do not use AllTypeVariants
  for (size_t index{0};index < chunk->size();index++) {
    std::vector<AllTypeVariant> insertion_values(chunk->column_count());
    for (ColumnID column_id{0}; column_id < chunk->column_count(); column_id++) {
      insertion_values[column_id] = chunk->get_segment(column_id)->operator[](index);
    }
    const auto insertion_index = insertion_indices[index];
    if (insertion_index == std::numeric_limits<size_t>::max()) {
      // all matching target chunks are full -> move to trash chunk;
      if (trash_table->empty() || trash_table->last_chunk()->size() == trash_table->target_chunk_size()) {
        trash_table->append_mutable_chunk();
      }
      auto trash_chunk = trash_table->last_chunk();
      trash_chunk->append(insertion_values);
    } else {
      target_chunks[insertion_index]->append(insertion_values);
    }
  }
}

void ChunkwiseClusteringAlgo::_perform_clustering() {

  for (const auto& [table_name, clustering_config] : clustering_by_table) {
    const auto& table = Hyrise::get().storage_manager.get_table(table_name);
    Assert(table, "table " + table_name + " does not exist");

    const auto& clustering_dimension = clustering_config[0];  // TODO multiple dimensions
    const auto& clustering_column = clustering_dimension.first;
    const auto  split_factor = clustering_dimension.second;
    //const auto num_new_chunks = split_factor; // TODO multiple dimensions
    const auto row_count = table->row_count();
    const auto rows_per_chunk = static_cast<size_t>(std::ceil(1.0 * row_count / split_factor));
    Assert(rows_per_chunk * split_factor >= row_count, "too stupid for math");

    const auto column_data_type = table->column_data_type(table->column_id_by_name(clustering_column));
    resolve_data_type(column_data_type, [&](const auto data_type_t) {
      using ColumnDataType = typename decltype(data_type_t)::type;
      const auto histogram = opossum::detail::HistogramGetter<ColumnDataType>::get_histogram(table, clustering_column);

            
      std::cout << clustering_column << " (" << table_name << ") has " << row_count - (histogram->total_count()) << " NULL values" << std::endl;
      // TODO: proper NULL handling
      const auto boundaries = _get_boundaries<ColumnDataType>(histogram, row_count, split_factor, rows_per_chunk);
      std::cout << "computed boundaries" << std::endl;

      auto tmp = 0;
      for (const auto& boundary : boundaries) {
        std::cout << "Chunk " << tmp << " boundaries: (" << boundary.first << ", " << boundary.second << ")" << std::endl;
        tmp++;
      }

      const auto first_chunk = table->get_chunk(ChunkID{0});
      Assert(first_chunk, "no first chunk");
      std::vector<std::shared_ptr<Chunk>> newly_created_chunks(split_factor);
      for (auto& chunk : newly_created_chunks) {
        chunk = _create_empty_chunk(table, rows_per_chunk);
      }

      const auto clustering_column_id = table->column_id_by_name(clustering_column);
      auto trash_table = std::make_shared<Table>(table->column_definitions(), TableType::Data, rows_per_chunk, UseMvcc::Yes);

      _distribute_chunk<ColumnDataType>(first_chunk, newly_created_chunks, boundaries, rows_per_chunk, clustering_column_id, trash_table);
      std::cout << "first chunk distributed" << std::endl;

      ChunkID trash_chunks_added {0};

      // TODO: perform MVCC checks whether the chunk has changed
      constexpr bool FIRST_CHUNK_UNCHANGED = true;
      const auto table_chunk_count = table->chunk_count();
      if (FIRST_CHUNK_UNCHANGED) {
        // TODO: make this threadsafe and atomic
        table->remove_chunk(ChunkID{0});
        Assert(!table->get_chunk(ChunkID{0}), "chunk still exists");
        _append_chunks_to_table(newly_created_chunks, table);
        for (;trash_chunks_added < trash_table->chunk_count();trash_chunks_added++) {
          _append_chunk_to_table(trash_table->get_chunk(trash_chunks_added), table);
        }
      }

      // distribute the rest of the chunks. MVCC checks missing
      constexpr bool CHUNK_UNCHANGED = true;
      for (ChunkID chunk_id{1}; chunk_id < table_chunk_count; chunk_id++) {
        if (CHUNK_UNCHANGED) {
          const auto& chunk = table->get_chunk(chunk_id);
          _distribute_chunk<ColumnDataType>(chunk, newly_created_chunks, boundaries, rows_per_chunk, clustering_column_id, trash_table);
          table->remove_chunk(chunk_id);
          for (;trash_chunks_added < trash_table->chunk_count();trash_chunks_added++) {
            _append_chunk_to_table(trash_table->get_chunk(trash_chunks_added), table);
          }
        }
      }

      const auto table_pre_sort_chunk_count = table->chunk_count();

      std::cout << "There are " << trash_chunks_added << " trash chunks" << std::endl;

      // sort and finalize chunks
      const auto& sort_column_name = clustering_config.back().first;
      const auto sort_column_id = table->column_id_by_name(sort_column_name);
      for (ChunkID chunk_id{0}; chunk_id < table_pre_sort_chunk_count; chunk_id++) {
        const auto& chunk = table->get_chunk(chunk_id);
        if (chunk) {
          //std::cout << "Sorting chunk " << chunk_id << std::endl;
          auto sorted_chunk = _sort_chunk(chunk, sort_column_id, table->column_definitions());
          _append_sorted_chunk_to_table(sorted_chunk, table);
          table->last_chunk()->finalize();
          table->remove_chunk(chunk_id);
        }
      }

      // perform encoding
      std::vector<ChunkID> existing_chunks;
      for (ChunkID chunk_id{0}; chunk_id < table->chunk_count(); chunk_id++) {
        if (table->get_chunk(chunk_id)) existing_chunks.push_back(chunk_id);
      }
      ChunkEncoder::encode_chunks(table, existing_chunks, EncodingType::Dictionary);
    });
  }
}

} // namespace opossum

