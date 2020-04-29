#include "disjoint_clusters_algo.hpp"

#include <algorithm>
#include <chrono>
#include <memory>
#include <utility>

#include "abstract_clustering_algo.hpp"
#include "concurrency/transaction_context.hpp"
#include "hyrise.hpp"
#include "operators/clustering_partitioner.hpp"
#include "operators/clustering_sorter.hpp"
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

#define SORT_WITHIN_CLUSTERS true

namespace opossum {

DisjointClustersAlgo::DisjointClustersAlgo(ClusteringByTable clustering) : AbstractClusteringAlgo(clustering) {}

const std::string DisjointClustersAlgo::description() const {
  return "DisjointClustersAlgo";
}


// NOTE: num_clusters is just an estimate.
// The greedy logic that computes the boundaries currently sacrifices exact cluster count rather than balanced clusters
template <typename ColumnDataType>
ClusterBoundaries DisjointClustersAlgo::_get_boundaries(const std::shared_ptr<const AbstractHistogram<ColumnDataType>>& histogram, const size_t row_count, const size_t num_clusters) const {
  Assert(histogram, "histogram was nullptr");
  Assert(num_clusters > 1, "having less than 2 clusters does not make sense (" + std::to_string(num_clusters) + " cluster(s) requested)");
  Assert(num_clusters < histogram->bin_count(), "more clusters (" + std::to_string(num_clusters) + ") than histogram bins (" + std::to_string(histogram->bin_count()) + ")");

  const auto num_null_values = row_count - histogram->total_count();
  ClusterBoundaries boundaries(num_clusters);
  size_t boundary_index = 0;
  if (num_null_values > 0) {
    boundaries.resize(num_clusters + 1);
    boundaries[0] = std::make_pair(NULL_VALUE, NULL_VALUE);
    boundary_index++;
  }

  const auto ideal_rows_per_cluster = std::max(size_t{1}, row_count / num_clusters);
  AllTypeVariant lower_bound;
  AllTypeVariant upper_bound;
  size_t rows_in_cluster = 0;
  bool lower_bound_set = false;
  bool cluster_full = false;
  size_t bin_id{0};
  for (; bin_id < histogram->bin_count(); bin_id++) {
    if (!lower_bound_set) {
      lower_bound = histogram->bin_minimum(bin_id);
      lower_bound_set = true;
    }

    const auto bin_size = histogram->bin_height(bin_id);
    Assert(bin_size < 2 * ideal_rows_per_cluster, "bin is too large: " + std::to_string(bin_size) + ", but a cluster should have about " + std::to_string(ideal_rows_per_cluster) + " rows");
    if (rows_in_cluster + bin_size < ideal_rows_per_cluster) {
      // cluster has not yet reached its target size
      rows_in_cluster += bin_size;
      upper_bound = histogram->bin_maximum(bin_id);
    } else if (rows_in_cluster + bin_size - ideal_rows_per_cluster < ideal_rows_per_cluster - rows_in_cluster) {
      // cluster gets larger than the target size with this bin, but it is still closer to the target size than without the bin
      upper_bound = histogram->bin_maximum(bin_id);
      cluster_full = true;
    } else {
      // cluster would get larger than intended - process the bin again in the next cluster
      bin_id--;
      cluster_full = true;
    }

    if (boundary_index == boundaries.size()) {
      boundaries.push_back(std::make_pair(AllTypeVariant{}, AllTypeVariant{}));
    }
    boundaries[boundary_index] = std::make_pair(lower_bound, upper_bound);

    if (cluster_full) {
      lower_bound_set = false;
      rows_in_cluster = 0;
      boundary_index++;
      cluster_full = false;   
    }
  }

  Assert(bin_id == histogram->bin_count(), "histogram has " + std::to_string(histogram->bin_count()) + " bins, but processed only " + std::to_string(bin_id));

  return boundaries;
}


template <typename ColumnDataType>
size_t _get_cluster_index(const ClusterBoundaries& cluster_boundaries, const std::optional<ColumnDataType>& optional_value) {
  size_t cluster_index = 0;
  
  if (!optional_value) {
    // null values are always in the first cluster
    return 0;
  } else {
    const ColumnDataType& value = *optional_value;
    
    for (const std::pair<AllTypeVariant, AllTypeVariant>& boundary : cluster_boundaries) {
      if (variant_is_null(boundary.first) || variant_is_null(boundary.second)) {
        // null values are handled above
        cluster_index++;
        continue;
      }

      const auto low = boost::lexical_cast<ColumnDataType>(boundary.first);
      const auto high = boost::lexical_cast<ColumnDataType>(boundary.second);
      if (low <= value && value <= high) {
        break;                
      }
      cluster_index++;
    }

    if (cluster_index == cluster_boundaries.size()) {
      std::cout << "no matching cluster found for " << value                 
                << " with boundaries [" << cluster_boundaries[1].first << ", " << cluster_boundaries[1].second << "]" << std::endl;
      Fail("no matching cluster");
    }
  }

  return cluster_index;
}

// Clustering key for a given chunk. Assumes that all rows have the same clustering key
const ClusterKey DisjointClustersAlgo::_clustering_key_for_chunk(const std::shared_ptr<Chunk>& chunk) const {
  ClusterKey indices;
  for (size_t index{0}; index < _boundaries.size(); index++) {
    const auto clustering_column_id = _clustering_column_ids[index];
    const auto column_data_type = _table->column_data_type(clustering_column_id);
    const auto& cluster_boundaries = _boundaries[index];

    resolve_data_type(column_data_type, [&](const auto data_type_t) {
      using ColumnDataType = typename decltype(data_type_t)::type;
      const auto segment = chunk->get_segment(clustering_column_id);
      Assert(segment, "segment was nullptr");

      std::optional<ColumnDataType> value;
      const auto variant_value = (*segment)[0];
      if (!variant_is_null(variant_value)) {
       value = boost::lexical_cast<ColumnDataType>(variant_value);
      }
      indices.push_back(_get_cluster_index<ColumnDataType>(cluster_boundaries, value));
    });      
  }
  return indices;
}


const std::vector<ClusterKey> DisjointClustersAlgo::_cluster_keys(const std::shared_ptr<Chunk>& chunk) const {
  std::vector<ClusterKey> cluster_keys(chunk->size());

  for (size_t index{0}; index < _boundaries.size(); index++) {
    const auto clustering_column_id = _clustering_column_ids[index];
    const auto column_data_type = _table->column_data_type(clustering_column_id);
    const auto& cluster_boundaries = _boundaries[index];

    resolve_data_type(column_data_type, [&](const auto data_type_t) {
      using ColumnDataType = typename decltype(data_type_t)::type;
      const auto segment = chunk->get_segment(clustering_column_id);
      Assert(segment, "segment was nullptr");

      ChunkOffset chunk_offset{0};      
      segment_iterate<ColumnDataType>(*segment, [&](const auto& position) {
          std::optional<ColumnDataType> value;
          if (!position.is_null()) {
            value = position.value();
          }
          cluster_keys[chunk_offset].push_back(_get_cluster_index<ColumnDataType>(cluster_boundaries, value));
          ++chunk_offset;
        });
    });
  }

  return cluster_keys;
}

std::vector<std::shared_ptr<Chunk>> DisjointClustersAlgo::_sort_and_encode_chunks(const std::vector<std::shared_ptr<Chunk>>& chunks, const ColumnID sort_column_id) const {
  std::vector<std::shared_ptr<Chunk>> sorted_chunks;
  for (const auto& chunk : chunks) {
    Assert(chunk->mvcc_data(), "no mvcc");
    auto sorted_chunk = _sort_chunk(chunk, sort_column_id, _table->column_definitions());
    Assert(sorted_chunk->mvcc_data(), "no mvcc");
    sorted_chunk->finalize();
    ChunkEncoder::encode_chunk(sorted_chunk, _table->column_data_types(), EncodingType::Dictionary);
    sorted_chunks.push_back(sorted_chunk);
    Assert(sorted_chunk->mvcc_data(), "no mvcc");
  }      

  return sorted_chunks;  
}

bool _lock_chunk(const std::shared_ptr<Chunk>& chunk, const std::shared_ptr<TransactionContext>& transaction_context) {
  const auto mvcc_data = chunk->mvcc_data();
  const auto transaction_id = transaction_context->transaction_id();

  for (ChunkOffset offset{0}; offset < chunk->size(); offset++) {
    const auto expected = 0u;
    auto success = mvcc_data->compare_exchange_tid(offset, expected, transaction_id);
    if (!success) {
      return false;
    }
  }

  return true;
}

std::vector<ClusterBoundaries> DisjointClustersAlgo::_all_cluster_boundaries(const std::vector<size_t>& num_clusters_per_dimension) const {
  std::vector<ClusterBoundaries> cluster_boundaries;
  const auto row_count = _table->row_count();

  for (size_t dimension = 0; dimension < _clustering_column_ids.size(); dimension++) {
    const auto num_clusters = num_clusters_per_dimension[dimension];
    const auto clustering_column_id = _clustering_column_ids[dimension];
    const auto column_data_type = _table->column_data_type(clustering_column_id);
    resolve_data_type(column_data_type, [&](const auto data_type_t) {
      using ColumnDataType = typename decltype(data_type_t)::type;
      const auto clustering_column = _table->column_name(clustering_column_id);


      const auto histogram = opossum::detail::HistogramGetter<ColumnDataType>::get_histogram(_table, clustering_column);

      //std::cout << clustering_column << " (" << table_name << ") has " << row_count - (histogram->total_count()) << " NULL values" << std::endl;
      const auto boundaries = _get_boundaries<ColumnDataType>(histogram, row_count, num_clusters);

      cluster_boundaries.push_back(boundaries);

      // debug prints
      std::cout << "computed boundaries for " << clustering_column << std::endl;
      auto boundary_index = 0;
      for (const auto& boundary : boundaries) {
        std::cout << "boundary " << boundary_index << ": [" << boundary.first << ", " << boundary.second << "]" << std::endl;
        boundary_index++;
      }
      std::cout << "requested " << num_clusters << " boundaries, got " << boundaries.size() << " (" << 100.0 * boundaries.size() / num_clusters << "%)" << std::endl;
    });

  } 

  return cluster_boundaries;
}

void DisjointClustersAlgo::_perform_clustering() {

  for (const auto& [table_name, clustering_config] : clustering_by_table) {
    _table = Hyrise::get().storage_manager.get_table(table_name);

    std::vector<ColumnID> clustering_column_ids;
    std::vector<size_t> num_clusters_per_dimension;
    for (const auto& clustering_dimension : clustering_config) {
      // When there is a cluster size of 1, it means the table should be sorted after this column
      if (clustering_dimension.second > 1) {
        clustering_column_ids.push_back(_table->column_id_by_name(clustering_dimension.first));
        num_clusters_per_dimension.push_back(clustering_dimension.second);
      }
    }

    _clustering_column_ids = clustering_column_ids;

    const auto& sort_column_name = clustering_config.back().first;
    const auto sort_column_id = _table->column_id_by_name(sort_column_name);
    
    // calculate boundaries
    _boundaries = _all_cluster_boundaries(num_clusters_per_dimension);
    

    const auto chunk_count_before_clustering = _table->chunk_count();
    std::map<ClusterKey, std::set<ChunkID>> chunk_ids_per_cluster;
    std::map<ClusterKey, std::shared_ptr<Chunk>> clusters;
    for (ChunkID chunk_id{0}; chunk_id < chunk_count_before_clustering; chunk_id++) {
      const auto initial_chunk = _table->get_chunk(chunk_id);
      if (initial_chunk) {
        const auto initial_invalidated_rows = initial_chunk->invalid_row_count();

        const auto cluster_keys = _cluster_keys(initial_chunk);


        auto partition_transaction = Hyrise::get().transaction_manager.new_transaction_context();

        auto clustering_partitioner = std::make_shared<ClusteringPartitioner>(nullptr, _table, initial_chunk, cluster_keys, clusters, chunk_ids_per_cluster);
        clustering_partitioner->set_transaction_context(partition_transaction);
        clustering_partitioner->execute();

        bool lock_successful = _lock_chunk(initial_chunk, partition_transaction);

        if (!lock_successful) {
          std::cout << "Chunk " << chunk_id << " could not be locked entirely. Trying again." << std::endl;
          chunk_id--;

          continue;
        }
        if (initial_invalidated_rows < initial_chunk->invalid_row_count()) {
          // Some tuples were updated or deleted. Try again.
          std::cout <<  "Chunk " << chunk_id << " was modified during clustering. Trying again." << std::endl;
          chunk_id--;

          continue;
        } else {
          partition_transaction->commit();
        }
      }        
    }

    // sort within clusters
    for (const auto& [key, chunk_ids] : chunk_ids_per_cluster) {
      auto sorting_table = std::make_shared<Table>(_table->column_definitions(), TableType::Data, _table->target_chunk_size(), UseMvcc::Yes);

      std::vector<size_t> invalid_row_counts;
      for (const auto chunk_id : chunk_ids) {
        const auto& chunk = _table->get_chunk(chunk_id);
        Assert(chunk, "chunk must not be deleted");
        _append_chunk_to_table(chunk, sorting_table);
        invalid_row_counts.push_back(chunk->invalid_row_count());
      }

      auto wrapper = std::make_shared<TableWrapper>(sorting_table);
      wrapper->execute();
      auto sort = std::make_shared<Sort>(wrapper, sort_column_id, OrderByMode::Ascending, _table->target_chunk_size());
      sort->execute();

      auto sort_transaction = Hyrise::get().transaction_manager.new_transaction_context();
      for (const auto chunk_id : chunk_ids) {
        const auto& chunk = _table->get_chunk(chunk_id);
        Assert(chunk, "chunk must not be deleted");
        Assert(_lock_chunk(chunk, sort_transaction), "could not lock chunk with id " + std::to_string(chunk_id));
      }

      size_t invalid_row_index = 0;
      for (const auto chunk_id : chunk_ids) {
        const auto& chunk = _table->get_chunk(chunk_id);
        Assert(chunk, "chunk must not be deleted");

        Assert(chunk->invalid_row_count() == invalid_row_counts[invalid_row_index], "chunk was modified since sorting");
        invalid_row_index++;
      }

      auto clustering_sorter = std::make_shared<ClusteringSorter>(nullptr, _table, chunk_ids, sort->get_output());
      clustering_sorter->set_transaction_context(sort_transaction);
      clustering_sorter->execute();

      sort_transaction->commit();
    }

    // pretend mvcc plugin were active and remove invalidated chunks
    for (ChunkID chunk_id{0}; chunk_id < _table->chunk_count(); chunk_id++) {
      const auto& chunk = _table->get_chunk(chunk_id);
      if (chunk && chunk->size() == chunk->invalid_row_count()) {
        _table->remove_chunk(chunk_id);
      }
    }

    std::cout << table_name << " has now " << _table->chunk_count() << " chunks (from originally " << chunk_count_before_clustering << ")" << std::endl;
  }
}

} // namespace opossum

