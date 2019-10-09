#pragma once

#include <algorithm>
#include <cstring>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "column_materializer_numa.hpp"
#include "hyrise.hpp"
#include "operators/join_sort_merge/radix_cluster_sort.hpp"
#include "resolve_type.hpp"

namespace opossum {

/**
* The RadixClusterOutputNUMA holds the data structures that belong to the output of the clustering stage.
*/
template <typename T>
struct RadixClusterOutputNUMA {
  std::unique_ptr<MaterializedNUMAPartitionList<T>> clusters_left;
  std::unique_ptr<MaterializedNUMAPartitionList<T>> clusters_right;
  std::unique_ptr<PosList> null_rows_left;
  std::unique_ptr<PosList> null_rows_right;
};

/*
*
* Performs radix clustering for the mpsm join. The radix clustering algorithm clusters on the basis
* of the least significant bits of the values because the values there are much more evenly distributed than for the
* most significant bits. As a result, equal values always get moved to the same cluster and the clusters are
* sorted in themselves but not in between the clusters. This is okay for the equi join, because we are only interested
* in equality. The non equi join is not considered because non equi joins over multiple partitions do not work well, so
* the mpsm join does not support non equi joins.
* General clustering process:
* -> Input chunks are materialized and sorted. Every value is stored together with its row id.
* -> Then, radix clustering is performed.
* -> The clusters of the left hand side parition are distributed to their corresponding numa nodes.
* -> At last, the resulting clusters are sorted.
*
* Radix clustering example:
* cluster_count = 4
* bits for 4 clusters: 2
*
*   000001|01
*   000000|11
*          ˆ right bits are used for clustering
*
**/
template <typename T>
class RadixClusterSortNUMA {
 public:
  RadixClusterSortNUMA(const std::shared_ptr<const Table> left, const std::shared_ptr<const Table> right,
                       const std::pair<ColumnID, ColumnID>& column_ids, const bool materialize_null_left,
                       const bool materialize_null_right, uint32_t cluster_count)
      : _input_table_left{left},
        _input_table_right{right},
        _left_column_id{column_ids.first},
        _right_column_id{column_ids.second},
        _cluster_count{cluster_count},
        _materialize_null_left{materialize_null_left},
        _materialize_null_right{materialize_null_right} {
    DebugAssert(cluster_count > 0, "cluster_count must be > 0");
    DebugAssert((cluster_count & (cluster_count - 1)) == 0, "cluster_count must be a power of two, i.e. 1, 2, 4, 8...");
    DebugAssert(left, "left input operator is null");
    DebugAssert(right, "right input operator is null");
  }

  virtual ~RadixClusterSortNUMA() = default;

 protected:
  /**
  * The ChunkInformation structure is used to gather statistics regarding a chunk's values in order to
  * be able to appropriately reserve space for the clustering output.
  **/
  struct ChunkInformation {
    explicit ChunkInformation(size_t cluster_count) {
      cluster_histogram.resize(cluster_count);
      insert_position.resize(cluster_count);
    }
    // Used to count the number of entries for each cluster from a specific chunk
    // Example cluster_histogram[3] = 5
    // -> 5 values from the chunk belong in cluster 3
    std::vector<size_t> cluster_histogram;

    // Stores the beginning of the range in cluster for this chunk.
    // Example: insert_position[3] = 5
    // -> This chunks value for cluster 3 are inserted at index 5 and forward.
    std::vector<size_t> insert_position;
  };

  /**
  * The NUMAPartitionInformation structure is used to gather statistics regarding the value distribution
  *  of a single NUMA Partition and its chunks in order to be able to appropriately reserve space for
  *  the radix clustering output.
  **/
  struct NUMAPartitionInformation {
    NUMAPartitionInformation(size_t chunk_count, size_t cluster_count) {
      cluster_histogram.resize(cluster_count);
      chunk_information.reserve(chunk_count);
      for (size_t i = 0; i < chunk_count; ++i) {
        chunk_information.push_back(ChunkInformation(cluster_count));
      }
    }

    std::vector<size_t> cluster_histogram;
    std::vector<ChunkInformation> chunk_information;
  };

  // Input parameters
  std::shared_ptr<const Table> _input_table_left;
  std::shared_ptr<const Table> _input_table_right;
  const ColumnID _left_column_id;
  const ColumnID _right_column_id;

  // The cluster count must be a power of two, i.e. 1, 2, 4, 8, 16, ...
  // It is asserted to be a power of two in the constructor.
  uint32_t _cluster_count;

  bool _materialize_null_left;
  bool _materialize_null_right;

  /**
  * Determines the total size of a materialized partition.
  **/
  static size_t _materialized_table_size(MaterializedNUMAPartition<T>& table) {
    auto total_size = size_t{0};
    for (auto chunk : table.materialized_segments) {
      total_size += chunk->size();
    }

    return total_size;
  }

  /**
  * Concatenates multiple materialized segments to a single materialized segment.
  **/
  static std::unique_ptr<MaterializedNUMAPartitionList<T>> _concatenate_chunks(
      std::unique_ptr<MaterializedNUMAPartitionList<T>>& input_table) {
    // We know input chunks has only one numa_node
    auto output_table = std::make_unique<MaterializedNUMAPartitionList<T>>();
    output_table->push_back(MaterializedNUMAPartition<T>(NodeID{0}, 1));

    // Reserve the required space and move the data to the output
    auto output_chunk = std::make_shared<MaterializedSegmentNUMA<T>>();
    output_chunk->reserve(_materialized_table_size((*input_table)[0]));
    for (auto& chunk : (*input_table)[0].materialized_segments) {
      output_chunk->insert(output_chunk->end(), chunk->begin(), chunk->end());
    }

    (*output_table)[0].materialized_segments[0] = output_chunk;

    return output_table;
  }

  /**
  * Performs the clustering on a materialized partition using a clustering function that determines for each
  * value the appropriate cluster id. This is how the clustering works:
  * -> Count for each chunk how many of its values belong in each of the clusters using histograms.
  * -> Aggregate the per-chunk histograms to a histogram for the whole table. For each chunk it is noted where
  *    it will be inserting values in each cluster.
  * -> Reserve the appropriate space for each output cluster to avoid ongoing vector resizing.
  * -> At last, each value of each chunk is moved to the appropriate cluster.
  **/
  MaterializedNUMAPartition<T> _cluster(MaterializedNUMAPartition<T>& input_chunks,
                                        std::function<size_t(const T&)> clusterer, NodeID node_id) {
    auto num_chunks = input_chunks.materialized_segments.size();
    auto output_table = MaterializedNUMAPartition<T>(node_id, _cluster_count);
    auto numa_partition_information = NUMAPartitionInformation(num_chunks, _cluster_count);

    // Count for every chunk the number of entries for each cluster in parallel
    for (auto chunk_number = size_t{0}; chunk_number < num_chunks; ++chunk_number) {
      auto& chunk_information = numa_partition_information.chunk_information[chunk_number];
      auto input_chunk = input_chunks.materialized_segments[chunk_number];

      for (auto& entry : *input_chunk) {
        auto cluster_id = clusterer(entry.value);
        ++chunk_information.cluster_histogram[cluster_id];
      }
    }

    // Aggregate the chunks histograms to a table histogram and initialize the insert positions for each chunk
    for (auto& chunk_information : numa_partition_information.chunk_information) {
      for (auto cluster_id = size_t{0}; cluster_id < _cluster_count; ++cluster_id) {
        chunk_information.insert_position[cluster_id] = numa_partition_information.cluster_histogram[cluster_id];
        numa_partition_information.cluster_histogram[cluster_id] += chunk_information.cluster_histogram[cluster_id];
      }
    }

    // Reserve the appropriate output space for the clusters
    for (auto cluster_id = size_t{0}; cluster_id < _cluster_count; ++cluster_id) {
      auto cluster_size = numa_partition_information.cluster_histogram[cluster_id];
      output_table.materialized_segments[cluster_id] =
          std::make_shared<MaterializedSegmentNUMA<T>>(cluster_size, output_table.alloc);
    }

    // Move each entry into its appropriate cluster in parallel
    std::vector<std::shared_ptr<AbstractTask>> cluster_jobs;
    for (auto chunk_number = size_t{0}; chunk_number < num_chunks; ++chunk_number) {
      auto job = std::make_shared<JobTask>(
          [chunk_number, &output_table, &input_chunks, &numa_partition_information, &clusterer] {
            auto& chunk_information = numa_partition_information.chunk_information[chunk_number];
            for (auto& entry : (*input_chunks.materialized_segments[chunk_number])) {
              auto cluster_id = clusterer(entry.value);
              auto& output_cluster = output_table.materialized_segments[cluster_id];
              auto& insert_position = chunk_information.insert_position[cluster_id];
              (*output_cluster)[insert_position] = entry;
              ++insert_position;
            }
          });
      cluster_jobs.push_back(job);
      job->schedule(node_id);
    }

    Hyrise::get().scheduler()->wait_for_tasks(cluster_jobs);

    DebugAssert(output_table.materialized_segments.size() == _cluster_count,
                "Error in clustering: Number of output segments does not match the number of clusters.");

    return output_table;
  }

  /**
  * Performs least significant bit radix clustering which is used in the equi join case.
  **/
  std::unique_ptr<MaterializedNUMAPartitionList<T>> _radix_cluster_numa(
      std::unique_ptr<MaterializedNUMAPartitionList<T>>& input_chunks) {
    auto output = std::make_unique<MaterializedNUMAPartitionList<T>>();

    auto radix_bitmask = _cluster_count - 1;

    output->resize(_cluster_count);

    std::vector<std::shared_ptr<AbstractTask>> cluster_jobs;

    for (NodeID node_id{0}; node_id < _cluster_count; node_id++) {
      DebugAssert(node_id < input_chunks->size(), "Node ID out of range. Node ID: " + std::to_string(node_id) +
                                                      " Cluster count: " + std::to_string(_cluster_count));
      auto job = std::make_shared<JobTask>([&output, &input_chunks, node_id, radix_bitmask, this]() {
        (*output)[node_id] = _cluster(
            (*input_chunks)[node_id],
            [=](const T& value) { return RadixClusterSort<T>::template get_radix<T>(value, radix_bitmask); }, node_id);
      });

      cluster_jobs.push_back(job);
      job->schedule(node_id);
    }

    Hyrise::get().scheduler()->wait_for_tasks(cluster_jobs);

    return output;
  }

  /**
  * Moves the values so that each cluster resides on a seperate node
  **/
  std::unique_ptr<MaterializedNUMAPartitionList<T>> _repartition_clusters(
      std::unique_ptr<MaterializedNUMAPartitionList<T>>& private_partitions) {
    auto homogenous_partitions = std::make_unique<MaterializedNUMAPartitionList<T>>();

    auto cluster_sizes = std::vector<size_t>();
    cluster_sizes.resize(_cluster_count, 0);

    for (const auto& partition : (*private_partitions)) {
      DebugAssert(partition.materialized_segments.size() == _cluster_count,
                  "Number of clusters does not match the number of NUMA partitions.");
      for (auto cluster_id = size_t{0}; cluster_id < _cluster_count; ++cluster_id) {
        cluster_sizes[cluster_id] += partition.materialized_segments[cluster_id]->size();
      }
    }

    auto repartition_jobs = std::vector<std::shared_ptr<AbstractTask>>();

    for (NodeID numa_node{0}; numa_node < _cluster_count; ++numa_node) {
      auto job = std::make_shared<JobTask>([this, numa_node, &private_partitions, &homogenous_partitions]() {
        homogenous_partitions->emplace_back(MaterializedNUMAPartition<T>(numa_node, 1));

        auto& homogenous_partition = (*homogenous_partitions)[numa_node];

        auto materialized_segment = std::make_shared<MaterializedSegmentNUMA<T>>();
        materialized_segment->reserve(_cluster_count);
        homogenous_partition.materialized_segments[0] = materialized_segment;

        for (const auto& partition : (*private_partitions)) {
          const auto& src = partition.materialized_segments[numa_node];

          std::copy(src->begin(), src->end(), std::back_inserter(*materialized_segment));
        }
      });

      repartition_jobs.push_back(job);
      job->schedule(numa_node);
    }

    Hyrise::get().scheduler()->wait_for_tasks(repartition_jobs);

    return homogenous_partitions;
  }

  /**
  * Sorts all clusters of a materialized table.
  **/
  void _sort_clusters(std::unique_ptr<MaterializedNUMAPartitionList<T>>& partitions) {
    auto sort_jobs = std::vector<std::shared_ptr<AbstractTask>>();

    for (auto& partition : (*partitions)) {
      for (auto cluster : partition.materialized_segments) {
        auto job = std::make_shared<JobTask>([cluster]() {
          std::sort(cluster->begin(), cluster->end(), [](auto& left, auto& right) { return left.value < right.value; });
        });

        sort_jobs.push_back(job);
        job->schedule(partition.node_id);
      }
    }

    Hyrise::get().scheduler()->wait_for_tasks(sort_jobs);
  }

 public:
  /**
  * Executes the clustering and sorting.
  **/
  RadixClusterOutputNUMA<T> execute() {
    auto output = RadixClusterOutputNUMA<T>();

    // Sort the chunks of the input tables in the non-equi cases
    auto left_column_materializer = ColumnMaterializerNUMA<T>(_materialize_null_left);
    auto right_column_materializer = ColumnMaterializerNUMA<T>(_materialize_null_right);
    auto materialization_left = left_column_materializer.materialize(_input_table_left, _left_column_id);
    auto materialization_right = right_column_materializer.materialize(_input_table_right, _right_column_id);
    auto materialized_left_segments = std::move(materialization_left.first);
    auto materialized_right_segments = std::move(materialization_right.first);

    output.null_rows_left = std::move(materialization_left.second);
    output.null_rows_right = std::move(materialization_right.second);

    if (_cluster_count == 1) {
      output.clusters_left = _concatenate_chunks(materialized_left_segments);
      output.clusters_right = _concatenate_chunks(materialized_right_segments);
    } else {
      output.clusters_left = _radix_cluster_numa(materialized_left_segments);
      output.clusters_right = _radix_cluster_numa(materialized_right_segments);
    }

    output.clusters_left = _repartition_clusters(output.clusters_left);
    _sort_clusters(output.clusters_left);
    _sort_clusters(output.clusters_right);

    return output;
  }
};

}  // namespace opossum
