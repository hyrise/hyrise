#pragma once

#include <algorithm>
#include <cstring>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <boost/sort/sort.hpp>

#include "column_materializer.hpp"
#include "hyrise.hpp"
#include "resolve_type.hpp"
#include "utils/timer.hpp"

namespace hyrise {

// The RadixClusterOutput holds the data structures that belong to the output of the clustering stage.
template <typename T>
struct RadixClusterOutput {
  MaterializedSegmentList<T> clusters_left;
  MaterializedSegmentList<T> clusters_right;
  RowIDPosList null_rows_left;
  RowIDPosList null_rows_right;
};

// Performs radix clustering for the sort merge join. The radix clustering algorithm clusters on the basis of the least
// significant bits of the values because the values there are much more evenly distributed than for the most
// significant bits. As a result, equal values always get moved to the same cluster and the clusters are sorted in
// themselves but not in between the clusters. This approach works for equi-joins, because we are only interested in
// equality. In the case of a non-equi-joins however, complete sortedness is required, because join matches exist beyond
// cluster borders. Therefore, the clustering defaults to a range clustering algorithm for the non-equi-join.
// General clustering process:
//  -> Input chunks are materialized and sorted. Every value is stored together with its row id.
//  -> Then, either radix clustering or range clustering is performed.
//  -> At last, the resulting clusters are sorted.
//
// Radix clustering example:
//  cluster_count = 4
//  bits for 4 clusters: 2
//
//    000001|01
//    000000|11
//           ˆ right-most bits are used for clustering
template <typename T>
class RadixClusterSort {
 public:
  RadixClusterSort(const std::shared_ptr<const Table> left, const std::shared_ptr<const Table> right,
                   const ColumnIDPair& column_ids, bool equi_case, const bool materialize_null_left,
                   const bool materialize_null_right, size_t cluster_count,
                   OperatorPerformanceData<JoinSortMerge::OperatorSteps>& performance_data)
      : _performance{performance_data},
        _left_input_table{left},
        _right_input_table{right},
        _left_column_id{column_ids.first},
        _right_column_id{column_ids.second},
        _equi_case{equi_case},
        _cluster_count{cluster_count},
        _materialize_null_left{materialize_null_left},
        _materialize_null_right{materialize_null_right} {
    DebugAssert(cluster_count > 0, "cluster_count must be > 0");
    DebugAssert((cluster_count & (cluster_count - 1)) == 0, "cluster_count must be a power of two");
    DebugAssert(left, "left input operator is null");
    DebugAssert(right, "right input operator is null");
  }

  virtual ~RadixClusterSort() = default;

  template <typename T2>
  static std::enable_if_t<std::is_integral_v<T2>, size_t> get_radix(T2 value, size_t radix_bitmask) {
    return static_cast<int64_t>(value) & radix_bitmask;
  }

  template <typename T2>
  static std::enable_if_t<!std::is_integral_v<T2>, size_t> get_radix(T2 value, size_t radix_bitmask) {
    PerformanceWarning("Using hash to perform bit_cast/radix partitioning of floating point number and strings");
    return std::hash<T2>{}(value)&radix_bitmask;
  }

 protected:
  // The ChunkInformation structure is used to gather statistics regarding a chunk's values in order to be able to
  // appropriately reserve space for the clustering output.
  OperatorPerformanceData<JoinSortMerge::OperatorSteps>& _performance;

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

  // The TableInformation structure is used to gather statistics regarding the value distribution of a table and its
  // chunks in order to be able to appropriately reserve space for the clustering output.
  struct TableInformation {
    TableInformation(size_t chunk_count, size_t cluster_count)
        : cluster_histogram(cluster_count, 0), chunk_information(chunk_count, ChunkInformation(cluster_count)) {}

    // Used to count the number of entries for each cluster from the whole table
    std::vector<size_t> cluster_histogram;
    std::vector<ChunkInformation> chunk_information;
  };

  // Input parameters
  std::shared_ptr<const Table> _left_input_table;
  std::shared_ptr<const Table> _right_input_table;
  const ColumnID _left_column_id;
  const ColumnID _right_column_id;
  bool _equi_case;

  // The cluster count must be a power of two, i.e. 1, 2, 4, 8, 16, ...
  // It is asserted to be a power of two in the constructor.
  size_t _cluster_count;

  bool _materialize_null_left;
  bool _materialize_null_right;

  // Determines the total size of a materialized segment list.
  static size_t _materialized_table_size(const MaterializedSegmentList<T>& table) {
    auto total_size = size_t{0};
    for (const auto& chunk : table) {
      total_size += chunk.size();
    }

    return total_size;
  }

  // Concatenates multiple materialized segments to a single materialized segment.
  static MaterializedSegmentList<T> _concatenate_chunks(const MaterializedSegmentList<T>& input_chunks) {
    auto output_table = MaterializedSegmentList<T>(1);

    // Reserve the required space and move the data to the output
    auto& output_chunk = output_table[0];
    output_chunk.reserve(_materialized_table_size(input_chunks));
    for (const auto& chunk : input_chunks) {
      output_chunk.insert(output_chunk.end(), chunk.begin(), chunk.end());
    }

    return output_table;
  }

  // Clusters a materialized table using a given clustering function that determines the appropriate cluster id for
  // each value.
  //    -> For each chunk, count how many of its values belong in each of the clusters using histograms.
  //    -> Aggregate the per-chunk histograms to a histogram for the whole table.
  //    -> Reserve the appropriate space for each output cluster to avoid ongoing vector resizing.
  //    -> At last, each value of each chunk is moved to the appropriate cluster. The created histogram denotes where
  //       the concurrent tasks can write the data to without the need for synchronization.
  MaterializedSegmentList<T> _cluster(const MaterializedSegmentList<T>& input_chunks,
                                      const std::function<size_t(const T&)>& clusterer) {
    auto output_table = MaterializedSegmentList<T>(_cluster_count);

    const auto input_chunk_count = input_chunks.size();
    TableInformation table_information(input_chunk_count, _cluster_count);

    // Count for every chunk the number of entries for each cluster in parallel
    auto histogram_jobs = std::vector<std::shared_ptr<AbstractTask>>{};
    for (auto chunk_number = size_t{0}; chunk_number < input_chunk_count; ++chunk_number) {
      auto& chunk_information = table_information.chunk_information[chunk_number];
      const auto& input_chunk = input_chunks[chunk_number];

      // Count the number of entries for each cluster to be able to reserve the appropriate output space later.
      auto histogram_job = [&] {
        for (const auto& entry : input_chunk) {
          const auto cluster_id = clusterer(entry.value);
          ++chunk_information.cluster_histogram[cluster_id];
        }
      };

      if (input_chunk.size() > JoinSortMerge::JOB_SPAWN_THRESHOLD) {
        histogram_jobs.push_back(std::make_shared<JobTask>(histogram_job));
      } else {
        histogram_job();
      }
    }

    Hyrise::get().scheduler()->schedule_and_wait_for_tasks(histogram_jobs);

    // Aggregate the chunks histograms to a table histogram and initialize the insert positions for each chunk
    for (auto& chunk_information : table_information.chunk_information) {
      for (auto cluster_id = size_t{0}; cluster_id < _cluster_count; ++cluster_id) {
        chunk_information.insert_position[cluster_id] = table_information.cluster_histogram[cluster_id];
        table_information.cluster_histogram[cluster_id] += chunk_information.cluster_histogram[cluster_id];
      }
    }

    // Reserve the appropriate output space for the clusters
    for (auto cluster_id = size_t{0}; cluster_id < _cluster_count; ++cluster_id) {
      const auto cluster_size = table_information.cluster_histogram[cluster_id];
      output_table[cluster_id] = MaterializedSegment<T>(cluster_size);
    }

    // Move each entry into its appropriate cluster in parallel
    auto cluster_jobs = std::vector<std::shared_ptr<AbstractTask>>{};
    for (auto chunk_number = size_t{0}; chunk_number < input_chunk_count; ++chunk_number) {
      const auto& input_chunk = input_chunks[chunk_number];
      auto cluster_job = [&, chunk_number] {
        auto& chunk_information = table_information.chunk_information[chunk_number];
        for (const auto& entry : input_chunk) {
          const auto cluster_id = clusterer(entry.value);
          auto& output_cluster = output_table[cluster_id];
          auto& insert_position = chunk_information.insert_position[cluster_id];
          output_cluster[insert_position] = entry;
          ++insert_position;
        }
      };

      if (input_chunk.size() > JoinSortMerge::JOB_SPAWN_THRESHOLD) {
        cluster_jobs.push_back(std::make_shared<JobTask>(cluster_job));
      } else {
        cluster_job();
      }
    }

    Hyrise::get().scheduler()->schedule_and_wait_for_tasks(cluster_jobs);

    return output_table;
  }

  // Performs least significant bit radix clustering which is used in the equi join case.
  MaterializedSegmentList<T> _radix_cluster(const MaterializedSegmentList<T>& input_chunks) {
    auto radix_bitmask = _cluster_count - 1;
    return _cluster(input_chunks, [radix_bitmask](const T& value) { return get_radix<T>(value, radix_bitmask); });
  }

  // Picks split values from the given sample values. Each split value denotes the inclusive upper bound of its
  // corresponding cluster (i.e., split #0 is the upper bound of cluster #0). As the last cluster does not require an
  // upper bound, the returned vector size is usually the cluster count minus one. However, it can be even shorter
  // (e.g., attributes where #distinct values < #cluster count; in this case, empty clusters will be created).
  // Procedure: passed values are sorted and samples are picked from the whole sample value range in constant
  // distances. Repeated values are not removed. Thereby, they have a higher chance of being picked which should
  // cover skewed inputs. However, the final split values
  // are unique. As a consequence, the split value vector might contain less values than `_cluster_count - 1`.
  const std::vector<T> _pick_split_values(std::vector<T>&& sample_values) const {
    boost::sort::pdqsort(sample_values.begin(), sample_values.end());

    if (sample_values.size() <= _cluster_count - 1) {
      const auto last = std::unique(sample_values.begin(), sample_values.end());
      sample_values.erase(last, sample_values.end());
      return std::move(sample_values);
    }

    auto split_values = std::vector<T>{};
    split_values.reserve(_cluster_count - 1);
    const auto jump_width = sample_values.size() / _cluster_count;
    for (auto sample_offset = size_t{0}; sample_offset < _cluster_count - 1; ++sample_offset) {
      split_values.push_back(sample_values[static_cast<size_t>((sample_offset + 1) * jump_width)]);
    }

    const auto last_split = std::unique(split_values.begin(), split_values.end());
    split_values.erase(last_split, split_values.end());
    return split_values;
  }

  // Performs the range cluster sort for the non-equi case (>, >=, <, <=, !=) which requires the complete table to be
  // sorted and not only the clusters in themselves. Returns the clustered data from the left table and the right table
  // as a pair.
  std::pair<MaterializedSegmentList<T>, MaterializedSegmentList<T>> _range_cluster(
      const MaterializedSegmentList<T>& left_input, const MaterializedSegmentList<T>& right_input,
      std::vector<T>&& sample_values) {
    const std::vector<T> split_values = _pick_split_values(std::move(sample_values));

    // Implements range clustering
    auto clusterer = [&split_values](const T& value) {
      // Find the first split value that is greater or equal to the entry.
      // The split values are sorted in ascending order.
      // Note: can we do this faster? (binary search?)
      for (size_t split_id = 0; split_id < split_values.size(); ++split_id) {
        if (value <= split_values[split_id]) {
          // Each split (e.g., split #0) is the upper bound for its corresponding cluster (i.e., cluster #0).
          return split_id;
        }
      }

      // The value is greater than all split values, which means it belongs in the last cluster.
      return split_values.size();
    };

    const auto output_left = _cluster(left_input, clusterer);
    const auto output_right = _cluster(right_input, clusterer);

    return {std::move(output_left), std::move(output_right)};
  }

  // Sorts all clusters of a materialized table.
  void _sort_clusters(MaterializedSegmentList<T>& clusters) {
    auto sort_jobs = std::vector<std::shared_ptr<AbstractTask>>{};
    for (auto cluster_id = size_t{0}; cluster_id < clusters.size(); ++cluster_id) {
      const auto cluster_size = clusters[cluster_id].size();
      auto sort_job = [&, cluster_id] {
        auto& cluster = clusters[cluster_id];
        boost::sort::pdqsort(cluster.begin(), cluster.end(),
                             [](const auto& left, const auto& right) { return left.value < right.value; });
      };

      if (cluster_size > JoinSortMerge::JOB_SPAWN_THRESHOLD) {
        sort_jobs.push_back(std::make_shared<JobTask>(sort_job));
      } else {
        sort_job();
      }
    }
    Hyrise::get().scheduler()->schedule_and_wait_for_tasks(sort_jobs);
  }

 public:
  RadixClusterOutput<T> execute() {
    RadixClusterOutput<T> output;

    Timer timer;
    // Sort the chunks of the input tables in the non-equi cases
    ColumnMaterializer<T> left_column_materializer(!_equi_case, _materialize_null_left);
    auto [materialized_left_segments, null_rows_left, samples_left] =
        left_column_materializer.materialize(_left_input_table, _left_column_id);
    output.null_rows_left = std::move(null_rows_left);
    _performance.set_step_runtime(JoinSortMerge::OperatorSteps::LeftSideMaterializing, timer.lap());

    ColumnMaterializer<T> right_column_materializer(!_equi_case, _materialize_null_right);
    auto [materialized_right_segments, null_rows_right, samples_right] =
        right_column_materializer.materialize(_right_input_table, _right_column_id);
    output.null_rows_right = std::move(null_rows_right);
    _performance.set_step_runtime(JoinSortMerge::OperatorSteps::RightSideMaterializing, timer.lap());

    // Append right samples to left samples and sort (reserve not necessary when insert can
    // determine the new capacity from the iterator: https://stackoverflow.com/a/35359472/1147726)
    samples_left.insert(samples_left.end(), samples_right.begin(), samples_right.end());

    if (_cluster_count == 1) {
      output.clusters_left = _concatenate_chunks(materialized_left_segments);
      output.clusters_right = _concatenate_chunks(materialized_right_segments);
    } else if (_equi_case) {
      output.clusters_left = _radix_cluster(materialized_left_segments);
      output.clusters_right = _radix_cluster(materialized_right_segments);
    } else {
      auto result = _range_cluster(materialized_left_segments, materialized_right_segments, std::move(samples_left));
      output.clusters_left = std::move(result.first);
      output.clusters_right = std::move(result.second);
    }
    _performance.set_step_runtime(JoinSortMerge::OperatorSteps::Clustering, timer.lap());

    _sort_clusters(output.clusters_left);
    _sort_clusters(output.clusters_right);
    _performance.set_step_runtime(JoinSortMerge::OperatorSteps::Sorting, timer.lap());

    return output;
  }
};

}  // namespace hyrise
