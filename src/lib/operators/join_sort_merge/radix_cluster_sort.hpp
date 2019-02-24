#include <algorithm>
#include <cstring>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "column_materializer.hpp"
#include "resolve_type.hpp"

namespace opossum {

/**
* The RadixClusterOutput holds the data structures that belong to the output of the clustering stage.
*/
template <typename T>
struct RadixClusterOutput {
  std::unique_ptr<MaterializedSegmentList<T>> clusters_left;
  std::unique_ptr<MaterializedSegmentList<T>> clusters_right;
  std::unique_ptr<PosList> null_rows_left;
  std::unique_ptr<PosList> null_rows_right;
};

/*
*
* Performs radix clustering for the sort merge join. The radix clustering algorithm clusters on the basis
* of the least significant bits of the values because the values there are much more evenly distributed than for the
* most significant bits. As a result, equal values always get moved to the same cluster and the clusters are
* sorted in themselves but not in between the clusters. This is okay for the equi join, because we are only interested
* in equality. In the case of a non-equi join however, complete sortedness is required, because join matches exist
* beyond cluster borders. Therefore, the clustering defaults to a range clustering algorithm for the non-equi-join.
* General clustering process:
* -> Input chunks are materialized and sorted. Every value is stored together with its row id.
* -> Then, either radix clustering or range clustering is performed.
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
class RadixClusterSort {
 public:
  RadixClusterSort(const std::shared_ptr<const Table> left, const std::shared_ptr<const Table> right,
                   const ColumnIDPair& column_ids, bool equi_case, const bool materialize_null_left,
                   const bool materialize_null_right, size_t cluster_count)
      : _input_table_left{left},
        _input_table_right{right},
        _left_column_id{column_ids.first},
        _right_column_id{column_ids.second},
        _equi_case{equi_case},
        _cluster_count{cluster_count},
        _materialize_null_left{materialize_null_left},
        _materialize_null_right{materialize_null_right} {
    DebugAssert(cluster_count > 0, "cluster_count must be > 0");
    DebugAssert((cluster_count & (cluster_count - 1)) == 0, "cluster_count must be a power of two");
    DebugAssert(left != nullptr, "left input operator is null");
    DebugAssert(right != nullptr, "right input operator is null");
  }

  virtual ~RadixClusterSort() = default;

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
  * The TableInformation structure is used to gather statistics regarding the value distribution of a table
  *  and its chunks in order to be able to appropriately reserve space for the clustering output.
  **/
  struct TableInformation {
    TableInformation(size_t chunk_count, size_t cluster_count) {
      cluster_histogram.resize(cluster_count);
      chunk_information.reserve(chunk_count);
      for (size_t i = 0; i < chunk_count; ++i) {
        chunk_information.push_back(ChunkInformation(cluster_count));
      }
    }
    // Used to count the number of entries for each cluster from the whole table
    std::vector<size_t> cluster_histogram;
    std::vector<ChunkInformation> chunk_information;
  };

  // Input parameters
  std::shared_ptr<const Table> _input_table_left;
  std::shared_ptr<const Table> _input_table_right;
  const ColumnID _left_column_id;
  const ColumnID _right_column_id;
  bool _equi_case;

  // The cluster count must be a power of two, i.e. 1, 2, 4, 8, 16, ...
  // It is asserted to be a power of two in the constructor.
  size_t _cluster_count;

  bool _materialize_null_left;
  bool _materialize_null_right;

  // Radix calculation for arithmetic types
  template <typename T2>
  static std::enable_if_t<std::is_arithmetic_v<T2>, uint32_t> get_radix(T2 value, size_t radix_bitmask) {
    return static_cast<uint32_t>(value) & radix_bitmask;
  }

  // Radix calculation for non-arithmetic types
  template <typename T2>
  static std::enable_if_t<std::is_same_v<T2, std::string>, uint32_t> get_radix(T2 value, size_t radix_bitmask) {
    uint32_t radix;
    std::memcpy(&radix, value.c_str(), std::min(value.size(), sizeof(radix)));
    return radix & radix_bitmask;
  }

  /**
  * Determines the total size of a materialized segment list.
  **/
  static size_t _materialized_table_size(std::unique_ptr<MaterializedSegmentList<T>>& table) {
    size_t total_size = 0;
    for (auto chunk : *table) {
      total_size += chunk->size();
    }

    return total_size;
  }

  /**
  * Concatenates multiple materialized segments to a single materialized segment.
  **/
  static std::unique_ptr<MaterializedSegmentList<T>> _concatenate_chunks(
      std::unique_ptr<MaterializedSegmentList<T>>& input_chunks) {
    auto output_table = std::make_unique<MaterializedSegmentList<T>>(1);
    (*output_table)[0] = std::make_shared<MaterializedSegment<T>>();

    // Reserve the required space and move the data to the output
    auto output_chunk = (*output_table)[0];
    output_chunk->reserve(_materialized_table_size(input_chunks));
    for (auto& chunk : *input_chunks) {
      output_chunk->insert(output_chunk->end(), chunk->begin(), chunk->end());
    }

    return output_table;
  }

  /**
  * Performs the clustering on a materialized table using a clustering function that determines for each
  * value the appropriate cluster id. This is how the clustering works:
  * -> Count for each chunk how many of its values belong in each of the clusters using histograms.
  * -> Aggregate the per-chunk histograms to a histogram for the whole table. For each chunk it is noted where
  *    it will be inserting values in each cluster.
  * -> Reserve the appropriate space for each output cluster to avoid ongoing vector resizing.
  * -> At last, each value of each chunk is moved to the appropriate cluster.
  **/
  std::unique_ptr<MaterializedSegmentList<T>> _cluster(const std::unique_ptr<MaterializedSegmentList<T>>& input_chunks,
                                                       std::function<size_t(const T&)> clusterer) {
    auto output_table = std::make_unique<MaterializedSegmentList<T>>(_cluster_count);
    TableInformation table_information(input_chunks->size(), _cluster_count);

    // Count for every chunk the number of entries for each cluster in parallel
    std::vector<std::shared_ptr<AbstractTask>> histogram_jobs;
    for (size_t chunk_number = 0; chunk_number < input_chunks->size(); ++chunk_number) {
      auto& chunk_information = table_information.chunk_information[chunk_number];
      auto input_chunk = (*input_chunks)[chunk_number];

      // Count the number of entries for each cluster to be able to reserve the appropriate output space later.
      auto job = std::make_shared<JobTask>([input_chunk, &clusterer, &chunk_information] {
        for (auto& entry : *input_chunk) {
          auto cluster_id = clusterer(entry.value);
          ++chunk_information.cluster_histogram[cluster_id];
        }
      });

      histogram_jobs.push_back(job);
      job->schedule();
    }

    CurrentScheduler::wait_for_tasks(histogram_jobs);

    // Aggregate the chunks histograms to a table histogram and initialize the insert positions for each chunk
    for (auto& chunk_information : table_information.chunk_information) {
      for (size_t cluster_id = 0; cluster_id < _cluster_count; ++cluster_id) {
        chunk_information.insert_position[cluster_id] = table_information.cluster_histogram[cluster_id];
        table_information.cluster_histogram[cluster_id] += chunk_information.cluster_histogram[cluster_id];
      }
    }

    // Reserve the appropriate output space for the clusters
    for (size_t cluster_id = 0; cluster_id < _cluster_count; ++cluster_id) {
      auto cluster_size = table_information.cluster_histogram[cluster_id];
      (*output_table)[cluster_id] = std::make_shared<MaterializedSegment<T>>(cluster_size);
    }

    // Move each entry into its appropriate cluster in parallel
    std::vector<std::shared_ptr<AbstractTask>> cluster_jobs;
    for (size_t chunk_number = 0; chunk_number < input_chunks->size(); ++chunk_number) {
      auto job =
          std::make_shared<JobTask>([chunk_number, &output_table, &input_chunks, &table_information, &clusterer] {
            auto& chunk_information = table_information.chunk_information[chunk_number];
            for (auto& entry : *(*input_chunks)[chunk_number]) {
              auto cluster_id = clusterer(entry.value);
              auto& output_cluster = *(*output_table)[cluster_id];
              auto& insert_position = chunk_information.insert_position[cluster_id];
              output_cluster[insert_position] = entry;
              ++insert_position;
            }
          });
      cluster_jobs.push_back(job);
      job->schedule();
    }

    CurrentScheduler::wait_for_tasks(cluster_jobs);

    return output_table;
  }

  /**
  * Performs least significant bit radix clustering which is used in the equi join case.
  * Note: if we used the most significant bits, we could also use this for non-equi joins.
  * Then, however we would have to deal with skewed clusters. Other ideas:
  * - manually select the clustering bits based on statistics.
  * - consolidate clusters in order to reduce skew.
  **/
  std::unique_ptr<MaterializedSegmentList<T>> _radix_cluster(
      std::unique_ptr<MaterializedSegmentList<T>>& input_chunks) {
    auto radix_bitmask = _cluster_count - 1;
    return _cluster(input_chunks, [=](const T& value) { return get_radix<T>(value, radix_bitmask); });
  }

  /**
  * Picks split values from the given sample values. Each split value denotes the inclusive
  * upper bound of its corresponding cluster (i.e., split #0 is the upper bound of cluster #0).
  * As the last cluster does not require an upper bound, the returned vector size is usually
  * the cluster count minus one. However, it can be even shorter (e.g., attributes where
  * #distinct values < #cluster count).
  *
  * Procedure: passed values are sorted and samples are picked from the whole sample
  * value range in fixed widths. Repeated values are not removed before picking to handle
  * skewed inputs. However, the final split values are unique. As a consequence, the split
  * value vector might contain less values than `_cluster_count - 1`.
  **/
  const std::vector<T> _pick_split_values(std::vector<T> sample_values) const {
    std::sort(sample_values.begin(), sample_values.end());

    if (sample_values.size() <= _cluster_count - 1) {
      const auto last = std::unique(sample_values.begin(), sample_values.end());
      sample_values.erase(last, sample_values.end());
      return sample_values;
    }

    std::vector<T> split_values;
    split_values.reserve(_cluster_count - 1);
    auto jump_width = sample_values.size() / _cluster_count;
    for (auto sample_offset = size_t{0}; sample_offset < _cluster_count - 1; ++sample_offset) {
      split_values.push_back(sample_values[static_cast<size_t>((sample_offset + 1) * jump_width)]);
    }

    const auto last_split = std::unique(split_values.begin(), split_values.end());
    split_values.erase(last_split, split_values.end());
    return split_values;
  }

  /**
  * Performs the range cluster sort for the non-equi case (>, >=, <, <=, !=) which requires the complete table to
  * be sorted and not only the clusters in themselves. Returns the clustered data from the left table and the
  * right table in a pair.
  **/
  std::pair<std::unique_ptr<MaterializedSegmentList<T>>, std::unique_ptr<MaterializedSegmentList<T>>> _range_cluster(
      const std::unique_ptr<MaterializedSegmentList<T>>& input_left,
      const std::unique_ptr<MaterializedSegmentList<T>>& input_right, std::vector<T> sample_values) {
    const std::vector<T> split_values = _pick_split_values(sample_values);

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

    auto output_left = _cluster(input_left, clusterer);
    auto output_right = _cluster(input_right, clusterer);

    return {std::move(output_left), std::move(output_right)};
  }

  /**
  * Sorts all clusters of a materialized table.
  **/
  void _sort_clusters(std::unique_ptr<MaterializedSegmentList<T>>& clusters) {
    for (auto cluster : *clusters) {
      std::sort(cluster->begin(), cluster->end(), [](auto& left, auto& right) { return left.value < right.value; });
    }
  }

 public:
  /**
  * Executes the clustering and sorting.
  **/
  RadixClusterOutput<T> execute() {
    RadixClusterOutput<T> output;

    // Sort the chunks of the input tables in the non-equi cases
    ColumnMaterializer<T> left_column_materializer(!_equi_case, _materialize_null_left);
    ColumnMaterializer<T> right_column_materializer(!_equi_case, _materialize_null_right);
    auto [materialized_left_segments, null_rows_left, samples_left] =  // NOLINT
        left_column_materializer.materialize(_input_table_left, _left_column_id);
    auto [materialized_right_segments, null_rows_right, samples_right] =  // NOLINT
        right_column_materializer.materialize(_input_table_right, _right_column_id);
    output.null_rows_left = std::move(null_rows_left);
    output.null_rows_right = std::move(null_rows_right);

    // Append right samples to left samples and sort (reserve not necessarity when insert can
    // determined the new capacity from iterator: https://stackoverflow.com/a/35359472/1147726)
    samples_left.insert(samples_left.end(), samples_right.begin(), samples_right.end());

    if (_cluster_count == 1) {
      output.clusters_left = _concatenate_chunks(materialized_left_segments);
      output.clusters_right = _concatenate_chunks(materialized_right_segments);
    } else if (_equi_case) {
      output.clusters_left = _radix_cluster(materialized_left_segments);
      output.clusters_right = _radix_cluster(materialized_right_segments);
    } else {
      auto result = _range_cluster(materialized_left_segments, materialized_right_segments, samples_left);
      output.clusters_left = std::move(result.first);
      output.clusters_right = std::move(result.second);
    }

    // Sort each cluster (right now std::sort -> but maybe can be replaced with
    // an more efficient algorithm, if subparts are already sorted [InsertionSort?!])
    _sort_clusters(output.clusters_left);
    _sort_clusters(output.clusters_right);

    return output;
  }
};

}  // namespace opossum
