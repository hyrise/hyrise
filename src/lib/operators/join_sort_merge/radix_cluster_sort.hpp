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
  std::unique_ptr<MaterializedColumnList<T>> clusters_left;
  std::unique_ptr<MaterializedColumnList<T>> clusters_right;
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
    DebugAssert((cluster_count & (cluster_count - 1)) == 0, "cluster_count must be a power of two, i.e. 1, 2, 4, 8...");
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
  static typename std::enable_if<std::is_arithmetic<T2>::value, uint32_t>::type get_radix(T2 value,
                                                                                          uint32_t radix_bitmask) {
    return static_cast<uint32_t>(value) & radix_bitmask;
  }

  // Radix calculation for non-arithmetic types
  template <typename T2>
  static typename std::enable_if<std::is_same<T2, std::string>::value, uint32_t>::type get_radix(
      T2 value, uint32_t radix_bitmask) {
    uint32_t radix;
    std::memcpy(&radix, value.c_str(), std::min(value.size(), sizeof(radix)));
    return radix & radix_bitmask;
  }

  /**
  * Determines the total size of a materialized column list.
  **/
  static size_t _materialized_table_size(std::unique_ptr<MaterializedColumnList<T>>& table) {
    size_t total_size = 0;
    for (auto chunk : *table) {
      total_size += chunk->size();
    }

    return total_size;
  }

  /**
  * Concatenates multiple materialized columns to a single materialized column.
  **/
  static std::unique_ptr<MaterializedColumnList<T>> _concatenate_chunks(
      std::unique_ptr<MaterializedColumnList<T>>& input_chunks) {
    auto output_table = std::make_unique<MaterializedColumnList<T>>(1);
    (*output_table)[0] = std::make_shared<MaterializedColumn<T>>();

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
  std::unique_ptr<MaterializedColumnList<T>> _cluster(std::unique_ptr<MaterializedColumnList<T>>& input_chunks,
                                                      std::function<size_t(const T&)> clusterer) {
    auto output_table = std::make_unique<MaterializedColumnList<T>>(_cluster_count);
    TableInformation table_information(input_chunks->size(), _cluster_count);

    // Count for every chunk the number of entries for each cluster in parallel
    std::vector<std::shared_ptr<JobTask>> histogram_jobs;
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
      (*output_table)[cluster_id] = std::make_shared<MaterializedColumn<T>>(cluster_size);
    }

    // Move each entry into its appropriate cluster in parallel
    std::vector<std::shared_ptr<JobTask>> cluster_jobs;
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
  * - hand select the clustering bits based on statistics.
  * - consolidate clusters in order to reduce skew.
  **/
  std::unique_ptr<MaterializedColumnList<T>> _radix_cluster(std::unique_ptr<MaterializedColumnList<T>>& input_chunks) {
    auto radix_bitmask = _cluster_count - 1;
    return _cluster(input_chunks, [=](const T& value) { return get_radix<T>(value, radix_bitmask); });
  }

  /**
  * Picks sample values from a materialized table that are used to determine cluster range bounds.
  **/
  void _pick_sample_values(std::vector<std::map<T, size_t>>& sample_values,
                           std::unique_ptr<MaterializedColumnList<T>>& materialized_columns) {
    // Note:
    // - The materialized chunks are sorted.
    // - In between the chunks there is no order
    // - Every chunk can contain values for every cluster
    // - To sample for range border values we look at the position where the values for each cluster
    //   would start if every chunk had an even values distribution for every cluster.
    // - Later, these values are aggregated to determine the actual cluster borders
    for (size_t chunk_number = 0; chunk_number < materialized_columns->size(); ++chunk_number) {
      auto chunk_values = (*materialized_columns)[chunk_number];
      for (size_t cluster_id = 0; cluster_id < _cluster_count - 1; ++cluster_id) {
        auto pos = chunk_values->size() * (cluster_id + 1) / static_cast<float>(_cluster_count);
        auto index = static_cast<size_t>(pos);
        ++sample_values[cluster_id][(*chunk_values)[index].value];
      }
    }
  }

  /**
  * Performs the range cluster sort for the non-equi case (>, >=, <, <=, !=) which requires the complete table to
  * be sorted and not only the clusters in themselves. Returns the clustered data from the left table and the
  * right table in a pair.
  **/
  std::pair<std::unique_ptr<MaterializedColumnList<T>>, std::unique_ptr<MaterializedColumnList<T>>> _range_cluster(
      std::unique_ptr<MaterializedColumnList<T>>& input_left, std::unique_ptr<MaterializedColumnList<T>>& input_right) {
    std::vector<std::map<T, size_t>> sample_values(_cluster_count - 1);

    _pick_sample_values(sample_values, input_left);
    _pick_sample_values(sample_values, input_right);

    // Pick the most common sample values for each cluster for the split values.
    // The last cluster does not need a split value because it covers all values that are bigger than all split values
    // Note: the split values mark the ranges of the clusters.
    // A split value is the end of a range and the start of the next one.
    std::vector<T> split_values(_cluster_count - 1);
    for (size_t cluster_id = 0; cluster_id < _cluster_count - 1; ++cluster_id) {
      // Pick the values with the highest count
      split_values[cluster_id] = std::max_element(sample_values[cluster_id].begin(), sample_values[cluster_id].end(),
                                                  // second is the count of the value
                                                  [](auto& a, auto& b) { return a.second < b.second; })
                                     ->second;
    }

    // Implements range clustering
    auto cluster_count = _cluster_count;
    auto clusterer = [cluster_count, &split_values](const T& value) {
      // Find the first split value that is greater or equal to the entry.
      // The split values are sorted in ascending order.
      // Note: can we do this faster? (binary search?)
      for (size_t cluster_id = 0; cluster_id < cluster_count - 1; ++cluster_id) {
        if (value <= split_values[cluster_id]) {
          return cluster_id;
        }
      }

      // The value is greater than all split values, which means it belongs in the last cluster.
      return cluster_count - 1;
    };

    auto output_left = _cluster(input_left, clusterer);
    auto output_right = _cluster(input_right, clusterer);

    return {std::move(output_left), std::move(output_right)};
  }

  /**
  * Sorts all clusters of a materialized table.
  **/
  void _sort_clusters(std::unique_ptr<MaterializedColumnList<T>>& clusters) {
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
    auto materialization_left = left_column_materializer.materialize(_input_table_left, _left_column_id);
    auto materialization_right = right_column_materializer.materialize(_input_table_right, _right_column_id);
    auto materialized_left_columns = std::move(materialization_left.first);
    auto materialized_right_columns = std::move(materialization_right.first);
    output.null_rows_left = std::move(materialization_left.second);
    output.null_rows_right = std::move(materialization_right.second);

    if (_cluster_count == 1) {
      output.clusters_left = _concatenate_chunks(materialized_left_columns);
      output.clusters_right = _concatenate_chunks(materialized_right_columns);
    } else if (_equi_case) {
      output.clusters_left = _radix_cluster(materialized_left_columns);
      output.clusters_right = _radix_cluster(materialized_right_columns);
    } else {
      auto result = _range_cluster(materialized_left_columns, materialized_right_columns);
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
