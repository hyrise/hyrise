#pragma once

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

/*
* The JoinSortMergeClusterer clusters a given pair of tables into joinable clusters. It clusters either
* either using (i) radix clustering or (ii) range clustering.
* (i)  The radix clustering algorithm clusters on the basis
*      of the least significant bits of the values because the values there are much more evenly distributed than for the
*      most significant bits. As a result, equal values always get moved to the same cluster and the clusters are each
*      sorted but there is no order over clusters. This is okay for the equi join, because we are only interested
*      in equality.
* (ii) In the case of a non-equi join however, complete sortedness is required, because join matches exist
*      beyond cluster borders. Therefore, the clustering defaults to a range clustering algorithm for the non-equi-join.
*
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
class JoinSortMergeClusterer {
 public:
  JoinSortMergeClusterer(const std::shared_ptr<const Table> left, const std::shared_ptr<const Table> right,
                         const ColumnIDPair& column_ids, const bool radix_clustering, const bool materialize_null_left,
                         const bool materialize_null_right, const size_t cluster_count);

  virtual ~JoinSortMergeClusterer() = default;

  template <typename T2>
  static std::enable_if_t<std::is_integral_v<T2>, size_t> get_radix(const T2 value, const size_t radix_bitmask) {
    return static_cast<size_t>(value) & radix_bitmask;
  }

  template <typename T2>
  static std::enable_if_t<!std::is_integral_v<T2>, size_t> get_radix(const T2 value, const size_t radix_bitmask) {
    if (std::is_floating_point<T2>::value) {
      PerformanceWarning("Using hash to perform bit_cast/radix partitioning of floating point number.");  
    }
    return std::hash<T2>{}(value) & radix_bitmask;
  }

  static void merge_partially_sorted_materialized_segment(MaterializedSegment<T>& materialized_segment,
                                                          std::unique_ptr<std::vector<size_t>> sorted_run_start_positions);

  /**
  * Concatenates multiple materialized segments to a single materialized segment.
  **/
  static MaterializedSegmentList<T> concatenate_materialized_segments(
      const MaterializedSegmentList<T>& materialized_segments, const bool segments_are_presorted);

  /**
  * Picks split values from the given sample values. Each split value denotes the exclusive
  * upper bound of its corresponding cluster (i.e., split #0 is the upper bound of cluster #0).
  * As the last cluster does not require an upper bound, the returned vector size is usually
  * the cluster count minus one. However, it can be even shorter (e.g., attributes where
  * #distinct values < #cluster count).
  *
  * Procedure: passed values are sorted and samples are picked from the whole sample
  * value range in fixed widths. By using fixed widths for the selection, large gaps (which might
  * be good split candidates) in the input sequence are not recognized. Repeated values are not
  * removed before picking to handle skewed inputs. However, the final split values are unique.
  * As a consequence, the split value vector might contain less values than `cluster_count - 1`.
  **/
  static std::vector<T> pick_split_values(std::vector<T> sample_values, const size_t cluster_count);

  /**
  * Performs the range cluster sort for the non-equi case (>, >=, <, <=, !=) which requires the complete table to
  * be sorted and not only the clusters in themselves. Returns the clustered data from the left table and the
  * right table in a pair.
  **/
  static std::pair<MaterializedSegmentList<T>, MaterializedSegmentList<T>> range_cluster(
      const MaterializedSegmentList<T>& input_left, const MaterializedSegmentList<T>& input_right,
      const std::vector<T> split_values, const size_t cluster_count, const std::pair<bool, bool> segments_are_presorted);

  /**
  * Performs least significant bit radix clustering which is used in the equi join case.
  * Note: if we used the most significant bits, we could also use this for non-equi joins.
  * Then, however we would have to deal with skewed clusters. Other ideas:
  * - manually select the clustering bits based on statistics.
  * - consolidate clusters in order to reduce skew.
  **/
  static MaterializedSegmentList<T> radix_cluster(const MaterializedSegmentList<T>& materialized_segments,
      const size_t cluster_count, const bool segments_are_presorted);

  /**
  * Performs the clustering on a materialized table using a clustering function that determines
  * for each value the appropriate cluster id. This is how the clustering works:
  * -> Count for each input segment how many of its values belong in each of the clusters using histograms.
  * -> Aggregate the per-segment histograms to a histogram for the whole table. For each input segment, it
  *    is noted where it will be inserting values in each cluster.
  * -> Reserve the appropriate space for each output cluster to avoid ongoing vector resizing.
  * -> At last, each value of each input segment is moved to the appropriate cluster.
  **/
  static MaterializedSegmentList<T> cluster(const MaterializedSegmentList<T>& materialized_input_segments,
                                      const std::function<size_t(const T&)> clusterer, const size_t cluster_count,
                                      const bool segments_are_presorted);

  /**
  * The ClusterOutput holds the data structures that belong to the output of the clustering stage.
  */
  struct ClusterOutput {
    MaterializedSegmentList<T> clusters_left;
    MaterializedSegmentList<T> clusters_right;
    std::unique_ptr<PosList> null_rows_left;
    std::unique_ptr<PosList> null_rows_right;
  };

  ClusterOutput execute();

 protected:

  /**
  * The SegmentHistogramAggregate structure is used to gather statistics regarding a segments's values
  * in order to be able to appropriately reserve space for the clustering output.
  **/
  struct SegmentHistogramAggregate {
    explicit SegmentHistogramAggregate(const size_t cluster_count) {
      cluster_histogram.resize(cluster_count);
      insert_position.resize(cluster_count);
    }
    // Used to count the number of entries for each cluster from a specific segment
    // Example cluster_histogram[3] = 5
    // -> 5 values from the segment belong in cluster 3
    std::vector<size_t> cluster_histogram;

    // Stores the beginning of the range in cluster for this segment.
    // Example: insert_position[3] = 5
    // -> This segment's values for cluster 3 are inserted at index 5 and forward.
    std::vector<size_t> insert_position;
  };

  /**
  * The TableInformation structure is used to gather statistics regarding the value distribution of the column
  * to be clustered in order to be able to appropriately reserve space for the clustering output.
  **/
  struct TableInformation {
    TableInformation(const size_t chunk_count, const size_t cluster_count) {
      cluster_histogram.resize(cluster_count);
      segment_histogram_aggregates.reserve(chunk_count);
      for (size_t i = 0; i < chunk_count; ++i) {
        segment_histogram_aggregates.push_back(SegmentHistogramAggregate(cluster_count));
      }
    }
    // Used to count the number of entries for each cluster from the whole table
    std::vector<size_t> cluster_histogram;
    std::vector<SegmentHistogramAggregate> segment_histogram_aggregates;
  };

  // Input parameters
  std::shared_ptr<const Table> _input_table_left;
  std::shared_ptr<const Table> _input_table_right;
  const ColumnID _left_column_id;
  const ColumnID _right_column_id;
  const bool _radix_clustering;

  // Denotes whether the materialized segments in the first phase are already being sorted
  // before the clustering phase. First elements of pairs for left input, second for right input.
  std::pair<bool, bool> _presort_segments = {true, true};

  // The cluster count must be a power of two (i.e. 1, 2, 4, 8, 16) for radix clustering.
  const size_t _cluster_count;

  const bool _materialize_null_left;
  const bool _materialize_null_right;

  /**
  * Determines the total size of a materialized segment list.
  **/
  static size_t _materialized_table_size(const MaterializedSegmentList<T>& materialized_segments);
};

}  // namespace opossum
