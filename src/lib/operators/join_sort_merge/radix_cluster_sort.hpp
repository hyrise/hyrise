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

namespace {

using namespace opossum;  // NOLINT

/**
* The ClusterOutput holds the data structures that belong to the output of the clustering stage.
*/
template <typename T>
struct ClusterOutput {
  MaterializedSegmentList<T> clusters_left;
  MaterializedSegmentList<T> clusters_right;
  std::unique_ptr<PosList> null_rows_left;
  std::unique_ptr<PosList> null_rows_right;
};

}  // anonymous namespace

namespace opossum {

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
class JoinSortMergeClusterer {
 public:
  JoinSortMergeClusterer(const std::shared_ptr<const Table> left, const std::shared_ptr<const Table> right,
                         const ColumnIDPair& column_ids, const bool equi_case, const bool materialize_null_left,
                         const bool materialize_null_right, const size_t cluster_count)
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
    DebugAssert(left, "left input operator is null");
    DebugAssert(right, "right input operator is null");
  }

  virtual ~JoinSortMergeClusterer() = default;

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
  /**
  * The SegmentInformation structure is used to gather statistics regarding a segments's values
  * in order to be able to appropriately reserve space for the clustering output.
  **/
  struct SegmentInformation {
    explicit SegmentInformation(const size_t cluster_count) {
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
      segment_information.reserve(chunk_count);
      for (size_t i = 0; i < chunk_count; ++i) {
        segment_information.push_back(SegmentInformation(cluster_count));
      }
    }
    // Used to count the number of entries for each cluster from the whole table
    std::vector<size_t> cluster_histogram;
    std::vector<SegmentInformation> segment_information;
  };

  // Input parameters
  std::shared_ptr<const Table> _input_table_left;
  std::shared_ptr<const Table> _input_table_right;
  const ColumnID _left_column_id;
  const ColumnID _right_column_id;
  const bool _equi_case;

  // The cluster count must be a power of two, i.e. 1, 2, 4, 8, 16, ...
  // It is asserted to be a power of two in the constructor.
  const size_t _cluster_count;

  const bool _materialize_null_left;
  const bool _materialize_null_right;

  /**
  * Determines the total size of a materialized segment list.
  **/
  static size_t _materialized_table_size(const MaterializedSegmentList<T>& materialized_segments) {
    size_t total_size = 0;
    for (const auto& materialized_segment : materialized_segments) {
      total_size += materialized_segment->size();
    }

    return total_size;
  }

  /**
  * Concatenates multiple materialized segments to a single materialized segment.
  **/
  // TODO(Bouncner): add test
  static MaterializedSegmentList<T> _concatenate_materialized_segments(
      const MaterializedSegmentList<T>& materialized_segments) {
    auto output = MaterializedSegment<T>();
    std::vector<size_t> sorted_run_start_positions;

    auto current_start_position = size_t{0};
    // Reserve the required space and copy the data to the output
    output.reserve(_materialized_table_size(materialized_segments));
    for (const auto& materialized_segment : materialized_segments) {
      output.insert(output.end(), materialized_segment->cbegin(), materialized_segment->cend());
      sorted_run_start_positions.push_back(current_start_position);
      current_start_position += materialized_segment->size();
    }

    // std::sort(output.begin(), output.end(),
    //               [](const auto& a, const auto& b) { return a.value < b.value; });

    sort_partially_sorted_materialized_segment(output, sorted_run_start_positions);

    const auto shared = std::make_shared<MaterializedSegment<T>>(output);

    return {shared};
  }

  // TODO(Bouncner): Add test
  static void sort_partially_sorted_materialized_segment(MaterializedSegment<T>& materialized_segment,
                                                         std::vector<size_t>& sorted_run_start_positions) {
    DebugAssert(sorted_run_start_positions.size() > 1, "Expecting at least two sorted runs to merge.");
    DebugAssert(std::is_sorted(sorted_run_start_positions.begin(), sorted_run_start_positions.end()),
                "Positions of the sorted runs need to be sorted ascendingly.");

    // To ease the iiterator assignment within the loop (see assignment of `last`)
    sorted_run_start_positions.push_back(materialized_segment.size());

    auto first = materialized_segment.begin();
    auto merge_step = size_t{0};
    // loop until `middle` iterator reaches .end(), which shows that `last` would point to an invalid address
    while ((first + sorted_run_start_positions[merge_step + 1]) != materialized_segment.end()) {
      auto middle = first + sorted_run_start_positions[merge_step + 1];
      auto last = first + sorted_run_start_positions[merge_step + 2];
      std::inplace_merge(first, middle, last, [](auto& left, auto& right) { return left.value < right.value; });

      ++merge_step;
    }

    DebugAssert(std::is_sorted(materialized_segment.begin(), materialized_segment.end(),
                               [](auto& left, auto& right) { return left.value < right.value; }),
                "Resulting materializied segment is unsorted.");
  }

  /**
  * Performs the clustering on a materialized table using a clustering function that determines
  * for each value the appropriate cluster id. This is how the clustering works:
  * -> Count for each input segment how many of its values belong in each of the clusters using histograms.
  * -> Aggregate the per-segment histograms to a histogram for the whole table. For each input segment, it
  *    is noted where it will be inserting values in each cluster.
  * -> Reserve the appropriate space for each output cluster to avoid ongoing vector resizing.
  * -> At last, each value of each input segment is moved to the appropriate cluster.
  **/
  MaterializedSegmentList<T> _cluster(const MaterializedSegmentList<T>& materialized_input_segments,
                                      const std::function<size_t(const T&)> clusterer) {
    DebugAssert(_cluster_count > 1,
                "_cluster() clusters the input data into multiple chunks, thus the cluster count needs to be > 1.");
    auto output = MaterializedSegmentList<T>(_cluster_count);
    TableInformation table_information(materialized_input_segments.size(), _cluster_count);

    // Count for every input segment the number of entries for each cluster in parallel
    std::vector<std::shared_ptr<AbstractTask>> histogram_jobs;
    // std::cout << "first phase" << std::endl;
    for (auto input_segment_id = size_t{0}; input_segment_id < materialized_input_segments.size(); ++input_segment_id) {
      // Count the number of entries for each cluster to be able to reserve the appropriate output space later.
      auto job = std::make_shared<JobTask>([&, input_segment_id, clusterer, this] {
        // clusterer is passed by value to have a job-local copy
        auto& segment_information = table_information.segment_information[input_segment_id];
        auto input_segment = materialized_input_segments[input_segment_id];

        // Ensure that the ColumnMaterializer sorts the materialized segments.
        DebugAssert(std::is_sorted(input_segment->begin(), input_segment->end(),
                                   [](const auto& a, const auto& b) { return a.value < b.value; }),
                    "Input segments need to be sorted.");
        if (_cluster_count > 1) {
          for (const auto& entry : *input_segment) {
            const auto cluster_id = clusterer(entry.value);
            ++segment_information.cluster_histogram[cluster_id];
          }
        }
      });

      histogram_jobs.push_back(job);
      job->schedule();
    }

    CurrentScheduler::wait_for_tasks(histogram_jobs);

    // In case more than one cluster will be created:
    //   -> Aggregate the segment histograms to get insert positions and preallocate each cluster accordingly
    // Otherwise:
    //   -> Size the output with the accumulated size of all segments
    if (_cluster_count > 1) {
      // Aggregate the segment histograms to a table histogram and initialize the insert positions for each segment
      for (auto& segment_information : table_information.segment_information) {
        for (auto cluster_id = size_t{0}; cluster_id < _cluster_count; ++cluster_id) {
          segment_information.insert_position[cluster_id] = table_information.cluster_histogram[cluster_id];
          table_information.cluster_histogram[cluster_id] += segment_information.cluster_histogram[cluster_id];
        }
      }

      // Reserve the appropriate output space for the clusters
      for (auto cluster_id = size_t{0}; cluster_id < _cluster_count; ++cluster_id) {
        const auto cluster_size = table_information.cluster_histogram[cluster_id];
        output[cluster_id] = std::make_shared<MaterializedSegment<T>>(cluster_size);
      }
    } else {
      auto single_cluster_size = size_t{0};
      for (const auto& segment : materialized_input_segments) {
        single_cluster_size += segment->size();
      }
      output[0] = std::make_shared<MaterializedSegment<T>>(single_cluster_size);
    }

    // Move each entry into its appropriate cluster in parallel
    std::vector<std::shared_ptr<AbstractTask>> cluster_jobs;
    const auto input_segment_count = materialized_input_segments.size();
    for (auto input_segment_id = size_t{0}; input_segment_id < input_segment_count; ++input_segment_id) {
      auto job = std::make_shared<JobTask>([&, input_segment_id, clusterer] {  // copy cluster functor per task
        auto& segment_information = table_information.segment_information[input_segment_id];
        for (const auto& entry : *(materialized_input_segments[input_segment_id])) {
          const auto cluster_id = clusterer(entry.value);
          auto& output_cluster = *(output[cluster_id]);
          auto& insert_position = segment_information.insert_position[cluster_id];
          output_cluster[insert_position] = entry;
          ++insert_position;
        }
      });
      cluster_jobs.push_back(job);
      job->schedule();
    }

    CurrentScheduler::wait_for_tasks(cluster_jobs);

    std::vector<std::shared_ptr<AbstractTask>> sort_jobs;
    auto segment_id = size_t{0};
    for (const auto& segment : output) {
      auto job = std::make_shared<JobTask>([&, segment, segment_id] {
        std::vector<size_t> sorted_run_start_positions;
        for (auto chunk_id = ChunkID{0}; chunk_id < materialized_input_segments.size(); ++chunk_id) {
          const auto& segment_information = table_information.segment_information[chunk_id];
          sorted_run_start_positions.push_back(segment_information.insert_position[segment_id] -
                                               segment_information.cluster_histogram[segment_id]);
        }
        sort_partially_sorted_materialized_segment(*segment, sorted_run_start_positions);
        DebugAssert(std::is_sorted(segment->begin(), segment->end(),
                                   [](auto& left, auto& right) { return left.value < right.value; }),
                    "Resulting clusters are expected to be sorted.");
      });
      sort_jobs.push_back(job);
      job->schedule();

      ++segment_id;
    }

    CurrentScheduler::wait_for_tasks(sort_jobs);

    return output;
  }

  /**
  * Performs least significant bit radix clustering which is used in the equi join case.
  * Note: if we used the most significant bits, we could also use this for non-equi joins.
  * Then, however we would have to deal with skewed clusters. Other ideas:
  * - manually select the clustering bits based on statistics.
  * - consolidate clusters in order to reduce skew.
  **/
  MaterializedSegmentList<T> _radix_cluster(const MaterializedSegmentList<T>& materialized_segments) {
    auto radix_bitmask = _cluster_count - 1;
    return _cluster(materialized_segments, [=](const T& value) { return get_radix<T>(value, radix_bitmask); });
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
  std::pair<MaterializedSegmentList<T>, MaterializedSegmentList<T>> _range_cluster(
      const MaterializedSegmentList<T>& input_left, const MaterializedSegmentList<T>& input_right,
      const std::vector<T> sample_values) {
    const std::vector<T> split_values = _pick_split_values(sample_values);
    DebugAssert(std::is_sorted(split_values.begin(), split_values.end()), "Split values need to be sorted.");

    // This functor returns the corresponding cluster for a given value. Since both the split values and values to
    // cluster are sorted, we can assign clusters in linear time. State within the functor is required. To ensure
    // multiple threads do not interfere with each other, each cluster job needs to have own instance of the functor.
    class RangeClusterFunctor {
     public:
      // split_values are copied to ensure data locality (sort-merge shines on large joins).
      explicit RangeClusterFunctor(const std::vector<T>& splits)
          : split_values(splits),
            current_value_and_split_id(std::make_pair(split_values.front(), 0)),
            max_split_value(split_values.back()) {}

      explicit RangeClusterFunctor(const RangeClusterFunctor& functor)
          : split_values(functor.split_values),
            current_value_and_split_id(functor.current_value_and_split_id),
            max_split_value(functor.max_split_value) {}

      size_t operator()(const T& value) {
        // For the majority of cases, this early exit should be taken
        if (value <= current_value_and_split_id.first) {
          return current_value_and_split_id.second;
        }

        // Early out when reached last cluster
        if (value > max_split_value) {
          return split_values.size();
        }

        while (current_value_and_split_id.first < value) {
          const auto new_split_id = ++current_value_and_split_id.second;
          current_value_and_split_id = std::make_pair(split_values[new_split_id], new_split_id);
        }
        return current_value_and_split_id.second;
      }

     private:
      const std::vector<T> split_values;
      std::pair<T, size_t> current_value_and_split_id;
      const T max_split_value;
    };

    auto output_left = _cluster(input_left, RangeClusterFunctor(split_values));
    auto output_right = _cluster(input_right, RangeClusterFunctor(split_values));

    return {std::move(output_left), std::move(output_right)};
  }

  /**
  * Sorts all clusters of a materialized table.
  **/
  void _sort_clusters(MaterializedSegmentList<T>& materialized_segments) {
    for (const auto& materialized_segment : materialized_segments) {
      std::sort(materialized_segment->begin(), materialized_segment->end(),
                [](auto& left, auto& right) { return left.value < right.value; });
    }
  }

 public:
  /**
  * Executes the clustering and sorting.
  **/
  ClusterOutput<T> execute() {
    ClusterOutput<T> output;

    /** First, chunks are fully materialized and sorted. The sorting used for two reasons:
     *  (i)  Writes to clusters are usually sped up (very much for range clustering,
     *       for radix partitioning when values occur multiple times)
     *  (ii) We can efficiently "sort" the resulting clusters with in place merging.
     */
    ColumnMaterializer<T> left_column_materializer(true, _materialize_null_left);
    ColumnMaterializer<T> right_column_materializer(true, _materialize_null_right);
    auto [materialized_left_segments, null_rows_left, samples_left] =
        left_column_materializer.materialize(_input_table_left, _left_column_id);
    auto [materialized_right_segments, null_rows_right, samples_right] =
        right_column_materializer.materialize(_input_table_right, _right_column_id);
    output.null_rows_left = std::move(null_rows_left);
    output.null_rows_right = std::move(null_rows_right);

    // Append right samples to left samples and sort (reserve not necessarity when insert can
    // determined the new capacity from iterator: https://stackoverflow.com/a/35359472/1147726)
    samples_left.insert(samples_left.end(), samples_right.begin(), samples_right.end());

    if (_cluster_count == 1) {
      output.clusters_left = std::move(_concatenate_materialized_segments(materialized_left_segments));
      output.clusters_right = std::move(_concatenate_materialized_segments(materialized_right_segments));
    } else if (_equi_case) {
      output.clusters_left = _radix_cluster(materialized_left_segments);
      output.clusters_right = _radix_cluster(materialized_right_segments);
    } else {
      auto result = _range_cluster(materialized_left_segments, materialized_right_segments, samples_left);
      output.clusters_left = std::move(result.first);
      output.clusters_right = std::move(result.second);
    }

    return output;
  }
};

}  // namespace opossum
