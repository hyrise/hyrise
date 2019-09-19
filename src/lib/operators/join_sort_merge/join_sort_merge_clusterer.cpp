#include <algorithm>
#include <cstring>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "join_sort_merge_clusterer.hpp"
#include "column_materializer.hpp"

namespace {

using namespace opossum;  // NOLINT

size_t estimated_cost_full_sorting(const size_t table_row_count, const size_t cluster_count) {
  // The cost is simply sorting (n*log(n)) each cluster
  const size_t avg_element_count = std::max(1ul, static_cast<size_t>(table_row_count / cluster_count));
  return cluster_count * static_cast<size_t>(avg_element_count * std::log2(avg_element_count));
}

size_t estimated_cost_partial_sorting(const size_t table_row_count, const size_t cluster_count, const size_t chunk_count) {
  // For each cluster, the sorted runs are iteratively sorted. In average, each run is cluster_size/2 large.
  // The number of sorted runs is determined by the number of input chunks.
  const size_t avg_element_count = std::max(1ul, static_cast<size_t>(table_row_count / cluster_count));
  return cluster_count * static_cast<size_t>(chunk_count * std::max(1ul, avg_element_count / 2));
}

size_t is_partial_sorting_beneficial(const size_t table_row_count, const size_t cluster_count, const size_t chunk_count) {
  return estimated_cost_partial_sorting(table_row_count, cluster_count, chunk_count) < estimated_cost_full_sorting(table_row_count, cluster_count);
}

}  // namespace


namespace opossum {

template <typename T>
JoinSortMergeClusterer<T>::JoinSortMergeClusterer(const std::shared_ptr<const Table> left, const std::shared_ptr<const Table> right,
                         const ColumnIDPair& column_ids, const bool radix_clustering, const bool materialize_null_left,
                         const bool materialize_null_right, const size_t cluster_count)
    : _input_table_left{left},
      _input_table_right{right},
      _left_column_id{column_ids.first},
      _right_column_id{column_ids.second},
      _radix_clustering{radix_clustering},
      _cluster_count{cluster_count},
      _materialize_null_left{materialize_null_left},
      _materialize_null_right{materialize_null_right} {
  DebugAssert(cluster_count > 0, "cluster count must be > 0");
  DebugAssert(!_radix_clustering || (_radix_clustering && (cluster_count & (cluster_count - 1)) == 0), "cluster count must be a power of two for radix clustering");
  DebugAssert(left, "left input operator is null");
  DebugAssert(right, "right input operator is null");

  /**
  * In certain situations, sorting the materialized segments of the first phase can be
  * advantageous. When segments are sorted, the later clustering creates a partially
  * sorted output. Depending on the number of sorted runs (i.e., the number of input
  * chunks), merging the sorted runs can be faster than sorting the final clusters.
  */
  const size_t left_input_rows = left->row_count();
  const size_t right_input_rows = right->row_count();
  const size_t left_input_chunk_count = left->chunk_count();
  const size_t right_input_chunk_count = right->chunk_count();

  /**
  * For range clustering, the materialized segments need to be sorted. For radix
  * clustering, a simple heuristic is used to determine if presorting is advantageous.
  **/
  if (_radix_clustering) {
  	_presort_segments.first = is_partial_sorting_beneficial(left_input_rows, cluster_count, left_input_chunk_count);
  	_presort_segments.second = is_partial_sorting_beneficial(right_input_rows, cluster_count, right_input_chunk_count);

  	  // std::cout << "Estimating merge costs with: L (part " << estimated_cost_partial_sorting(left_input_rows, cluster_count, left_input_chunk_count) << " vs. full " << estimated_cost_full_sorting(left_input_rows, cluster_count) << ") and R(part " << estimated_cost_partial_sorting(right_input_rows, cluster_count, right_input_chunk_count) << " vs. full " << estimated_cost_full_sorting(right_input_rows, cluster_count) << ")" << std::endl;
  }
}

template <typename T>
void JoinSortMergeClusterer<T>::merge_partially_sorted_materialized_segment(MaterializedSegment<T>& materialized_segment,
                                                        std::unique_ptr<std::vector<size_t>> sorted_run_start_positions) {
  if (materialized_segment.size() < 2 || (sorted_run_start_positions->size() == 1 && (*sorted_run_start_positions)[0] == 0)) {
    // Trivial cases for early outs.
    return;
  }

  DebugAssert(!sorted_run_start_positions->empty(), "List of sorted runs to merge cannot be empty.");
  DebugAssert((*sorted_run_start_positions)[0] == 0, "First sorted run does not start at the beginning.");
  DebugAssert(std::is_sorted(sorted_run_start_positions->begin(), sorted_run_start_positions->end()),
              "Positions of the sorted runs need to be sorted ascendingly.");

  /**
   * To sort the partially sorted lists, we merge two sorted lists in each iteration.
   * The `first` iterator stays at the very beginning of the input segment, while the
   * `middle` and `last` are the next two positions from the `sorted_run_start_positions`
   * vector (note, `last` points behind the last position to sort).
   * We first add a position which denotes segment.end() (i.e., segment.size()) to the
   * position list to ease the assignments within the loop.
   */
  sorted_run_start_positions->push_back(materialized_segment.size());

  auto first = materialized_segment.begin();
  auto merge_step = size_t{0};
  for (;;) {
    const auto middle = first + (*sorted_run_start_positions)[merge_step + 1];
    const auto last = first + (*sorted_run_start_positions)[merge_step + 2];
    std::inplace_merge(first, middle, last, [](auto& left, auto& right) { return left.value < right.value; });

    ++merge_step;

    if (last == materialized_segment.end()) {
      break;
    }
  }
}

/**
* Concatenates multiple materialized segments to a single materialized segment.
**/
template <typename T>
MaterializedSegmentList<T> JoinSortMergeClusterer<T>::concatenate_materialized_segments(
    const MaterializedSegmentList<T>& materialized_segments, const bool segments_are_presorted) {
  auto output = std::make_shared<MaterializedSegment<T>>();
  auto sorted_run_start_positions = std::make_unique<std::vector<size_t>>();

  auto current_start_position = size_t{0};
  // Reserve the required space and copy the data to the output
  output->reserve(_materialized_table_size(materialized_segments));
  for (const auto& materialized_segment : materialized_segments) {
    output->insert(output->end(), materialized_segment->cbegin(), materialized_segment->cend());
    sorted_run_start_positions->push_back(current_start_position);
    current_start_position += materialized_segment->size();
  }

  if (segments_are_presorted && is_partial_sorting_beneficial(current_start_position, materialized_segments.size(), materialized_segments.size())) {
    merge_partially_sorted_materialized_segment(*output, std::move(sorted_run_start_positions));
  } else {
    std::sort(output->begin(), output->end(),
              [](auto& left, auto& right) { return left.value < right.value; });
  }

  return {output};
}

template <typename T>
std::vector<T> JoinSortMergeClusterer<T>::pick_split_values(std::vector<T> sample_values, const size_t cluster_count) {
  DebugAssert(cluster_count > 1, "Split values are only necessary for at least two clusters.");
  std::sort(sample_values.begin(), sample_values.end());

  if (sample_values.size() <= cluster_count - 1) {
    const auto last = std::unique(sample_values.begin(), sample_values.end());
    sample_values.erase(last, sample_values.end());
    return sample_values;
  }

  std::vector<T> split_values;
  split_values.reserve(cluster_count - 1);
  auto jump_width = sample_values.size() / cluster_count;
  for (auto sample_offset = size_t{0}; sample_offset < cluster_count - 1; ++sample_offset) {
    split_values.push_back(sample_values[static_cast<size_t>((sample_offset + 1) * jump_width)]);
  }

  const auto last_split = std::unique(split_values.begin(), split_values.end());
  split_values.erase(last_split, split_values.end());
  return split_values;
}

template <typename T>
std::pair<MaterializedSegmentList<T>, MaterializedSegmentList<T>> JoinSortMergeClusterer<T>::range_cluster(
    const MaterializedSegmentList<T>& input_left, const MaterializedSegmentList<T>& input_right,
    const std::vector<T> split_values, const size_t cluster_count, const std::pair<bool, bool> segments_are_presorted) {
  DebugAssert(std::is_sorted(split_values.begin(), split_values.end()), "Split values need to be sorted.");
  Assert(split_values.size() < cluster_count, "Number of split values must be smaller than number of clusters to generate.");

  // This functor returns the corresponding cluster for a given value. Since both the split values and values to
  // cluster are sorted, we can assign clusters in linear time. State within the functor is required. To ensure
  // multiple threads do not interfere with each other, each cluster job needs to have own instance of the functor.
  class RangeClusterFunctor {
   public:
    // split_values are copied to ensure data locality (sort-merge shines on large joins).
    explicit RangeClusterFunctor(const std::vector<T>& splits)
        : split_values(splits),
          current_excl_split_value_and_split_id(std::make_pair(split_values.front(), 0)),
          max_split_value(split_values.back()) {}

    explicit RangeClusterFunctor(const RangeClusterFunctor& functor)
        : split_values(functor.split_values),
          current_excl_split_value_and_split_id(functor.current_excl_split_value_and_split_id),
          max_split_value(functor.max_split_value) {}

    inline size_t operator()(const T& value) {
      // For the majority of cases, this early exit should be taken
      if (value < current_excl_split_value_and_split_id.first) {
        return current_excl_split_value_and_split_id.second;
      }

      // Early out when reached last cluster
      if (value >= max_split_value) {
        return split_values.size();
      }

      while (current_excl_split_value_and_split_id.first <= value) {
        const auto new_split_id = ++current_excl_split_value_and_split_id.second;
        current_excl_split_value_and_split_id = std::make_pair(split_values[new_split_id], new_split_id);
      }
      return current_excl_split_value_and_split_id.second;
    }

   private:
    const std::vector<T> split_values;
    std::pair<T, size_t> current_excl_split_value_and_split_id;
    const T max_split_value;
  };

  MaterializedSegmentList<T> output_left;
  MaterializedSegmentList<T> output_right;
  std::vector<std::shared_ptr<AbstractTask>> cluster_tasks;

  cluster_tasks.emplace_back(std::make_shared<JobTask>([&] {
    output_left = cluster(input_left, RangeClusterFunctor(split_values), cluster_count, true);
  }));
  cluster_tasks.back()->schedule();

  cluster_tasks.emplace_back(std::make_shared<JobTask>([&] {
    output_right = cluster(input_right, RangeClusterFunctor(split_values), cluster_count, true);
  }));
  cluster_tasks.back()->schedule();

  Hyrise::get().scheduler().wait_for_tasks(cluster_tasks);

  return {std::move(output_left), std::move(output_right)};
}

template <typename T>
MaterializedSegmentList<T> JoinSortMergeClusterer<T>::radix_cluster(const MaterializedSegmentList<T>& materialized_segments,
    const size_t cluster_count, const bool segments_are_presorted) {
  const auto radix_bitmask = cluster_count - 1;
  return cluster(materialized_segments, [=](const T& value) { return get_radix(value, radix_bitmask); }, cluster_count, segments_are_presorted);
}

template <typename T>
MaterializedSegmentList<T> JoinSortMergeClusterer<T>::cluster(const MaterializedSegmentList<T>& materialized_input_segments,
                                    const std::function<size_t(const T&)> clusterer, const size_t cluster_count,
                                    const bool segments_are_presorted) {
  DebugAssert(cluster_count > 1,
              "cluster() splits the input data into multiple clusters, thus the cluster count needs to be > 1.");
  auto output = MaterializedSegmentList<T>(cluster_count);
  TableInformation table_information(materialized_input_segments.size(), cluster_count);

  // Count for every input segment the number of entries for each cluster in parallel
  std::vector<std::shared_ptr<AbstractTask>> histogram_jobs;
  for (auto input_segment_id = size_t{0}; input_segment_id < materialized_input_segments.size(); ++input_segment_id) {
    // Count the number of entries for each cluster to be able to reserve the appropriate output space later.
    auto job = std::make_shared<JobTask>([&, input_segment_id, clusterer] {
      // clusterer is passed by value to have a job-local copy
      auto& segment_histogram_aggregate = table_information.segment_histogram_aggregates[input_segment_id];
      const auto input_segment = materialized_input_segments[input_segment_id];

      if (segments_are_presorted) {
	      DebugAssert(std::is_sorted(input_segment->begin(), input_segment->end(),
	                                 [](const auto& a, const auto& b) { return a.value < b.value; }),
	                  "Input segments need to be sorted.");
	    }

      if (cluster_count > 1) {
        for (const auto& entry : *input_segment) {
          const auto cluster_id = clusterer(entry.value);
          ++segment_histogram_aggregate.cluster_histogram[cluster_id];
        }
      }
    });

    histogram_jobs.emplace_back(job);
    job->schedule();
  }

  Hyrise::get().scheduler().wait_for_tasks(histogram_jobs);

  /**
   * In case more than one cluster will be created:
   *   -> Aggregate the segment histograms to get insert positions and preallocate each cluster accordingly
   * Otherwise:
   *   -> Size the output with the accumulated size of all segments
   **/
  auto accumulated_size = size_t{0};
  if (cluster_count > 1) {
    // Aggregate the segment histograms to a table histogram and initialize the insert positions for each segment
    for (auto& segment_histogram_aggregate : table_information.segment_histogram_aggregates) {
      for (auto cluster_id = size_t{0}; cluster_id < cluster_count; ++cluster_id) {
        segment_histogram_aggregate.insert_position[cluster_id] = table_information.cluster_histogram[cluster_id];
        table_information.cluster_histogram[cluster_id] += segment_histogram_aggregate.cluster_histogram[cluster_id];
      }
    }

    // Reserve the appropriate output space for the clusters
    for (auto cluster_id = size_t{0}; cluster_id < cluster_count; ++cluster_id) {
      const auto cluster_size = table_information.cluster_histogram[cluster_id];
      output[cluster_id] = std::make_shared<MaterializedSegment<T>>(cluster_size);
      accumulated_size += cluster_size;
    }
  } else {
    for (const auto& segment : materialized_input_segments) {
      accumulated_size += segment->size();
    }
    output[0] = std::make_shared<MaterializedSegment<T>>(accumulated_size);
  }

  // Move each entry into its appropriate cluster in parallel
  std::vector<std::shared_ptr<AbstractTask>> cluster_jobs;
  const auto input_segment_count = materialized_input_segments.size();
  for (auto input_segment_id = size_t{0}; input_segment_id < input_segment_count; ++input_segment_id) {
    auto job = std::make_shared<JobTask>([&, input_segment_id, clusterer] {  // copy cluster functor per task
      auto& segment_histogram_aggregate = table_information.segment_histogram_aggregates[input_segment_id];
      for (const auto& entry : *(materialized_input_segments[input_segment_id])) {
        const auto cluster_id = clusterer(entry.value);
        auto& output_cluster = *(output[cluster_id]);
        auto& insert_position = segment_histogram_aggregate.insert_position[cluster_id];
        output_cluster[insert_position] = entry;
        ++insert_position;
      }
    });
    cluster_jobs.emplace_back(job);
    job->schedule();
  }

  Hyrise::get().scheduler().wait_for_tasks(cluster_jobs);

  std::vector<std::shared_ptr<AbstractTask>> sort_jobs;
  auto segment_id = size_t{0};
  for (const auto& segment : output) {
    auto job = std::make_shared<JobTask>([&, segment, segment_id] {
      auto sorted_run_start_positions = std::make_unique<std::vector<size_t>>();
      for (auto chunk_id = ChunkID{0}; chunk_id < materialized_input_segments.size(); ++chunk_id) {
        const auto& segment_histogram_aggregate = table_information.segment_histogram_aggregates[chunk_id];
        sorted_run_start_positions->push_back(segment_histogram_aggregate.insert_position[segment_id] -
                                             segment_histogram_aggregate.cluster_histogram[segment_id]);
      }

      // Sorting of final output cluster. If materialized segments have been presorted, output is partially sorted and
      // the sorted lists can be merged. In this case, estimate costs for partial and full sorting and chose more
      // efficient method.
      assert(materialized_input_segments.size() == sorted_run_start_positions->size());
      if (segments_are_presorted && is_partial_sorting_beneficial(accumulated_size, materialized_input_segments.size(), materialized_input_segments.size())) {
        merge_partially_sorted_materialized_segment(*segment, std::move(sorted_run_start_positions));
      } else {
        std::sort(segment->begin(), segment->end(),
                  [](auto& left, auto& right) { return left.value < right.value; });
      }

      DebugAssert(std::is_sorted(segment->begin(), segment->end(),
                                   [](auto& left, auto& right) { return left.value < right.value; }),
                    "Resulting clusters are expected to be sorted.");
    });
    sort_jobs.emplace_back(job);
    job->schedule();

    ++segment_id;
  }

  Hyrise::get().scheduler().wait_for_tasks(sort_jobs);

  return output;
}

template <typename T>
size_t JoinSortMergeClusterer<T>::_materialized_table_size(const MaterializedSegmentList<T>& materialized_segments) {
  size_t total_size = 0;
  for (const auto& materialized_segment : materialized_segments) {
    total_size += materialized_segment->size();
  }

  return total_size;
}

template <typename T>
typename JoinSortMergeClusterer<T>::ClusterOutput JoinSortMergeClusterer<T>::execute() {
  JoinSortMergeClusterer<T>::ClusterOutput output;

  /** First, chunks are fully materialized and sorted. The clusters are sorted for two reasons:
   *  (i)  Writes to clusters are usually sped up (very much for range clustering,
   *       for radix partitioning when values occur multiple times)
   *  (ii) We can efficiently sort the resulting output clusters efficiently with in place
   *       merging as each chunk's values (sorted) are written in sequential order, hence the
   *       cluster is partially sorted.
   *  For equality join, we neither need the columns sorted nor samples being gathered.
   */
  ColumnMaterializer<T> left_column_materializer(_presort_segments.first, _materialize_null_left, !_radix_clustering);
  ColumnMaterializer<T> right_column_materializer(_presort_segments.second, _materialize_null_right, !_radix_clustering);

  auto [materialized_left_segments, null_rows_left, samples_left] =
      left_column_materializer.materialize(_input_table_left, _left_column_id);
  auto [materialized_right_segments, null_rows_right, samples_right] =
      right_column_materializer.materialize(_input_table_right, _right_column_id);
  output.null_rows_left = std::move(null_rows_left);
  output.null_rows_right = std::move(null_rows_right);

  // Append right samples to left samples and sort (reserve not necessarity when insert can
  // determined the new capacity from iterator: https://stackoverflow.com/a/35359472/1147726)
  samples_left.insert(samples_left.end(), samples_right.begin(), samples_right.end());

  std::cout << "Using a cluster count of " << _cluster_count << std::endl;
  if (_cluster_count == 1) {
    output.clusters_left = std::move(concatenate_materialized_segments(materialized_left_segments, _presort_segments.first));
    output.clusters_right = std::move(concatenate_materialized_segments(materialized_right_segments, _presort_segments.second));
  } else if (_radix_clustering) {
    output.clusters_left = radix_cluster(materialized_left_segments, _cluster_count, _presort_segments.first);
    output.clusters_right = radix_cluster(materialized_right_segments, _cluster_count, _presort_segments.second);
  } else {
    const std::vector<T> split_values = pick_split_values(samples_left, _cluster_count);
    auto result = range_cluster(materialized_left_segments, materialized_right_segments, split_values, _cluster_count, _presort_segments);
    output.clusters_left = std::move(result.first);
    output.clusters_right = std::move(result.second);
  }

  return output;
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(JoinSortMergeClusterer);

}  // namespace opossum
