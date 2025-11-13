#include "equal_distinct_count_histogram.hpp"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <numeric>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include <boost/sort/pdqsort/pdqsort.hpp>
#include <boost/unordered/unordered_flat_map.hpp>

#include "all_type_variant.hpp"
#include "hyrise.hpp"
#include "resolve_type.hpp"
#include "scheduler/job_task.hpp"
#include "statistics/statistics_objects/abstract_histogram.hpp"
#include "statistics/statistics_objects/histogram_domain.hpp"
#include "storage/abstract_segment.hpp"
#include "storage/segment_iterate.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace {

using namespace hyrise;  // NOLINT(build/namespaces)

// Think of this as an unordered_map<T, HistogramCountType>. The hash, equals, and allocator template parameter are
// defaults so that we can set the last parameter. It controls whether the hash for a value should be cached. Doing
// so reduces the cost of rehashing at the cost of slightly higher memory consumption. We only do it for strings,
// where hashing is somewhat expensive.
template <typename T>
using ValueDistributionMap = boost::unordered_flat_map<T, size_t>;

template <typename T>
using ValueDistributionVector = std::vector<std::pair<T, size_t>>;

// constexpr auto MIN_CHUNK_COUNT_TO_INCLUDE = ChunkID{10'000};
constexpr auto MIN_CHUNK_COUNT_TO_INCLUDE = ChunkID{10'000'000};  // For testing.

template <typename T>
void process_segment(AbstractSegment& segment, ValueDistributionVector<T>& value_distribution_vector,
                     const HistogramDomain<T>& domain) {
  auto value_distribution_map = ValueDistributionMap<T>{};
  value_distribution_map.reserve(segment.size());

  segment_iterate<T>(segment, [&](const auto& iterator_value) {
    if (iterator_value.is_null()) {
      return;
    }

    if constexpr (std::is_same_v<T, pmr_string>) {
      const auto current_string = domain.string_to_domain(iterator_value.value());
      ++value_distribution_map[current_string];
    } else {
      ++value_distribution_map[iterator_value.value()];
    }
  });

  value_distribution_vector.insert(value_distribution_vector.begin(), value_distribution_map.cbegin(),
                                   value_distribution_map.cend());
}

template <typename T>
ValueDistributionVector<T> add_segment_to_value_distribution(const size_t max_parallel_level, const size_t level,
                                                             auto segment_iterator_begin, auto segment_iterator_end,
                                                             const HistogramDomain<T>& domain) {
  if (std::distance(segment_iterator_begin, segment_iterator_end) == 1) {
    // Final recursive step.
    const auto [chunk_id, segment] = *segment_iterator_begin;

    auto value_distribution_vector = ValueDistributionVector<T>{};

    // From a performance perspective, `process_segment` should be specialized for strings as they are expensive to
    // handle and we already use some form of aggregation (distinct sorted dictionaries) to store string columns.
    process_segment<T>(*segment, value_distribution_vector, domain);

    boost::sort::pdqsort(value_distribution_vector.begin(), value_distribution_vector.end(),
                         [&](const auto& lhs, const auto& rhs) {
                           return lhs.first < rhs.first;
                         });

    return value_distribution_vector;
  }

  auto left = ValueDistributionVector<T>{};
  auto right = ValueDistributionVector<T>{};

  auto middle = segment_iterator_begin + (std::distance(segment_iterator_begin, segment_iterator_end) / 2);

  auto left_task = [&]() {
    left = add_segment_to_value_distribution(max_parallel_level, level + 1, segment_iterator_begin, middle, domain);
  };
  auto right_task = [&]() {
    right = add_segment_to_value_distribution(max_parallel_level, level + 1, middle, segment_iterator_end, domain);
  };

  if (level < max_parallel_level) {
    auto tasks = std::vector<std::shared_ptr<AbstractTask>>{std::make_shared<JobTask>(left_task),
                                                            std::make_shared<JobTask>(right_task)};
    Hyrise::get().scheduler()->schedule_and_wait_for_tasks(tasks);
  } else {
    left_task();
    right_task();
  }

  auto result = ValueDistributionVector<T>{};
  result.reserve(left.size() + right.size());

  auto left_iter = left.begin();
  auto right_iter = right.begin();

  while (left_iter != left.end() || right_iter != right.end()) {
    if (left_iter == left.end()) {
      result.emplace_back(*right_iter);
      ++right_iter;
      continue;
    } else if (right_iter == right.end()) {
      result.emplace_back(*left_iter);
      ++left_iter;
      continue;
    }

    if (left_iter->first == right_iter->first) {
      result.emplace_back(left_iter->first, left_iter->second + right_iter->second);
      ++left_iter;
      ++right_iter;
    } else if (left_iter->first < right_iter->first) {
      result.emplace_back(*left_iter);
      ++left_iter;
    } else {
      result.emplace_back(*right_iter);
      ++right_iter;
    }
  }

  DebugAssert(std::is_sorted(result.begin(), result.end()), "Result should be sorted.");
  return result;
}

template <typename T>
ValueDistributionVector<T> value_distribution_from_column(const Table& table, const ColumnID column_id,
                                                          const HistogramDomain<T>& domain) {
  const auto chunk_count = table.chunk_count();
  auto segments_to_process = std::vector<std::pair<ChunkID, std::shared_ptr<AbstractSegment>>>{};
  segments_to_process.reserve(std::min(chunk_count, MIN_CHUNK_COUNT_TO_INCLUDE));

  // On average, we sample every 2.5th segment. Previous analyses of Hyrise histograms showed that the accuracy of
  // sample histograms quickly deteriorates with sampling rates below 33 %. We thus choose a safer rate of ~40%.
  auto random_engine = std::ranlux24_base{17};  // Fast random engine. Sufficient for our case.
  auto skip_lengths = std::uniform_int_distribution<>{0, 4};

  auto current_chunk_id = ChunkID{0};
  while (current_chunk_id < chunk_count) {
    const auto chunk = table.get_chunk(current_chunk_id);
    if (!chunk) {
      continue;
    }

    segments_to_process.emplace_back(current_chunk_id, chunk->get_segment(column_id));

    if (current_chunk_id < MIN_CHUNK_COUNT_TO_INCLUDE / 2 ||
        current_chunk_id >= (chunk_count - (MIN_CHUNK_COUNT_TO_INCLUDE / 2))) {
      ++current_chunk_id;
      continue;
    }

    current_chunk_id += (skip_lengths(random_engine) % 4) + 1;
  }

  auto result = ValueDistributionVector<T>{};
  if (chunk_count > 0) {
    // We determine the recursion steps (i.e., merge levels) that we want to parallelize. We try to create up to 2x the
    // number of workers to fully utilize a system with a bit of straggler mitigation (thus 2x) while not overloading the
    // scheduler. As the leaves of the created merge tree are single chunks, we would otherwise create tens of
    // thousands of nested jobs for SF 100 TPC-H data. When the limit is reached, each worker executes the recursion on
    // its own sequentially.
    auto worker_count = size_t{1};
    if (const auto node_queue_scheduler = std::dynamic_pointer_cast<NodeQueueScheduler>(Hyrise::get().scheduler())) {
      worker_count = node_queue_scheduler.workers().size();
    }
    const auto max_parallel_levels = std::bit_width(worker_count) + 1;
    result = add_segment_to_value_distribution<T>(max_parallel_levels, 0, segments_to_process.begin(),
                                                  segments_to_process.end(), domain);
  }

  return std::move(result);
}

}  // namespace

namespace hyrise {

template <typename T>
EqualDistinctCountHistogram<T>::EqualDistinctCountHistogram(std::vector<T>&& bin_minima, std::vector<T>&& bin_maxima,
                                                            std::vector<HistogramCountType>&& bin_heights,
                                                            const HistogramCountType distinct_count_per_bin,
                                                            const BinID bin_count_with_extra_value,
                                                            const HistogramDomain<T>& domain)
    : AbstractHistogram<T>(domain),
      _bin_minima(std::move(bin_minima)),
      _bin_maxima(std::move(bin_maxima)),
      _bin_heights(std::move(bin_heights)),
      _distinct_count_per_bin(distinct_count_per_bin),
      _bin_count_with_extra_value(bin_count_with_extra_value) {
  Assert(_bin_minima.size() == _bin_maxima.size(), "Must have the same number of lower as upper bin edges.");
  Assert(_bin_minima.size() == _bin_heights.size(), "Must have the same number of edges and heights.");
  Assert(_distinct_count_per_bin > 0, "Cannot have bins with no distinct values.");
  Assert(_bin_count_with_extra_value < _bin_minima.size(), "Cannot have more bins with extra value than bins.");

  AbstractHistogram<T>::_assert_bin_validity();

  _total_count = std::accumulate(_bin_heights.cbegin(), _bin_heights.cend(), HistogramCountType{0});
  _total_distinct_count = _distinct_count_per_bin * static_cast<HistogramCountType>(_bin_heights.size()) +
                          static_cast<HistogramCountType>(_bin_count_with_extra_value);
}

template <typename T>
std::shared_ptr<EqualDistinctCountHistogram<T>> EqualDistinctCountHistogram<T>::from_column(
    const Table& table, const ColumnID column_id, const BinID max_bin_count, const HistogramDomain<T>& domain) {
  Assert(max_bin_count > 0, "max_bin_count must be greater than zero ");

  const auto value_distribution = value_distribution_from_column(table, column_id, domain);

  if (value_distribution.empty()) {
    return nullptr;
  }

  // If there are fewer distinct values than the number of desired bins use that instead.
  const auto bin_count =
      value_distribution.size() < max_bin_count ? static_cast<BinID>(value_distribution.size()) : max_bin_count;

  // Split values evenly among bins.
  const auto distinct_count_per_bin = static_cast<size_t>(value_distribution.size() / bin_count);
  const BinID bin_count_with_extra_value = value_distribution.size() % bin_count;

  auto bin_minima = std::vector<T>(bin_count);
  auto bin_maxima = std::vector<T>(bin_count);
  auto bin_heights = std::vector<HistogramCountType>(bin_count);

  // `min_value_idx` and `max_value_idx` are indices into the sorted vector `value_distribution` describing which range
  // of distinct values goes into a bin.
  auto min_value_idx = BinID{0};
  for (BinID bin_idx = 0; bin_idx < bin_count; bin_idx++) {
    auto max_value_idx = min_value_idx + distinct_count_per_bin - 1;
    if (bin_idx < bin_count_with_extra_value) {
      max_value_idx++;
    }

    // We'd like to move strings, but have to copy if we need the same string for the bin_maximum
    if (std::is_same_v<T, std::string> && min_value_idx != max_value_idx) {
      bin_minima[bin_idx] = std::move(value_distribution[min_value_idx].first);
    } else {
      bin_minima[bin_idx] = value_distribution[min_value_idx].first;
    }

    bin_maxima[bin_idx] = std::move(value_distribution[max_value_idx].first);

    bin_heights[bin_idx] =
        std::accumulate(value_distribution.cbegin() + min_value_idx, value_distribution.cbegin() + max_value_idx + 1,
                        HistogramCountType{0}, [](HistogramCountType bin_height, const auto& value_and_count) {
                          return bin_height + static_cast<HistogramCountType>(value_and_count.second);
                        });

    min_value_idx = max_value_idx + 1;
  }

  return std::make_shared<EqualDistinctCountHistogram<T>>(
      std::move(bin_minima), std::move(bin_maxima), std::move(bin_heights),
      static_cast<HistogramCountType>(distinct_count_per_bin), bin_count_with_extra_value);
}

template <typename T>
uint16_t EqualDistinctCountHistogram<T>::determine_bin_count(size_t table_row_count) {
  // Determine bin count, within mostly arbitrarily chosen bounds: 16 (for tables with <= 16 000 rows) up to 128 bins
  // (for tables with >= 256 000 rows) are created. If the table does not have a high number of distinct values, the
  // EqualDistinctCountHistogram automatically uses fewer bins.
  return static_cast<uint16_t>(std::min<size_t>(128, std::max<size_t>(16, table_row_count / 2'000)));
}

template <typename T>
std::string EqualDistinctCountHistogram<T>::name() const {
  return "EqualDistinctCount";
}

template <typename T>
std::shared_ptr<AbstractHistogram<T>> EqualDistinctCountHistogram<T>::clone() const {
  // The new histogram needs a copy of the data
  auto bin_minima_copy = _bin_minima;
  auto bin_maxima_copy = _bin_maxima;
  auto bin_heights_copy = _bin_heights;

  return std::make_shared<EqualDistinctCountHistogram<T>>(std::move(bin_minima_copy), std::move(bin_maxima_copy),
                                                          std::move(bin_heights_copy), _distinct_count_per_bin,
                                                          _bin_count_with_extra_value);
}

template <typename T>
BinID EqualDistinctCountHistogram<T>::bin_count() const {
  return _bin_heights.size();
}

template <typename T>
BinID EqualDistinctCountHistogram<T>::bin_for_value(const T& value) const {
  const auto iter = std::lower_bound(_bin_maxima.cbegin(), _bin_maxima.cend(), value);
  const auto index = static_cast<BinID>(std::distance(_bin_maxima.cbegin(), iter));

  if (iter == _bin_maxima.cend() || value < bin_minimum(index) || value > bin_maximum(index)) {
    return INVALID_BIN_ID;
  }

  return index;
}

template <typename T>
BinID EqualDistinctCountHistogram<T>::next_bin_for_value(const T& value) const {
  const auto it = std::upper_bound(_bin_maxima.cbegin(), _bin_maxima.cend(), value);

  if (it == _bin_maxima.cend()) {
    return INVALID_BIN_ID;
  }

  return static_cast<BinID>(std::distance(_bin_maxima.cbegin(), it));
}

template <typename T>
const T& EqualDistinctCountHistogram<T>::bin_minimum(const BinID index) const {
  DebugAssert(index < _bin_minima.size(), "Index is not a valid bin.");
  return _bin_minima[index];
}

template <typename T>
const T& EqualDistinctCountHistogram<T>::bin_maximum(const BinID index) const {
  DebugAssert(index < _bin_maxima.size(), "Index is not a valid bin.");
  return _bin_maxima[index];
}

template <typename T>
HistogramCountType EqualDistinctCountHistogram<T>::bin_height(const BinID index) const {
  DebugAssert(index < _bin_heights.size(), "Index is not a valid bin.");
  return _bin_heights[index];
}

template <typename T>
HistogramCountType EqualDistinctCountHistogram<T>::bin_distinct_count(const BinID index) const {
  DebugAssert(index < bin_count(), "Index is not a valid bin.");
  return HistogramCountType{_distinct_count_per_bin + (index < _bin_count_with_extra_value ? 1 : 0)};
}

template <typename T>
HistogramCountType EqualDistinctCountHistogram<T>::total_count() const {
  return _total_count;
}

template <typename T>
HistogramCountType EqualDistinctCountHistogram<T>::total_distinct_count() const {
  return _total_distinct_count;
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(EqualDistinctCountHistogram);

}  // namespace hyrise
