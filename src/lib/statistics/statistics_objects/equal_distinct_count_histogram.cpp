#include "equal_distinct_count_histogram.hpp"

#include <cmath>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include <tsl/robin_map.h>  // NOLINT

#include "generic_histogram.hpp"
#include "resolve_type.hpp"
#include "storage/segment_iterate.hpp"

namespace {

using namespace opossum;  // NOLINT

// Think of this as an unordered_map<T, HistogramCountType>. The hash, equals, and allocator template parameter are
// defaults so that we can set the last parameter. It controls whether the hash for a value should be cached. Doing
// so reduces the cost of rehashing at the cost of slightly higher memory consumption. We only do it for strings,
// where hashing is somewhat expensive.
template <typename T>
using ValueDistributionMap =
    tsl::robin_map<T, HistogramCountType, std::hash<T>, std::equal_to<T>,
                   std::allocator<std::pair<T, HistogramCountType>>, std::is_same_v<std::decay_t<T>, pmr_string>>;

template <typename T>
void add_segment_to_value_distribution(const AbstractSegment& segment, ValueDistributionMap<T>& value_distribution,
                                       const HistogramDomain<T>& domain) {
  segment_iterate<T>(segment, [&](const auto& iterator_value) {
    if (iterator_value.is_null()) return;

    if constexpr (std::is_same_v<T, pmr_string>) {
      // Do "contains()" check first to avoid the string copy incurred by string_to_domain() where possible
      if (domain.contains(iterator_value.value())) {
        ++value_distribution[iterator_value.value()];
      } else {
        ++value_distribution[domain.string_to_domain(iterator_value.value())];
      }
    } else {
      ++value_distribution[iterator_value.value()];
    }
  });
}

template <typename T>
std::vector<std::pair<T, HistogramCountType>> value_distribution_from_column(const Table& table,
                                                                             const ColumnID column_id,
                                                                             const HistogramDomain<T>& domain) {
  // TODO(anybody) If you want to look into performance, this would probably benefit greatly from monotonic buffer
  //               resources.
  ValueDistributionMap<T> value_distribution_map;

  const auto chunk_count = table.chunk_count();
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto chunk = table.get_chunk(chunk_id);
    if (!chunk) continue;
    add_segment_to_value_distribution<T>(*chunk->get_segment(column_id), value_distribution_map, domain);
  }

  auto value_distribution =
      std::vector<std::pair<T, HistogramCountType>>{value_distribution_map.begin(), value_distribution_map.end()};
  std::sort(value_distribution.begin(), value_distribution.end(),
            [&](const auto& l, const auto& r) { return l.first < r.first; });

  return value_distribution;
}

}  // namespace

namespace opossum {

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
  _total_distinct_count = _distinct_count_per_bin * static_cast<HistogramCountType>(bin_count()) +
                          static_cast<HistogramCountType>(_bin_count_with_extra_value);
}

template <typename T>
std::shared_ptr<EqualDistinctCountHistogram<T>> EqualDistinctCountHistogram<T>::from_column(
    const Table& table, const ColumnID column_id, const BinID max_bin_count, const HistogramDomain<T>& domain) {
  Assert(max_bin_count > 0, "max_bin_count must be greater than zero");

  const auto value_distribution = value_distribution_from_column(table, column_id, domain);

  return _from_value_distribution(value_distribution, max_bin_count);
}

template <typename T>
std::shared_ptr<EqualDistinctCountHistogram<T>> EqualDistinctCountHistogram<T>::from_segment(
    const Table& table, const ColumnID column_id, const ChunkID chunk_id, const BinID max_bin_count,
    const HistogramDomain<T>& domain) {
  Assert(max_bin_count > 0, "max_bin_count must be greater than zero");
  ValueDistributionMap<T> value_distribution_map;
  const auto chunk = table.get_chunk(chunk_id);
  if (!chunk) {
    return nullptr;
  }
  add_segment_to_value_distribution<T>(*chunk->get_segment(column_id), value_distribution_map, domain);
  auto value_distribution =
      std::vector<std::pair<T, HistogramCountType>>{value_distribution_map.begin(), value_distribution_map.end()};
  std::sort(value_distribution.begin(), value_distribution.end(),
            [&](const auto& lhs, const auto& rhs) { return lhs.first < rhs.first; });
  return _from_value_distribution(value_distribution, max_bin_count);
}

template <typename T>
std::shared_ptr<EqualDistinctCountHistogram<T>> EqualDistinctCountHistogram<T>::_from_value_distribution(
    const std::vector<std::pair<T, HistogramCountType>>& value_distribution, const BinID max_bin_count) {
  if (value_distribution.empty()) {
    return nullptr;
  }

  // If there are fewer distinct values than the number of desired bins use that instead.
  const auto bin_count =
      value_distribution.size() < max_bin_count ? static_cast<BinID>(value_distribution.size()) : max_bin_count;

  // Split values evenly among bins.
  const auto distinct_count_per_bin = static_cast<size_t>(value_distribution.size() / bin_count);
  const BinID bin_count_with_extra_value = value_distribution.size() % bin_count;

  std::vector<T> bin_minima(bin_count);
  std::vector<T> bin_maxima(bin_count);
  std::vector<HistogramCountType> bin_heights(bin_count);

  // `min_value_idx` and `max_value_idx` are indices into the sorted vector `value_distribution`
  // describing which range of distinct values goes into a bin
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
                        HistogramCountType{0},
                        [](HistogramCountType a, const std::pair<T, HistogramCountType>& b) { return a + b.second; });

    min_value_idx = max_value_idx + 1;
  }

  return std::make_shared<EqualDistinctCountHistogram<T>>(
      std::move(bin_minima), std::move(bin_maxima), std::move(bin_heights),
      static_cast<HistogramCountType>(distinct_count_per_bin), bin_count_with_extra_value);
}

/**
 * Merging string histograms has the difficulty that strings cannot be interpolated or incremented/decremented
 * what is required for the current approach.
 * Some other existing methods such as split_at_bin_bounds also do not work with strings for this reason.
 * Therefore, string histograms are unmergeable.
 */
template <>
std::pair<std::vector<pmr_string>, std::vector<pmr_string>> EqualDistinctCountHistogram<pmr_string>::_combine_bounds(
    const std::vector<std::shared_ptr<EqualDistinctCountHistogram<pmr_string>>>& histograms) {
  throw std::invalid_argument("Cannot split string histograms.");
}

template <typename T>
std::pair<std::vector<T>, std::vector<T>> EqualDistinctCountHistogram<T>::_combine_bounds(
    const std::vector<std::shared_ptr<EqualDistinctCountHistogram<T>>>& histograms) {
  auto global_min = histograms[0]->bin_minimum(0);
  auto global_max = histograms[0]->bin_maximum(histograms[0]->bin_count() - 1);

  for (const auto& hist : histograms) {
    global_min = std::min(global_min, hist->bin_minimum(0));
    global_max = std::max(global_max, hist->bin_maximum(hist->bin_count() - 1));
  }

  /**
   * We create a helper histogram which is split with the bin bounds of all histograms.
   * We later use the fine grained intervals we get from this to combine the distinct_counts and heights.
   */
  std::shared_ptr<AbstractHistogram<T>> split_helper_histogram =
      std::make_shared<EqualDistinctCountHistogram<T>>(std::vector<T>{global_min}, std::vector<T>{global_max},
                                                       std::vector<HistogramCountType>{1}, HistogramCountType{1}, 0);

  for (const auto& hist : histograms) {
    split_helper_histogram = split_helper_histogram->split_at_bin_bounds(hist->bin_bounds(), true);
  }

  const auto split_interval_bounds = split_helper_histogram->bin_bounds();
  const auto split_interval_count = split_interval_bounds.size();
  auto split_interval_minima = std::vector<T>(split_interval_bounds.size());
  auto split_interval_maxima = std::vector<T>(split_interval_bounds.size());

  for (auto interval_index = 0u; interval_index < split_interval_count; interval_index++) {
    split_interval_minima[interval_index] = split_interval_bounds[interval_index].first;
    split_interval_maxima[interval_index] = split_interval_bounds[interval_index].second;
  }

  return std::make_pair(std::move(split_interval_minima), std::move(split_interval_maxima));
}

/**
 * Merging string histograms has the difficulty that strings cannot be interpolated or incremented/decremented
 * what is required for the current approach.
 * Some other existing methods such as split_at_bin_bounds also do not work with strings for this reason.
 * Therefore, string histograms are unmergeable.
 */
template <>
std::tuple<pmr_string, pmr_string, HistogramCountType> EqualDistinctCountHistogram<pmr_string>::_create_one_bin(
    typename std::vector<pmr_string>::iterator& interval_minima_begin,
    typename std::vector<pmr_string>::iterator& interval_minima_end,
    typename std::vector<pmr_string>::iterator& interval_maxima_begin,
    typename std::vector<pmr_string>::iterator& interval_maxima_end,
    typename std::vector<HistogramCountType>::iterator& interval_heights_begin,
    typename std::vector<HistogramCountType>::iterator& interval_heights_end,
    typename std::vector<HistogramCountType>::iterator& interval_distinct_counts_begin,
    typename std::vector<HistogramCountType>::iterator& interval_distinct_counts_end, const int distinct_count_target,
    const HistogramDomain<pmr_string> domain, const bool is_last_bin) {
  throw std::invalid_argument("Cannot create bin for string histograms.");
}

template <typename T>
std::tuple<T, T, HistogramCountType> EqualDistinctCountHistogram<T>::_create_one_bin(
    typename std::vector<T>::iterator& interval_minima_begin, typename std::vector<T>::iterator& interval_minima_end,
    typename std::vector<T>::iterator& interval_maxima_begin, typename std::vector<T>::iterator& interval_maxima_end,
    typename std::vector<HistogramCountType>::iterator& interval_heights_begin,
    typename std::vector<HistogramCountType>::iterator& interval_heights_end,
    typename std::vector<HistogramCountType>::iterator& interval_distinct_counts_begin,
    typename std::vector<HistogramCountType>::iterator& interval_distinct_counts_end, const int distinct_count_target,
    const HistogramDomain<T> domain, const bool is_last_bin) {
  const auto remaining_interval_count = std::distance(interval_minima_begin, interval_minima_end);
  DebugAssert(remaining_interval_count == std::distance(interval_maxima_begin, interval_maxima_end),
              "All interval vectors need to have the same size.");
  DebugAssert(remaining_interval_count == std::distance(interval_heights_begin, interval_heights_end),
              "All interval vectors need to have the same size.");
  DebugAssert(remaining_interval_count == std::distance(interval_distinct_counts_begin, interval_distinct_counts_end),
              "All interval vectors need to have the same size.");
  DebugAssert(remaining_interval_count > 0, "remaining_interval_count has to be greater than zero.");

  const auto bin_start = *interval_minima_begin;
  auto bin_end = bin_start;
  auto bin_height = HistogramCountType{0};
  auto bin_distinct_count = HistogramCountType{0};

  for (auto interval_index = 0u; interval_index < remaining_interval_count; interval_index++,
            std::advance(interval_minima_begin, 1), std::advance(interval_maxima_begin, 1),
            std::advance(interval_heights_begin, 1), std::advance(interval_distinct_counts_begin, 1)) {
    const auto interval_height = *interval_heights_begin;
    const auto interval_distinct_count = *interval_distinct_counts_begin;
    const auto interval_start = *interval_minima_begin;
    const auto interval_end = *interval_maxima_begin;

    const auto remaining_distinct_count_to_fill_bin =
        static_cast<HistogramCountType>(distinct_count_target) - bin_distinct_count;
    const auto is_splittable_interval = (interval_start != interval_end);

    // The last bin gets all remaining values, to offset small floating point errors.
    if ((remaining_distinct_count_to_fill_bin > interval_distinct_count) || is_last_bin) {
      // The current interval fits into the current bin and there will be some space left (or it is the last bin).
      bin_end = interval_end;
      bin_height += interval_height;
      bin_distinct_count += interval_distinct_count;
    } else if ((remaining_distinct_count_to_fill_bin == interval_distinct_count) || !is_splittable_interval) {
      // The current interval fits into the current bin but afterwards the bin will be full.
      // Also used when the interval is too large, but cannot be splitted (an interval from 1 to 1).
      bin_end = interval_end;
      bin_height += interval_height;
      bin_distinct_count += interval_distinct_count;

      // Increment the passed iterators so that the next call starts with the next interval.
      std::advance(interval_minima_begin, 1);
      std::advance(interval_maxima_begin, 1);
      std::advance(interval_heights_begin, 1);
      std::advance(interval_distinct_counts_begin, 1);

      // As the bin is now full, we don't want to add more intervals.
      break;
    } else {
      // Only a fraction of the interval fits into the new bin. We need to split it.
      const auto interval_fraction_for_bin = remaining_distinct_count_to_fill_bin / interval_distinct_count;

      bin_end =
          static_cast<T>(static_cast<HistogramCountType>(interval_start) +
                         static_cast<HistogramCountType>(interval_end - interval_start) * interval_fraction_for_bin);
      bin_height += interval_height * interval_fraction_for_bin;
      bin_distinct_count += interval_distinct_count * interval_fraction_for_bin;

      const auto new_interval_start = domain.next_value_clamped(bin_end);
      const auto new_interval_height = interval_height - interval_height * interval_fraction_for_bin;
      const auto new_interval_distinct_count =
          interval_distinct_count - interval_distinct_count * interval_fraction_for_bin;

      *interval_minima_begin = new_interval_start;
      *interval_heights_begin = new_interval_height;
      *interval_distinct_counts_begin = new_interval_distinct_count;

      // The bin is full, so do not add more intervals.
      break;
    }
  }

  return std::make_tuple(bin_start, bin_end, bin_height);
}

/**
 * Merging string histograms has the difficulty that strings cannot be interpolated or incremented/decremented
 * what is required for the current approach.
 * Some other existing methods such as split_at_bin_bounds also do not work with strings for this reason.
 * Therefore, string histograms are unmergeable.
 */
template <>
std::shared_ptr<EqualDistinctCountHistogram<pmr_string>>
EqualDistinctCountHistogram<pmr_string>::_balance_bins_into_histogram(
    std::vector<HistogramCountType>& interval_distinct_counts, std::vector<HistogramCountType>& interval_heights,
    std::vector<pmr_string>& interval_minima, std::vector<pmr_string>& interval_maxima,
    const HistogramCountType total_distinct_count, const BinID max_bin_count,
    const HistogramDomain<pmr_string> domain) {
  throw std::invalid_argument("Cannot balance string histograms.");
}

template <typename T>
std::shared_ptr<EqualDistinctCountHistogram<T>> EqualDistinctCountHistogram<T>::_balance_bins_into_histogram(
    std::vector<HistogramCountType>& interval_distinct_counts, std::vector<HistogramCountType>& interval_heights,
    std::vector<T>& interval_minima, std::vector<T>& interval_maxima, const HistogramCountType total_distinct_count,
    const BinID max_bin_count, const HistogramDomain<T> domain) {
  /**
   * When the total_distinct_count cannot be rounded properly or the elements cannot be distributed equally,
   * we allow the last bin to deviate in distinct_count, even though this is not typical for
   * EqualDistinctCountHistograms (but this cannot be avoided). 
   */
  const auto bin_count = std::min(static_cast<BinID>(std::round(total_distinct_count)), max_bin_count);
  const auto distinct_count_per_bin_target = static_cast<int>(std::round(total_distinct_count)) / bin_count;

  // Number of bins that will get an extra distinct value to avoid smaller last bin
  const auto bin_count_with_extra_value =
      BinID{static_cast<int>(std::round(total_distinct_count)) - distinct_count_per_bin_target * bin_count};

  auto merged_histogram_bin_heights = std::vector<HistogramCountType>();
  auto merged_histogram_bin_minima = std::vector<T>();
  auto merged_histogram_bin_maxima = std::vector<T>();
  merged_histogram_bin_heights.reserve(bin_count);
  merged_histogram_bin_minima.reserve(bin_count);
  merged_histogram_bin_maxima.reserve(bin_count);

  /**
   * These iterators are passed into create_one_bin iteratively; after each call they point to the next
   * unprocessed interval. This is more efficient than modifying the interval containers in each call.
   */
  auto interval_minima_begin = interval_minima.begin();
  auto interval_minima_end = interval_minima.end();
  auto interval_maxima_begin = interval_maxima.begin();
  auto interval_maxima_end = interval_maxima.end();
  auto interval_heights_begin = interval_heights.begin();
  auto interval_heights_end = interval_heights.end();
  auto interval_distinct_counts_begin = interval_distinct_counts.begin();
  auto interval_distinct_counts_end = interval_distinct_counts.end();

  for (auto interval_index = 0u; interval_index < bin_count; interval_index++) {
    auto bin_distinct_count_target = distinct_count_per_bin_target;

    // EqualDistinctCountHistograms want the first bins to be larger, rather than the last one be smaller.
    if (interval_index < bin_count_with_extra_value) {
      bin_distinct_count_target++;
    }

    const auto is_last_bin = (bin_count - 1) == interval_index;

    const auto [bin_start, bin_end, bin_height] =
        _create_one_bin(interval_minima_begin, interval_minima_end, interval_maxima_begin, interval_maxima_end,
                        interval_heights_begin, interval_heights_end, interval_distinct_counts_begin,
                        interval_distinct_counts_end, bin_distinct_count_target, domain, is_last_bin);

    merged_histogram_bin_minima.push_back(bin_start);
    merged_histogram_bin_maxima.push_back(bin_end);
    merged_histogram_bin_heights.push_back(bin_height);
  }

  auto merged_histogram = std::make_shared<EqualDistinctCountHistogram<T>>(
      std::move(merged_histogram_bin_minima), std::move(merged_histogram_bin_maxima),
      std::move(merged_histogram_bin_heights), static_cast<HistogramCountType>(distinct_count_per_bin_target),
      bin_count_with_extra_value, domain);

  return merged_histogram;
}

/**
 * Merging string histograms has the difficulty that strings cannot be interpolated or incremented/decremented
 * what is required for the current approach.
 * Some other existing methods such as split_at_bin_bounds also do not work with strings for this reason.
 * Therefore, string histograms are unmergeable.
 */
template <>
std::tuple<std::vector<HistogramCountType>, std::vector<HistogramCountType>, std::vector<pmr_string>,
           std::vector<pmr_string>, HistogramCountType>
EqualDistinctCountHistogram<pmr_string>::_create_merged_intervals(
    const std::vector<pmr_string>& splitted_bounds_minima, const std::vector<pmr_string>& splitted_bounds_maxima,
    const std::vector<std::shared_ptr<EqualDistinctCountHistogram<pmr_string>>>& histograms,
    const HistogramDomain<pmr_string>& domain) {
  throw std::invalid_argument("Cannot merge string intervals.");
}

template <typename T>
std::tuple<std::vector<HistogramCountType>, std::vector<HistogramCountType>, std::vector<T>, std::vector<T>,
           HistogramCountType>
EqualDistinctCountHistogram<T>::_create_merged_intervals(
    const std::vector<T>& combined_bounds_minima, const std::vector<T>& combined_bounds_maxima,
    const std::vector<std::shared_ptr<EqualDistinctCountHistogram<T>>>& histograms, const HistogramDomain<T>& domain) {
  DebugAssert(combined_bounds_minima.size() == combined_bounds_maxima.size(), "Minima and maxima differ in size.");

  auto interval_distinct_counts = std::vector<HistogramCountType>();
  auto interval_heights = std::vector<HistogramCountType>();
  auto interval_minima = std::vector<T>();
  auto interval_maxima = std::vector<T>();

  /**
   * Simply adding up distinct_counts of different histograms can be problematic in some cases.
   * E.g. we might have 2 intervals from 2 histograms, with 42 elements each.
   * Since we don't know how many of these values are in both histograms, we might make errors in the estimation.
   * But we know that the actual combined distinct_count is between 42 and 84, so we can compute the worst case error.
   */
  auto max_estimation_error = HistogramCountType{0};

  // We want to remove unnecessary empty bins that are artifacts from previous merging steps.
  // These do not occur directly after each other, so it is required to know if we removed the previous one.
  auto kept_previous_split_bin = false;
  const auto combined_bounds_count = combined_bounds_minima.size();

  for (auto bin_bound_index = 0u; bin_bound_index < combined_bounds_count; bin_bound_index++) {
    const auto interval_start = combined_bounds_minima[bin_bound_index];
    const auto interval_end = combined_bounds_maxima[bin_bound_index];

    const auto domain_value_step_size = domain.next_value_clamped(interval_start) - interval_start;

    // How many distinct elements fit into this interval.
    // E.g. 3 for the int interval 1-3 or some huge number for the same float interval.
    const auto interval_max_distinct_capacity =
        (static_cast<HistogramCountType>(interval_end - interval_start) / domain_value_step_size) + 1;

    auto combined_distinct_count = HistogramCountType{0};
    auto summed_distinct_count = HistogramCountType{0};
    auto largest_distinct_count = HistogramCountType{0};
    auto combined_cardinality = HistogramCountType{0};
    auto total_exclusive_cardinality = HistogramCountType{0};

    for (const auto& hist : histograms) {
      const auto estimate = hist->estimate_cardinality_and_distinct_count(PredicateCondition::BetweenInclusive,
                                                                          interval_start, interval_end);
      const auto cardinality = estimate.first;
      const auto distinct_count = estimate.second;

      const auto exclusive_estimate = hist->estimate_cardinality_and_distinct_count(
          PredicateCondition::BetweenExclusive, interval_start, interval_end);
      total_exclusive_cardinality += exclusive_estimate.first;

      if (cardinality > 0.0) {
        combined_cardinality += cardinality;
        combined_distinct_count += distinct_count;
        summed_distinct_count += distinct_count;
        largest_distinct_count = std::max(largest_distinct_count, distinct_count);
      }
    }

    // If this interval is just an artifact created by step 1 (bounds splitting)
    // and therefore does not contain any values, we ignore it.
    if (((total_exclusive_cardinality == 0.0) && kept_previous_split_bin && (interval_start != interval_end)) ||
        (combined_distinct_count == 0.0)) {
      kept_previous_split_bin = false;
      continue;
    }

    // To combine the distinct_counts we generally just add them up and consider a few edge cases.

    if (combined_distinct_count > interval_max_distinct_capacity) {
      combined_distinct_count = interval_max_distinct_capacity;
    }

    if (interval_start == interval_end) {
      // interval_start == interval_end -> distinct_count must be exactly 1
      combined_distinct_count = 1.0;
    }

    max_estimation_error += std::abs(combined_distinct_count - largest_distinct_count);

    interval_minima.push_back(interval_start);
    interval_maxima.push_back(interval_end);
    interval_distinct_counts.push_back(combined_distinct_count);
    interval_heights.push_back(combined_cardinality);
    kept_previous_split_bin = true;
  }

  return std::tie(interval_distinct_counts, interval_heights, interval_minima, interval_maxima, max_estimation_error);
}

/**
 * Merging string histograms has the difficulty that strings cannot be interpolated or incremented/decremented
 * what is required for the current approach.
 * Some other existing methods such as split_at_bin_bounds also do not work with strings for this reason.
 * Therefore, string histograms are unmergeable.
 */
template <>
std::pair<std::shared_ptr<EqualDistinctCountHistogram<pmr_string>>, HistogramCountType>
EqualDistinctCountHistogram<pmr_string>::merge(
    const std::vector<std::shared_ptr<EqualDistinctCountHistogram<pmr_string>>>& histograms, BinID bin_count_target) {
  throw std::invalid_argument("Cannot merge string histograms.");
}

template <typename T>
std::pair<std::shared_ptr<EqualDistinctCountHistogram<T>>, HistogramCountType> EqualDistinctCountHistogram<T>::merge(
    const std::vector<std::shared_ptr<EqualDistinctCountHistogram<T>>>& input_histograms, const BinID max_bin_count) {
  Assert(max_bin_count > 0, "max_bin_count must be greater than zero");

  auto histograms = input_histograms;
  std::erase_if(histograms, [](std::shared_ptr<EqualDistinctCountHistogram<T>> histogram) {
    return !histogram || histogram->bin_count() == 0;
  });

  if (histograms.empty()) {
    return std::make_pair(nullptr, HistogramCountType{0});
  }
  if (histograms.size() == 1) {
    return std::make_pair(histograms[0], HistogramCountType{0});
  }

  // Create a set of bounds that fits all histograms.
  const auto combined_bounds = _combine_bounds(histograms);
  const auto combined_bounds_minima = combined_bounds.first;
  const auto combined_bounds_maxima = combined_bounds.second;

  // Assuming that all histograms have the same domain.
  const auto domain = histograms[0]->domain();

  // Create intervals and calculate their merged distinct count.
  auto [interval_distinct_counts, interval_heights, interval_minima, interval_maxima, max_estimation_error] =
      _create_merged_intervals(combined_bounds_minima, combined_bounds_maxima, histograms, domain);
  const auto total_distinct_count = HistogramCountType{
      std::accumulate(interval_distinct_counts.begin(), interval_distinct_counts.end(), HistogramCountType{0})};

  // Finally we need to create EqualDistinctHistogram from our interval bins, for that we need to balance them.
  const auto merged_histogram =
      _balance_bins_into_histogram(interval_distinct_counts, interval_heights, interval_minima, interval_maxima,
                                   total_distinct_count, max_bin_count, domain);

  return std::make_pair(merged_histogram, max_estimation_error);
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
BinID EqualDistinctCountHistogram<T>::_bin_for_value(const T& value) const {
  const auto it = std::lower_bound(_bin_maxima.cbegin(), _bin_maxima.cend(), value);
  const auto index = static_cast<BinID>(std::distance(_bin_maxima.cbegin(), it));

  if (it == _bin_maxima.cend() || value < bin_minimum(index) || value > bin_maximum(index)) {
    return INVALID_BIN_ID;
  }

  return index;
}

template <typename T>
BinID EqualDistinctCountHistogram<T>::_next_bin_for_value(const T& value) const {
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

}  // namespace opossum
