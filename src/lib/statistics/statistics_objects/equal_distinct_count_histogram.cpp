#include "equal_distinct_count_histogram.hpp"

#include <cmath>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "generic_histogram.hpp"
#include "resolve_type.hpp"
#include "storage/segment_iterate.hpp"

namespace {

using namespace opossum;  // NOLINT

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
  Assert(max_bin_count > 0, "max_bin_count must be greater than zero ");

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
  if (!chunk) return nullptr;
  add_segment_to_value_distribution<T>(*chunk->get_segment(column_id), value_distribution_map, domain);

  auto value_distribution =
      std::vector<std::pair<T, HistogramCountType>>{value_distribution_map.begin(), value_distribution_map.end()};
  std::sort(value_distribution.begin(), value_distribution.end(),
            [&](const auto& l, const auto& r) { return l.first < r.first; });

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

int32_t bin_lerp(int32_t a, int32_t b, double r) { return (a - b) * r; }
int64_t bin_lerp(int64_t a, int64_t b, double r) { return (a - b) * r; }
float bin_lerp(float a, float b, double r) { return (a - b) * r; }
double bin_lerp(double a, double b, double r) { return (a - b) * r; }
std::string bin_lerp(std::string a, std::string b, double r) { return a; }
pmr_string bin_lerp(pmr_string a, pmr_string b, double r) { return a; }
AllTypeVariant bin_lerp(AllTypeVariant a, AllTypeVariant b, double r) { return a; }


template <typename T>
HistogramCountType combineDistinctCounts(T bin_min, T bin_max, HistogramCountType cardinality,
                                         HistogramCountType distinct_a, HistogramCountType distinct_b) {
  // TODO: Improve by using additional statistics
  return std::max(distinct_a, distinct_b);
}

template<>
HistogramCountType combineDistinctCounts<float>(float bin_min, float bin_max, HistogramCountType cardinality,
                                         HistogramCountType distinct_a, HistogramCountType distinct_b) {
  // TODO: Improve by using additional statistics
  if (bin_min == bin_max) return std::max(distinct_a, distinct_b); //should be one anyways...
  return distinct_a + distinct_b;
}

template <>
std::shared_ptr<EqualDistinctCountHistogram<pmr_string>> EqualDistinctCountHistogram<pmr_string>::merge(
    const std::vector<std::shared_ptr<EqualDistinctCountHistogram<pmr_string>>>& histograms,
    BinID bin_count_target) { return histograms[0]; }

#define STRING(X) #X
#define print(X) std::cout << STRING(X) << " ";
#define println(X) std::cout << STRING(X) << " " << std::endl;
#define show(X) std::cout << STRING(X) << " = " << X << " ";
#define showln(X) std::cout << STRING(X) << " = " << X << std::endl;
#define TIMEIT(X, Y) begin = std::chrono::steady_clock::now(); X; end = std::chrono::steady_clock::now(); std::cout << Y << ": Time difference = \t" << std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() << "[µs]" << std::endl;


template<typename T> 
const std::vector<T> & EqualDistinctCountHistogram<T>::bin_minima() const { return _bin_minima; }
template<typename T> 
const std::vector<T> & EqualDistinctCountHistogram<T>::bin_maxima() const { return _bin_maxima; }

template<>
std::pair<std::vector<pmr_string>, std::vector<pmr_string>> EqualDistinctCountHistogram<pmr_string>::_merge_splitted_bounds(const std::vector<std::shared_ptr<EqualDistinctCountHistogram<pmr_string>>>& histograms) 
{
  return std::make_pair(std::vector<pmr_string>(), std::vector<pmr_string>());
}

template<typename T>
std::pair<std::vector<T>, std::vector<T>> EqualDistinctCountHistogram<T>::_merge_splitted_bounds(const std::vector<std::shared_ptr<EqualDistinctCountHistogram<T>>>& histograms) {
  auto global_min = histograms[0]->bin_minimum(0);
  auto global_max = histograms[0]->bin_maximum(histograms[0]->bin_count()-1);
  for (const auto& hist : histograms) {
    global_min = std::min(global_min, hist->bin_minimum(0));
    global_max = std::max(global_max, hist->bin_maximum(hist->bin_count()-1));
  }
  // const auto domain = histograms[0]->domain();
  // global_min = global_min + (domain.previous_value_clamped(global_min) - global_min) *2;
  // global_max = global_max + (domain.next_value_clamped(global_max) - global_max) *2;
  // showln(global_max);
  std::shared_ptr<AbstractHistogram<T>> split_helper_histogram = std::make_shared<EqualDistinctCountHistogram<T>>(std::vector<T>{global_min}, std::vector<T>{global_max}, std::vector<HistogramCountType>{1}, HistogramCountType{1}, 0);
  for (const auto& hist : histograms) {
    split_helper_histogram = split_helper_histogram->split_at_bin_bounds(hist->bin_bounds(), true);
  }
  const auto split_interval_bounds = split_helper_histogram->bin_bounds();
  auto split_interval_minima = std::vector<T>(split_interval_bounds.size());
  auto split_interval_maxima = std::vector<T>(split_interval_bounds.size());
  
  auto split_interval_count = split_interval_bounds.size();
  for (auto i = 0u; i < split_interval_count; i++) {
    split_interval_minima[i] = split_interval_bounds[i].first;
    split_interval_maxima[i] = split_interval_bounds[i].second;
  }

  return std::make_pair(split_interval_minima, split_interval_maxima);
}

template <>
std::tuple<pmr_string, pmr_string, HistogramCountType> EqualDistinctCountHistogram<pmr_string>::_create_one_bin(
  typename std::vector<pmr_string>::iterator& interval_minima_begin, typename std::vector<pmr_string>::iterator& interval_minima_end,
  typename std::vector<pmr_string>::iterator& interval_maxima_begin, typename std::vector<pmr_string>::iterator& interval_maxima_end,
  typename std::vector<HistogramCountType>::iterator& interval_heights_begin, typename std::vector<HistogramCountType>::iterator& interval_heights_end,
  typename std::vector<HistogramCountType>::iterator& interval_distinct_counts_begin, typename std::vector<HistogramCountType>::iterator& interval_distinct_counts_end,
  const int distinct_count_target,
  const HistogramDomain<pmr_string> domain,
  const bool is_last_bin
) { return std::tuple{pmr_string(), pmr_string(), 0.0}; }

template <typename T>
std::tuple<T, T, HistogramCountType> EqualDistinctCountHistogram<T>::_create_one_bin(
  typename std::vector<T>::iterator& interval_minima_begin, typename std::vector<T>::iterator& interval_minima_end,
  typename std::vector<T>::iterator& interval_maxima_begin, typename std::vector<T>::iterator& interval_maxima_end,
  typename std::vector<HistogramCountType>::iterator& interval_heights_begin, typename std::vector<HistogramCountType>::iterator& interval_heights_end,
  typename std::vector<HistogramCountType>::iterator& interval_distinct_counts_begin, typename std::vector<HistogramCountType>::iterator& interval_distinct_counts_end,
  const int distinct_count_target,
  const HistogramDomain<T> domain,
  const bool is_last_bin // We will probably need to be less precise for the last one and just stuff everything in there...
) {
  auto remainingIntervalCount = std::distance(interval_minima_begin, interval_minima_end);
  DebugAssert(remainingIntervalCount == std::distance(interval_maxima_begin, interval_maxima_end), "All interval vectors need to have the same size.");
  DebugAssert(remainingIntervalCount == std::distance(interval_heights_begin, interval_heights_end), "All interval vectors need to have the same size.");
  DebugAssert(remainingIntervalCount == std::distance(interval_distinct_counts_begin, interval_distinct_counts_end), "All interval vectors need to have the same size.");

  DebugAssert(remainingIntervalCount > 0, "RemainingIntervalCount has to be greater than zero.");
  auto bin_start = *interval_minima_begin;
  auto bin_end = bin_start;
  auto bin_height = HistogramCountType{0};
  auto bin_distinct_count = HistogramCountType{0};
  for(auto i = 0u; i < remainingIntervalCount; i++, interval_minima_begin++, interval_maxima_begin++, interval_heights_begin++, interval_distinct_counts_begin++) {
    const auto interval_height = *interval_heights_begin;
    const auto interval_distinct_count = *interval_distinct_counts_begin;
    const auto interval_start = *interval_minima_begin;
    const auto interval_end = *interval_maxima_begin;
    
    const auto remaining_distinct_count_to_fill_bin = distinct_count_target - bin_distinct_count;
    const bool is_splittable_interval = (interval_start != interval_end);
    if ((remaining_distinct_count_to_fill_bin > interval_distinct_count) || is_last_bin) {
      bin_end = interval_end;
      bin_height += interval_height;
      bin_distinct_count += interval_distinct_count;
    }
    else if (remaining_distinct_count_to_fill_bin == interval_distinct_count || !is_splittable_interval) {
      bin_end = interval_end;
      bin_height += interval_height;
      bin_distinct_count += interval_distinct_count;

      interval_minima_begin++;
      interval_maxima_begin++;
      interval_heights_begin++;
      interval_distinct_counts_begin++;
      break;
    }
    else { //Only a fraction of the interval fits into the new bin
      const auto interval_fraction_for_bin = remaining_distinct_count_to_fill_bin / interval_distinct_count;
      bin_end = static_cast<T>(interval_start + (interval_end - interval_start) * interval_fraction_for_bin); // Might be somewhat wrong for integers, but we just have to accept it.
      bin_height += interval_height * interval_fraction_for_bin;
      bin_distinct_count += interval_distinct_count * interval_fraction_for_bin;

      const auto new_interval_start = domain.next_value_clamped(bin_end);
      const auto new_interval_height = interval_height - (interval_height * interval_fraction_for_bin);
      const auto new_interval_distinct_count = interval_distinct_count - (interval_distinct_count * interval_fraction_for_bin);
      *interval_minima_begin = new_interval_start;
      *interval_heights_begin = new_interval_height;
      *interval_distinct_counts_begin = new_interval_distinct_count;
      break;
    }
  }
  
  return std::make_tuple(bin_start, bin_end, bin_height);
}

template <>
std::shared_ptr<EqualDistinctCountHistogram<pmr_string>> EqualDistinctCountHistogram<pmr_string>::_balance_bins(
      std::vector<HistogramCountType>& interval_distinct_counts,
      std::vector<HistogramCountType>& interval_heights,
      std::vector<pmr_string>& interval_minima,
      std::vector<pmr_string>& interval_maxima,
      const int distinct_count_per_bin_target,
      const BinID bin_count,
      const BinID bin_count_with_extra_value,
      const HistogramDomain<pmr_string> domain) { return std::make_shared<EqualDistinctCountHistogram<pmr_string>>(); }


template <typename T>
std::shared_ptr<EqualDistinctCountHistogram<T>> EqualDistinctCountHistogram<T>::_balance_bins(
      std::vector<HistogramCountType>& interval_distinct_counts,
      std::vector<HistogramCountType>& interval_heights,
      std::vector<T>& interval_minima,
      std::vector<T>& interval_maxima,
      const int distinct_count_per_bin_target,
      const BinID bin_count,
      const BinID bin_count_with_extra_value,
      const HistogramDomain<T> domain) {
        //Rename bin_minima/maxima to interval_minima/maxima?
  auto merged_histogram_bin_heights = std::vector<HistogramCountType>();
  auto merged_histogram_bin_minima = std::vector<T>();
  auto merged_histogram_bin_maxima = std::vector<T>();
  merged_histogram_bin_heights.reserve(bin_count);
  merged_histogram_bin_minima.reserve(bin_count);
  merged_histogram_bin_maxima.reserve(bin_count);

  auto interval_minima_begin = interval_minima.begin();
  auto interval_minima_end = interval_minima.end();
  auto interval_maxima_begin = interval_maxima.begin();
  auto interval_maxima_end = interval_maxima.end();
  auto interval_heights_begin = interval_heights.begin();
  auto interval_heights_end = interval_heights.end();
  auto interval_distinct_counts_begin = interval_distinct_counts.begin();
  auto interval_distinct_counts_end = interval_distinct_counts.end();

  // println("Balancing Bins");
  // showln(bin_count);

  for (auto i = 0u; i < bin_count; i++) {
    auto bin_distinct_count_target = distinct_count_per_bin_target;
    if (i < bin_count_with_extra_value) {
      bin_distinct_count_target++;
    }

    auto is_last_bin = (bin_count - 1) == i;

    // show(i);
    // show(is_last_bin);
    // showln(bin_distinct_count_target);
    auto [bin_start, bin_end, bin_height] = _create_one_bin(
      interval_minima_begin, interval_minima_end,
      interval_maxima_begin, interval_maxima_end,
      interval_heights_begin,  interval_heights_end,
      interval_distinct_counts_begin, interval_distinct_counts_end,
      bin_distinct_count_target,
      domain,
      is_last_bin
    );
    // show(bin_start); show(bin_end); show(bin_height);
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

template <typename T>
std::shared_ptr<EqualDistinctCountHistogram<T>> EqualDistinctCountHistogram<T>::merge(
    const std::vector<std::shared_ptr<EqualDistinctCountHistogram<T>>>& histograms,
    const BinID max_bin_count) {
  // NOLINTNEXTLINE clang-tidy is crazy and sees a "potentially unintended semicolon" here...
  // if constexpr (std::is_same_v<T, pmr_string>) {
  //   Fail("Cannot split_at_bin_bounds() on string histogram");
  // }
  Assert(max_bin_count > 0, "max_bin_count must be greater than zero");
  std::chrono::steady_clock::time_point begin;
  std::chrono::steady_clock::time_point end;
  TIMEIT(
  auto splitted_bounds = _merge_splitted_bounds(histograms);
  , "\t_merge_splitted_bounds")
  auto splitted_bounds_minima = splitted_bounds.first;
  auto splitted_bounds_maxima = splitted_bounds.second;

  // println("###############  merge() ##############");
  // println("Splitted Bounds:")
  // std::cout << "\t";
  // for(auto i = 0u; i < splitted_bounds_minima.size(); i++) {
  //   std::cout << "("<< splitted_bounds_minima[i] << ", " << splitted_bounds_maxima[i] << "); ";
  // }
  
  auto distinct_counts = std::vector<HistogramCountType>();
  auto bin_heights = std::vector<HistogramCountType>();
  auto bin_minima = std::vector<T>();
  auto bin_maxima = std::vector<T>();
  auto kept_previous_split_bin = false;

  std::vector<std::shared_ptr<EqualDistinctCountHistogram<T>>> involved_histograms;
  const auto split_interval_count = splitted_bounds_minima.size();
  TIMEIT(
  for (auto s = 0u; s < split_interval_count; s++) {
    const auto interval_start = splitted_bounds_minima[s];
    const auto interval_end = splitted_bounds_maxima[s];
    auto combined_cardinality = HistogramCountType{0.0};
    auto combined_distinct_count = HistogramCountType{0.0};
    auto total_exclusive_cardinality = HistogramCountType{0.0};

    involved_histograms.clear();

    for (const auto& hist : histograms) {
      const auto estimate = hist->estimate_cardinality_and_distinct_count(PredicateCondition::BetweenInclusive, interval_start, interval_end);
      const auto exclusive_estimate = hist->estimate_cardinality_and_distinct_count(PredicateCondition::BetweenExclusive, interval_start, interval_end);
      const auto cardinality = estimate.first;
      const auto distinct_count = estimate.second;

      total_exclusive_cardinality += exclusive_estimate.first;

      if (cardinality > 0.0) {
        involved_histograms.push_back(hist);
        combined_cardinality += cardinality;
        combined_distinct_count += distinct_count;
      }
    }
    if (((total_exclusive_cardinality == 0.0) && (kept_previous_split_bin) && (interval_start != interval_end)) || combined_distinct_count == 0.0) {
      kept_previous_split_bin = false;
      // std::cout << "Deleting interval" << interval_start << ", " << interval_end << std::endl;
      continue;
    }
    bin_minima.push_back(interval_start);
    bin_maxima.push_back(interval_end);
    kept_previous_split_bin = true;

    if (typeid(T) != typeid(float) && combined_distinct_count > (interval_end - interval_start + 1)) {
      combined_distinct_count = interval_end - interval_start + 1;
    }

    if (combined_distinct_count < 1.0 || interval_start == interval_end) {
      // Assumes that all input bins of histograms have a distinct_count >= 1.0.
      // interval_start == interval_end -> distinct_count must be exactly 1
      combined_distinct_count = 1.0;
    }

    distinct_counts.push_back(combined_distinct_count);
    bin_heights.push_back(combined_cardinality);
  }
  , "\testimating_interval_counts");
  const auto total_distinct_count = HistogramCountType{std::accumulate(distinct_counts.begin(), distinct_counts.end(), HistogramCountType{0.0})};

  // When the total_distinct_count cannot be rounded properly or the elements cannot be distributed equally, we allow the last bin to deviate in distinct_count.
  const auto bin_count = std::min(static_cast<BinID>(std::round(total_distinct_count)), max_bin_count);
  const auto distinct_count_per_bin_target = static_cast<int>(std::round(total_distinct_count)) / bin_count;

  // number of bins that will get an extra distinct value to avoid smaller last bin
  const auto bin_count_with_extra_value =
      BinID{static_cast<int>(std::round(total_distinct_count)) - distinct_count_per_bin_target * bin_count};
  const auto domain = histograms[0]->domain();

  // showln(distinct_count_per_bin_target);
  // showln(bin_count);
  // showln(total_distinct_count);
  // showln(bin_count_with_extra_value);
  // showln(distinct_counts.size());

  // println("Estimated Intervals:")
  // std::cout << "\t";
  // for(auto i = 0u; i < bin_maxima.size(); i++) {
  //   std::cout << "("<< bin_minima[i] << ", " << bin_maxima[i] << ", h=" << bin_heights[i] << ", d="<< distinct_counts[i] << "); ";
  // }


  TIMEIT(auto merged_histogram = _balance_bins(distinct_counts, bin_heights, bin_minima, bin_maxima, distinct_count_per_bin_target, bin_count, bin_count_with_extra_value, domain);
  , "\t_balance_bins"
  );
  return merged_histogram;
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
