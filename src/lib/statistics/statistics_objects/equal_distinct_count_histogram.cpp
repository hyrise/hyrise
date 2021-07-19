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
                                                            std::vector<HyperLogLog>&& bin_hlls,
                                                            const HistogramCountType distinct_count_per_bin,
                                                            const BinID bin_count_with_extra_value,
                                                            const HistogramDomain<T>& domain)
    : AbstractHistogram<T>(domain),
      _bin_minima(std::move(bin_minima)),
      _bin_maxima(std::move(bin_maxima)),
      _bin_heights(std::move(bin_heights)),
      _bin_hlls(std::move(bin_hlls)),
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
  std::vector<HyperLogLog> bin_hlls;
  bin_hlls.reserve(bin_count);
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

    unsigned char p, q;
    p = 12;
    q = 255;
    bin_hlls.push_back(HyperLogLog(p, q));

    for (auto i = min_value_idx; i <= max_value_idx; i++) {
      uint_fast64_t hash_value = MurmurHash64A(&value_distribution[i].first, sizeof(T), 42);
      bin_hlls[bin_idx].add(hash_value);
    }
  
    min_value_idx = max_value_idx + 1;
  }

  return std::make_shared<EqualDistinctCountHistogram<T>>(
      std::move(bin_minima), std::move(bin_maxima), std::move(bin_heights), std::move(bin_hlls),
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
    BinID bin_count_target, bool use_hlls) { return histograms[0];}

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
  const auto domain = histograms[0]->domain();
  global_max = global_max + (domain.next_value_clamped(global_max) - global_max) *2;
  showln(global_max);
  std::shared_ptr<AbstractHistogram<T>> split_helper_histogram = std::make_shared<EqualDistinctCountHistogram<T>>(std::vector<T>{global_min}, std::vector<T>{global_max}, std::vector<HistogramCountType>{1}, HistogramCountType{1}, 0);
  for (const auto& hist : histograms) {
    split_helper_histogram = split_helper_histogram->split_at_bin_bounds(hist->bin_bounds());
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
template<>
HistogramCountType EqualDistinctCountHistogram<pmr_string>::_estimate_interval_overlap_with_hlls(const std::vector<std::shared_ptr<EqualDistinctCountHistogram<pmr_string>>>& involved_histograms,
  const pmr_string interval_start, const pmr_string interval_end)
{
  return 0.0;
}

template<typename T>
HistogramCountType EqualDistinctCountHistogram<T>::_estimate_interval_overlap_with_hlls(const std::vector<std::shared_ptr<EqualDistinctCountHistogram<T>>>& involved_histograms,
  const T interval_start, const T interval_end)
{
  auto overlap_estimate = HistogramCountType{0};
  for (auto h1 = 0u; h1 < involved_histograms.size(); h1++) {
    const auto hist_1 = involved_histograms[h1];
    const auto& hist_1_bin_minima = hist_1->bin_minima();
    const auto& hist_1_bin_maxima = hist_1->bin_maxima();

    const auto hist_1_first_bin_in_interval = hist_1->_bin_for_value(interval_start);
    const auto hist_1_last_bin_in_interval = hist_1->_bin_for_value(interval_end);

    for (auto h2 = h1+1; h2 < involved_histograms.size(); h2++) {
      const auto hist_2 = involved_histograms[h2];
      const auto& hist_2_bin_minima = hist_2->bin_minima();
      const auto& hist_2_bin_maxima = hist_2->bin_maxima();

      // const auto hist_2_first_bin_in_interval = hist_2->_bin_for_value(interval_start);
      // const auto hist_2_last_bin_in_interval = hist_2->_bin_for_value(interval_end);

      // Check for each pair of bins from the two histograms in this intersection if they intersect. 
      // If they do, use HLLs to compute how many distinct elements they have in common.
      // TODO: To make this more efficient (not quadratic in number of bins in intersection), use something like
      /*
        for (hist_1_bin : hist 1 bins in interval) {
          a = hist_2_first_bin_that_might_overlap_with_hist_1_bin
          b = hist_2_last_bin_that_might_overlap_with_hist_1_bin
          for (hist_2_bin : range(a,b)) {
            check intersection
          }
        }
      */
      for (auto i = hist_1_first_bin_in_interval; i <= hist_1_last_bin_in_interval; i++) {
        const auto hist_1_bin_min = hist_1_bin_minima[i];
        const auto hist_1_bin_max = hist_1_bin_maxima[i];

        auto hist_2_first_bin_overlapping_with_hist_1_bin = hist_2->_bin_for_value(std::max(hist_1_bin_min, interval_start));
        auto hist_2_last_bin_overlapping_with_hist_1_bin = hist_2->_bin_for_value(std::min(hist_1_bin_max, interval_end));
        for (auto j = hist_2_first_bin_overlapping_with_hist_1_bin; j <= hist_2_last_bin_overlapping_with_hist_1_bin; j++) {
          const auto hist_2_bin_min = hist_2_bin_minima[j];
          const auto hist_2_bin_max = hist_2_bin_maxima[j];

          // I think we never continue here, we should look into this.
          if ((hist_1_bin_max < interval_start) || (hist_1_bin_min > interval_end) ||
              (hist_2_bin_max < interval_start) || (hist_2_bin_min > interval_end)) {
            DebugAssert(false, "We have a bin that is not inside the interval at all, why???");
            continue;
          }

          auto intersection_length = std::min(hist_1_bin_max, hist_2_bin_max) - std::max(hist_1_bin_min, hist_2_bin_min);
          if (intersection_length < 0) {
            DebugAssert(false, "We selected them so that they must intersect, right???");
            continue;
          };
          
          const auto hist_1_hll = hist_1->_bin_hlls[i];
          const auto hist_2_hll = hist_2->_bin_hlls[j];
          const auto hll_stat = HyperLogLog::getJointStatistic(hist_1_hll, hist_2_hll);
          const auto equal_counts = hll_stat.getEqualCounts();
          auto estimated_overlap_of_whole_bins = HistogramCountType(std::accumulate(equal_counts.begin() + 1, equal_counts.end(), 0)); 

          const auto intersection_within_interval_min = std::max(std::max(hist_1_bin_min, hist_2_bin_min), interval_start);
          const auto intersection_within_interval_max = std::min(std::min(hist_1_bin_max, hist_2_bin_max), interval_end);
          const auto intersection_length_within_interval = intersection_within_interval_max - intersection_within_interval_min;
          if (intersection_length_within_interval < 0) {
            continue;
          }
          auto ratio_of_intersection_in_bin = 1.0;
          // If intersection_length == 0, we have two bins with the same single value, thus all their values overlap.
          if (intersection_length != 0.0) {
             ratio_of_intersection_in_bin = intersection_length_within_interval / intersection_length;
          }
          /*
          mi        1                 7                   ma    10                           40
                    #####################################|#######                      
                                      ###################|####################################
                                      ###################|#######
                                              A              B            Intersection length = A+B = C

          A / (A+B);

          bin_intersection = how much is in the intersection of the two bins * how much of that is in (interval_start, interval_end)
                          = how much is in the intersection of the two bins * how much of that is in A

          */
          auto bin_intersection_count = estimated_overlap_of_whole_bins * ratio_of_intersection_in_bin;
          overlap_estimate += bin_intersection_count;
        }
      }
    }
  }

  return overlap_estimate;
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

  println("Balancing Bins");
  showln(bin_count);

  for (auto i = 0u; i < bin_count; i++) {
    auto bin_distinct_count_target = distinct_count_per_bin_target;
    if (i < bin_count_with_extra_value) {
      bin_distinct_count_target++;
    }

    auto is_last_bin = (bin_count - 1) == i;

    show(i);
    show(is_last_bin);
    showln(bin_distinct_count_target);
    auto [bin_start, bin_end, bin_height] = _create_one_bin(
      interval_minima_begin, interval_minima_end,
      interval_maxima_begin, interval_maxima_end,
      interval_heights_begin,  interval_heights_end,
      interval_distinct_counts_begin, interval_distinct_counts_end,
      bin_distinct_count_target,
      domain,
      is_last_bin
    );
    show(bin_start); show(bin_end); show(bin_height);
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
    const BinID max_bin_count, bool use_hlls) {
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

  println("###############  merge() ##############");
  println("Splitted Bounds:")
  std::cout << "\t";
  for(auto i = 0u; i < splitted_bounds_minima.size(); i++) {
    std::cout << "("<< splitted_bounds_minima[i] << ", " << splitted_bounds_maxima[i] << "); ";
  }
  
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
      std::cout << "Deleting interval" << interval_start << ", " << interval_end << std::endl;
      continue;
    }
    bin_minima.push_back(interval_start);
    bin_maxima.push_back(interval_end);
    kept_previous_split_bin = true;

    if (use_hlls) {
      // Backup 2
      combined_distinct_count -= _estimate_interval_overlap_with_hlls(involved_histograms, interval_start, interval_end);
    }
    else if (typeid(T) != typeid(float) && combined_distinct_count > (interval_end - interval_start + 1)) {
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

  println("Estimated Intervals:")
  std::cout << "\t";
  for(auto i = 0u; i < bin_maxima.size(); i++) {
    std::cout << "("<< bin_minima[i] << ", " << bin_maxima[i] << ", h=" << bin_heights[i] << ", d="<< distinct_counts[i] << "); ";
  }


  TIMEIT(auto merged_histogram = _balance_bins(distinct_counts, bin_heights, bin_minima, bin_maxima, distinct_count_per_bin_target, bin_count, bin_count_with_extra_value, domain);
  , "\t_balance_bins"
  );
  return merged_histogram;
}

template <>      
std::shared_ptr<EqualDistinctCountHistogram<pmr_string>> EqualDistinctCountHistogram<pmr_string>::merge(
    std::shared_ptr<EqualDistinctCountHistogram<pmr_string>> histogram_1,
    std::shared_ptr<EqualDistinctCountHistogram<pmr_string>> histogram_2,
    BinID bin_count_target) {return histogram_1;}

template <typename T>
std::shared_ptr<EqualDistinctCountHistogram<T>> EqualDistinctCountHistogram<T>::merge(
    std::shared_ptr<EqualDistinctCountHistogram<T>> histogram_1,
    std::shared_ptr<EqualDistinctCountHistogram<T>> histogram_2,
    BinID bin_count_target) {
  if (!histogram_1) return histogram_2;
  if (!histogram_2) return histogram_1;

  //Please make sure that the input histograms are valid and non empty.

  const auto splitted_histogram_1 = histogram_1->split_at_bin_bounds(histogram_2->bin_bounds());
  const auto splitted_histogram_1_bins = splitted_histogram_1->bin_bounds();
  auto splitted_histogram_1_it = splitted_histogram_1_bins.begin();

  const auto splitted_histogram_2 = histogram_2->split_at_bin_bounds(splitted_histogram_1_bins);
  const auto splitted_histogram_2_bins = splitted_histogram_2->bin_bounds();
  auto splitted_histogram_2_it = splitted_histogram_2_bins.begin();

  auto combined_histogram_bin_minima = std::vector<T>();
  auto combined_histogram_bin_maxima = std::vector<T>();
  int iteration_count = 0;
  while ((splitted_histogram_1_it != splitted_histogram_1_bins.end()) &&
         (splitted_histogram_2_it != splitted_histogram_2_bins.end())) {
  iteration_count++;
    if (splitted_histogram_1_it->first < splitted_histogram_2_it->first) {
      combined_histogram_bin_minima.push_back(splitted_histogram_1_it->first);
      combined_histogram_bin_maxima.push_back(splitted_histogram_1_it->second);
      splitted_histogram_1_it++;
    } else if (splitted_histogram_1_it->first == splitted_histogram_2_it->first) {
      combined_histogram_bin_minima.push_back(splitted_histogram_1_it->first);
      combined_histogram_bin_maxima.push_back(splitted_histogram_1_it->second);
      splitted_histogram_1_it++;
      splitted_histogram_2_it++;
    } else {
      combined_histogram_bin_minima.push_back(splitted_histogram_2_it->first);
      combined_histogram_bin_maxima.push_back(splitted_histogram_2_it->second);
      splitted_histogram_2_it++;
    }
  }
  for (; splitted_histogram_1_it < splitted_histogram_1_bins.end(); splitted_histogram_1_it++) {
    combined_histogram_bin_minima.push_back(splitted_histogram_1_it->first);
    combined_histogram_bin_maxima.push_back(splitted_histogram_1_it->second);
  }
  for (; splitted_histogram_2_it < splitted_histogram_2_bins.end(); splitted_histogram_2_it++) {
    combined_histogram_bin_minima.push_back(splitted_histogram_2_it->first);
    combined_histogram_bin_maxima.push_back(splitted_histogram_2_it->second);
  }

  const auto bin_count = combined_histogram_bin_minima.size();
  auto total_distinct_count = HistogramCountType{0};
  // First, we need to estimate the total_distinct_count of both histograms combined.
  for (auto i = BinID{0}; i < bin_count; i++) {
    const auto estimate_1 = splitted_histogram_1->estimate_cardinality_and_distinct_count(
        PredicateCondition::BetweenInclusive, combined_histogram_bin_minima[i], combined_histogram_bin_maxima[i]);
    const auto estimate_2 = splitted_histogram_2->estimate_cardinality_and_distinct_count(
        PredicateCondition::BetweenInclusive, combined_histogram_bin_minima[i], combined_histogram_bin_maxima[i]);
    const auto cardinality = estimate_1.first + estimate_2.first;
    total_distinct_count +=
        combineDistinctCounts(combined_histogram_bin_minima[i], combined_histogram_bin_maxima[i], cardinality,
                              estimate_1.second, estimate_2.second);
  }

  auto distinct_count_per_bin_target = static_cast<int>(std::round(total_distinct_count)) / bin_count_target;
  // If bin_count_target is larger than necessary, decrease it.
  if (distinct_count_per_bin_target == 0) {
    distinct_count_per_bin_target = 1;
    bin_count_target = std::round(total_distinct_count);
  }
  // number of bins that will get an extra distinct value to avoid smaller last bin
  const auto bin_count_with_extra_value =
      BinID{static_cast<int>(std::round(total_distinct_count)) - distinct_count_per_bin_target * bin_count_target};
  // showln(bin_count_with_extra_value);
  // showln(bin_count_target);
  // showln(distinct_count_per_bin_target);
  // showln(total_distinct_count);
  auto merged_histogram_bin_heights = std::vector<HistogramCountType>();
  auto merged_histogram_bin_minima = std::vector<T>();
  auto merged_histogram_bin_maxima = std::vector<T>();

  auto current_bin_distinct_count = HistogramCountType{0};
  auto current_bin_height = HistogramCountType{0};
  auto current_bin_start = combined_histogram_bin_minima[0];
  auto current_bin_count = BinID{0};
  auto domain = histogram_1->domain();
  auto debug_distinct_count = 0.0;

  for (auto i = BinID{0}; i < bin_count;) {
  //  std::cout << "Merging unlimited for loop " << i << "; bin_count = " << bin_count << "; current_start = " << current_bin_start << std::endl;
    auto defacto_bin_start = std::max(current_bin_start, combined_histogram_bin_minima[i]);
    const auto estimate_1 = splitted_histogram_1->estimate_cardinality_and_distinct_count(
        PredicateCondition::BetweenInclusive, defacto_bin_start, combined_histogram_bin_maxima[i]);
    const auto estimate_2 = splitted_histogram_2->estimate_cardinality_and_distinct_count(
        PredicateCondition::BetweenInclusive, defacto_bin_start, combined_histogram_bin_maxima[i]);
    const auto cardinality = estimate_1.first + estimate_2.first;
    auto distinct_count =
        combineDistinctCounts(combined_histogram_bin_minima[i], combined_histogram_bin_maxima[i], cardinality,
                              estimate_1.second, estimate_2.second);
                          
    if (current_bin_start > combined_histogram_bin_minima[i]) {
      showln(distinct_count);
      println("Actual distinct_count is wrong, correcting it");
      auto bin_length = static_cast<HistogramCountType>(combined_histogram_bin_maxima[i] - combined_histogram_bin_minima[i]);
      if (bin_length > 0) {
        auto remaining_ratio_of_bin = static_cast<HistogramCountType>(combined_histogram_bin_maxima[i] - current_bin_start) / bin_length;
        showln(remaining_ratio_of_bin);
        distinct_count *= remaining_ratio_of_bin;
      }
    }
    auto current_bin_distinct_count_target = distinct_count_per_bin_target;
    showln(i); showln(bin_count); showln(current_bin_count); showln(current_bin_distinct_count); showln(distinct_count);
    showln(combined_histogram_bin_minima[i]);
    showln(combined_histogram_bin_maxima[i]);
    if (current_bin_count < bin_count_with_extra_value) {
      current_bin_distinct_count_target++;
    }
    if ((current_bin_distinct_count + distinct_count < current_bin_distinct_count_target) && (i != bin_count - 1)) {
      println("There's still place for it in the last one.");
      current_bin_distinct_count += distinct_count;
      current_bin_height += cardinality;
      i++;
    } else {
      println("Finishing a bin");
      const auto remaining_distinct_count_to_fill = current_bin_distinct_count_target - current_bin_distinct_count;
      std::cout << "remaining_distinct_count_to_fill = " << remaining_distinct_count_to_fill << std::endl;
      std::cout << "distinct_count = " << distinct_count << std::endl;
      const auto bin_split_ratio = std::min(remaining_distinct_count_to_fill / distinct_count, 1.0f);
      std::cout << "bin_split_ratio = " << bin_split_ratio << std::endl;
      showln(current_bin_distinct_count);
      showln(current_bin_start);
      const auto split_bin_maximum =
          std::max(current_bin_start, combined_histogram_bin_minima[i]) +
          bin_lerp(combined_histogram_bin_maxima[i], std::max(current_bin_start, combined_histogram_bin_minima[i]), bin_split_ratio);

      // const auto split_bin_maximum =
      //     combined_histogram_bin_minima[i] +
      //     bin_lerp(combined_histogram_bin_maxima[i], combined_histogram_bin_minima[i], bin_split_ratio);
      // std::cout <<"i=" << i<< " split_bin_maximum " << split_bin_maximum << std::endl;
      current_bin_height += cardinality * bin_split_ratio;

      debug_distinct_count += distinct_count * bin_split_ratio;

      current_bin_distinct_count = current_bin_distinct_count_target;
      merged_histogram_bin_minima.push_back(current_bin_start);
      merged_histogram_bin_maxima.push_back(split_bin_maximum);
      merged_histogram_bin_heights.push_back(current_bin_height);

      if (split_bin_maximum != combined_histogram_bin_maxima[i]) {
        current_bin_start = domain.next_value_clamped(split_bin_maximum);
      } else if (i != bin_count - 1) {
        current_bin_start = combined_histogram_bin_minima[i + 1];
      } else {
        //split_bin_maximum = combined_bin_maxima[i] && i == bin_count-1
        i++;
        // current_bin_start = combined_histogram_bin_maxima[i];
      }

      current_bin_count++;
      current_bin_height = 0;
      current_bin_distinct_count = 0;

      if ((bin_split_ratio == 1.0) || (current_bin_count == bin_count_target)) {
        i++;
      }
    }
  }
  // for(BinID i = BinID{0}; i < combined_histogram_bin_minima.size(); i++) {
  //   std::cout << "combined Bin("<< combined_histogram_bin_minima[i] << ", " << combined_histogram_bin_maxima[i] << "), ";
  // }
  // for(BinID i = BinID{0}; i < merged_histogram_bin_minima.size(); i++) {
  //   std::cout << "Bin("<< merged_histogram_bin_minima[i] << ", " << merged_histogram_bin_maxima[i] << "), ";
  // }
  // std::cout << std::endl;
  showln(current_bin_count);
  showln(debug_distinct_count);
  auto merged_histogram = std::make_shared<EqualDistinctCountHistogram<T>>(
      std::move(merged_histogram_bin_minima), std::move(merged_histogram_bin_maxima),
      std::move(merged_histogram_bin_heights), static_cast<HistogramCountType>(distinct_count_per_bin_target),
      bin_count_with_extra_value, domain);
 
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
