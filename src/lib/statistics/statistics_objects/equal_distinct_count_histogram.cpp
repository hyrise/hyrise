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
    BinID bin_count_target) { return histograms[0];}

#define STRING(X) #X
#define print(X) std::cout << STRING(X) << " ";
#define println(X) std::cout << STRING(X) << " " << std::endl;
#define show(X) std::cout << STRING(X) << " = " << X << " ";
#define showln(X) std::cout << STRING(X) << " = " << X << std::endl;

template <typename T>
std::shared_ptr<EqualDistinctCountHistogram<T>> EqualDistinctCountHistogram<T>::merge(
    const std::vector<std::shared_ptr<EqualDistinctCountHistogram<T>>>& histograms,
    BinID bin_count_target) {
  // NOLINTNEXTLINE clang-tidy is crazy and sees a "potentially unintended semicolon" here...
  // if constexpr (std::is_same_v<T, pmr_string>) {
  //   Fail("Cannot split_at_bin_bounds() on string histogram");
  // }
  auto global_min = histograms[0]->bin_minimum(0);
  auto global_max = histograms[0]->bin_maximum(histograms[0]->bin_count()-1);
  for (const auto& hist : histograms) {
    global_min = std::min(global_min, hist->bin_minimum(0));
    global_max = std::max(global_max, hist->bin_maximum(hist->bin_count()-1));
  }
  std::shared_ptr<AbstractHistogram<T>> split_helper_histogram = std::make_shared<EqualDistinctCountHistogram<T>>(std::vector<T>{global_min}, std::vector<T>{global_max}, std::vector<HistogramCountType>{1}, HistogramCountType{1}, 0);
  for (const auto& hist : histograms) {
    split_helper_histogram = split_helper_histogram->split_at_bin_bounds(hist->bin_bounds());
  }
  const auto& splitted_bounds = split_helper_histogram->bin_bounds();
  auto splitted_bounds_minima = std::vector<T>();
  auto splitted_bounds_maxima = std::vector<T>();
  for (const auto& bounds : splitted_bounds) {
    splitted_bounds_minima.push_back(bounds.first);
    splitted_bounds_maxima.push_back(bounds.second);
  }

  auto distinct_counts = std::vector<HistogramCountType>();
  auto bin_heights = std::vector<HistogramCountType>();
  auto bin_minima = std::vector<T>();
  auto bin_maxima = std::vector<T>();
  auto kept_previous_split_bin = false;

  std::vector<std::shared_ptr<EqualDistinctCountHistogram<T>>> involved_histograms;
  for (auto s = 0u; s < splitted_bounds.size(); s++) {
    const auto& bin_bound = splitted_bounds[s];
    const auto bin_min = bin_bound.first;
    const auto bin_max = bin_bound.second;
    involved_histograms.clear();
    auto combined_cardinality = HistogramCountType{0.0};
    auto combined_distinct_count = HistogramCountType{0.0};

    auto total_exclusive_cardinality = HistogramCountType{0.0};
    for (const auto& hist : histograms) {
      const auto estimate = hist->estimate_cardinality_and_distinct_count(PredicateCondition::BetweenInclusive, bin_min, bin_max);
      const auto exclusive_estimate = hist->estimate_cardinality_and_distinct_count(PredicateCondition::BetweenExclusive, bin_min, bin_max);
      total_exclusive_cardinality += exclusive_estimate.first;
      const auto cardinality = estimate.first;
      const auto distinct_count = estimate.second;

      if (cardinality > 0.0) {
        involved_histograms.push_back(hist);
        combined_cardinality += cardinality;
        combined_distinct_count += distinct_count;
      }
    }
    if ((total_exclusive_cardinality == 0.0) && (kept_previous_split_bin) && (bin_min != bin_max)) {
      kept_previous_split_bin = false;
      continue;
    }
    bin_minima.push_back(bin_min);
    bin_maxima.push_back(bin_max);
    kept_previous_split_bin = true;
    /*
    ---- --- ---- ----- -- ---------------------
      -------------------------  --- -- ----------
    */

    for (auto h1 = 0u; h1 < involved_histograms.size(); h1++) {    
      const auto hist_1 = involved_histograms[h1];
      const auto bounds_1 = hist_1->bin_bounds();
      
      const auto hist_1_first_bin = hist_1->_bin_for_value(bin_min);
      const auto hist_1_last_bin = hist_1->_bin_for_value(bin_max);
      for (auto h2 = h1+1; h2 < involved_histograms.size(); h2++) {
        const auto hist_2 = involved_histograms[h2];
        const auto bounds_2 = hist_2->bin_bounds();

        const auto hist_2_first_bin = hist_2->_bin_for_value(bin_min);
        const auto hist_2_last_bin = hist_2->_bin_for_value(bin_max);

        for (auto i = hist_1_first_bin; i <= hist_1_last_bin; i++) {
          const auto bin_bounds_1 = bounds_1[i];
          for (auto j = hist_2_first_bin; j <= hist_2_last_bin; j++) {
            const auto bin_bounds_2 = bounds_2[j];
            if ((bin_bounds_1.second < bin_min) || (bin_bounds_1.first > bin_max)) {
              continue;
            }
            if ((bin_bounds_2.second < bin_min) || (bin_bounds_2.first > bin_max)) {
              continue;
            }
            auto intersection_length = -1.0; // There can be intersections of size 0, i.e. if both bins are the same bin with only one value.
            if (bin_bounds_1.first <= bin_bounds_2.first && bin_bounds_1.second >= bin_bounds_2.first){
              //b1     ###################?????????????
              //b2            #####################
              intersection_length = std::min(bin_bounds_1.second, bin_bounds_2.second) - bin_bounds_2.first;
            }
            else if (bin_bounds_1.first <= bin_bounds_2.second && bin_bounds_1.second >= bin_bounds_2.second) {
              //b1      ???????????????###################
              //b2            #####################
              intersection_length = std::min(bin_bounds_1.second, bin_bounds_2.second) - bin_bounds_1.first;
            }
            else if (bin_bounds_1.first >= bin_bounds_2.first && bin_bounds_1.second <= bin_bounds_2.second) {
              //b1                  ##########
              //b2            #####################
              intersection_length = bin_bounds_1.second - bin_bounds_1.first;
            }
            if (intersection_length > -1.0) {
              const auto intersection_min = std::max(bin_bounds_1.first, bin_bounds_2.first);
              const auto intersection_max = std::min(bin_bounds_1.second, bin_bounds_2.second);
              const auto hll1 = hist_1->_bin_hlls[i];
              const auto hll2 = hist_2->_bin_hlls[j];
              const auto stat = HyperLogLog::getJointStatistic(hll1, hll2);
              const auto equal_counts = stat.getEqualCounts();
              auto bin_intersection_count = HistogramCountType(std::accumulate(equal_counts.begin() +1, equal_counts.end(), 0)); 
              const auto intersection_length_in_bin = (float)(intersection_max - intersection_min);
              auto ratio_of_intersection_in_bin = intersection_length_in_bin / intersection_length;
              if (intersection_length == 0.0) {
                ratio_of_intersection_in_bin = intersection_length_in_bin;
                if (intersection_length_in_bin == 0.0) {
                  ratio_of_intersection_in_bin = 1.0; //because 0/0 = 1, obviously.¯\_(ツ)_/¯
                }
              }

              //Kind of wrong if we have 0.3-0.4 and 0.4-0.7...
              /*
              mi        1                 7                   ma    10                           40
                        #####################################|#######                      
                                          ###################|####################################
                                          ###################|#######
                                                  A              B            Intersection length = A+B = C

              A / (A+B);

              bin_intersection = how much is in the intersection of the two bins * how much of that is in (bin_min, bin_max)
                               = how much is in the intersection of the two bins * how much of that is in A

              */
              bin_intersection_count = bin_intersection_count * ratio_of_intersection_in_bin;
              combined_distinct_count -= bin_intersection_count; 
            }
          }
        }
      }
    }
    if (combined_distinct_count < 1.0) {
      combined_distinct_count = 1.0; //Assumes that all input bins of histograms have a distinct_count >= 1.0.
    }
    distinct_counts.push_back(combined_distinct_count);
    bin_heights.push_back(combined_cardinality);
  }

  const auto bin_count = distinct_counts.size();
  auto total_distinct_count = HistogramCountType{0};
  // First, we need to estimate the total_distinct_count of both histograms combined.
  for (auto i = BinID{0}; i < bin_count; i++) {
    total_distinct_count += distinct_counts[i];
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
  auto merged_histogram_bin_heights = std::vector<HistogramCountType>();
  auto merged_histogram_bin_minima = std::vector<T>();
  auto merged_histogram_bin_maxima = std::vector<T>();
  auto current_bin_distinct_count = HistogramCountType{0.0};
  auto current_bin_height = HistogramCountType{0.0};
  auto current_bin_start = bin_minima[0];
  auto current_bin_count = BinID{0};
  auto domain = histograms[0]->domain();
  for (auto i = BinID{0}; i < bin_count;) {
    // std::cout << "Merging unlimited for loop " << i << "; bin_count = " << bin_count << "; current_start = " << current_bin_start << std::endl;
    // auto defacto_bin_start = std::max(current_bin_start, splitted_bounds_minima[i]);
    const auto cardinality = bin_heights[i];
    const auto distinct_count = distinct_counts[i];
    auto current_bin_distinct_count_target = distinct_count_per_bin_target;
    if (current_bin_count < bin_count_with_extra_value) {
      current_bin_distinct_count_target++;
    }
    if ((current_bin_distinct_count + distinct_count < current_bin_distinct_count_target) && (i != bin_count - 1)) {
      current_bin_distinct_count += distinct_count;
      current_bin_height += cardinality;
      i++;
    } else {
      const auto remaining_distinct_count_to_fill = current_bin_distinct_count_target - current_bin_distinct_count;
      // std::cout << "remaining_distinct_count_to_fill = " << remaining_distinct_count_to_fill << std::endl;
      // std::cout << "distinct_count = " << distinct_count << std::endl;
      const auto bin_split_ratio = std::min(remaining_distinct_count_to_fill / distinct_count, 1.0f);
      // std::cout << "bin_split_ratio = " << bin_split_ratio << std::endl;
      const auto split_bin_maximum =
          bin_minima[i] +
          bin_lerp(bin_maxima[i], bin_minima[i], bin_split_ratio);
      // std::cout <<"i=" << i<< " split_bin_maximum " << split_bin_maximum << std::endl;
      current_bin_height += cardinality * bin_split_ratio;
      current_bin_distinct_count = current_bin_distinct_count_target;
      merged_histogram_bin_minima.push_back(current_bin_start);
      merged_histogram_bin_maxima.push_back(split_bin_maximum);
      merged_histogram_bin_heights.push_back(current_bin_height);
      if (split_bin_maximum != bin_maxima[i]) {
        current_bin_start = domain.next_value_clamped(split_bin_maximum);
      } else if (i != bin_count - 1) {
        current_bin_start = bin_minima[i + 1];
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
  auto merged_histogram = std::make_shared<EqualDistinctCountHistogram<T>>(
      std::move(merged_histogram_bin_minima), std::move(merged_histogram_bin_maxima),
      std::move(merged_histogram_bin_heights), static_cast<HistogramCountType>(distinct_count_per_bin_target),
      bin_count_with_extra_value, domain);
  return merged_histogram;
}

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
  auto merged_histogram_bin_heights = std::vector<HistogramCountType>();
  auto merged_histogram_bin_minima = std::vector<T>();
  auto merged_histogram_bin_maxima = std::vector<T>();

  auto current_bin_distinct_count = HistogramCountType{0};
  auto current_bin_height = HistogramCountType{0};
  auto current_bin_start = combined_histogram_bin_minima[0];
  auto current_bin_count = BinID{0};
  auto domain = histogram_1->domain();
  
  for (auto i = BinID{0}; i < bin_count;) {
    // std::cout << "Merging unlimited for loop " << i << "; bin_count = " << bin_count << "; current_start = " << current_bin_start << std::endl;
    auto defacto_bin_start = std::max(current_bin_start, combined_histogram_bin_minima[i]);
    const auto estimate_1 = splitted_histogram_1->estimate_cardinality_and_distinct_count(
        PredicateCondition::BetweenInclusive, defacto_bin_start, combined_histogram_bin_maxima[i]);
    const auto estimate_2 = splitted_histogram_2->estimate_cardinality_and_distinct_count(
        PredicateCondition::BetweenInclusive, defacto_bin_start, combined_histogram_bin_maxima[i]);
    const auto cardinality = estimate_1.first + estimate_2.first;
    const auto distinct_count =
        combineDistinctCounts(combined_histogram_bin_minima[i], combined_histogram_bin_maxima[i], cardinality,
                              estimate_1.second, estimate_2.second);
    auto current_bin_distinct_count_target = distinct_count_per_bin_target;

    if (current_bin_count < bin_count_with_extra_value) {
      current_bin_distinct_count_target++;
    }
    if ((current_bin_distinct_count + distinct_count < current_bin_distinct_count_target) && (i != bin_count - 1)) {
      current_bin_distinct_count += distinct_count;
      current_bin_height += cardinality;
      i++;
    } else {
      const auto remaining_distinct_count_to_fill = current_bin_distinct_count_target - current_bin_distinct_count;
      // std::cout << "remaining_distinct_count_to_fill = " << remaining_distinct_count_to_fill << std::endl;
      // std::cout << "distinct_count = " << distinct_count << std::endl;
      const auto bin_split_ratio = std::min(remaining_distinct_count_to_fill / distinct_count, 1.0f);
      // std::cout << "bin_split_ratio = " << bin_split_ratio << std::endl;
      const auto split_bin_maximum =
          combined_histogram_bin_minima[i] +
          bin_lerp(combined_histogram_bin_maxima[i], combined_histogram_bin_minima[i], bin_split_ratio);
      // std::cout <<"i=" << i<< " split_bin_maximum " << split_bin_maximum << std::endl;
      current_bin_height += cardinality * bin_split_ratio;
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
