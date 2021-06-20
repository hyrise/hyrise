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

template <typename T>
std::shared_ptr<EqualDistinctCountHistogram<T>> EqualDistinctCountHistogram<T>::merge(
    std::shared_ptr<EqualDistinctCountHistogram<T>> histogram_1,
    std::shared_ptr<EqualDistinctCountHistogram<T>> histogram_2) {
  //Please make sure that the input histograms are valid and non empty.

  const auto splitted_histogram_1 = histogram_1->split_at_bin_bounds(histogram_2->bin_bounds());
  const auto splitted_histogram_1_bins = splitted_histogram_1->bin_bounds();
  auto splitted_histogram_1_it = splitted_histogram_1_bins.begin();

  const auto splitted_histogram_2 = histogram_2->split_at_bin_bounds(splitted_histogram_1_bins);
  const auto splitted_histogram_2_bins = splitted_histogram_2->bin_bounds();
  auto splitted_histogram_2_it = splitted_histogram_2_bins.begin();

  auto combined_histogram_bin_minima = std::vector<T>();
  auto combined_histogram_bin_maxima = std::vector<T>();

  while ((splitted_histogram_1_it != splitted_histogram_1_bins.end()) &&
         (splitted_histogram_2_it != splitted_histogram_2_bins.end())) {
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
  
  const auto bin_count_target = histogram_1->bin_count();
  const auto distinct_count_per_bin_target = static_cast<int>(std::round(total_distinct_count)) / bin_count_target;
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

  // TODO: Delete
  // std::cout << "dist.count target " << distinct_count_per_bin_target << std::endl;
  // std::cout << "bin count target " << bin_count_target << std::endl;
  // for (auto i = BinID{0}; i < bin_count; i++) {
  //   std::cout << "["<< combined_histogram_bin_minima[i] << ", " << combined_histogram_bin_maxima[i] << "], ";
  // }
  // std::cout << std::endl;
  // TODO: Fix these names (╯°□°）╯︵ ┻━┻
  for (auto i = BinID{0}; i < bin_count;) {
    auto defacto_bin_start = std::max(current_bin_start, combined_histogram_bin_minima[i]);
    const auto estimate_1 = splitted_histogram_1->estimate_cardinality_and_distinct_count(
        PredicateCondition::BetweenInclusive, defacto_bin_start, combined_histogram_bin_maxima[i]);
    const auto estimate_2 = splitted_histogram_2->estimate_cardinality_and_distinct_count(
        PredicateCondition::BetweenInclusive, defacto_bin_start, combined_histogram_bin_maxima[i]);
    const auto cardinality = estimate_1.first + estimate_2.first;
    const auto distinct_count =
        combineDistinctCounts(combined_histogram_bin_minima[i], combined_histogram_bin_maxima[i], cardinality,
                              estimate_1.second, estimate_2.second);  // TODO: Very very wrong
    auto current_bin_distinct_count_target = distinct_count_per_bin_target;
    // std::cout << "Bin " << i << " " << defacto_bin_start << " -> " << combined_histogram_bin_maxima[i] << " : " << distinct_count << std::endl;

    if (current_bin_count < bin_count_with_extra_value) {
      current_bin_distinct_count_target++;
    }
    if ((current_bin_distinct_count + distinct_count < current_bin_distinct_count_target) && (i != bin_count - 1)) {
      // std::cout << "Bin still has some space: " <<current_bin_distinct_count+distinct_count - current_bin_distinct_count_target<<  std::endl;
      current_bin_distinct_count += distinct_count;
      current_bin_height += cardinality;
      i++;
    } else {
      // std::cout << "Bin finished." << std::endl;
      const auto remaining_distinct_count_to_fill = current_bin_distinct_count_target - current_bin_distinct_count;
      const auto bin_split_ratio = remaining_distinct_count_to_fill / distinct_count;
      const auto split_bin_maximum =
          combined_histogram_bin_minima[i] +
          bin_lerp(combined_histogram_bin_maxima[i], combined_histogram_bin_minima[i], bin_split_ratio);
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
        current_bin_start = combined_histogram_bin_maxima[i];
      }

      current_bin_count++;
      current_bin_height = 0;
      current_bin_distinct_count = 0;

      if ((bin_split_ratio == 1.0) || (current_bin_count == bin_count_target)) {
        i++;
      }
    }
  }

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
