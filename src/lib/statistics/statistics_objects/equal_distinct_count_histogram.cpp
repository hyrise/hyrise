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


template <typename T>
std::shared_ptr<EqualDistinctCountHistogram<T>> EqualDistinctCountHistogram<T>::merge(std::shared_ptr<EqualDistinctCountHistogram<T>> histogram_1,
                                                               std::shared_ptr<EqualDistinctCountHistogram<T>> histogram_2) {
                                                                 //Please make sure that the input histograms are valid and non empty.
  // auto bounds_1 = histogram_1.bin_bounds();
  // auto bounds_2 = histogram_2.bin_bounds();

  // h1 [(10,50),(51,100)]
  // h2 [(40,50),(51,60)]
  // merged_histogram_1 = [(10,40),(41,50),(51,60),(61,100)]
  // merged_histogram_2 = [(40,40),(41,41),(41,50),(51,60)]

  std::cout << "Histogram_1 {";
  for(auto & bin : histogram_1->bin_bounds()) {
    std::cout << "[" << bin.first << ", " << bin.second << " (";
    const auto estimate = histogram_1->estimate_cardinality_and_distinct_count(PredicateCondition::BetweenInclusive, bin.first, bin.second);
    std::cout << estimate.first << ", ";
    std::cout << estimate.second << ")], ";
  }
  std::cout << "}"<<std::endl;
  std::cout << "Histogram_2 {";
  for(auto & bin : histogram_2->bin_bounds()) {
    std::cout << "[" << bin.first << ", " << bin.second << " (";
    const auto estimate = histogram_2->estimate_cardinality_and_distinct_count(PredicateCondition::BetweenInclusive, bin.first, bin.second);
    std::cout << estimate.first << ", ";
    std::cout << estimate.second << ")], ";
  }
  std::cout << "}"<<std::endl;

  auto merged_histogram_1 = histogram_1->split_at_bin_bounds(histogram_2->bin_bounds());
  auto merged_histogram_2 = histogram_2->split_at_bin_bounds(merged_histogram_1->bin_bounds());
  

  std::cout << "merged_histogram_1 {";
  for(auto & bin : merged_histogram_1->bin_bounds()) {
    std::cout << "[" << bin.first << ", " << bin.second << " (";
    const auto estimate = merged_histogram_1->estimate_cardinality_and_distinct_count(PredicateCondition::BetweenInclusive, bin.first, bin.second);
    std::cout << estimate.first << ", ";
    std::cout << estimate.second << ")], ";
  }
  std::cout << "}"<<std::endl;


  std::cout << "merged_histogram_2 {";
  for(auto & bin : merged_histogram_2->bin_bounds()) {
    std::cout << "[" << bin.first << ", " << bin.second << " (";
    const auto estimate = merged_histogram_2->estimate_cardinality_and_distinct_count(PredicateCondition::BetweenInclusive, bin.first, bin.second);
    std::cout << estimate.first << ", ";
    std::cout << estimate.second << ")], ";
  }
  std::cout << "}"<<std::endl;

  return nullptr;
  // std::vector<T> merged_bounds;
  // merged_bounds.insert(merged_bounds.end(), histogram_1._bin_minima.begin(), histogram_1._bin_minima.end());
  // merged_bounds.insert(merged_bounds.end(), histogram_2._bin_minima.begin(), histogram_2._bin_minima.end());
  // merged_bounds.insert(merged_bounds.end(), histogram_1._bin_maxima.begin(), histogram_1._bin_maxima.end());
  // merged_bounds.insert(merged_bounds.end(), histogram_2._bin_maxima.begin(), histogram_2._bin_maxima.end());
  // std::sort(merged_bounds.begin(), merged_bounds.end());
  // auto last = std::unique(merged_bounds.begin(), merged_bounds.end());
  // merged_bounds.erase(last, merged_bounds.end());

  // std::vector<std::pair<T,T>> potential_bins;
  // const auto bounds_length = merged_bounds.size();
  // for (auto bounds_index = 1; bounds_index < bounds_length, bounds_index++) {
  //   potential_bounds.push_back(merged_bounds[bounds_index-1], merged_bounds[bounds_index]);
  // }

  // auto it_bounds_1 = bounds_1.begin();
  // auto it_bounds_2 = bounds_2.begin();
  // auto it_1_first_valid = true;
  // auto it_2_first_valid = true;

  // std::vector<std::pair<T,T>> potential_bins;
  // std::vector<T> bin_maxima, bin_minima;
  // std::vector<HistogramCountType> bin_heights;
  // auto boundary = std::min(it_bounds_1->first, it_bounds_2->first);
  /*
  [(10, 70),(70,80)]
  [(50, 60),(62,67)]

  [(10, 50)]

  [(50, 70),(70,80)]
  [(50, 60),(62,67)]

  [(50, 60)]

  [(60, 70),(70,80)]
  [(62,67)]

  [(60,62)]
  
  [(62, 70),(70,80)]
  [(62,67)]

  [(10,50), (50,60),(60,62),(62,67), (67,70), (70,80)]
  [   5        4        3     3         5         4  ]

n Bins mit gleich großen Values
n= 3
-> 8 values per bin

-Split (50,60) - 3 to left, 1 to right

   * E.g., for a histogram with bins {[0, 10], [15, 20]}, split_at_bin_bounds({{-4, 5}, {16, 18}}) returns
   * a histogram with bins {[0, 5], [6, 10], [15, 15], [16, 18], [19, 20]}
  [(10, 70),(70,80)] -> split at [(50,60), (62,67)]
  [(50, 60),(62,67)]

  {[10, 50], [51,60], [61,62],[63,67],[68,70], [71,80]}
  {[49,50],[51,60], [61,62],[63,67]}


  -> [(10,50), (50,51),(51,60), (60,100)]
  -> [(10,50), (51,60), (61,100)]
  -> [(10.0,50.0), (50.0000000001,60.0), (60.00000000001,100.0)]
 */

 //Not a good idea...
  // while (it_bounds_1 != bounds_1.end() && it_bounds_2 != bounds_2.end()) {
  //   T bin_max, bin_min;
  //   if (it_bounds_1->first < it_bounds_2->first){
  //     bin_min = it_bounds_1->first;
  //     bin_max = std::min(it_bounds_1->second, it_bounds_2->first);
  //     if (bin_max == it_bounds_1->second) {
  //       it_bounds_1++;
  //     }
  //     else {
  //       if (it_bounds_1->second != it_bounds_2->first) {
  //         it_bounds_1->first = it_bounds_2->first;
  //       }
  //       else {
  //         it_bounds_1++;
  //       }
        
  //     }
  //   } else if (it_bounds_1->first > it_bounds_2->first) {
  //     bin_min = it_bounds_2->first;
  //     bin_max = std::min(it_bounds_2->second, it_bounds_1->first);
  //     if (bin_max == it_bounds_2->second) {
  //       it_bounds_2++;
  //     }
  //     else {
  //       if (it_bounds_2->second != it_bounds_1->first) {
  //         it_bounds_2->first = it_bounds_1->first;
  //       }
  //       else {
  //         it_bounds_2++;
  //       }

  //     }
  //   }
  //   else {
  //     //Equal
  //     bin_min = it_bounds_2->first;
  //     bin_max = std::min(it_bounds_1->second, it_bounds_2->second);
  //     if (bin_max == it_bounds_2->second) {
  //       if (it_bounds_1->first != it_bounds_2->second) {
  //         it_bounds_1->first = it_bounds_2->second;
  //       }
  //       else {
  //         it_bounds_1++;
  //       }
  //       it_bounds_2++;
  //     } else {
  //       if (it_bounds_2->first != it_bounds_1->second) {
  //         it_bounds_2->first = it_bounds_1->second;
  //       }
  //       else {
  //         it_bounds_2++;
  //       }
  //       it_bounds_1++;
  //     }
  //   }
    
  //   merged_bounds.push_back(std::make_pair(bin_min, bin_max));
  // }
  // merged_bounds.insert(merged_bounds.end(), it_bounds_1, bounds_1.end());
  // merged_bounds.insert(merged_bounds.end(), it_bounds_2, bounds_2.end());
 //Make the bins equal distinct count - at least probably.

 
  // auto merged = std::make_shared<EqualDistinctCountHistogram<T>>(EqualDistinctCountHistogram<T>(...));
  // return merged;
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
