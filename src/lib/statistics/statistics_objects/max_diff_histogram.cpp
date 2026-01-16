#include "max_diff_histogram.hpp"

#include <cstddef>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include <boost/sort/pdqsort/pdqsort.hpp>
#include <boost/unordered/unordered_flat_map.hpp>

#include "all_type_variant.hpp"
#include "statistics/statistics_objects/abstract_histogram.hpp"
#include "statistics/statistics_objects/histogram_domain.hpp"
#include "storage/abstract_segment.hpp"
#include "storage/segment_iterate.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace {

using namespace hyrise;  // NOLINT

// Think of this as an unordered_map<T, HistogramCountType>. The hash, equals, and allocator template parameter are
// defaults so that we can set the last parameter. It controls whether the hash for a value should be cached. Doing
// so reduces the cost of rehashing at the cost of slightly higher memory consumption. We only do it for strings,
// where hashing is somewhat expensive.
template <typename T>
using ValueDistributionMap = boost::unordered_flat_map<T, HistogramCountType>;

template <typename T>
void add_segment_to_value_distribution(const AbstractSegment& segment, ValueDistributionMap<T>& value_distribution,
                                       const HistogramDomain<T>& domain) {
  segment_iterate<T>(segment, [&](const auto& iterator_value) {
    if (iterator_value.is_null()) {
      return;
    }

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
  auto value_distribution_map = ValueDistributionMap<T>{};
  const auto chunk_count = table.chunk_count();
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto chunk = table.get_chunk(chunk_id);
    if (!chunk) {
      continue;
    }

    add_segment_to_value_distribution<T>(*chunk->get_segment(column_id), value_distribution_map, domain);
  }

  auto value_distribution =
      std::vector<std::pair<T, HistogramCountType>>{value_distribution_map.begin(), value_distribution_map.end()};
  value_distribution_map.clear();  // Maps can be large and sorting slow. Free space early.
  boost::sort::pdqsort(value_distribution.begin(), value_distribution.end(), [&](const auto& lhs, const auto& rhs) {
    return lhs.first < rhs.first;
  });

  return value_distribution;
}
}  // namespace

namespace hyrise {

template <typename T>
MaxDiffHistogram<T>::MaxDiffHistogram(std::vector<T>&& bin_minima, std::vector<T>&& bin_maxima,
                                      std::vector<HistogramCountType>&& bin_heights,
                                      std::vector<HistogramCountType>&& bin_distinct_counts,
                                      const HistogramDomain<T>& domain)
    : AbstractHistogram<T>(domain),
      _bin_minima(std::move(bin_minima)),
      _bin_maxima(std::move(bin_maxima)),
      _bin_heights(std::move(bin_heights)),
      _bin_distinct_counts(std::move(bin_distinct_counts)) {
  Assert(_bin_minima.size() == _bin_maxima.size(), "Must have the same number of lower as upper bin edges.");
  Assert(_bin_minima.size() == _bin_heights.size(), "Must have the same number of edges and heights.");
  Assert(_bin_minima.size() == _bin_distinct_counts.size(), "Must have the same number of edges and distinct counts.");
  // Assert(_distinct_count_per_bin > 0, "Cannot have bins with no distinct values.");
  // Assert(_bin_count_with_extra_value < _bin_minima.size(), "Cannot have more bins with extra value than bins.");

  AbstractHistogram<T>::_assert_bin_validity();

  _total_count = std::accumulate(_bin_heights.cbegin(), _bin_heights.cend(), HistogramCountType{0});
  _total_distinct_count =
      std::accumulate(_bin_distinct_counts.cbegin(), _bin_distinct_counts.cend(), HistogramCountType{0});
}

template <typename T>
std::shared_ptr<MaxDiffHistogram<T>> MaxDiffHistogram<T>::from_column(const Table& table, const ColumnID column_id,
                                                                      const BinID max_bin_count,
                                                                      const HistogramDomain<T>& domain) {
  Assert(max_bin_count > 0, "max_bin_count must be greater than zero ");

  const auto value_distribution = value_distribution_from_column(table, column_id, domain);

  if (value_distribution.empty()) {
    return nullptr;
  }

  std::vector<std::pair<int, int>> frequency_diffs_idx =
      std::vector<std::pair<int, int>>(value_distribution.size() - 1);
  for (auto i = size_t{1}; i < value_distribution.size(); ++i) {
    frequency_diffs_idx[i - 1] =
        std::make_pair(std::abs(value_distribution[i].second - value_distribution[i - 1].second), i - 1);
  }

  frequency_diffs_idx.push_back(
      std::make_pair(0, 0));  // In case number of distinct values equals bin count, we also "split in the beginning"

  boost::sort::pdqsort(frequency_diffs_idx.begin(), frequency_diffs_idx.end(), [&](const auto& lhs, const auto& rhs) {
    return lhs.first > rhs.first;
  });

  // If there are fewer distinct values than the number of desired bins use that instead.
  const auto bin_count =
      value_distribution.size() < max_bin_count ? static_cast<BinID>(value_distribution.size()) : max_bin_count;

  std::vector<BinID> bin_boundaries = std::vector<BinID>(bin_count);

  if (value_distribution.size() == table.row_count() || frequency_diffs_idx[bin_count - 1].first <= 1) {
    // In case all values are distinct or their frequencies are very similar, 
    // we create equal distinct count histogram by splitting the values equally
    // TODO(paulroes): correct boundaries so buckets are more balanced.
    auto bins_with_extra_value = value_distribution.size() % bin_count;
    for (auto i = BinID{0}; i < bin_count; ++i) {
      bin_boundaries[i] = static_cast<BinID>((i + 1) * (value_distribution.size() / bin_count)) - 1;
      if (i < bins_with_extra_value) {
        bin_boundaries[i] += i + 1;
      } else {
        bin_boundaries[i] += bins_with_extra_value;
      }
    }
  } else {
    for (auto i = BinID{0}; i < bin_count; ++i) {
      bin_boundaries[i] = frequency_diffs_idx[i].second;
    }
    boost::sort::pdqsort(bin_boundaries.begin(), bin_boundaries.end());
  }

  std::vector<T> bin_minima(bin_count);
  std::vector<T> bin_maxima(bin_count);
  std::vector<HistogramCountType> bin_heights(bin_count);

  std::vector<HistogramCountType> bin_distinct_counts(bin_count);

  // `min_value_idx` and `max_value_idx` are indices into the sorted vector `value_distribution`
  // describing which range of distinct values goes into a bin
  auto min_value_idx = BinID{0};
  for (BinID bin_idx = 0; bin_idx < bin_count; bin_idx++) {
    auto max_value_idx = bin_idx == bin_count - 1 ? value_distribution.size() - 1 : bin_boundaries[bin_idx];

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
                        [](HistogramCountType bin_height, const std::pair<T, HistogramCountType>& value_and_count) {
                          return bin_height + value_and_count.second;
                        });

    bin_distinct_counts[bin_idx] = static_cast<HistogramCountType>(max_value_idx - min_value_idx + 1);

    min_value_idx = max_value_idx + 1;
  }

  return std::make_shared<MaxDiffHistogram<T>>(std::move(bin_minima), std::move(bin_maxima), std::move(bin_heights),
                                               std::move(bin_distinct_counts));
}

template <typename T>
std::string MaxDiffHistogram<T>::name() const {
  return "MaxDiffHistogram";
}

template <typename T>
std::shared_ptr<AbstractHistogram<T>> MaxDiffHistogram<T>::clone() const {
  // The new histogram needs a copy of the data
  auto bin_minima_copy = _bin_minima;
  auto bin_maxima_copy = _bin_maxima;
  auto bin_heights_copy = _bin_heights;
  auto bin_distinct_counts_copy = _bin_distinct_counts;

  return std::make_shared<MaxDiffHistogram<T>>(std::move(bin_minima_copy), std::move(bin_maxima_copy),
                                               std::move(bin_heights_copy), std::move(bin_distinct_counts_copy));
}

template <typename T>
BinID MaxDiffHistogram<T>::bin_count() const {
  return _bin_heights.size();
}

template <typename T>
BinID MaxDiffHistogram<T>::bin_for_value(const T& value) const {
  const auto iter = std::lower_bound(_bin_maxima.cbegin(), _bin_maxima.cend(), value);
  const auto index = static_cast<BinID>(std::distance(_bin_maxima.cbegin(), iter));

  if (iter == _bin_maxima.cend() || value < bin_minimum(index) || value > bin_maximum(index)) {
    return INVALID_BIN_ID;
  }

  return index;
}

template <typename T>
BinID MaxDiffHistogram<T>::next_bin_for_value(const T& value) const {
  const auto it = std::upper_bound(_bin_maxima.cbegin(), _bin_maxima.cend(), value);

  if (it == _bin_maxima.cend()) {
    return INVALID_BIN_ID;
  }

  return static_cast<BinID>(std::distance(_bin_maxima.cbegin(), it));
}

template <typename T>
const T& MaxDiffHistogram<T>::bin_minimum(const BinID index) const {
  DebugAssert(index < _bin_minima.size(), "Index is not a valid bin.");
  return _bin_minima[index];
}

template <typename T>
const T& MaxDiffHistogram<T>::bin_maximum(const BinID index) const {
  DebugAssert(index < _bin_maxima.size(), "Index is not a valid bin.");
  return _bin_maxima[index];
}

template <typename T>
HistogramCountType MaxDiffHistogram<T>::bin_height(const BinID index) const {
  DebugAssert(index < _bin_heights.size(), "Index is not a valid bin.");
  return _bin_heights[index];
}

template <typename T>
HistogramCountType MaxDiffHistogram<T>::bin_distinct_count(const BinID index) const {
  DebugAssert(index < bin_count(), "Index is not a valid bin.");
  return _bin_distinct_counts[index];
}

template <typename T>
HistogramCountType MaxDiffHistogram<T>::total_count() const {
  return _total_count;
}

template <typename T>
HistogramCountType MaxDiffHistogram<T>::total_distinct_count() const {
  return _total_distinct_count;
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(MaxDiffHistogram);

}  // namespace hyrise
