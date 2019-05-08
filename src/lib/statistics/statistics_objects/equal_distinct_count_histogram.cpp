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
std::unordered_map<T, HistogramCountType> value_distribution_from_segment_impl(
    const BaseSegment& segment, std::unordered_map<T, HistogramCountType> value_distribution,
    const HistogramDomain<T>& domain) {
  segment_iterate<T>(segment, [&](const auto& iterator_value) {
    if (!iterator_value.is_null()) {
      if constexpr (std::is_same_v<T, pmr_string>) {
        if (domain.contains(iterator_value.value())) {
          ++value_distribution[iterator_value.value()];
        } else {
          ++value_distribution[domain.string_to_domain(iterator_value.value())];
        }
      } else {
        ++value_distribution[iterator_value.value()];
      }
    }
  });

  return value_distribution;
}

template <typename T>
std::vector<std::pair<T, HistogramCountType>> value_distribution_from_column(const Table& table,
                                                                             const ColumnID column_id,
                                                                             const HistogramDomain<T>& domain) {
  std::unordered_map<T, HistogramCountType> value_distribution_map;

  for (const auto& chunk : table.chunks()) {
    value_distribution_map = value_distribution_from_segment_impl<T>(*chunk->get_segment(column_id),
                                                                     std::move(value_distribution_map), domain);
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
      _bin_data({std::move(bin_minima), std::move(bin_maxima), std::move(bin_heights), distinct_count_per_bin,
                 bin_count_with_extra_value}) {
  Assert(_bin_data.bin_minima.size() == _bin_data.bin_maxima.size(),
         "Must have the same number of lower as upper bin edges.");
  Assert(_bin_data.bin_minima.size() == _bin_data.bin_heights.size(),
         "Must have the same number of edges and heights.");
  Assert(_bin_data.distinct_count_per_bin > 0, "Cannot have bins with no distinct values.");
  Assert(_bin_data.bin_count_with_extra_value < _bin_data.bin_minima.size(),
         "Cannot have more bins with extra value than bins.");

  AbstractHistogram<T>::_assert_bin_validity();
}

template <typename T>
EqualDistinctCountBinData<T> EqualDistinctCountHistogram<T>::_build_bins(
    std::vector<std::pair<T, HistogramCountType>>&& value_counts, const BinID max_bin_count) {
  // If there are fewer distinct values than the number of desired bins use that instead.
  const auto bin_count = value_counts.size() < max_bin_count ? static_cast<BinID>(value_counts.size()) : max_bin_count;

  // Split values evenly among bins.
  const auto distinct_count_per_bin = static_cast<size_t>(value_counts.size() / bin_count);
  const BinID bin_count_with_extra_value = value_counts.size() % bin_count;

  std::vector<T> bin_minima(bin_count);
  std::vector<T> bin_maxima(bin_count);
  std::vector<HistogramCountType> bin_heights(bin_count);

  auto current_bin_begin_index = BinID{0};
  for (BinID bin_index = 0; bin_index < bin_count; bin_index++) {
    auto current_bin_end_index = current_bin_begin_index + distinct_count_per_bin - 1;
    if (bin_index < bin_count_with_extra_value) {
      current_bin_end_index++;
    }

    // We'd like to move strings, but have to copy if we need the same string for the bin_maximum
    if (std::is_same_v<T, std::string> && current_bin_begin_index != current_bin_end_index) {
      bin_minima[bin_index] = std::move(value_counts[current_bin_begin_index].first);
    } else {
      bin_minima[bin_index] = value_counts[current_bin_begin_index].first;
    }

    bin_maxima[bin_index] = std::move(value_counts[current_bin_end_index].first);

    bin_heights[bin_index] =
        std::accumulate(value_counts.cbegin() + current_bin_begin_index,
                        value_counts.cbegin() + current_bin_end_index + 1, HistogramCountType{0},
                        [](HistogramCountType a, const std::pair<T, HistogramCountType>& b) { return a + b.second; });

    current_bin_begin_index = current_bin_end_index + 1;
  }

  return {std::move(bin_minima), std::move(bin_maxima), std::move(bin_heights),
          static_cast<HistogramCountType>(distinct_count_per_bin), bin_count_with_extra_value};
}

template <typename T>
std::shared_ptr<EqualDistinctCountHistogram<T>> EqualDistinctCountHistogram<T>::from_column(
    const Table& table, const ColumnID column_id, const BinID max_bin_count, const HistogramDomain<T>& domain) {
  Assert(max_bin_count > 0, "max_bin_count must be greater than zero ");

  auto value_distribution = value_distribution_from_column(table, column_id, domain);

  if (value_distribution.empty()) {
    return nullptr;
  }

  auto bins = EqualDistinctCountHistogram<T>::_build_bins(std::move(value_distribution), max_bin_count);

  return std::make_shared<EqualDistinctCountHistogram<T>>(std::move(bins.bin_minima), std::move(bins.bin_maxima),
                                                          std::move(bins.bin_heights), bins.distinct_count_per_bin,
                                                          bins.bin_count_with_extra_value, domain);
}

template <typename T>
std::string EqualDistinctCountHistogram<T>::histogram_name() const {
  return "EqualDistinctCount";
}

template <typename T>
std::shared_ptr<AbstractHistogram<T>> EqualDistinctCountHistogram<T>::clone() const {
  auto bin_minima = _bin_data.bin_minima;
  auto bin_maxima = _bin_data.bin_maxima;
  auto bin_heights = _bin_data.bin_heights;

  return std::make_shared<EqualDistinctCountHistogram<T>>(std::move(bin_minima), std::move(bin_maxima),
                                                          std::move(bin_heights), _bin_data.distinct_count_per_bin,
                                                          _bin_data.bin_count_with_extra_value);
}

template <typename T>
BinID EqualDistinctCountHistogram<T>::bin_count() const {
  return _bin_data.bin_heights.size();
}

template <typename T>
BinID EqualDistinctCountHistogram<T>::_bin_for_value(const T& value) const {
  const auto it = std::lower_bound(_bin_data.bin_maxima.cbegin(), _bin_data.bin_maxima.cend(), value);
  const auto index = static_cast<BinID>(std::distance(_bin_data.bin_maxima.cbegin(), it));

  if (it == _bin_data.bin_maxima.cend() || value < bin_minimum(index) || value > bin_maximum(index)) {
    return INVALID_BIN_ID;
  }

  return index;
}

template <typename T>
BinID EqualDistinctCountHistogram<T>::_next_bin_for_value(const T& value) const {
  const auto it = std::upper_bound(_bin_data.bin_maxima.cbegin(), _bin_data.bin_maxima.cend(), value);

  if (it == _bin_data.bin_maxima.cend()) {
    return INVALID_BIN_ID;
  }

  return static_cast<BinID>(std::distance(_bin_data.bin_maxima.cbegin(), it));
}

template <typename T>
T EqualDistinctCountHistogram<T>::bin_minimum(const BinID index) const {
  DebugAssert(index < _bin_data.bin_minima.size(), "Index is not a valid bin.");
  return _bin_data.bin_minima[index];
}

template <typename T>
T EqualDistinctCountHistogram<T>::bin_maximum(const BinID index) const {
  DebugAssert(index < _bin_data.bin_maxima.size(), "Index is not a valid bin.");
  return _bin_data.bin_maxima[index];
}

template <typename T>
HistogramCountType EqualDistinctCountHistogram<T>::bin_height(const BinID index) const {
  DebugAssert(index < _bin_data.bin_heights.size(), "Index is not a valid bin.");
  return _bin_data.bin_heights[index];
}

template <typename T>
HistogramCountType EqualDistinctCountHistogram<T>::bin_distinct_count(const BinID index) const {
  DebugAssert(index < bin_count(), "Index is not a valid bin.");
  return HistogramCountType{_bin_data.distinct_count_per_bin + (index < _bin_data.bin_count_with_extra_value ? 1 : 0)};
}

template <typename T>
HistogramCountType EqualDistinctCountHistogram<T>::total_count() const {
  return std::accumulate(_bin_data.bin_heights.cbegin(), _bin_data.bin_heights.cend(), HistogramCountType{0});
}

template <typename T>
HistogramCountType EqualDistinctCountHistogram<T>::total_distinct_count() const {
  return static_cast<HistogramCountType>(_bin_data.distinct_count_per_bin * bin_count() +
                                         _bin_data.bin_count_with_extra_value);
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(EqualDistinctCountHistogram);

}  // namespace opossum
