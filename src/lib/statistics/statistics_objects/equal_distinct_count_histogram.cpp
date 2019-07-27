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
std::unordered_map<T, HistogramCountType> add_segment_to_value_distribution(
    const BaseSegment& segment, std::unordered_map<T, HistogramCountType> value_distribution,
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

  return value_distribution;
}

template <typename T>
std::vector<std::pair<T, HistogramCountType>> value_distribution_from_column(const Table& table,
                                                                             const ColumnID column_id,
                                                                             const HistogramDomain<T>& domain) {
  // TODO(anybody) If you want to look into performance, this would probably benefit greatly from monotonic buffer
  //               resources.
  std::unordered_map<T, HistogramCountType> value_distribution_map;

  const auto chunk_count = table.chunk_count();
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto chunk = table.get_chunk(chunk_id);
    if (!chunk) continue;

    value_distribution_map =
        add_segment_to_value_distribution<T>(*chunk->get_segment(column_id), std::move(value_distribution_map), domain);
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
  _total_distinct_count =
      static_cast<HistogramCountType>(_distinct_count_per_bin * bin_count() + _bin_count_with_extra_value);
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
T EqualDistinctCountHistogram<T>::bin_minimum(const BinID index) const {
  DebugAssert(index < _bin_minima.size(), "Index is not a valid bin.");
  return _bin_minima[index];
}

template <typename T>
T EqualDistinctCountHistogram<T>::bin_maximum(const BinID index) const {
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
