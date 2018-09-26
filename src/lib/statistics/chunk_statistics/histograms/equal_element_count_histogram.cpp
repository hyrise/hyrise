#include "equal_element_count_histogram.hpp"

#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "histogram_utils.hpp"

namespace opossum {

template <typename T>
EqualElementCountHistogram<T>::EqualElementCountHistogram(const std::vector<T>& mins, const std::vector<T>& maxs,
                                                          const std::vector<HistogramCountType>& heights,
                                                          const HistogramCountType distinct_count_per_bin,
                                                          const BinID bin_count_with_extra_value)
    : AbstractHistogram<T>(),
      _mins(mins),
      _maxs(maxs),
      _heights(heights),
      _distinct_count_per_bin(distinct_count_per_bin),
      _bin_count_with_extra_value(bin_count_with_extra_value) {
  DebugAssert(mins.size() > 0, "Cannot have histogram without any bins.");
  DebugAssert(mins.size() == maxs.size(), "Must have the same number of lower as upper bin edges.");
  DebugAssert(mins.size() == heights.size(), "Must have the same number of edges and heights.");
  DebugAssert(distinct_count_per_bin > 0, "Cannot have bins with no distinct values.");
  DebugAssert(bin_count_with_extra_value < mins.size(), "Cannot have more bins with extra value than bins.");

  for (auto bin_id = 0u; bin_id < mins.size(); bin_id++) {
    DebugAssert(heights[bin_id] > 0, "Cannot have empty bins.");
    DebugAssert(mins[bin_id] <= maxs[bin_id], "Cannot have overlapping bins.");

    if (bin_id < maxs.size() - 1) {
      DebugAssert(mins[bin_id] < mins[bin_id + 1], "Bins must be sorted and cannot overlap.");
      DebugAssert(maxs[bin_id] < maxs[bin_id + 1], "Bins must be sorted and cannot overlap.");
    }
  }
}

template <>
EqualElementCountHistogram<std::string>::EqualElementCountHistogram(const std::vector<std::string>& mins,
                                                                    const std::vector<std::string>& maxs,
                                                                    const std::vector<HistogramCountType>& heights,
                                                                    const HistogramCountType distinct_count_per_bin,
                                                                    const BinID bin_count_with_extra_value,
                                                                    const std::string& supported_characters,
                                                                    const uint32_t string_prefix_length)
    : AbstractHistogram<std::string>(supported_characters, string_prefix_length),
      _mins(mins),
      _maxs(maxs),
      _heights(heights),
      _distinct_count_per_bin(distinct_count_per_bin),
      _bin_count_with_extra_value(bin_count_with_extra_value) {
  DebugAssert(mins.size() > 0, "Cannot have histogram without any bins.");
  DebugAssert(mins.size() == maxs.size(), "Must have the same number of lower as upper bin edges.");
  DebugAssert(mins.size() == heights.size(), "Must have the same number of edges and heights.");
  DebugAssert(distinct_count_per_bin > 0, "Cannot have bins with no distinct values.");
  DebugAssert(bin_count_with_extra_value < mins.size(), "Cannot have more bins with extra value than bins.");

  for (auto bin_id = 0u; bin_id < mins.size(); bin_id++) {
    DebugAssert(heights[bin_id] > 0, "Cannot have empty bins.");
    DebugAssert(mins[bin_id].find_first_not_of(supported_characters) == std::string::npos, "Unsupported characters.");
    DebugAssert(maxs[bin_id].find_first_not_of(supported_characters) == std::string::npos, "Unsupported characters.");
    DebugAssert(mins[bin_id] <= maxs[bin_id], "Cannot have upper bin edge higher than lower bin edge.");

    if (bin_id < maxs.size() - 1) {
      DebugAssert(mins[bin_id] < mins[bin_id + 1], "Bins must be sorted and cannot overlap.");
      DebugAssert(maxs[bin_id] < maxs[bin_id + 1], "Bins must be sorted and cannot overlap.");
    }
  }
}

template <typename T>
EqualElementCountBinStats<T> EqualElementCountHistogram<T>::_get_bin_stats(
    const std::vector<std::pair<T, HistogramCountType>>& value_counts, const BinID max_bin_count) {
  // If there are fewer distinct values than the number of desired bins use that instead.
  const auto bin_count = value_counts.size() < max_bin_count ? static_cast<BinID>(value_counts.size()) : max_bin_count;

  // Split values evenly among bins.
  const HistogramCountType distinct_count_per_bin = static_cast<HistogramCountType>(value_counts.size() / bin_count);
  const BinID bin_count_with_extra_value = value_counts.size() % bin_count;

  std::vector<T> mins;
  std::vector<T> maxs;
  std::vector<HistogramCountType> heights;
  mins.reserve(bin_count);
  maxs.reserve(bin_count);
  heights.reserve(bin_count);

  auto begin_index = 0ul;
  for (BinID bin_index = 0; bin_index < bin_count; bin_index++) {
    auto end_index = begin_index + distinct_count_per_bin - 1;
    if (bin_index < bin_count_with_extra_value) {
      end_index++;
    }

    mins.emplace_back(value_counts[begin_index].first);
    maxs.emplace_back(value_counts[end_index].first);
    heights.emplace_back(std::accumulate(
        value_counts.cbegin() + begin_index, value_counts.cbegin() + end_index + 1, HistogramCountType{0},
        [](HistogramCountType a, const std::pair<T, HistogramCountType>& b) { return a + b.second; }));

    begin_index = end_index + 1;
  }

  return {mins, maxs, heights, distinct_count_per_bin, bin_count_with_extra_value};
}

template <typename T>
std::shared_ptr<EqualElementCountHistogram<T>> EqualElementCountHistogram<T>::from_segment(
    const std::shared_ptr<const BaseSegment>& segment, const BinID max_bin_count,
    const std::optional<std::string>& supported_characters, const std::optional<uint32_t>& string_prefix_length) {
  const auto value_counts = AbstractHistogram<T>::_value_counts_in_segment(segment);

  if (value_counts.empty()) {
    return nullptr;
  }

  const auto bin_stats = EqualElementCountHistogram<T>::_get_bin_stats(value_counts, max_bin_count);

  if constexpr (std::is_same_v<T, std::string>) {
    const auto [characters, prefix_length] =  // NOLINT (Extra space before [)
        get_default_or_check_prefix_settings(supported_characters, string_prefix_length);
    return std::make_shared<EqualElementCountHistogram<T>>(
        bin_stats.mins, bin_stats.maxs, bin_stats.heights, bin_stats.distinct_count_per_bin,
        bin_stats.bin_count_with_extra_value, characters, prefix_length);
  } else {
    DebugAssert(!static_cast<bool>(supported_characters) && !static_cast<bool>(string_prefix_length),
                "Do not provide string prefix prefix arguments for non-string histograms.");
    return std::make_shared<EqualElementCountHistogram<T>>(bin_stats.mins, bin_stats.maxs, bin_stats.heights,
                                                           bin_stats.distinct_count_per_bin,
                                                           bin_stats.bin_count_with_extra_value);
  }
}

template <typename T>
HistogramType EqualElementCountHistogram<T>::histogram_type() const {
  return HistogramType::EqualElementCount;
}

template <typename T>
BinID EqualElementCountHistogram<T>::bin_count() const {
  return _heights.size();
}

template <typename T>
BinID EqualElementCountHistogram<T>::_bin_for_value(const T value) const {
  const auto it = std::lower_bound(_maxs.cbegin(), _maxs.cend(), value);
  const auto index = static_cast<BinID>(std::distance(_maxs.cbegin(), it));

  if (it == _maxs.cend() || value < _bin_min(index) || value > _bin_max(index)) {
    return INVALID_BIN_ID;
  }

  return index;
}

template <typename T>
BinID EqualElementCountHistogram<T>::_upper_bound_for_value(const T value) const {
  const auto it = std::upper_bound(_maxs.cbegin(), _maxs.cend(), value);

  if (it == _maxs.cend()) {
    return INVALID_BIN_ID;
  }

  return static_cast<BinID>(std::distance(_maxs.cbegin(), it));
}

template <typename T>
T EqualElementCountHistogram<T>::_bin_min(const BinID index) const {
  DebugAssert(index < _mins.size(), "Index is not a valid bin.");
  return _mins[index];
}

template <typename T>
T EqualElementCountHistogram<T>::_bin_max(const BinID index) const {
  DebugAssert(index < _maxs.size(), "Index is not a valid bin.");
  return _maxs[index];
}

template <typename T>
HistogramCountType EqualElementCountHistogram<T>::_bin_height(const BinID index) const {
  DebugAssert(index < _heights.size(), "Index is not a valid bin.");
  return _heights[index];
}

template <typename T>
HistogramCountType EqualElementCountHistogram<T>::_bin_distinct_count(const BinID index) const {
  DebugAssert(index < this->bin_count(), "Index is not a valid bin.");
  return _distinct_count_per_bin + (index < _bin_count_with_extra_value ? 1 : 0);
}

template <typename T>
HistogramCountType EqualElementCountHistogram<T>::total_count() const {
  return std::accumulate(_heights.cbegin(), _heights.cend(), HistogramCountType{0});
}

template <typename T>
HistogramCountType EqualElementCountHistogram<T>::total_distinct_count() const {
  return _distinct_count_per_bin * bin_count() + _bin_count_with_extra_value;
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(EqualElementCountHistogram);

}  // namespace opossum
