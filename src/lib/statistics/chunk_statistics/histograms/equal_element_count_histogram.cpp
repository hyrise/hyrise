#include "equal_element_count_histogram.hpp"

#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "histogram_utils.hpp"

namespace opossum {

template <typename T>
EqualElementCountHistogram<T>::EqualElementCountHistogram(const std::vector<T>& bin_minimums,
                                                          const std::vector<T>& bin_maximums,
                                                          const std::vector<HistogramCountType>& bin_heights,
                                                          const HistogramCountType distinct_count_per_bin,
                                                          const BinID bin_count_with_extra_value)
    : AbstractHistogram<T>(),
      _bin_minimums(bin_minimums),
      _bin_maximums(bin_maximums),
      _bin_heights(bin_heights),
      _distinct_count_per_bin(distinct_count_per_bin),
      _bin_count_with_extra_value(bin_count_with_extra_value) {
  DebugAssert(bin_minimums.size() > 0, "Cannot have histogram without any bins.");
  DebugAssert(bin_minimums.size() == bin_maximums.size(), "Must have the same number of lower as upper bin edges.");
  DebugAssert(bin_minimums.size() == bin_heights.size(), "Must have the same number of edges and heights.");
  DebugAssert(distinct_count_per_bin > 0, "Cannot have bins with no distinct values.");
  DebugAssert(bin_count_with_extra_value < bin_minimums.size(), "Cannot have more bins with extra value than bins.");

  for (BinID bin_id = 0; bin_id < bin_minimums.size(); bin_id++) {
    DebugAssert(bin_heights[bin_id] > 0, "Cannot have empty bins.");
    DebugAssert(bin_minimums[bin_id] <= bin_maximums[bin_id], "Cannot have overlapping bins.");

    if (bin_id < bin_maximums.size() - 1) {
      DebugAssert(bin_maximums[bin_id] < bin_minimums[bin_id + 1], "Bins must be sorted and cannot overlap.");
    }
  }
}

template <>
EqualElementCountHistogram<std::string>::EqualElementCountHistogram(const std::vector<std::string>& bin_minimums,
                                                                    const std::vector<std::string>& bin_maximums,
                                                                    const std::vector<HistogramCountType>& bin_heights,
                                                                    const HistogramCountType distinct_count_per_bin,
                                                                    const BinID bin_count_with_extra_value,
                                                                    const std::string& supported_characters,
                                                                    const uint32_t string_prefix_length)
    : AbstractHistogram<std::string>(supported_characters, string_prefix_length),
      _bin_minimums(bin_minimums),
      _bin_maximums(bin_maximums),
      _bin_heights(bin_heights),
      _distinct_count_per_bin(distinct_count_per_bin),
      _bin_count_with_extra_value(bin_count_with_extra_value) {
  DebugAssert(bin_minimums.size() > 0, "Cannot have histogram without any bins.");
  DebugAssert(bin_minimums.size() == bin_maximums.size(), "Must have the same number of lower as upper bin edges.");
  DebugAssert(bin_minimums.size() == bin_heights.size(), "Must have the same number of edges and heights.");
  DebugAssert(distinct_count_per_bin > 0, "Cannot have bins with no distinct values.");
  DebugAssert(bin_count_with_extra_value < bin_minimums.size(), "Cannot have more bins with extra value than bins.");

  for (BinID bin_id = 0u; bin_id < bin_minimums.size(); bin_id++) {
    DebugAssert(bin_heights[bin_id] > 0, "Cannot have empty bins.");
    DebugAssert(bin_minimums[bin_id].find_first_not_of(supported_characters) == std::string::npos,
                "Unsupported characters.");
    DebugAssert(bin_maximums[bin_id].find_first_not_of(supported_characters) == std::string::npos,
                "Unsupported characters.");
    DebugAssert(bin_minimums[bin_id] <= bin_maximums[bin_id], "Cannot have upper bin edge higher than lower bin edge.");

    if (bin_id < bin_maximums.size() - 1) {
      DebugAssert(bin_maximums[bin_id] < bin_minimums[bin_id + 1], "Bins must be sorted and cannot overlap.");
    }
  }
}

template <typename T>
EqualElementCountBinData<T> EqualElementCountHistogram<T>::_build_bins(
    const std::vector<std::pair<T, HistogramCountType>>& value_counts, const BinID max_bin_count) {
  // If there are fewer distinct values than the number of desired bins use that instead.
  const auto bin_count = value_counts.size() < max_bin_count ? static_cast<BinID>(value_counts.size()) : max_bin_count;

  // Split values evenly among bins.
  const auto distinct_count_per_bin = static_cast<HistogramCountType>(value_counts.size() / bin_count);
  const BinID bin_count_with_extra_value = value_counts.size() % bin_count;

  std::vector<T> bin_minimums;
  std::vector<T> bin_maximums;
  std::vector<HistogramCountType> bin_heights;
  bin_minimums.reserve(bin_count);
  bin_maximums.reserve(bin_count);
  bin_heights.reserve(bin_count);

  auto current_bin_begin_index = BinID{0};
  for (BinID bin_index = 0; bin_index < bin_count; bin_index++) {
    auto current_bin_end_index = current_bin_begin_index + distinct_count_per_bin - 1;
    if (bin_index < bin_count_with_extra_value) {
      current_bin_end_index++;
    }

    bin_minimums.emplace_back(value_counts[current_bin_begin_index].first);
    bin_maximums.emplace_back(value_counts[current_bin_end_index].first);
    bin_heights.emplace_back(
        std::accumulate(value_counts.cbegin() + current_bin_begin_index,
                        value_counts.cbegin() + current_bin_end_index + 1, HistogramCountType{0},
                        [](HistogramCountType a, const std::pair<T, HistogramCountType>& b) { return a + b.second; }));

    current_bin_begin_index = current_bin_end_index + 1;
  }

  return {bin_minimums, bin_maximums, bin_heights, distinct_count_per_bin, bin_count_with_extra_value};
}

template <typename T>
std::shared_ptr<EqualElementCountHistogram<T>> EqualElementCountHistogram<T>::from_segment(
    const std::shared_ptr<const BaseSegment>& segment, const BinID max_bin_count,
    const std::optional<std::string>& supported_characters, const std::optional<uint32_t>& string_prefix_length) {
  const auto value_counts = AbstractHistogram<T>::_gather_value_distribution(segment);

  if (value_counts.empty()) {
    return nullptr;
  }

  const auto bins = EqualElementCountHistogram<T>::_build_bins(value_counts, max_bin_count);

  if constexpr (std::is_same_v<T, std::string>) {
    const auto [characters, prefix_length] =  // NOLINT (Extra space before [)
        get_default_or_check_string_histogram_prefix_settings(supported_characters, string_prefix_length);
    return std::make_shared<EqualElementCountHistogram<T>>(bins.bin_minimums, bins.bin_maximums, bins.bin_heights,
                                                           bins.distinct_count_per_bin, bins.bin_count_with_extra_value,
                                                           characters, prefix_length);
  } else {
    DebugAssert(!supported_characters && !string_prefix_length,
                "Do not provide string prefix prefix arguments for non-string histograms.");
    return std::make_shared<EqualElementCountHistogram<T>>(bins.bin_minimums, bins.bin_maximums, bins.bin_heights,
                                                           bins.distinct_count_per_bin,
                                                           bins.bin_count_with_extra_value);
  }
}

template <typename T>
HistogramType EqualElementCountHistogram<T>::histogram_type() const {
  return HistogramType::EqualElementCount;
}

template <typename T>
std::string EqualElementCountHistogram<T>::histogram_name() const {
  return "EqualElementCount";
}

template <typename T>
BinID EqualElementCountHistogram<T>::bin_count() const {
  return _bin_heights.size();
}

template <typename T>
BinID EqualElementCountHistogram<T>::_bin_for_value(const T value) const {
  const auto it = std::lower_bound(_bin_maximums.cbegin(), _bin_maximums.cend(), value);
  const auto index = static_cast<BinID>(std::distance(_bin_maximums.cbegin(), it));

  if (it == _bin_maximums.cend() || value < _bin_minimum(index) || value > _bin_maximum(index)) {
    return INVALID_BIN_ID;
  }

  return index;
}

template <typename T>
BinID EqualElementCountHistogram<T>::_next_bin(const T value) const {
  const auto it = std::upper_bound(_bin_maximums.cbegin(), _bin_maximums.cend(), value);

  if (it == _bin_maximums.cend()) {
    return INVALID_BIN_ID;
  }

  return static_cast<BinID>(std::distance(_bin_maximums.cbegin(), it));
}

template <typename T>
T EqualElementCountHistogram<T>::_bin_minimum(const BinID index) const {
  DebugAssert(index < _bin_minimums.size(), "Index is not a valid bin.");
  return _bin_minimums[index];
}

template <typename T>
T EqualElementCountHistogram<T>::_bin_maximum(const BinID index) const {
  DebugAssert(index < _bin_maximums.size(), "Index is not a valid bin.");
  return _bin_maximums[index];
}

template <typename T>
HistogramCountType EqualElementCountHistogram<T>::_bin_height(const BinID index) const {
  DebugAssert(index < _bin_heights.size(), "Index is not a valid bin.");
  return _bin_heights[index];
}

template <typename T>
HistogramCountType EqualElementCountHistogram<T>::_bin_distinct_count(const BinID index) const {
  DebugAssert(index < this->bin_count(), "Index is not a valid bin.");
  return _distinct_count_per_bin + (index < _bin_count_with_extra_value ? 1 : 0);
}

template <typename T>
HistogramCountType EqualElementCountHistogram<T>::total_count() const {
  return std::accumulate(_bin_heights.cbegin(), _bin_heights.cend(), HistogramCountType{0});
}

template <typename T>
HistogramCountType EqualElementCountHistogram<T>::total_distinct_count() const {
  return _distinct_count_per_bin * bin_count() + _bin_count_with_extra_value;
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(EqualElementCountHistogram);

}  // namespace opossum
