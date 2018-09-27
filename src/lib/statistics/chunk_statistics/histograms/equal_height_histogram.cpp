#include "equal_height_histogram.hpp"

#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "histogram_utils.hpp"

namespace opossum {

template <typename T>
EqualHeightHistogram<T>::EqualHeightHistogram(const std::vector<T>& bin_maximums,
                                              const std::vector<HistogramCountType>& bin_distinct_counts, T minimum,
                                              const HistogramCountType total_count)
    : AbstractHistogram<T>(),
      _bin_maximums(bin_maximums),
      _bin_distinct_counts(bin_distinct_counts),
      _minimum(minimum),
      _total_count(total_count) {
  DebugAssert(total_count > 0, "Cannot have histogram without any values.");
  DebugAssert(bin_maximums.size() > 0, "Cannot have histogram without any bins.");
  DebugAssert(bin_maximums.size() == bin_distinct_counts.size(), "Must have maxs and distinct counts for each bin.");
  DebugAssert(minimum <= bin_maximums[0], "Must have maxs and distinct counts for each bin.");

  for (BinID bin_id = 0; bin_id < bin_maximums.size(); bin_id++) {
    DebugAssert(bin_distinct_counts[bin_id] > 0, "Cannot have bins with no distinct values.");

    if (bin_id < bin_maximums.size() - 1) {
      DebugAssert(bin_maximums[bin_id] < bin_maximums[bin_id + 1], "Bins must be sorted and cannot overlap.");
    }
  }
}

template <>
EqualHeightHistogram<std::string>::EqualHeightHistogram(const std::vector<std::string>& bin_maximums,
                                                        const std::vector<HistogramCountType>& bin_distinct_counts,
                                                        const std::string& minimum,
                                                        const HistogramCountType total_count,
                                                        const std::string& supported_characters,
                                                        const uint32_t string_prefix_length)
    : AbstractHistogram<std::string>(supported_characters, string_prefix_length),
      _bin_maximums(bin_maximums),
      _bin_distinct_counts(bin_distinct_counts),
      _minimum(minimum),
      _total_count(total_count) {
  DebugAssert(total_count > 0, "Cannot have histogram without any values.");
  DebugAssert(bin_maximums.size() > 0, "Cannot have histogram without any bins.");
  DebugAssert(bin_maximums.size() == bin_distinct_counts.size(), "Must have maxs and distinct counts for each bin.");
  DebugAssert(minimum <= bin_maximums[0], "Must have maxs and distinct counts for each bin.");

  for (BinID bin_id = 0; bin_id < bin_maximums.size(); bin_id++) {
    DebugAssert(bin_distinct_counts[bin_id] > 0, "Cannot have bins with no distinct values.");
    DebugAssert(bin_maximums[bin_id].find_first_not_of(supported_characters) == std::string::npos,
                "Unsupported characters.");

    if (bin_id < bin_maximums.size() - 1) {
      DebugAssert(bin_maximums[bin_id] < bin_maximums[bin_id + 1], "Bins must be sorted and cannot overlap.");
    }
  }
}

template <typename T>
EqualHeightBinData<T> EqualHeightHistogram<T>::_build_bins(
    const std::vector<std::pair<T, HistogramCountType>>& value_counts, const BinID max_bin_count) {
  const auto min = value_counts.front().first;
  // If there are fewer distinct values than the number of desired bins use that instead.
  const auto bin_count = max_bin_count <= value_counts.size() ? max_bin_count : value_counts.size();

  // Bins shall have (approximately) the same height.
  const auto total_count =
      std::accumulate(value_counts.cbegin(), value_counts.cend(), HistogramCountType{0},
                      [](HistogramCountType a, const std::pair<T, HistogramCountType>& b) { return a + b.second; });

  // Make sure that we never create more bins than requested.
  const auto count_per_bin = total_count / bin_count + (total_count % bin_count > 0u ? 1 : 0);

  std::vector<T> bin_maximums;
  std::vector<HistogramCountType> bin_distinct_counts;
  bin_maximums.reserve(bin_count);
  bin_distinct_counts.reserve(bin_count);

  auto current_bin_first_value_idx = size_t{0};
  auto current_bin_height = size_t{0};
  for (auto current_end_first_value_idx = size_t{0}; current_end_first_value_idx < value_counts.size();
       current_end_first_value_idx++) {
    current_bin_height += value_counts[current_end_first_value_idx].second;

    // Make sure to create last bin.
    if (current_bin_height >= count_per_bin || current_end_first_value_idx == value_counts.size() - 1) {
      bin_maximums.emplace_back(value_counts[current_end_first_value_idx].first);
      bin_distinct_counts.emplace_back(current_end_first_value_idx - current_bin_first_value_idx + 1);
      current_bin_height = 0ul;
      current_bin_first_value_idx = current_end_first_value_idx + 1;
    }
  }

  return {bin_maximums, bin_distinct_counts, min, total_count};
}

template <typename T>
std::shared_ptr<EqualHeightHistogram<T>> EqualHeightHistogram<T>::from_segment(
    const std::shared_ptr<const BaseSegment>& segment, const BinID max_bin_count,
    const std::optional<std::string>& supported_characters, const std::optional<uint32_t>& string_prefix_length) {
  const auto value_counts = AbstractHistogram<T>::_gather_value_distribution(segment);

  if (value_counts.empty()) {
    return nullptr;
  }

  const auto bins = EqualHeightHistogram<T>::_build_bins(value_counts, max_bin_count);

  if constexpr (std::is_same_v<T, std::string>) {
    const auto [characters, prefix_length] =  // NOLINT (Extra space before [)
        get_default_or_check_string_histogram_prefix_settings(supported_characters, string_prefix_length);
    return std::make_shared<EqualHeightHistogram<T>>(bins.bin_maximums, bins.bin_distinct_counts, bins.minimum,
                                                     bins.total_count, characters, prefix_length);
  } else {
    DebugAssert(!supported_characters && !string_prefix_length,
                "Do not provide string prefix prefix arguments for non-string histograms.");
    return std::make_shared<EqualHeightHistogram<T>>(bins.bin_maximums, bins.bin_distinct_counts, bins.minimum,
                                                     bins.total_count);
  }
}

template <typename T>
HistogramType EqualHeightHistogram<T>::histogram_type() const {
  return HistogramType::EqualHeight;
}

template <typename T>
std::string EqualHeightHistogram<T>::histogram_name() const {
  return "EqualHeight";
}

template <typename T>
BinID EqualHeightHistogram<T>::bin_count() const {
  return _bin_maximums.size();
}

template <typename T>
BinID EqualHeightHistogram<T>::_bin_for_value(const T value) const {
  if (value < _minimum) {
    return INVALID_BIN_ID;
  }

  const auto it = std::lower_bound(_bin_maximums.cbegin(), _bin_maximums.cend(), value);

  if (it == _bin_maximums.cend()) {
    return INVALID_BIN_ID;
  }

  return static_cast<BinID>(std::distance(_bin_maximums.cbegin(), it));
}

template <typename T>
BinID EqualHeightHistogram<T>::_next_bin(const T value) const {
  const auto it = std::upper_bound(_bin_maximums.cbegin(), _bin_maximums.cend(), value);

  if (it == _bin_maximums.cend()) {
    return INVALID_BIN_ID;
  }

  return static_cast<BinID>(std::distance(_bin_maximums.cbegin(), it));
}

template <typename T>
T EqualHeightHistogram<T>::_bin_minimum(const BinID index) const {
  DebugAssert(index < this->bin_count(), "Index is not a valid bin.");

  // If it's the first bin, return _minimum.
  if (index == 0u) {
    return _minimum;
  }

  // Otherwise, return the next representable value of the previous bin's max.
  return this->_get_next_value(_bin_maximum(index - 1));
}

template <typename T>
T EqualHeightHistogram<T>::_bin_maximum(const BinID index) const {
  DebugAssert(index < _bin_maximums.size(), "Index is not a valid bin.");
  return _bin_maximums[index];
}

template <typename T>
HistogramCountType EqualHeightHistogram<T>::_bin_height(const BinID index) const {
  DebugAssert(index < this->bin_count(), "Index is not a valid bin.");
  return total_count() / bin_count() + (total_count() % bin_count() > 0 ? 1 : 0);
}

template <typename T>
HistogramCountType EqualHeightHistogram<T>::_bin_distinct_count(const BinID index) const {
  DebugAssert(index < _bin_distinct_counts.size(), "Index is not a valid bin.");
  return _bin_distinct_counts[index];
}

template <typename T>
HistogramCountType EqualHeightHistogram<T>::total_count() const {
  return _total_count;
}

template <typename T>
HistogramCountType EqualHeightHistogram<T>::total_distinct_count() const {
  return std::accumulate(_bin_distinct_counts.cbegin(), _bin_distinct_counts.cend(), HistogramCountType{0});
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(EqualHeightHistogram);

}  // namespace opossum
