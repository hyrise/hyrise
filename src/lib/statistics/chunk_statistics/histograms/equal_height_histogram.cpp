#include "equal_height_histogram.hpp"

#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "histogram_utils.hpp"

namespace opossum {

using namespace opossum::histogram;  // NOLINT

template <typename T>
EqualHeightHistogram<T>::EqualHeightHistogram(const T minimum, std::vector<T>&& bin_maxima,
                                              const HistogramCountType total_count,
                                              std::vector<HistogramCountType>&& bin_distinct_counts)
    : AbstractHistogram<T>(), _bin_data({minimum, std::move(bin_maxima), total_count, std::move(bin_distinct_counts)}) {
  Assert(_bin_data.total_count > 0, "Cannot have histogram without any values.");
  Assert(!_bin_data.bin_maxima.empty(), "Cannot have histogram without any bins.");
  Assert(_bin_data.bin_maxima.size() == _bin_data.bin_distinct_counts.size(),
         "Must have maxs and distinct counts for each bin.");
  Assert(_bin_data.minimum <= _bin_data.bin_maxima[0], "Must have maxs and distinct counts for each bin.");

  for (BinID bin_id = 0; bin_id < _bin_data.bin_maxima.size(); bin_id++) {
    Assert(_bin_data.bin_distinct_counts[bin_id] > 0, "Cannot have bins with no distinct values.");

    if (bin_id < _bin_data.bin_maxima.size() - 1) {
      Assert(_bin_data.bin_maxima[bin_id] < _bin_data.bin_maxima[bin_id + 1],
             "Bins must be sorted and cannot overlap.");
    }
  }
}

template <>
EqualHeightHistogram<std::string>::EqualHeightHistogram(const std::string& minimum,
                                                        std::vector<std::string>&& bin_maxima,
                                                        const HistogramCountType total_count,
                                                        std::vector<HistogramCountType>&& bin_distinct_counts,
                                                        const std::string& supported_characters,
                                                        const size_t string_prefix_length)
    : AbstractHistogram<std::string>(supported_characters, string_prefix_length),
      _bin_data({minimum, std::move(bin_maxima), total_count, std::move(bin_distinct_counts)}) {
  Assert(_bin_data.total_count > 0, "Cannot have histogram without any values.");
  Assert(!_bin_data.bin_maxima.empty(), "Cannot have histogram without any bins.");
  Assert(_bin_data.bin_maxima.size() == _bin_data.bin_distinct_counts.size(),
         "Must have maxs and distinct counts for each bin.");
  Assert(_bin_data.minimum <= _bin_data.bin_maxima[0], "Must have maxs and distinct counts for each bin.");

  for (BinID bin_id = 0; bin_id < _bin_data.bin_maxima.size(); bin_id++) {
    Assert(_bin_data.bin_distinct_counts[bin_id] > 0, "Cannot have bins with no distinct values.");
    Assert(_bin_data.bin_maxima[bin_id].find_first_not_of(supported_characters) == std::string::npos,
           "Unsupported characters.");

    if (bin_id < _bin_data.bin_maxima.size() - 1) {
      Assert(_bin_data.bin_maxima[bin_id] < _bin_data.bin_maxima[bin_id + 1],
             "Bins must be sorted and cannot overlap.");
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

  std::vector<T> bin_maxima;
  std::vector<HistogramCountType> bin_distinct_counts;
  bin_maxima.reserve(bin_count);
  bin_distinct_counts.reserve(bin_count);

  auto current_bin_first_value_idx = size_t{0};
  auto current_bin_height = size_t{0};
  for (auto current_bin_last_value_idx = size_t{0}; current_bin_last_value_idx < value_counts.size();
       current_bin_last_value_idx++) {
    current_bin_height += value_counts[current_bin_last_value_idx].second;

    // Make sure to create last bin.
    if (current_bin_height >= count_per_bin || current_bin_last_value_idx == value_counts.size() - 1) {
      bin_maxima.emplace_back(value_counts[current_bin_last_value_idx].first);
      bin_distinct_counts.emplace_back(current_bin_last_value_idx - current_bin_first_value_idx + 1);
      current_bin_height = 0ul;
      current_bin_first_value_idx = current_bin_last_value_idx + 1;
    }
  }

  return {min, std::move(bin_maxima), total_count, std::move(bin_distinct_counts)};
}

template <typename T>
std::shared_ptr<EqualHeightHistogram<T>> EqualHeightHistogram<T>::from_segment(
    const std::shared_ptr<const BaseSegment>& segment, const BinID max_bin_count,
    const std::optional<std::string>& supported_characters, const std::optional<uint32_t>& string_prefix_length) {
  const auto value_counts = AbstractHistogram<T>::_gather_value_distribution(segment);

  if (value_counts.empty()) {
    return nullptr;
  }

  auto bins = EqualHeightHistogram<T>::_build_bins(value_counts, max_bin_count);

  if constexpr (std::is_same_v<T, std::string>) {
    const auto [characters, prefix_length] =  // NOLINT (Extra space before [)
        get_default_or_check_string_histogram_prefix_settings(supported_characters, string_prefix_length);
    return std::make_shared<EqualHeightHistogram<T>>(bins.minimum, std::move(bins.bin_maxima), bins.total_count,
                                                     std::move(bins.bin_distinct_counts), characters, prefix_length);
  } else {
    DebugAssert(!supported_characters && !string_prefix_length,
                "Do not provide string prefix prefix arguments for non-string histograms.");
    return std::make_shared<EqualHeightHistogram<T>>(bins.minimum, std::move(bins.bin_maxima), bins.total_count,
                                                     std::move(bins.bin_distinct_counts));
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
  return _bin_data.bin_maxima.size();
}

template <typename T>
BinID EqualHeightHistogram<T>::_bin_for_value(const T& value) const {
  if (value < _bin_data.minimum) {
    return INVALID_BIN_ID;
  }

  const auto it = std::lower_bound(_bin_data.bin_maxima.cbegin(), _bin_data.bin_maxima.cend(), value);

  if (it == _bin_data.bin_maxima.cend()) {
    return INVALID_BIN_ID;
  }

  return static_cast<BinID>(std::distance(_bin_data.bin_maxima.cbegin(), it));
}

template <typename T>
BinID EqualHeightHistogram<T>::_next_bin_for_value(const T& value) const {
  const auto it = std::upper_bound(_bin_data.bin_maxima.cbegin(), _bin_data.bin_maxima.cend(), value);

  if (it == _bin_data.bin_maxima.cend()) {
    return INVALID_BIN_ID;
  }

  return static_cast<BinID>(std::distance(_bin_data.bin_maxima.cbegin(), it));
}

template <typename T>
T EqualHeightHistogram<T>::_bin_minimum(const BinID index) const {
  DebugAssert(index < bin_count(), "Index is not a valid bin.");

  // If it's the first bin, return _minimum.
  if (index == 0u) {
    return _bin_data.minimum;
  }

  // Otherwise, return the next representable value of the previous bin's max.
  return this->_get_next_value(_bin_maximum(index - 1));
}

template <typename T>
T EqualHeightHistogram<T>::_bin_maximum(const BinID index) const {
  DebugAssert(index < _bin_data.bin_maxima.size(), "Index is not a valid bin.");
  return _bin_data.bin_maxima[index];
}

template <typename T>
HistogramCountType EqualHeightHistogram<T>::_bin_height(const BinID index) const {
  DebugAssert(index < bin_count(), "Index is not a valid bin.");
  return total_count() / bin_count() + (total_count() % bin_count() > 0 ? 1 : 0);
}

template <typename T>
HistogramCountType EqualHeightHistogram<T>::_bin_distinct_count(const BinID index) const {
  DebugAssert(index < _bin_data.bin_distinct_counts.size(), "Index is not a valid bin.");
  return _bin_data.bin_distinct_counts[index];
}

template <typename T>
HistogramCountType EqualHeightHistogram<T>::total_count() const {
  return _bin_data.total_count;
}

template <typename T>
HistogramCountType EqualHeightHistogram<T>::total_distinct_count() const {
  return std::accumulate(_bin_data.bin_distinct_counts.cbegin(), _bin_data.bin_distinct_counts.cend(),
                         HistogramCountType{0});
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(EqualHeightHistogram);

}  // namespace opossum
