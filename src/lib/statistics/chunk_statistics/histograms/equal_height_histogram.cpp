#include "equal_height_histogram.hpp"

#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "histogram_utils.hpp"

namespace opossum {

template <typename T>
EqualHeightHistogram<T>::EqualHeightHistogram(const std::vector<T>& maxs, const std::vector<size_t>& distinct_counts,
                                              T min, const size_t total_count)
    : AbstractHistogram<T>(), _maxs(maxs), _distinct_counts(distinct_counts), _min(min), _total_count(total_count) {
  DebugAssert(total_count > 0, "Cannot have histogram without any values.");
  DebugAssert(maxs.size() > 0, "Cannot have histogram without any bins.");
  DebugAssert(maxs.size() == distinct_counts.size(), "Must have maxs and distinct counts for each bin.");
  DebugAssert(min <= maxs[0], "Must have maxs and distinct counts for each bin.");

  for (auto bin_id = 0u; bin_id < maxs.size(); bin_id++) {
    DebugAssert(distinct_counts[bin_id] > 0, "Cannot have bins with no distinct values.");

    if (bin_id < maxs.size() - 1) {
      DebugAssert(maxs[bin_id] < maxs[bin_id + 1], "Bins must be sorted and cannot overlap.");
    }
  }
}

template <>
EqualHeightHistogram<std::string>::EqualHeightHistogram(const std::vector<std::string>& maxs,
                                                        const std::vector<size_t>& distinct_counts,
                                                        const std::string& min, const size_t total_count,
                                                        const std::string& supported_characters,
                                                        const size_t string_prefix_length)
    : AbstractHistogram<std::string>(supported_characters, string_prefix_length),
      _maxs(maxs),
      _distinct_counts(distinct_counts),
      _min(min),
      _total_count(total_count) {
  DebugAssert(total_count > 0, "Cannot have histogram without any values.");
  DebugAssert(maxs.size() > 0, "Cannot have histogram without any bins.");
  DebugAssert(maxs.size() == distinct_counts.size(), "Must have maxs and distinct counts for each bin.");
  DebugAssert(min <= maxs[0], "Must have maxs and distinct counts for each bin.");

  for (auto bin_id = 0u; bin_id < maxs.size(); bin_id++) {
    DebugAssert(distinct_counts[bin_id] > 0, "Cannot have bins with no distinct values.");
    DebugAssert(maxs[bin_id].find_first_not_of(supported_characters) == std::string::npos, "Unsupported characters.");

    if (bin_id < maxs.size() - 1) {
      DebugAssert(maxs[bin_id] < maxs[bin_id + 1], "Bins must be sorted and cannot overlap.");
    }
  }
}

template <typename T>
EqualHeightBinStats<T> EqualHeightHistogram<T>::_get_bin_stats(const std::vector<std::pair<T, size_t>>& value_counts,
                                                               const size_t max_bin_count) {
  const auto min = value_counts.front().first;
  // If there are fewer distinct values than the number of desired bins use that instead.
  const auto bin_count = max_bin_count <= value_counts.size() ? max_bin_count : value_counts.size();

  // Bins shall have (approximately) the same height.
  const auto total_count = std::accumulate(value_counts.cbegin(), value_counts.cend(), size_t{0},
                                           [](size_t a, const std::pair<T, size_t>& b) { return a + b.second; });

  // Make sure that we never create more bins than requested.
  const auto count_per_bin = total_count / bin_count + (total_count % bin_count > 0u ? 1 : 0);

  std::vector<T> maxs;
  std::vector<size_t> distinct_counts;
  maxs.reserve(bin_count);
  distinct_counts.reserve(bin_count);

  auto current_begin = 0ul;
  auto current_height = 0ul;
  for (auto current_end = 0ul; current_end < value_counts.size(); current_end++) {
    current_height += value_counts[current_end].second;

    // Make sure to create last bin.
    if (current_height >= count_per_bin || current_end == value_counts.size() - 1) {
      maxs.emplace_back(value_counts[current_end].first);
      distinct_counts.emplace_back(current_end - current_begin + 1);
      current_height = 0ul;
      current_begin = current_end + 1;
    }
  }

  return {maxs, distinct_counts, min, total_count};
}

template <typename T>
std::shared_ptr<EqualHeightHistogram<T>> EqualHeightHistogram<T>::from_segment(
    const std::shared_ptr<const BaseSegment>& segment, const size_t max_bin_count,
    const std::optional<std::string>& supported_characters, const std::optional<size_t>& string_prefix_length) {
  const auto value_counts = AbstractHistogram<T>::_value_counts_in_segment(segment);

  if (value_counts.empty()) {
    return nullptr;
  }

  const auto bin_stats = EqualHeightHistogram<T>::_get_bin_stats(value_counts, max_bin_count);

  if constexpr (std::is_same_v<T, std::string>) {
    const auto [characters, prefix_length] =  // NOLINT (Extra space before [)
        get_default_or_check_prefix_settings(supported_characters, string_prefix_length);
    return std::make_shared<EqualHeightHistogram<T>>(bin_stats.maxs, bin_stats.distinct_counts, bin_stats.min,
                                                     bin_stats.total_count, characters, prefix_length);
  } else {
    DebugAssert(!static_cast<bool>(supported_characters) && !static_cast<bool>(string_prefix_length),
                "Do not provide string prefix prefix arguments for non-string histograms.");
    return std::make_shared<EqualHeightHistogram<T>>(bin_stats.maxs, bin_stats.distinct_counts, bin_stats.min,
                                                     bin_stats.total_count);
  }
}

template <typename T>
HistogramType EqualHeightHistogram<T>::histogram_type() const {
  return HistogramType::EqualHeight;
}

template <typename T>
size_t EqualHeightHistogram<T>::bin_count() const {
  return _maxs.size();
}

template <typename T>
BinID EqualHeightHistogram<T>::_bin_for_value(const T value) const {
  if (value < _min) {
    return INVALID_BIN_ID;
  }

  const auto it = std::lower_bound(_maxs.cbegin(), _maxs.cend(), value);

  if (it == _maxs.cend()) {
    return INVALID_BIN_ID;
  }

  return static_cast<BinID>(std::distance(_maxs.cbegin(), it));
}

template <typename T>
BinID EqualHeightHistogram<T>::_upper_bound_for_value(const T value) const {
  const auto it = std::upper_bound(_maxs.cbegin(), _maxs.cend(), value);

  if (it == _maxs.cend()) {
    return INVALID_BIN_ID;
  }

  return static_cast<BinID>(std::distance(_maxs.cbegin(), it));
}

template <typename T>
T EqualHeightHistogram<T>::_bin_min(const BinID index) const {
  DebugAssert(index < this->bin_count(), "Index is not a valid bin.");

  // If it's the first bin, return _min.
  if (index == 0u) {
    return _min;
  }

  // Otherwise, return the next representable value of the previous bin's max.
  return this->_get_next_value(this->_bin_max(index - 1));
}

template <typename T>
T EqualHeightHistogram<T>::_bin_max(const BinID index) const {
  DebugAssert(index < _maxs.size(), "Index is not a valid bin.");
  return _maxs[index];
}

template <typename T>
size_t EqualHeightHistogram<T>::_bin_height(const BinID index) const {
  DebugAssert(index < this->bin_count(), "Index is not a valid bin.");
  return total_count() / bin_count() + (total_count() % bin_count() > 0 ? 1 : 0);
}

template <typename T>
size_t EqualHeightHistogram<T>::_bin_distinct_count(const BinID index) const {
  DebugAssert(index < _distinct_counts.size(), "Index is not a valid bin.");
  return _distinct_counts[index];
}

template <typename T>
size_t EqualHeightHistogram<T>::total_count() const {
  return _total_count;
}

template <typename T>
size_t EqualHeightHistogram<T>::total_distinct_count() const {
  return std::accumulate(_distinct_counts.cbegin(), _distinct_counts.cend(), size_t{0});
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(EqualHeightHistogram);

}  // namespace opossum
