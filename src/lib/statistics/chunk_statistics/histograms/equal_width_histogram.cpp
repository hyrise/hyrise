#include "equal_width_histogram.hpp"

#include <algorithm>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "histogram_utils.hpp"

namespace opossum {

template <typename T>
EqualWidthHistogram<T>::EqualWidthHistogram(const T min, const T max, const std::vector<uint64_t>& counts,
                                            const std::vector<uint64_t>& distinct_counts,
                                            const uint64_t num_bins_with_larger_range)
    : AbstractHistogram<T>(),
      _min(min),
      _max(max),
      _counts(counts),
      _distinct_counts(distinct_counts),
      _num_bins_with_larger_range(num_bins_with_larger_range) {
  DebugAssert(counts.size() > 0, "Cannot have histogram without any bins.");
  DebugAssert(counts.size() == distinct_counts.size(), "Must have counts and distinct counts for each bin.");
  DebugAssert(min <= max, "Cannot have upper bound of histogram smaller than lower bound.");
  if constexpr (std::is_floating_point_v<T>) {
    DebugAssert(num_bins_with_larger_range == 0, "Cannot have bins with extra value in floating point histograms.")
  } else {
    DebugAssert(num_bins_with_larger_range < counts.size(), "Cannot have more bins with extra value than bins.");
  }
}

template <>
EqualWidthHistogram<std::string>::EqualWidthHistogram(const std::string& min, const std::string& max,
                                                      const std::vector<uint64_t>& counts,
                                                      const std::vector<uint64_t>& distinct_counts,
                                                      const uint64_t num_bins_with_larger_range,
                                                      const std::string& supported_characters,
                                                      const uint64_t string_prefix_length)
    : AbstractHistogram<std::string>(supported_characters, string_prefix_length),
      _min(min),
      _max(max),
      _counts(counts),
      _distinct_counts(distinct_counts),
      _num_bins_with_larger_range(num_bins_with_larger_range) {
  DebugAssert(counts.size() > 0, "Cannot have histogram without any bins.");
  DebugAssert(counts.size() == distinct_counts.size(), "Must have counts and distinct counts for each bin.");
  DebugAssert(min <= max, "Cannot have upper bound of histogram smaller than lower bound.");
  DebugAssert(num_bins_with_larger_range < counts.size(), "Cannot have more bins with extra value than bins.");
  DebugAssert(min.find_first_not_of(supported_characters) == std::string::npos, "Unsupported characters.");
  DebugAssert(max.find_first_not_of(supported_characters) == std::string::npos, "Unsupported characters.");
}

template <>
EqualWidthBinStats<std::string> EqualWidthHistogram<std::string>::_get_bin_stats(
    const std::vector<std::pair<std::string, uint64_t>>& value_counts, const size_t max_num_bins,
    const std::string& supported_characters, const uint64_t string_prefix_length) {
  // Bins shall have the same range.
  const auto min = value_counts.front().first;
  const auto max = value_counts.back().first;

  const auto repr_min = convert_string_to_number_representation(min, supported_characters, string_prefix_length);
  const auto repr_max = convert_string_to_number_representation(max, supported_characters, string_prefix_length);
  const auto base_width = repr_max - repr_min + 1;

  // Never have more bins than representable values.
  const auto num_bins = max_num_bins <= base_width ? max_num_bins : base_width;

  std::vector<uint64_t> counts;
  std::vector<uint64_t> distinct_counts;
  counts.reserve(num_bins);
  distinct_counts.reserve(num_bins);

  const auto bin_width = base_width / num_bins;
  const uint64_t num_bins_with_larger_range = base_width % num_bins;

  auto repr_current_begin_value =
      convert_string_to_number_representation(min, supported_characters, string_prefix_length);
  auto current_begin_it = value_counts.cbegin();

  for (auto current_bin_id = 0ul; current_bin_id < num_bins; current_bin_id++) {
    const auto repr_next_begin_value =
        repr_current_begin_value + bin_width + (current_bin_id < num_bins_with_larger_range ? 1 : 0);
    const auto current_end_value =
        convert_number_representation_to_string(repr_next_begin_value - 1, supported_characters, string_prefix_length);

    auto next_begin_it = current_begin_it;
    while (next_begin_it != value_counts.cend() && (*next_begin_it).first <= current_end_value) {
      next_begin_it++;
    }

    counts.emplace_back(std::accumulate(current_begin_it, next_begin_it, uint64_t{0},
                                        [](uint64_t a, std::pair<std::string, uint64_t> b) { return a + b.second; }));
    distinct_counts.emplace_back(std::distance(current_begin_it, next_begin_it));

    current_begin_it = next_begin_it;
    repr_current_begin_value = repr_next_begin_value;
  }

  return {min, max, counts, distinct_counts, num_bins_with_larger_range};
}

template <typename T>
std::shared_ptr<EqualWidthHistogram<T>> EqualWidthHistogram<T>::from_segment(
    const std::shared_ptr<const BaseSegment>& segment, const size_t max_num_bins,
    const std::optional<std::string>& supported_characters, const std::optional<uint64_t>& string_prefix_length) {
  const auto value_counts = AbstractHistogram<T>::_calculate_value_counts(segment);

  if (value_counts.empty()) {
    return nullptr;
  }

  if constexpr (std::is_same_v<T, std::string>) {
    const auto [characters, prefix_length] =  // NOLINT (Extra space before [)
        get_default_or_check_prefix_settings(supported_characters, string_prefix_length);
    const auto bin_stats =
        EqualWidthHistogram<T>::_get_bin_stats(value_counts, max_num_bins, characters, prefix_length);
    return std::make_shared<EqualWidthHistogram<T>>(bin_stats.min, bin_stats.max, bin_stats.counts,
                                                    bin_stats.distinct_counts, bin_stats.num_bins_with_larger_range,
                                                    characters, prefix_length);
  } else {
    DebugAssert(!static_cast<bool>(supported_characters) && !static_cast<bool>(string_prefix_length),
                "Do not provide string prefix prefix arguments for non-string histograms.");
    const auto bin_stats = EqualWidthHistogram<T>::_get_bin_stats(value_counts, max_num_bins);
    return std::make_shared<EqualWidthHistogram<T>>(bin_stats.min, bin_stats.max, bin_stats.counts,
                                                    bin_stats.distinct_counts, bin_stats.num_bins_with_larger_range);
  }
}

template <typename T>
HistogramType EqualWidthHistogram<T>::histogram_type() const {
  return HistogramType::EqualWidth;
}

template <typename T>
size_t EqualWidthHistogram<T>::num_bins() const {
  return _counts.size();
}

template <typename T>
uint64_t EqualWidthHistogram<T>::_bin_count(const BinID index) const {
  DebugAssert(index < _counts.size(), "Index is not a valid bin.");
  return _counts[index];
}

template <typename T>
uint64_t EqualWidthHistogram<T>::total_count() const {
  return std::accumulate(_counts.cbegin(), _counts.cend(), uint64_t{0});
}

template <typename T>
uint64_t EqualWidthHistogram<T>::total_count_distinct() const {
  return std::accumulate(_distinct_counts.cbegin(), _distinct_counts.cend(), uint64_t{0});
}

template <typename T>
uint64_t EqualWidthHistogram<T>::_bin_count_distinct(const BinID index) const {
  DebugAssert(index < _distinct_counts.size(), "Index is not a valid bin.");
  return _distinct_counts[index];
}

template <typename T>
typename AbstractHistogram<T>::HistogramWidthType EqualWidthHistogram<T>::_bin_width(const BinID index) const {
  DebugAssert(index < num_bins(), "Index is not a valid bin.");

  const auto base_width = this->get_next_value(_max - _min) / this->num_bins();

  if constexpr (std::is_integral_v<T>) {
    return base_width + (index < _num_bins_with_larger_range ? 1 : 0);
  }

  return base_width;
}

template <>
typename AbstractHistogram<std::string>::HistogramWidthType EqualWidthHistogram<std::string>::_bin_width(
    const BinID index) const {
  return AbstractHistogram<std::string>::_bin_width(index);
}

template <typename T>
T EqualWidthHistogram<T>::_bin_min(const BinID index) const {
  DebugAssert(index < num_bins(), "Index is not a valid bin.");

  // If it's the first bin, return _min.
  if (index == 0u) {
    return _min;
  }

  // Otherwise, return the next representable value of the previous bin's max.
  return this->get_next_value(this->_bin_max(index - 1));
}

template <typename T>
T EqualWidthHistogram<T>::_bin_max(const BinID index) const {
  DebugAssert(index < num_bins(), "Index is not a valid bin.");

  // If it's the last bin, return max.
  if (index == num_bins() - 1) {
    return _max;
  }

  // Calculate the lower edge of the bin right after index, assuming every bin has the same width.
  // If there are no wider bins, take the previous value from the lower edge of the following bin to get the
  // upper edge of this one.
  // Otherwise, add the index to compensate one element for every bin preceding this bin.
  // Add at most _num_bins_with_larger_range - 1 because we already start adding from the next bin's lower edge.
  if constexpr (std::is_same_v<T, std::string>) {
    const auto num_min = this->_convert_string_to_number_representation(_min);
    const auto num_max = this->_convert_string_to_number_representation(_max);
    const auto base = num_min + (index + 1u) * ((num_max - num_min + 1) / num_bins());
    const auto bin_max = _num_bins_with_larger_range == 0u ? previous_value(base)
                                                           : base + std::min(index, _num_bins_with_larger_range - 1u);
    return this->_convert_number_representation_to_string(bin_max);
  } else {
    const auto base = _min + (index + 1u) * _bin_width(num_bins() - 1u);
    return _num_bins_with_larger_range == 0u ? previous_value(base)
                                             : base + std::min(index, _num_bins_with_larger_range - 1u);
  }
}

template <typename T>
BinID EqualWidthHistogram<T>::_bin_for_value(const T value) const {
  if (value < _min || value > _max) {
    return INVALID_BIN_ID;
  }

  if (_num_bins_with_larger_range == 0u || value <= _bin_max(_num_bins_with_larger_range - 1u)) {
    // All bins up to that point have the exact same width, so we can use index 0.
    return (value - _min) / _bin_width(0u);
  }

  // All bins after that point have the exact same width as well, so we use that as the new base and add it up.
  return _num_bins_with_larger_range +
         (value - _bin_min(_num_bins_with_larger_range)) / _bin_width(_num_bins_with_larger_range);
}

template <>
BinID EqualWidthHistogram<std::string>::_bin_for_value(const std::string value) const {
  if (value < _min || value > _max) {
    return INVALID_BIN_ID;
  }

  const auto num_value = this->_convert_string_to_number_representation(value);

  BinID bin_id;
  if (_num_bins_with_larger_range == 0u || value <= _bin_max(_num_bins_with_larger_range - 1u)) {
    const auto num_min = this->_convert_string_to_number_representation(_min);
    bin_id = (num_value - num_min) / this->_bin_width(0u);
  } else {
    const auto num_base_min = this->_convert_string_to_number_representation(_bin_min(_num_bins_with_larger_range));
    bin_id = _num_bins_with_larger_range + (num_value - num_base_min) / this->_bin_width(_num_bins_with_larger_range);
  }

  // We calculate numerical values for strings with substrings, and the bin edge calculation works with that.
  // Therefore, if the search string is longer than the supported prefix length and starts with the upper bin edge,
  // we have to return the next bin.
  return bin_id + (value.length() > _string_prefix_length && value.find(_bin_max(bin_id)) == 0 ? 1 : 0);
}

template <typename T>
BinID EqualWidthHistogram<T>::_upper_bound_for_value(const T value) const {
  if (value < _min) {
    return 0ul;
  }

  const auto index = _bin_for_value(value);
  return index < num_bins() - 1 ? index + 1 : INVALID_BIN_ID;
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(EqualWidthHistogram);

}  // namespace opossum
