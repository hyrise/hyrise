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
EqualWidthHistogram<T>::EqualWidthHistogram(const T minimum, const T maximum,
                                            const std::vector<HistogramCountType>& bin_heights,
                                            const std::vector<HistogramCountType>& bin_distinct_counts,
                                            const BinID bin_count_with_larger_range)
    : AbstractHistogram<T>(),
      _minimum(minimum),
      _maximum(maximum),
      _bin_heights(bin_heights),
      _bin_distinct_counts(bin_distinct_counts),
      _bin_count_with_larger_range(bin_count_with_larger_range) {
  DebugAssert(bin_heights.size() > 0, "Cannot have histogram without any bins.");
  DebugAssert(bin_heights.size() == bin_distinct_counts.size(), "Must have heights and distinct counts for each bin.");
  DebugAssert(minimum <= maximum, "Cannot have upper bound of histogram smaller than lower bound.");
  if constexpr (std::is_floating_point_v<T>) {
    DebugAssert(bin_count_with_larger_range == 0, "Cannot have bins with extra value in floating point histograms.")
  } else {
    DebugAssert(bin_count_with_larger_range < bin_heights.size(), "Cannot have more bins with extra value than bins.");
  }
}

template <>
EqualWidthHistogram<std::string>::EqualWidthHistogram(const std::string& minimum, const std::string& maximum,
                                                      const std::vector<HistogramCountType>& bin_heights,
                                                      const std::vector<HistogramCountType>& bin_distinct_counts,
                                                      const BinID bin_count_with_larger_range,
                                                      const std::string& supported_characters,
                                                      const uint32_t string_prefix_length)
    : AbstractHistogram<std::string>(supported_characters, string_prefix_length),
      _minimum(minimum),
      _maximum(maximum),
      _bin_heights(bin_heights),
      _bin_distinct_counts(bin_distinct_counts),
      _bin_count_with_larger_range(bin_count_with_larger_range) {
  DebugAssert(bin_heights.size() > 0, "Cannot have histogram without any bins.");
  DebugAssert(bin_heights.size() == bin_distinct_counts.size(), "Must have heights and distinct counts for each bin.");
  DebugAssert(minimum <= maximum, "Cannot have upper bound of histogram smaller than lower bound.");
  DebugAssert(bin_count_with_larger_range < bin_heights.size(), "Cannot have more bins with extra value than bins.");
  DebugAssert(minimum.find_first_not_of(supported_characters) == std::string::npos, "Unsupported characters.");
  DebugAssert(maximum.find_first_not_of(supported_characters) == std::string::npos, "Unsupported characters.");
}

template <>
EqualWidthBinData<std::string> EqualWidthHistogram<std::string>::_build_bins(
    const std::vector<std::pair<std::string, HistogramCountType>>& value_counts, const BinID max_bin_count,
    const std::string& supported_characters, const uint32_t string_prefix_length) {
  // Bins shall have the same range.
  const auto min = value_counts.front().first;
  const auto max = value_counts.back().first;

  const auto repr_min = convert_string_to_number_representation(min, supported_characters, string_prefix_length);
  const auto repr_max = convert_string_to_number_representation(max, supported_characters, string_prefix_length);
  const auto base_width = repr_max - repr_min + 1;

  // Never have more bins than representable values.
  const auto bin_count = max_bin_count <= base_width ? max_bin_count : base_width;

  std::vector<HistogramCountType> bin_counts;
  std::vector<HistogramCountType> bin_distinct_counts;
  bin_counts.reserve(bin_count);
  bin_distinct_counts.reserve(bin_count);

  const auto bin_width = base_width / bin_count;
  const BinID bin_count_with_larger_range = base_width % bin_count;

  auto repr_current_bin_begin_value =
      convert_string_to_number_representation(min, supported_characters, string_prefix_length);
  auto current_bin_begin_it = value_counts.cbegin();

  for (auto current_bin_id = BinID{0}; current_bin_id < bin_count; current_bin_id++) {
    const auto repr_next_bin_begin_value =
        repr_current_bin_begin_value + bin_width + (current_bin_id < bin_count_with_larger_range ? 1 : 0);
    const auto current_bin_end_value = convert_number_representation_to_string(
        repr_next_bin_begin_value - 1, supported_characters, string_prefix_length);

    auto next_bin_begin_it = current_bin_begin_it;
    while (next_bin_begin_it != value_counts.cend() && (*next_bin_begin_it).first <= current_bin_end_value) {
      next_bin_begin_it++;
    }

    bin_counts.emplace_back(std::accumulate(
        current_bin_begin_it, next_bin_begin_it, HistogramCountType{0},
        [](HistogramCountType a, std::pair<std::string, HistogramCountType> b) { return a + b.second; }));
    bin_distinct_counts.emplace_back(std::distance(current_bin_begin_it, next_bin_begin_it));

    current_bin_begin_it = next_bin_begin_it;
    repr_current_bin_begin_value = repr_next_bin_begin_value;
  }

  return {min, max, bin_counts, bin_distinct_counts, bin_count_with_larger_range};
}

template <typename T>
std::shared_ptr<EqualWidthHistogram<T>> EqualWidthHistogram<T>::from_segment(
    const std::shared_ptr<const BaseSegment>& segment, const BinID max_bin_count,
    const std::optional<std::string>& supported_characters, const std::optional<uint32_t>& string_prefix_length) {
  const auto value_counts = AbstractHistogram<T>::_gather_value_distribution(segment);

  if (value_counts.empty()) {
    return nullptr;
  }

  if constexpr (std::is_same_v<T, std::string>) {
    const auto [characters, prefix_length] =  // NOLINT (Extra space before [)
        get_default_or_check_string_histogram_prefix_settings(supported_characters, string_prefix_length);
    const auto bins = EqualWidthHistogram<T>::_build_bins(value_counts, max_bin_count, characters, prefix_length);
    return std::make_shared<EqualWidthHistogram<T>>(bins.minimum, bins.maximum, bins.bin_counts,
                                                    bins.bin_distinct_counts, bins.bin_count_with_larger_range,
                                                    characters, prefix_length);
  } else {
    DebugAssert(!supported_characters && !string_prefix_length,
                "Do not provide string prefix prefix arguments for non-string histograms.");
    const auto bins = EqualWidthHistogram<T>::_build_bins(value_counts, max_bin_count);
    return std::make_shared<EqualWidthHistogram<T>>(bins.minimum, bins.maximum, bins.bin_counts,
                                                    bins.bin_distinct_counts, bins.bin_count_with_larger_range);
  }
}

template <typename T>
HistogramType EqualWidthHistogram<T>::histogram_type() const {
  return HistogramType::EqualWidth;
}

template <typename T>
std::string EqualWidthHistogram<T>::histogram_name() const {
  return "EqualWidth";
}

template <typename T>
BinID EqualWidthHistogram<T>::bin_count() const {
  return _bin_heights.size();
}

template <typename T>
HistogramCountType EqualWidthHistogram<T>::_bin_height(const BinID index) const {
  DebugAssert(index < _bin_heights.size(), "Index is not a valid bin.");
  return _bin_heights[index];
}

template <typename T>
HistogramCountType EqualWidthHistogram<T>::total_count() const {
  return std::accumulate(_bin_heights.cbegin(), _bin_heights.cend(), HistogramCountType{0});
}

template <typename T>
HistogramCountType EqualWidthHistogram<T>::total_distinct_count() const {
  return std::accumulate(_bin_distinct_counts.cbegin(), _bin_distinct_counts.cend(), HistogramCountType{0});
}

template <typename T>
HistogramCountType EqualWidthHistogram<T>::_bin_distinct_count(const BinID index) const {
  DebugAssert(index < _bin_distinct_counts.size(), "Index is not a valid bin.");
  return _bin_distinct_counts[index];
}

template <typename T>
typename AbstractHistogram<T>::HistogramWidthType EqualWidthHistogram<T>::_bin_width(const BinID index) const {
  DebugAssert(index < bin_count(), "Index is not a valid bin.");

  const auto base_width = this->_get_next_value(_maximum - _minimum) / this->bin_count();

  if constexpr (std::is_integral_v<T>) {
    return base_width + (index < _bin_count_with_larger_range ? 1 : 0);
  }

  return base_width;
}

template <>
AbstractHistogram<std::string>::HistogramWidthType EqualWidthHistogram<std::string>::_bin_width(
    const BinID index) const {
  return AbstractHistogram<std::string>::_bin_width(index);
}

template <typename T>
T EqualWidthHistogram<T>::_bin_minimum(const BinID index) const {
  DebugAssert(index < bin_count(), "Index is not a valid bin.");

  // If it's the first bin, return _minimum.
  if (index == 0u) {
    return _minimum;
  }

  // Otherwise, return the next representable value of the previous bin's max.
  return this->_get_next_value(_bin_maximum(index - 1));
}

template <typename T>
T EqualWidthHistogram<T>::_bin_maximum(const BinID index) const {
  DebugAssert(index < bin_count(), "Index is not a valid bin.");

  // If it's the last bin, return max.
  if (index == bin_count() - 1) {
    return _maximum;
  }

  // Calculate the lower edge of the bin right after index, assuming every bin has the same width.
  // If there are no wider bins, take the previous value from the lower edge of the following bin to get the
  // upper edge of this one.
  // Otherwise, add the index to compensate one element for every bin preceding this bin.
  // Add at most _bin_count_with_larger_range - 1 because we already start adding from the next bin's lower edge.
  if constexpr (std::is_same_v<T, std::string>) {
    const auto num_min = this->_convert_string_to_number_representation(_minimum);
    const auto num_max = this->_convert_string_to_number_representation(_maximum);
    const auto base = num_min + (index + 1u) * ((num_max - num_min + 1) / bin_count());
    const auto bin_max = _bin_count_with_larger_range == 0u ? previous_value(base)
                                                            : base + std::min(index, _bin_count_with_larger_range - 1u);
    return this->_convert_number_representation_to_string(bin_max);
  } else {
    const auto base = _minimum + (index + 1u) * _bin_width(bin_count() - 1u);
    return _bin_count_with_larger_range == 0u ? previous_value(base)
                                              : base + std::min(index, _bin_count_with_larger_range - 1u);
  }
}

template <typename T>
BinID EqualWidthHistogram<T>::_bin_for_value(const T value) const {
  if (value < _minimum || value > _maximum) {
    return INVALID_BIN_ID;
  }

  if (_bin_count_with_larger_range == 0u || value <= _bin_maximum(_bin_count_with_larger_range - 1u)) {
    // All bins up to that point have the exact same width, so we can use index 0.
    return (value - _minimum) / _bin_width(0u);
  }

  // All bins after that point have the exact same width as well, so we use that as the new base and add it up.
  return _bin_count_with_larger_range +
         (value - _bin_minimum(_bin_count_with_larger_range)) / _bin_width(_bin_count_with_larger_range);
}

template <>
BinID EqualWidthHistogram<std::string>::_bin_for_value(const std::string value) const {
  if (value < _minimum || value > _maximum) {
    return INVALID_BIN_ID;
  }

  const auto num_value = this->_convert_string_to_number_representation(value);

  BinID bin_id;
  if (_bin_count_with_larger_range == 0u || value <= _bin_maximum(_bin_count_with_larger_range - 1u)) {
    const auto num_min = this->_convert_string_to_number_representation(_minimum);
    bin_id = (num_value - num_min) / this->_bin_width(0u);
  } else {
    const auto num_base_min =
        this->_convert_string_to_number_representation(_bin_minimum(_bin_count_with_larger_range));
    bin_id = _bin_count_with_larger_range + (num_value - num_base_min) / this->_bin_width(_bin_count_with_larger_range);
  }

  // We calculate numerical values for strings with substrings, and the bin edge calculation works with that.
  // Therefore, if the search string is longer than the supported prefix length and starts with the upper bin edge,
  // we have to return the next bin.
  // The exception is if this is the last bin, then it is actually part of the last bin,
  // because that edge is stored separately and therefore not trimmed to the prefix length.
  // We checked earlier that it is not larger than maximum().
  if (value.length() > _string_prefix_length && value.find(_bin_maximum(bin_id)) == 0 &&
      bin_id < this->bin_count() - 1) {
    return bin_id + 1;
  }

  return bin_id;
}

template <typename T>
BinID EqualWidthHistogram<T>::_upper_bound_for_value(const T value) const {
  if (value < _minimum) {
    return 0ul;
  }

  const auto index = _bin_for_value(value);
  return index < bin_count() - 1 ? index + 1 : INVALID_BIN_ID;
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(EqualWidthHistogram);

}  // namespace opossum
