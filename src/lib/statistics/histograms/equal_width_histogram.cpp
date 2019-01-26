#include "equal_width_histogram.hpp"

#include <cmath>

#include <algorithm>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "histogram_utils.hpp"
#include "statistics/statistics_utils.hpp"

namespace opossum {

using namespace opossum::histogram;  // NOLINT

template <typename T>
EqualWidthHistogram<T>::EqualWidthHistogram(const T minimum, const T maximum,
                                            std::vector<HistogramCountType>&& bin_heights,
                                            std::vector<HistogramCountType>&& bin_distinct_counts,
                                            const BinID bin_count_with_larger_range)
    : AbstractHistogram<T>(),
      _bin_data(
          {minimum, maximum, std::move(bin_heights), std::move(bin_distinct_counts), bin_count_with_larger_range}) {
  Assert(_bin_data.bin_heights.size() == _bin_data.bin_distinct_counts.size(),
         "Must have heights and distinct counts for each bin.");
  Assert(_bin_data.minimum <= _bin_data.maximum, "Cannot have upper bound of histogram smaller than lower bound.");
  if constexpr (std::is_floating_point_v<T>) {
    Assert(_bin_data.bin_count_with_larger_range == 0, "Cannot have varying bin sizes in float histograms.")
  } else {
    Assert(_bin_data.bin_count_with_larger_range < _bin_data.bin_heights.size(),
           "Cannot have more or the same number of bins with a wider range than the number of bins itself.");
  }

  AbstractHistogram<T>::_assert_bin_validity();
}

template <>
EqualWidthHistogram<std::string>::EqualWidthHistogram(const std::string& minimum, const std::string& maximum,
                                                      std::vector<HistogramCountType>&& bin_heights,
                                                      std::vector<HistogramCountType>&& bin_distinct_counts,
                                                      const BinID bin_count_with_larger_range,
                                                      const StringHistogramDomain& string_domain)
    : AbstractHistogram<std::string>(string_domain),
      _bin_data(
          {minimum, maximum, std::move(bin_heights), std::move(bin_distinct_counts), bin_count_with_larger_range}) {
  Assert(!_bin_data.bin_heights.empty(), "Cannot have histogram without any bins.");
  Assert(_bin_data.bin_heights.size() == _bin_data.bin_distinct_counts.size(),
         "Must have heights and distinct counts for each bin.");
  Assert(_bin_data.minimum <= _bin_data.maximum, "Cannot have upper bound of histogram smaller than lower bound.");
  Assert(_bin_data.bin_count_with_larger_range < _bin_data.bin_heights.size(),
         "Cannot have more or the same number of bins with a wider range than the number of bins itself.");

  _assert_bin_validity();
}

template <>
EqualWidthBinData<std::string> EqualWidthHistogram<std::string>::_build_bins(
    const std::vector<std::pair<std::string, HistogramCountType>>& value_counts, const BinID max_bin_count,
    const StringHistogramDomain& string_domain) {

  // Bins shall have the same range.
  const auto min = value_counts.front().first;
  const auto max = value_counts.back().first;

  const auto repr_min = string_domain.string_to_number(min);
  const auto repr_max = string_domain.string_to_number(max);
  const auto base_width = repr_max - repr_min + 1;

  // Never have more bins than representable values.
  const auto bin_count = max_bin_count <= base_width ? max_bin_count : base_width;

  std::vector<HistogramCountType> bin_heights(bin_count);
  std::vector<HistogramCountType> bin_distinct_counts(bin_count);

  const auto bin_width = base_width / bin_count;
  const BinID bin_count_with_larger_range = base_width % bin_count;

  auto repr_current_bin_begin_value =
  string_domain.string_to_number(min);
  auto current_bin_begin_it = value_counts.cbegin();

  for (auto current_bin_id = BinID{0}; current_bin_id < bin_count; current_bin_id++) {
    const auto repr_next_bin_begin_value =
        repr_current_bin_begin_value + bin_width + (current_bin_id < bin_count_with_larger_range ? 1 : 0);
    const auto current_bin_end_value = string_domain.number_to_string(
        repr_next_bin_begin_value - 1);

    // This could be a binary search,
    // but we decided that for the relatively small number of values and bins it is not worth it,
    // as linear search tends to be just as fast or faster due to hardware optimizations for small vectors.
    // Feel free to change if this comes up in a profiler.
    auto next_bin_begin_it = current_bin_begin_it;
    while (next_bin_begin_it != value_counts.cend() && (*next_bin_begin_it).first <= current_bin_end_value) {
      next_bin_begin_it++;
    }

    bin_heights[current_bin_id] = std::accumulate(
        current_bin_begin_it, next_bin_begin_it, HistogramCountType{0},
        [](HistogramCountType a, std::pair<std::string, HistogramCountType> b) { return a + b.second; });
    bin_distinct_counts[current_bin_id] =
        static_cast<HistogramCountType>(std::distance(current_bin_begin_it, next_bin_begin_it));

    current_bin_begin_it = next_bin_begin_it;
    repr_current_bin_begin_value = repr_next_bin_begin_value;
  }

  return {min, max, std::move(bin_heights), std::move(bin_distinct_counts), bin_count_with_larger_range};
}

template <typename T>
std::shared_ptr<EqualWidthHistogram<T>> EqualWidthHistogram<T>::from_segment(
    const std::shared_ptr<const BaseSegment>& segment, const BinID max_bin_count,
    const std::optional<StringHistogramDomain>& string_domain) {
  const auto value_counts = AbstractHistogram<T>::_gather_value_distribution(segment);

  if (value_counts.empty()) {
    return nullptr;
  }

  if constexpr (std::is_same_v<T, std::string>) {
    auto bins = EqualWidthHistogram<T>::_build_bins(value_counts, max_bin_count, string_domain.value_or(StringHistogramDomain{}));
    return std::make_shared<EqualWidthHistogram<T>>(bins.minimum, bins.maximum, std::move(bins.bin_heights),
                                                    std::move(bins.bin_distinct_counts),
                                                    bins.bin_count_with_larger_range, string_domain.value_or(StringHistogramDomain{}));
  } else {
    DebugAssert(!string_domain,
                "Do not provide string prefix prefix arguments for non-string histograms.");
    auto bins = EqualWidthHistogram<T>::_build_bins(value_counts, max_bin_count);
    return std::make_shared<EqualWidthHistogram<T>>(bins.minimum, bins.maximum, std::move(bins.bin_heights),
                                                    std::move(bins.bin_distinct_counts),
                                                    bins.bin_count_with_larger_range);
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
std::shared_ptr<AbstractHistogram<T>> EqualWidthHistogram<T>::clone() const {
  auto bin_distinct_counts = _bin_data.bin_distinct_counts;
  auto bin_heights = _bin_data.bin_heights;

  return std::make_shared<EqualWidthHistogram<T>>(_bin_data.minimum, _bin_data.maximum, std::move(bin_heights),
                                                  std::move(bin_distinct_counts),
                                                  _bin_data.bin_count_with_larger_range);
}

template <typename T>
BinID EqualWidthHistogram<T>::bin_count() const {
  return _bin_data.bin_heights.size();
}

template <typename T>
HistogramCountType EqualWidthHistogram<T>::bin_height(const BinID index) const {
  DebugAssert(index < _bin_data.bin_heights.size(), "Index is not a valid bin.");
  return _bin_data.bin_heights[index];
}

template <typename T>
HistogramCountType EqualWidthHistogram<T>::total_count() const {
  return std::accumulate(_bin_data.bin_heights.cbegin(), _bin_data.bin_heights.cend(), HistogramCountType{0});
}

template <typename T>
HistogramCountType EqualWidthHistogram<T>::total_distinct_count() const {
  return std::accumulate(_bin_data.bin_distinct_counts.cbegin(), _bin_data.bin_distinct_counts.cend(),
                         HistogramCountType{0});
}

template <typename T>
HistogramCountType EqualWidthHistogram<T>::bin_distinct_count(const BinID index) const {
  DebugAssert(index < _bin_data.bin_distinct_counts.size(), "Index is not a valid bin.");
  return _bin_data.bin_distinct_counts[index];
}

template <typename T>
typename AbstractHistogram<T>::HistogramWidthType EqualWidthHistogram<T>::bin_width([
    [maybe_unused]] const BinID index) const {
  DebugAssert(index < bin_count(), "Index is not a valid bin.");

  const auto base_width = static_cast<T>(this->get_next_value(_bin_data.maximum - _bin_data.minimum) / bin_count());

  if constexpr (std::is_integral_v<T>) {  // NOLINT
    return base_width + (index < _bin_data.bin_count_with_larger_range ? 1 : 0);
  }

  return base_width;
}

template <>
AbstractHistogram<std::string>::HistogramWidthType EqualWidthHistogram<std::string>::bin_width(
    const BinID index) const {
  return AbstractHistogram<std::string>::bin_width(index);
}

template <typename T>
T EqualWidthHistogram<T>::bin_minimum(const BinID index) const {
  DebugAssert(index < bin_count(), "Index is not a valid bin.");

  // If it's the first bin, return _minimum.
  if (index == 0u) {
    return _bin_data.minimum;
  }

  // Otherwise, return the next representable value of the previous bin's max.
  return this->get_next_value(bin_maximum(index - 1));
}

template <typename T>
T EqualWidthHistogram<T>::bin_maximum(const BinID index) const {
  DebugAssert(index < bin_count(), "Index is not a valid bin.");

  // If it's the last bin, return max.
  if (index == bin_count() - 1) {
    return _bin_data.maximum;
  }

  // Calculate the lower edge of the bin right after index, assuming every bin has the same width.
  // If there are no wider bins, take the previous value from the lower edge of the following bin to get the
  // upper edge of this one.
  // Otherwise, add the index to compensate one element for every bin preceding this bin.
  // Add at most bin_count_with_larger_range - 1 because we already start adding from the next bin's lower edge.
  if constexpr (std::is_same_v<T, std::string>) {
    const auto repr_min = this->_string_domain->string_to_number(_bin_data.minimum);
    const auto repr_max = this->_string_domain->string_to_number(_bin_data.maximum);
    const auto base = repr_min + (index + 1u) * ((repr_max - repr_min + 1) / bin_count());
    const auto bin_max = _bin_data.bin_count_with_larger_range == 0u
                             ? previous_value(base)
                             : base + std::min(index, _bin_data.bin_count_with_larger_range - 1u);
    return this->_string_domain->number_to_string(bin_max);
  } else {
    const auto base = static_cast<T>(_bin_data.minimum + (index + 1u) * bin_width(bin_count() - 1u));
    return _bin_data.bin_count_with_larger_range == 0u
               ? previous_value(base)
               : base + static_cast<T>(std::min(index, _bin_data.bin_count_with_larger_range - 1u));
  }
}

template <typename T>
BinID EqualWidthHistogram<T>::_bin_for_value(const T& value) const {
  if (value < _bin_data.minimum || value > _bin_data.maximum) {
    return INVALID_BIN_ID;
  }

  if constexpr (std::is_same_v<T, std::string>) {
    const auto num_value = this->_string_domain->string_to_number(value);

    BinID bin_id;
    if (_bin_data.bin_count_with_larger_range == 0u ||
        value <= bin_maximum(_bin_data.bin_count_with_larger_range - 1u)) {
      const auto repr_min = this->_string_domain->string_to_number(_bin_data.minimum);
      bin_id = (num_value - repr_min) / this->bin_width(0u);
    } else {
      const auto num_base_min =
          this->_string_domain->string_to_number(bin_minimum(_bin_data.bin_count_with_larger_range));
      bin_id = _bin_data.bin_count_with_larger_range +
               (num_value - num_base_min) / this->bin_width(_bin_data.bin_count_with_larger_range);
    }

    // We calculate numerical values for strings with substrings, and the bin edge calculation works with that.
    // Therefore, if the search string is longer than the supported prefix length and starts with the upper bin edge,
    // we have to return the next bin.
    // The exception is if this is the last bin, then it is actually part of the last bin,
    // because that edge is stored separately and therefore not trimmed to the prefix length.
    // We checked earlier that it is not larger than maximum().
    if (value.length() > this->_string_domain->prefix_length && value.find(bin_maximum(bin_id)) == 0 &&
        bin_id < bin_count() - 1) {
      return bin_id + 1;
    }

    return bin_id;
  } else {
    if (_bin_data.bin_count_with_larger_range == 0u ||
        value <= bin_maximum(_bin_data.bin_count_with_larger_range - 1u)) {
      // All bins up to that point have the exact same width, so we can use index 0.
      const auto bin_id = (value - _bin_data.minimum) / bin_width(0u);

      // The above calculation can lead to an index that is equal to or larger than the number of bins there are,
      // due to floating point arithmetic.
      // We checked before that the value is not larger than the maximum of the histogram,
      // so in that case, simply return the last bin.
      return std::min(static_cast<BinID>(bin_id), BinID{_bin_data.bin_heights.size() - 1});
    }

    // All bins after that point have the exact same width as well, so we use that as the new base and add it up.
    return _bin_data.bin_count_with_larger_range +
           static_cast<BinID>((value - bin_minimum(_bin_data.bin_count_with_larger_range)) /
                              bin_width(_bin_data.bin_count_with_larger_range));
  }
}

template <typename T>
BinID EqualWidthHistogram<T>::_next_bin_for_value(const T& value) const {
  if (value < _bin_data.minimum) {
    return 0ul;
  }

  const auto index = _bin_for_value(value);
  if (index == INVALID_BIN_ID) return INVALID_BIN_ID;

  return index < bin_count() - 1 ? index + 1 : INVALID_BIN_ID;
}

template <typename T>
std::shared_ptr<AbstractStatisticsObject> EqualWidthHistogram<T>::scaled_with_selectivity(
    const Selectivity selectivity) const {
  // Scale the number of values in the bin with the given selectivity.
  // Round up the numbers such that we tend to over- rather than underestimate.
  // Also, we avoid 0 as a height.
  auto bin_heights = std::vector<HistogramCountType>(_bin_data.bin_heights.size());
  auto bin_distinct_counts = std::vector<HistogramCountType>(_bin_data.bin_heights.size());
  for (auto bin_id = BinID{0}; bin_id < _bin_data.bin_heights.size(); bin_id++) {
    bin_heights[bin_id] = static_cast<HistogramCountType>(std::ceil(_bin_data.bin_heights[bin_id] * selectivity));
    bin_distinct_counts[bin_id] = static_cast<HistogramCountType>(std::ceil(
        scale_distinct_count(selectivity, _bin_data.bin_heights[bin_id], _bin_data.bin_distinct_counts[bin_id])));
  }

  return std::make_shared<EqualWidthHistogram<T>>(_bin_data.minimum, _bin_data.maximum, std::move(bin_heights),
                                                  std::move(bin_distinct_counts),
                                                  _bin_data.bin_count_with_larger_range);
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(EqualWidthHistogram);

}  // namespace opossum
