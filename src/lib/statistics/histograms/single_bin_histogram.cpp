#include "single_bin_histogram.hpp"

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
SingleBinHistogram<T>::SingleBinHistogram(const T& minimum, const T& maximum, HistogramCountType total_count,
                                          HistogramCountType distinct_count)
    : AbstractHistogram<T>(),
      _minimum(minimum),
      _maximum(maximum),
      _total_count(total_count),
      _distinct_count(distinct_count) {
  Assert(minimum <= maximum, "Minimum must be smaller than maximum.");
  Assert(distinct_count <= total_count, "Cannot have more distinct values than total values.");
}

template <>
SingleBinHistogram<std::string>::SingleBinHistogram(const std::string& minimum, const std::string& maximum,
                                                    HistogramCountType total_count, HistogramCountType distinct_count,
                                                    const std::string& supported_characters,
                                                    const size_t string_prefix_length)
    : AbstractHistogram<std::string>(supported_characters, string_prefix_length),
      _minimum(minimum),
      _maximum(maximum),
      _total_count(total_count),
      _distinct_count(distinct_count) {
  Assert(minimum <= maximum, "Minimum must be smaller than maximum.");
  Assert(distinct_count <= total_count, "Cannot have more distinct values than total values.");
}

template <typename T>
std::shared_ptr<SingleBinHistogram<T>> SingleBinHistogram<T>::from_segment(
    const std::shared_ptr<const BaseSegment>& segment, const std::optional<std::string>& supported_characters,
    const std::optional<uint32_t>& string_prefix_length) {
  const auto value_counts = AbstractHistogram<T>::_gather_value_distribution(segment);

  if (value_counts.empty()) {
    return nullptr;
  }

  const auto minimum = value_counts.front().first;
  const auto maximum = value_counts.back().first;
  const auto total_count =
      std::accumulate(value_counts.cbegin(), value_counts.cend(), HistogramCountType{0},
                      [](HistogramCountType a, const std::pair<T, HistogramCountType>& b) { return a + b.second; });
  const auto distinct_count = value_counts.size();

  if constexpr (std::is_same_v<T, std::string>) {
    const auto [characters, prefix_length] =  // NOLINT (Extra space before [)
        get_default_or_check_string_histogram_prefix_settings(supported_characters, string_prefix_length);
    return std::make_shared<SingleBinHistogram<T>>(minimum, maximum, total_count, distinct_count, characters,
                                                   prefix_length);
  } else {
    DebugAssert(!supported_characters && !string_prefix_length,
                "Do not provide string prefix prefix arguments for non-string histograms.");
    return std::make_shared<SingleBinHistogram<T>>(minimum, maximum, total_count, distinct_count);
  }
}

template <typename T>
HistogramType SingleBinHistogram<T>::histogram_type() const {
  return HistogramType::SingleBin;
}

template <typename T>
std::string SingleBinHistogram<T>::histogram_name() const {
  return "SingleBin";
}

template <typename T>
std::shared_ptr<AbstractHistogram<T>> SingleBinHistogram<T>::clone() const {
  return std::make_shared<SingleBinHistogram<T>>(_minimum, _maximum, _total_count, _distinct_count);
}

template <typename T>
BinID SingleBinHistogram<T>::bin_count() const {
  return 1;
}

template <typename T>
BinID SingleBinHistogram<T>::_bin_for_value(const T& value) const {
  if (value < _minimum || value > _maximum) {
    return INVALID_BIN_ID;
  }

  return 0;
}

template <typename T>
BinID SingleBinHistogram<T>::_next_bin_for_value(const T& value) const {
  if (value < _minimum) {
    return 0;
  }

  return INVALID_BIN_ID;
}

template <typename T>
T SingleBinHistogram<T>::bin_minimum(const BinID index) const {
  DebugAssert(index == 0, "Index is not a valid bin.");
  return _minimum;
}

template <typename T>
T SingleBinHistogram<T>::bin_maximum(const BinID index) const {
  DebugAssert(index == 0, "Index is not a valid bin.");
  return _maximum;
}

template <typename T>
HistogramCountType SingleBinHistogram<T>::bin_height(const BinID index) const {
  DebugAssert(index == 0, "Index is not a valid bin.");
  return _total_count;
}

template <typename T>
HistogramCountType SingleBinHistogram<T>::bin_distinct_count(const BinID index) const {
  DebugAssert(index == 0, "Index is not a valid bin.");
  return _distinct_count;
}

template <typename T>
HistogramCountType SingleBinHistogram<T>::total_count() const {
  return _total_count;
}

template <typename T>
HistogramCountType SingleBinHistogram<T>::total_distinct_count() const {
  return _distinct_count;
}

template <typename T>
std::shared_ptr<AbstractStatisticsObject> SingleBinHistogram<T>::scaled_with_selectivity(
    const Selectivity selectivity) const {
  const auto distinct_count = scale_distinct_count(selectivity, _total_count, _distinct_count);
  return std::make_shared<SingleBinHistogram<T>>(_minimum, _maximum, _total_count * selectivity, distinct_count);
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(SingleBinHistogram);

}  // namespace opossum
