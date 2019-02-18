#include "equal_distinct_count_histogram.hpp"

#include <cmath>

#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "generic_histogram.hpp"
#include "histogram_utils.hpp"
#include "statistics/statistics_utils.hpp"

namespace opossum {

using namespace opossum::histogram;  // NOLINT

template <typename T>
EqualDistinctCountHistogram<T>::EqualDistinctCountHistogram(std::vector<T>&& bin_minima, std::vector<T>&& bin_maxima,
                                                            std::vector<HistogramCountType>&& bin_heights,
                                                            const HistogramCountType distinct_count_per_bin,
                                                            const BinID bin_count_with_extra_value)
    : AbstractHistogram<T>(),
      _bin_data({std::move(bin_minima), std::move(bin_maxima), std::move(bin_heights), distinct_count_per_bin,
                 bin_count_with_extra_value}) {
  Assert(_bin_data.bin_minima.size() == _bin_data.bin_maxima.size(),
         "Must have the same number of lower as upper bin edges.");
  Assert(_bin_data.bin_minima.size() == _bin_data.bin_heights.size(),
         "Must have the same number of edges and heights.");
  Assert(_bin_data.distinct_count_per_bin > 0, "Cannot have bins with no distinct values.");
  Assert(_bin_data.bin_count_with_extra_value < _bin_data.bin_minima.size(),
         "Cannot have more bins with extra value than bins.");

  AbstractHistogram<T>::_assert_bin_validity();
}

template <>
EqualDistinctCountHistogram<std::string>::EqualDistinctCountHistogram(std::vector<std::string>&& bin_minima,
                                                                      std::vector<std::string>&& bin_maxima,
                                                                      std::vector<HistogramCountType>&& bin_heights,
                                                                      const HistogramCountType distinct_count_per_bin,
                                                                      const BinID bin_count_with_extra_value,
                                                                      const StringHistogramDomain& string_domain)
    : AbstractHistogram<std::string>(string_domain),
      _bin_data({std::move(bin_minima), std::move(bin_maxima), std::move(bin_heights), distinct_count_per_bin,
                 bin_count_with_extra_value}) {
  Assert(!_bin_data.bin_minima.empty(), "Cannot have histogram without any bins.");
  Assert(_bin_data.bin_minima.size() == _bin_data.bin_maxima.size(),
         "Must have the same number of lower as upper bin edges.");
  Assert(_bin_data.bin_minima.size() == _bin_data.bin_heights.size(),
         "Must have the same number of edges and heights.");
  Assert(_bin_data.distinct_count_per_bin > 0, "Cannot have bins with no distinct values.");
  Assert(_bin_data.bin_count_with_extra_value < _bin_data.bin_minima.size(),
         "Cannot have more bins with extra value than bins.");

  _assert_bin_validity();
}

template <typename T>
EqualDistinctCountBinData<T> EqualDistinctCountHistogram<T>::_build_bins(
    const std::vector<std::pair<T, HistogramCountType>>& value_counts, const BinID max_bin_count,
    const std::optional<StringHistogramDomain>& string_domain) {
  // If there are fewer distinct values than the number of desired bins use that instead.
  const auto bin_count = value_counts.size() < max_bin_count ? static_cast<BinID>(value_counts.size()) : max_bin_count;

  // Split values evenly among bins.
  const auto distinct_count_per_bin = static_cast<size_t>(value_counts.size() / bin_count);
  const BinID bin_count_with_extra_value = value_counts.size() % bin_count;

  std::vector<T> bin_minima(bin_count);
  std::vector<T> bin_maxima(bin_count);
  std::vector<HistogramCountType> bin_heights(bin_count);

  auto current_bin_begin_index = BinID{0};
  for (BinID bin_index = 0; bin_index < bin_count; bin_index++) {
    auto current_bin_end_index = current_bin_begin_index + distinct_count_per_bin - 1;
    if (bin_index < bin_count_with_extra_value) {
      current_bin_end_index++;
    }

    bin_minima[bin_index] = value_counts[current_bin_begin_index].first;
    bin_maxima[bin_index] = value_counts[current_bin_end_index].first;

    bin_heights[bin_index] =
        std::accumulate(value_counts.cbegin() + current_bin_begin_index,
                        value_counts.cbegin() + current_bin_end_index + 1, HistogramCountType{0},
                        [](HistogramCountType a, const std::pair<T, HistogramCountType>& b) { return a + b.second; });

    current_bin_begin_index = current_bin_end_index + 1;
  }

  return {std::move(bin_minima), std::move(bin_maxima), std::move(bin_heights),
          static_cast<HistogramCountType>(distinct_count_per_bin), bin_count_with_extra_value};
  //  }
}

template <typename T>
std::shared_ptr<EqualDistinctCountHistogram<T>> EqualDistinctCountHistogram<T>::from_distribution(
    const std::vector<std::pair<T, HistogramCountType>>& value_distribution, const BinID max_bin_count,
    const std::optional<StringHistogramDomain>& string_domain) {
  if (string_domain.has_value() != std::is_same_v<T, std::string>) {
    Fail("Provide domain iff T is std::string");
  }

  if (value_distribution.empty()) {
    return nullptr;
  }

  auto bins = EqualDistinctCountHistogram<T>::_build_bins(value_distribution, max_bin_count, string_domain);

  if constexpr (std::is_same_v<T, std::string>) {
    return std::make_shared<EqualDistinctCountHistogram<T>>(std::move(bins.bin_minima), std::move(bins.bin_maxima),
                                                            std::move(bins.bin_heights), bins.distinct_count_per_bin,
                                                            bins.bin_count_with_extra_value, *string_domain);
  } else {
    DebugAssert(!string_domain, "Do not provide string domain non-string histograms.");
    return std::make_shared<EqualDistinctCountHistogram<T>>(std::move(bins.bin_minima), std::move(bins.bin_maxima),
                                                            std::move(bins.bin_heights), bins.distinct_count_per_bin,
                                                            bins.bin_count_with_extra_value);
  }
}

template <typename T>
std::shared_ptr<EqualDistinctCountHistogram<T>> EqualDistinctCountHistogram<T>::from_segment(
    const std::shared_ptr<BaseSegment>& segment, const BinID max_bin_count,
    const std::optional<StringHistogramDomain>& string_domain) {
  const auto distribution = histogram::value_distribution_from_segment<T>(*segment, string_domain);
  return from_distribution(distribution, max_bin_count, string_domain);
}

template <typename T>
HistogramType EqualDistinctCountHistogram<T>::histogram_type() const {
  return HistogramType::EqualDistinctCount;
}

template <typename T>
std::string EqualDistinctCountHistogram<T>::histogram_name() const {
  return "EqualDistinctCount";
}

template <typename T>
std::shared_ptr<AbstractHistogram<T>> EqualDistinctCountHistogram<T>::clone() const {
  auto bin_minima = _bin_data.bin_minima;
  auto bin_maxima = _bin_data.bin_maxima;
  auto bin_heights = _bin_data.bin_heights;

  return std::make_shared<EqualDistinctCountHistogram<T>>(std::move(bin_minima), std::move(bin_maxima),
                                                          std::move(bin_heights), _bin_data.distinct_count_per_bin,
                                                          _bin_data.bin_count_with_extra_value);
}

template <typename T>
BinID EqualDistinctCountHistogram<T>::bin_count() const {
  return _bin_data.bin_heights.size();
}

template <typename T>
BinID EqualDistinctCountHistogram<T>::_bin_for_value(const T& value) const {
  const auto it = std::lower_bound(_bin_data.bin_maxima.cbegin(), _bin_data.bin_maxima.cend(), value);
  const auto index = static_cast<BinID>(std::distance(_bin_data.bin_maxima.cbegin(), it));

  if (it == _bin_data.bin_maxima.cend() || value < bin_minimum(index) || value > bin_maximum(index)) {
    return INVALID_BIN_ID;
  }

  return index;
}

template <typename T>
BinID EqualDistinctCountHistogram<T>::_next_bin_for_value(const T& value) const {
  const auto it = std::upper_bound(_bin_data.bin_maxima.cbegin(), _bin_data.bin_maxima.cend(), value);

  if (it == _bin_data.bin_maxima.cend()) {
    return INVALID_BIN_ID;
  }

  return static_cast<BinID>(std::distance(_bin_data.bin_maxima.cbegin(), it));
}

template <typename T>
T EqualDistinctCountHistogram<T>::bin_minimum(const BinID index) const {
  DebugAssert(index < _bin_data.bin_minima.size(), "Index is not a valid bin.");
  return _bin_data.bin_minima[index];
}

template <typename T>
T EqualDistinctCountHistogram<T>::bin_maximum(const BinID index) const {
  DebugAssert(index < _bin_data.bin_maxima.size(), "Index is not a valid bin.");
  return _bin_data.bin_maxima[index];
}

template <typename T>
HistogramCountType EqualDistinctCountHistogram<T>::bin_height(const BinID index) const {
  DebugAssert(index < _bin_data.bin_heights.size(), "Index is not a valid bin.");
  return _bin_data.bin_heights[index];
}

template <typename T>
HistogramCountType EqualDistinctCountHistogram<T>::bin_distinct_count(const BinID index) const {
  DebugAssert(index < bin_count(), "Index is not a valid bin.");
  return _bin_data.distinct_count_per_bin + (index < _bin_data.bin_count_with_extra_value ? 1 : 0);
}

template <typename T>
HistogramCountType EqualDistinctCountHistogram<T>::total_count() const {
  return std::accumulate(_bin_data.bin_heights.cbegin(), _bin_data.bin_heights.cend(), HistogramCountType{0});
}

template <typename T>
HistogramCountType EqualDistinctCountHistogram<T>::total_distinct_count() const {
  return static_cast<HistogramCountType>(_bin_data.distinct_count_per_bin * bin_count() +
                                         _bin_data.bin_count_with_extra_value);
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(EqualDistinctCountHistogram);

}  // namespace opossum
