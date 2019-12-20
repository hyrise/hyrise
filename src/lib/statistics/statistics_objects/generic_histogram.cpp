#include "generic_histogram.hpp"

#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

namespace opossum {

template <typename T>
GenericHistogram<T>::GenericHistogram(std::vector<T>&& bin_minima, std::vector<T>&& bin_maxima,
                                      std::vector<HistogramCountType>&& bin_heights,
                                      std::vector<HistogramCountType>&& bin_distinct_counts,
                                      const HistogramDomain<T>& domain)
    : AbstractHistogram<T>(domain),
      _bin_minima(std::move(bin_minima)),
      _bin_maxima(std::move(bin_maxima)),
      _bin_heights(std::move(bin_heights)),
      _bin_distinct_counts(std::move(bin_distinct_counts)) {
  Assert(_bin_minima.size() == _bin_maxima.size(), "Must have the same number of lower as upper bin edges.");
  Assert(_bin_minima.size() == _bin_heights.size(), "Must have the same number of edges and heights.");
  Assert(_bin_minima.size() == _bin_distinct_counts.size(), "Must have the same number of edges and distinct counts.");

  AbstractHistogram<T>::_assert_bin_validity();

  _total_count = std::accumulate(_bin_heights.cbegin(), _bin_heights.cend(), HistogramCountType{0});
  _total_distinct_count =
      std::accumulate(_bin_distinct_counts.cbegin(), _bin_distinct_counts.cend(), HistogramCountType{0});
}

template <typename T>
std::shared_ptr<GenericHistogram<T>> GenericHistogram<T>::with_single_bin(const T& min, const T& max,
                                                                          const HistogramCountType& height,
                                                                          const HistogramCountType& distinct_count,
                                                                          const HistogramDomain<T>& domain) {
  return std::make_shared<GenericHistogram>(std::vector{min}, std::vector{max}, std::vector{height},
                                            std::vector{distinct_count}, domain);
}

template <typename T>
std::string GenericHistogram<T>::name() const {
  return "Generic";
}

template <typename T>
std::shared_ptr<AbstractHistogram<T>> GenericHistogram<T>::clone() const {
  auto bin_minima = _bin_minima;
  auto bin_maxima = _bin_maxima;
  auto bin_heights = _bin_heights;
  auto bin_distinct_counts = _bin_distinct_counts;

  return std::make_shared<GenericHistogram<T>>(std::move(bin_minima), std::move(bin_maxima), std::move(bin_heights),
                                               std::move(bin_distinct_counts));
}

template <typename T>
BinID GenericHistogram<T>::bin_count() const {
  return _bin_heights.size();
}

template <typename T>
BinID GenericHistogram<T>::_bin_for_value(const T& value) const {
  const auto it = std::lower_bound(_bin_maxima.cbegin(), _bin_maxima.cend(), value);
  const auto index = static_cast<BinID>(std::distance(_bin_maxima.cbegin(), it));

  if (it == _bin_maxima.cend() || value < bin_minimum(index) || value > bin_maximum(index)) {
    return INVALID_BIN_ID;
  }

  return index;
}

template <typename T>
BinID GenericHistogram<T>::_next_bin_for_value(const T& value) const {
  const auto it = std::upper_bound(_bin_maxima.cbegin(), _bin_maxima.cend(), value);

  if (it == _bin_maxima.cend()) {
    return INVALID_BIN_ID;
  }

  return static_cast<BinID>(std::distance(_bin_maxima.cbegin(), it));
}

template <typename T>
const T& GenericHistogram<T>::bin_minimum(const BinID index) const {
  DebugAssert(index < _bin_minima.size(), "Index is not a valid bin.");
  return _bin_minima[index];
}

template <typename T>
const T& GenericHistogram<T>::bin_maximum(const BinID index) const {
  DebugAssert(index < _bin_maxima.size(), "Index is not a valid bin.");
  return _bin_maxima[index];
}

template <typename T>
HistogramCountType GenericHistogram<T>::bin_height(const BinID index) const {
  DebugAssert(index < _bin_heights.size(), "Index is not a valid bin.");
  return _bin_heights[index];
}

template <typename T>
HistogramCountType GenericHistogram<T>::bin_distinct_count(const BinID index) const {
  DebugAssert(index < _bin_distinct_counts.size(), "Index is not a valid bin.");
  return _bin_distinct_counts[index];
}

template <typename T>
HistogramCountType GenericHistogram<T>::total_count() const {
  return _total_count;
}

template <typename T>
HistogramCountType GenericHistogram<T>::total_distinct_count() const {
  return _total_distinct_count;
}

template <typename T>
bool GenericHistogram<T>::operator==(const GenericHistogram<T>& rhs) const {
  return _bin_minima == rhs._bin_minima && _bin_maxima == rhs._bin_maxima && _bin_heights == rhs._bin_heights &&
         _bin_distinct_counts == rhs._bin_distinct_counts;
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(GenericHistogram);

}  // namespace opossum
