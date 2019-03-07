#include "single_bin_histogram.hpp"

#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

namespace opossum {

template <typename T>
SingleBinHistogram<T>::SingleBinHistogram(const T& minimum, const T& maximum, HistogramCountType total_count,
                                          HistogramCountType distinct_count, const HistogramDomain<T>& domain)
    : AbstractHistogram<T>(domain),
      _minimum(minimum),
      _maximum(maximum),
      _total_count(total_count),
      _distinct_count(distinct_count) {
  Assert(minimum <= maximum, "Minimum must be smaller than maximum.");
  Assert(distinct_count <= total_count, "Cannot have more distinct values than total values.");
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
std::shared_ptr<AbstractStatisticsObject> SingleBinHistogram<T>::scaled(const Selectivity selectivity) const {
  // Special impl for SingleBinHistogram to return a SingleBinHistogram. AbstractHistogram::scaled would return
  // a GenericHistogram
  const auto total_count = HistogramCountType{_total_count * selectivity};
  const auto distinct_count = AbstractHistogram<T>::_scale_distinct_count(selectivity, _total_count, _distinct_count);
  return std::make_shared<SingleBinHistogram<T>>(_minimum, _maximum, total_count, distinct_count);
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(SingleBinHistogram);

}  // namespace opossum
