#include "scaled_histogram.hpp"

#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "utils/assert.hpp"

namespace hyrise {

template <typename T>
ScaledHistogram<T>::ScaledHistogram(const std::shared_ptr<const AbstractHistogram<T>>& referenced_histogram,
                                    const Selectivity selectivity, const HistogramDomain<T>& domain)
    : AbstractHistogram<T>(domain),
      _referenced_histogram(referenced_histogram),
      _selectivity{selectivity},
      _total_count{referenced_histogram->total_count() * selectivity} {}

template <typename T>
std::shared_ptr<ScaledHistogram<T>> ScaledHistogram<T>::from_referenced_histogram(
    const std::shared_ptr<const AbstractHistogram<T>>& referenced_histogram, const Selectivity selectivity) {
  // Reference the original histogram and adapt the selectivity if the input itself is a ScaledHistogram.
  if (const auto scaled_histogram = std::dynamic_pointer_cast<const ScaledHistogram>(referenced_histogram)) {
    return std::make_shared<ScaledHistogram<T>>(scaled_histogram->_referenced_histogram,
                                                scaled_histogram->_selectivity * selectivity);
  }

  return std::make_shared<ScaledHistogram<T>>(referenced_histogram, selectivity);
}

template <typename T>
std::string ScaledHistogram<T>::name() const {
  return "Scaled";
}

template <typename T>
std::shared_ptr<AbstractHistogram<T>> ScaledHistogram<T>::clone() const {
  return std::make_shared<ScaledHistogram<T>>(_referenced_histogram, _selectivity);
}

template <typename T>
BinID ScaledHistogram<T>::bin_count() const {
  return _referenced_histogram->bin_count();
}

template <typename T>
const T& ScaledHistogram<T>::bin_minimum(const BinID index) const {
  DebugAssert(index < bin_count(), "Index is not a valid bin.");
  return _referenced_histogram->bin_minimum(index);
}

template <typename T>
const T& ScaledHistogram<T>::bin_maximum(const BinID index) const {
  DebugAssert(index < bin_count(), "Index is not a valid bin.");
  return _referenced_histogram->bin_maximum(index);
}

template <typename T>
HistogramCountType ScaledHistogram<T>::bin_height(const BinID index) const {
  DebugAssert(index < bin_count(), "Index is not a valid bin.");
  return _referenced_histogram->bin_height(index) * _selectivity;
}

template <typename T>
HistogramCountType ScaledHistogram<T>::bin_distinct_count(const BinID index) const {
  DebugAssert(index < bin_count(), "Index is not a valid bin.");
  return AbstractHistogram<T>::_scale_distinct_count(_referenced_histogram->bin_height(index),
                                                     _referenced_histogram->bin_distinct_count(index), _selectivity);
}

template <typename T>
HistogramCountType ScaledHistogram<T>::total_count() const {
  return _total_count;
}

template <typename T>
HistogramCountType ScaledHistogram<T>::total_distinct_count() const {
  auto distinct_count = HistogramCountType{0};
  const auto bin_count = _referenced_histogram->bin_count();
  for (auto bin_id = BinID{0}; bin_id < bin_count; ++bin_id) {
    distinct_count += bin_distinct_count(bin_id);
  }
  return distinct_count;
}

template <typename T>
BinID ScaledHistogram<T>::bin_for_value(const T& value) const {
  return _referenced_histogram->bin_for_value(value);
}

template <typename T>
BinID ScaledHistogram<T>::next_bin_for_value(const T& value) const {
  return _referenced_histogram->next_bin_for_value(value);
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(ScaledHistogram);

}  // namespace hyrise
