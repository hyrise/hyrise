#include "scaled_histogram.hpp"

#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "utils/assert.hpp"

namespace hyrise {

/*
template <typename T>
ScaledHistogram<T>::ScaledHistogram(const std::shared_ptr<const AbstractHistogram<T>>& referenced_histogram,
                                      std::vector<HistogramCountType>&& bin_heights,
                                      std::vector<HistogramCountType>&& bin_distinct_counts,
                                      const HistogramDomain<T>& domain)
    : AbstractHistogram<T>(domain),
      _referenced_histogram(referenced_histogram),
      _bin_heights(std::move(bin_heights)),
      _bin_distinct_counts(std::move(bin_distinct_counts)) {
  Assert(_bin_heights.size() == _bin_distinct_counts.size(), "Must have the same number of counts and distinct counts.");

  AbstractHistogram<T>::_assert_bin_validity();

  _total_count = std::accumulate(_bin_heights.cbegin(), _bin_heights.cend(), HistogramCountType{0});
  _total_distinct_count =
      std::accumulate(_bin_distinct_counts.cbegin(), _bin_distinct_counts.cend(), HistogramCountType{0});
}
*/

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
  //const auto bin_count = referenced_histogram->bin_count();
  //auto bin_heights = std::vector<HistogramCountType>(bin_count);
  // auto bin_distinct_counts = std::vector<HistogramCountType>(bin_count);

  if (const auto scaled_histogram = std::dynamic_pointer_cast<const ScaledHistogram>(referenced_histogram)) {
    return std::make_shared<ScaledHistogram<T>>(scaled_histogram->_referenced_histogram,
                                                scaled_histogram->_selectivity * selectivity);
  }

  return std::make_shared<ScaledHistogram<T>>(referenced_histogram, selectivity);

  /*
  if (const auto scaled_histogram = std::dynamic_pointer_cast<const ScaledHistogram>(referenced_histogram)) {
    for (auto bin_id = size_t{0}; bin_id < bin_count; ++bin_id) {
      bin_heights[bin_id] = scaled_histogram->_bin_heights[bin_id] * selectivity;
      bin_distinct_counts[bin_id] =  AbstractHistogram<T>::_scale_distinct_count(scaled_histogram->_bin_heights[bin_id], scaled_histogram->_bin_distinct_counts[bin_id], selectivity);
    }

    return std::make_shared<ScaledHistogram<T>>(scaled_histogram->_referenced_histogram, std::move(bin_heights), std::move(bin_distinct_counts), scaled_histogram->domain());
  }

  for (auto bin_id = size_t{0}; bin_id < bin_count; ++bin_id) {
      const auto bin_height = referenced_histogram->bin_height(bin_id);
      bin_heights[bin_id] = bin_height * selectivity;
      bin_distinct_counts[bin_id] = AbstractHistogram<T>::_scale_distinct_count(bin_height, referenced_histogram->bin_distinct_count(bin_id), selectivity);
  }

  return std::make_shared<ScaledHistogram<T>>(referenced_histogram, std::move(bin_heights), std::move(bin_distinct_counts), referenced_histogram->domain());
  */
}

template <typename T>
std::string ScaledHistogram<T>::name() const {
  return "Scaled";
}

template <typename T>
std::shared_ptr<AbstractHistogram<T>> ScaledHistogram<T>::clone() const {
  return std::make_shared<ScaledHistogram<T>>(_referenced_histogram, _selectivity);
  /*auto bin_heights = _bin_heights;
  auto bin_distinct_counts = _bin_distinct_counts;

  return std::make_shared<ScaledHistogram<T>>(_referenced_histogram, std::move(bin_heights),
                                               std::move(bin_distinct_counts));*/
}

template <typename T>
BinID ScaledHistogram<T>::bin_count() const {
  return _referenced_histogram->bin_count();
  // return _bin_heights.size();
}

template <typename T>
const T& ScaledHistogram<T>::bin_minimum(const BinID index) const {
  //DebugAssert(index < _bin_heights.size(), "Index is not a valid bin.");
  return _referenced_histogram->bin_minimum(index);
}

template <typename T>
const T& ScaledHistogram<T>::bin_maximum(const BinID index) const {
  // DebugAssert(index < _bin_heights.size(), "Index is not a valid bin.");
  return _referenced_histogram->bin_maximum(index);
}

template <typename T>
HistogramCountType ScaledHistogram<T>::bin_height(const BinID index) const {
  // DebugAssert(index < _bin_heights.size(), "Index is not a valid bin.");
  // return _bin_heights[index];
  return _referenced_histogram->bin_height(index) * _selectivity;
}

template <typename T>
HistogramCountType ScaledHistogram<T>::bin_distinct_count(const BinID index) const {
  // DebugAssert(index < _bin_distinct_counts.size(), "Index is not a valid bin.");
  // return _bin_distinct_counts[index];
  return AbstractHistogram<T>::_scale_distinct_count(_referenced_histogram->bin_height(index),
                                                     _referenced_histogram->bin_distinct_count(index), _selectivity);
}

template <typename T>
HistogramCountType ScaledHistogram<T>::total_count() const {
  return _total_count;
  //return _referenced_histogram->total_count() * _selectivity;
}

template <typename T>
HistogramCountType ScaledHistogram<T>::total_distinct_count() const {
  //return _total_distinct_count;
  auto distinct_count = HistogramCountType{0};
  const auto bin_count = this->bin_count();
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
