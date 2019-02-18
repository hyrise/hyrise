#pragma once

#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "abstract_histogram.hpp"
#include "statistics/selectivity.hpp"
#include "types.hpp"

namespace opossum {

/**
 * We use multiple vectors rather than a vector of structs for ease-of-use with STL library functions.
 */
template <typename T>
struct GenericBinData {
  // Min values on a per-bin basis.
  std::vector<T> bin_minima;

  // Max values on a per-bin basis.
  std::vector<T> bin_maxima;

  // Number of values on a per-bin basis.
  std::vector<HistogramCountType> bin_heights;

  // Number of distinct values on a per-bin basis.
  std::vector<HistogramCountType> bin_distinct_counts;
};

/**
 * Generic histogram.
 * Bins do not necessarily share any common traits such as height or width or distinct count.
 * This histogram should only be used to create temporary statistics objects, as its space complexity is high.
 */
template <typename T>
class GenericHistogram : public AbstractHistogram<T> {
 public:
  using AbstractHistogram<T>::AbstractHistogram;
  GenericHistogram(std::vector<T>&& bin_minima, std::vector<T>&& bin_maxima,
                   std::vector<HistogramCountType>&& bin_heights,
                   std::vector<HistogramCountType>&& bin_count_distincts);
  GenericHistogram(std::vector<T>&& bin_minima, std::vector<T>&& bin_maxima,
                   std::vector<HistogramCountType>&& bin_heights, std::vector<HistogramCountType>&& bin_count_distincts,
                   const StringHistogramDomain& string_domain);

  HistogramType histogram_type() const override;
  std::string histogram_name() const override;
  std::shared_ptr<AbstractHistogram<T>> clone() const override;
  HistogramCountType total_distinct_count() const override;
  HistogramCountType total_count() const override;

  BinID bin_count() const override;

  T bin_minimum(const BinID index) const override;
  T bin_maximum(const BinID index) const override;
  HistogramCountType bin_height(const BinID index) const override;
  HistogramCountType bin_distinct_count(const BinID index) const override;

  bool operator==(const GenericHistogram<T>& rhs) const;

 protected:
  BinID _bin_for_value(const T& value) const override;

  BinID _next_bin_for_value(const T& value) const override;

 private:
  const GenericBinData<T> _bin_data;
};

template <typename T>
std::ostream& operator<<(std::ostream& stream, const GenericHistogram<T>& histogram) {
  stream << histogram.description(true) << std::endl;
  return stream;
}

}  // namespace opossum
