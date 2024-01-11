#pragma once

#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "abstract_histogram.hpp"
#include "types.hpp"

namespace hyrise {

/**
 * Generic histogram.
 * Bins do not necessarily share any common traits such as height or width or distinct count.
 * This histogram should only be used to create temporary statistics objects, as its memory requirements are higher than
 * those of other histogram types.
 */
template <typename T>
class ScaledHistogram : public AbstractHistogram<T> {
 public:
  using AbstractHistogram<T>::AbstractHistogram;

  /*ScaledHistogram(const std::shared_ptr<const AbstractHistogram<T>>& referenced_histogram,
                   std::vector<HistogramCountType>&& bin_heights, std::vector<HistogramCountType>&& bin_distinct_counts,
                   const HistogramDomain<T>& domain = {});*/

  ScaledHistogram(const std::shared_ptr<const AbstractHistogram<T>>& referenced_histogram,
                  const Selectivity selectivity, const HistogramDomain<T>& domain = {});

  // Convenience builder for a ScaledHistogram from the referenced histogram.
  static std::shared_ptr<ScaledHistogram<T>> from_referenced_histogram(
      const std::shared_ptr<const AbstractHistogram<T>>& referenced_histogram, const Selectivity selectivity);

  std::string name() const override;
  std::shared_ptr<AbstractHistogram<T>> clone() const override;
  HistogramCountType total_distinct_count() const override;
  HistogramCountType total_count() const override;

  BinID bin_count() const override;

  const T& bin_minimum(const BinID index) const override;
  const T& bin_maximum(const BinID index) const override;
  HistogramCountType bin_height(const BinID index) const override;
  HistogramCountType bin_distinct_count(const BinID index) const override;

  BinID bin_for_value(const T& value) const override;
  BinID next_bin_for_value(const T& value) const override;

 private:
  const std::shared_ptr<const AbstractHistogram<T>> _referenced_histogram;

  const Selectivity _selectivity;

  /**
   * We use multiple vectors rather than a vector of structs for ease-of-use with STL library functions.
   */

  // Number of values on a per-bin basis.
  //const std::vector<HistogramCountType> _bin_heights;

  // Number of distinct values on a per-bin basis.
  //const std::vector<HistogramCountType> _bin_distinct_counts;

  // Aggregated counts over all bins, to avoid redundant computation.
  const HistogramCountType _total_count;
  // HistogramCountType _total_distinct_count{0};
};

// For gtest
template <typename T>
std::ostream& operator<<(std::ostream& stream, const ScaledHistogram<T>& histogram) {
  stream << histogram.description() << std::endl;
  return stream;
}

EXPLICITLY_DECLARE_DATA_TYPES(ScaledHistogram);

}  // namespace hyrise
