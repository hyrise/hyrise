#pragma once

#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "abstract_histogram.hpp"
#include "types.hpp"

namespace opossum {

/**
 * Generic histogram.
 * Bins do not necessarily share any common traits such as height or width or distinct count.
 * This histogram should only be used to create temporary statistics objects, as its memory requirements are higher than
 * those of other histogram types.
 */
template <typename T>
class GenericHistogram : public AbstractHistogram<T> {
 public:
  using AbstractHistogram<T>::AbstractHistogram;

  GenericHistogram(std::vector<T>&& bin_minima, std::vector<T>&& bin_maxima,
                   std::vector<HistogramCountType>&& bin_heights, std::vector<HistogramCountType>&& bin_distinct_counts,
                   const HistogramDomain<T>& domain = {});

  // Convenience builder for a GenericHistogram wiht a single bin
  static std::shared_ptr<GenericHistogram<T>> with_single_bin(const T& min, const T& max,
                                                              const HistogramCountType& height,
                                                              const HistogramCountType& distinct_count,
                                                              const HistogramDomain<T>& domain = {});

  std::string name() const override;
  std::shared_ptr<AbstractHistogram<T>> clone() const override;
  HistogramCountType total_distinct_count() const override;
  HistogramCountType total_count() const override;

  BinID bin_count() const override;

  const T& bin_minimum(const BinID index) const override;
  const T& bin_maximum(const BinID index) const override;
  HistogramCountType bin_height(const BinID index) const override;
  HistogramCountType bin_distinct_count(const BinID index) const override;

  bool operator==(const GenericHistogram<T>& rhs) const;

 protected:
  BinID _bin_for_value(const T& value) const override;

  BinID _next_bin_for_value(const T& value) const override;

 private:
  /**
   * We use multiple vectors rather than a vector of structs for ease-of-use with STL library functions.
   */
  // Min values on a per-bin basis.
  std::vector<T> _bin_minima;

  // Max values on a per-bin basis.
  std::vector<T> _bin_maxima;

  // Number of values on a per-bin basis.
  std::vector<HistogramCountType> _bin_heights;

  // Number of distinct values on a per-bin basis.
  std::vector<HistogramCountType> _bin_distinct_counts;

  // Aggregated counts over all bins, to avoid redundant computation
  HistogramCountType _total_count;
  HistogramCountType _total_distinct_count;
};

// For gtest
template <typename T>
std::ostream& operator<<(std::ostream& stream, const GenericHistogram<T>& histogram) {
  stream << histogram.description() << std::endl;
  return stream;
}

}  // namespace opossum
