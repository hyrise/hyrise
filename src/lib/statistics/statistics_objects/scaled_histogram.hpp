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
 * "Scaled" histogram for temporary statistics objects, e.g., during cardinality estimation. This class is a wrapper
 * around a referenced histogram that adds a selectivity for scaling, which forwards most calls to the original
 * histogram. Thus, we do not have to copy all bins of the original histogram over and over. As is the case for
 * ReferenceSegments, we always keep one single level of indirection. Doing so has shown significant improvements in the
 * cardinality estimation.
 *
 * Currently, we only use this wrapper for scaled histograms, but we could also use it for sliced and pruned histograms
 * where (ranges of) bins are simply sliced/pruned away.
 * TODO(anyone): Add support for sliced and pruned histograms when we consider copying their bins a bottleneck. Another
 *               opportunity for improvement could be to hold only a pointer to the domain if we often encounter copying
 *               string domains.
 */
template <typename T>
class ScaledHistogram : public AbstractHistogram<T> {
 public:
  using AbstractHistogram<T>::AbstractHistogram;

  ScaledHistogram(const AbstractHistogram<T>& referenced_histogram, const Selectivity selectivity,
                  const HistogramDomain<T>& domain = {});

  // Convenience builder for a ScaledHistogram from the referenced histogram. Ensures exactly one indirection.
  static std::shared_ptr<ScaledHistogram<T>> from_referenced_histogram(const AbstractHistogram<T>& referenced_histogram,
                                                                       const Selectivity selectivity);

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
  friend class ScaledHistogramTest;

  const std::shared_ptr<const AbstractHistogram<T>> _referenced_histogram;

  const Selectivity _selectivity;
};

// For gtest.
template <typename T>
std::ostream& operator<<(std::ostream& stream, const ScaledHistogram<T>& histogram) {
  return stream << histogram.description() << '\n';
}

EXPLICITLY_DECLARE_DATA_TYPES(ScaledHistogram);

}  // namespace hyrise
