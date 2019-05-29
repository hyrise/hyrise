#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "abstract_histogram.hpp"
#include "types.hpp"

namespace opossum {

/**
 * Histogram with a single bin.
 * This histogram type is intended for statistics where only minimal information is known or required (e.g. in Tests).
 */
template <typename T>
class SingleBinHistogram : public AbstractHistogram<T> {
 public:
  using AbstractHistogram<T>::AbstractHistogram;

  SingleBinHistogram(const T& minimum, const T& maximum, HistogramCountType total_count,
                     HistogramCountType distinct_count, const HistogramDomain<T>& domain = {});

  std::string name() const override;
  std::shared_ptr<AbstractHistogram<T>> clone() const override;
  HistogramCountType total_distinct_count() const override;
  HistogramCountType total_count() const override;

  BinID bin_count() const override;

  T bin_minimum(const BinID index) const override;
  T bin_maximum(const BinID index) const override;
  HistogramCountType bin_height(const BinID index) const override;
  HistogramCountType bin_distinct_count(const BinID index) const override;

  std::shared_ptr<AbstractStatisticsObject> scaled(const Selectivity selectivity) const override;

 protected:
  BinID _bin_for_value(const T& value) const override;
  BinID _next_bin_for_value(const T& value) const override;

 private:
  T _minimum;
  T _maximum;
  HistogramCountType _total_count;
  HistogramCountType _distinct_count;
};

}  // namespace opossum
