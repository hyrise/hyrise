#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "abstract_histogram.hpp"
#include "statistics/selectivity.hpp"
#include "types.hpp"

namespace opossum {

/**
 * Histogram with a single bin.
 * This histogram type is intended for statistics where only minimal information is known or required.
 */
template <typename T>
class SingleBinHistogram : public AbstractHistogram<T> {
 public:
  using AbstractHistogram<T>::AbstractHistogram;
  SingleBinHistogram(const T& minimum, const T& maximum, HistogramCountType total_count,
                     HistogramCountType distinct_count);
  SingleBinHistogram(const std::string& minimum, const std::string& maximum, HistogramCountType total_count,
                     HistogramCountType distinct_count, const StringHistogramDomain& string_domain);

  /**
   * Create a histogram based on a value distribution.
   * @param value_distribution      For each value, the number of occurrences. Must be sorted
   */
  static std::shared_ptr<SingleBinHistogram<T>> from_distribution(
      const std::vector<std::pair<T, HistogramCountType>>& value_distribution,
      const std::optional<StringHistogramDomain>& string_domain = std::nullopt);

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
