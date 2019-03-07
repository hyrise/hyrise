#pragma once

#include <iostream>
#include <memory>

#include "base_column_statistics.hpp"
#include "statistics/statistics_objects/equal_distinct_count_histogram.hpp"
#include "statistics/statistics_objects/generic_histogram.hpp"
#include "statistics/statistics_objects/null_value_ratio_statistics.hpp"
#include "statistics/statistics_objects/single_bin_histogram.hpp"
#include "types.hpp"

namespace opossum {

template <typename T>
class AbstractHistogram;
class AbstractStatisticsObject;
template <typename T>
class MinMaxFilter;
template <typename T>
class RangeFilter;

/**
 * Statistically represents a slice of a Column. Might cover any number of rows or Chunks.
 *
 * Contains any number of AbstractStatisticsObjects (Histograms, Filters, etc.).
 */
template <typename T>
class ColumnStatistics : public BaseColumnStatistics {
 public:
  ColumnStatistics();

  void set_statistics_object(const std::shared_ptr<AbstractStatisticsObject>& statistics_object) override;

  std::shared_ptr<BaseColumnStatistics> scaled(const Selectivity selectivity) const override;

  std::shared_ptr<BaseColumnStatistics> sliced(
      const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
      const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const override;

  std::shared_ptr<AbstractHistogram<T>> histogram;
  std::shared_ptr<MinMaxFilter<T>> min_max_filter;
  std::shared_ptr<RangeFilter<T>> range_filter;
  std::shared_ptr<NullValueRatioStatistics> null_value_ratio;
};

template <typename T>
std::ostream& operator<<(std::ostream& stream, const ColumnStatistics<T>& column_statistics) {
  stream << "{" << std::endl;

  if (column_statistics.histogram) {
    stream << column_statistics.histogram->description() << std::endl;
  }

  if (column_statistics.min_max_filter) {
    // TODO(anybody) implement printing of MinMaxFilter if ever required
    stream << "Has MinMaxFilter" << std::endl;
  }

  if (column_statistics.range_filter) {
    // TODO(anybody) implement printing of RangeFilter if ever required
    stream << "Has RangeFilter" << std::endl;
  }
  if (column_statistics.null_value_ratio) {
    stream << "NullValueRatio: " << column_statistics.null_value_ratio->ratio << std::endl;
  }

  stream << "}" << std::endl;

  return stream;
}

}  // namespace opossum
