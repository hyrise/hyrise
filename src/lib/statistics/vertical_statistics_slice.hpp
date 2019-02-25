#pragma once

#include <iostream>
#include <memory>

#include "base_vertical_statististics_slice.hpp"
#include "histograms/equal_distinct_count_histogram.hpp"
#include "histograms/generic_histogram.hpp"
#include "histograms/single_bin_histogram.hpp"
#include "selectivity.hpp"
#include "statistics_objects/null_value_ratio.hpp"

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
class VerticalStatisticsSlice : public BaseVerticalStatisticsSlice {
 public:
  VerticalStatisticsSlice();

  void set_statistics_object(const std::shared_ptr<AbstractStatisticsObject>& statistics_object) override;

  std::shared_ptr<BaseVerticalStatisticsSlice> scaled(const Selectivity selectivity) const override;

  std::shared_ptr<BaseVerticalStatisticsSlice> sliced(
      const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
      const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const override;

  std::shared_ptr<AbstractHistogram<T>> histogram;
  std::shared_ptr<MinMaxFilter<T>> min_max_filter;
  std::shared_ptr<RangeFilter<T>> range_filter;
  std::shared_ptr<NullValueRatio> null_value_ratio;
};

template <typename T>
std::ostream& operator<<(std::ostream& stream, const VerticalStatisticsSlice<T>& vertical_slices) {
  stream << "{" << std::endl;

  if (vertical_slices.histogram) {
    stream << vertical_slices.histogram->description() << std::endl;
  }

  if (vertical_slices.min_max_filter) {
    // TODO(anybody) implement printing of MinMaxFilter if ever required
    stream << "Has MinMaxFilter" << std::endl;
  }

  if (vertical_slices.range_filter) {
    // TODO(anybody) implement printing of v if ever required
    stream << "Has RangeFilter" << std::endl;
  }
  if (vertical_slices.null_value_ratio) {
    stream << "NullValueRatio: " << vertical_slices.null_value_ratio->null_value_ratio << std::endl;
  }

  stream << "}" << std::endl;

  return stream;
}

}  // namespace opossum
