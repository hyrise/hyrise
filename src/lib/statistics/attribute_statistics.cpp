#include "attribute_statistics.hpp"

#include <cmath>
#include <cstddef>
#include <limits>
#include <memory>
#include <optional>

#include "all_type_variant.hpp"
#include "resolve_type.hpp"
#include "statistics/base_attribute_statistics.hpp"
#include "statistics/statistics_objects/abstract_histogram.hpp"
#include "statistics/statistics_objects/distinct_value_count.hpp"
#include "statistics/statistics_objects/min_max_filter.hpp"
#include "statistics/statistics_objects/null_value_ratio_statistics.hpp"
#include "statistics/statistics_objects/range_filter.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

template <typename T>
AttributeStatistics<T>::AttributeStatistics() : BaseAttributeStatistics(data_type_from_type<T>()) {}

template <typename T>
void AttributeStatistics<T>::set_statistics_object(
    const std::shared_ptr<const AbstractStatisticsObject>& statistics_object) {
  // We allow call patterns like `c.set_statistics_object(o.scaled(0.1f))` where `o.scaled()` might return nullptr
  // because, e.g., scaling is not possible for `o`.
  if (!statistics_object) {
    return;
  }

  if (const auto histogram_object = std::dynamic_pointer_cast<const AbstractHistogram<T>>(statistics_object)) {
    histogram = histogram_object;
  } else if (const auto min_max_object = std::dynamic_pointer_cast<const MinMaxFilter<T>>(statistics_object)) {
    min_max_filter = min_max_object;
  } else if (const auto null_value_ratio_object =
                 std::dynamic_pointer_cast<const NullValueRatioStatistics>(statistics_object)) {
    null_value_ratio = null_value_ratio_object;
  } else if (const auto distinct_value_count_object =
                 std::dynamic_pointer_cast<const DistinctValueCount>(statistics_object)) {
    distinct_value_count = distinct_value_count_object;
  } else {
    if constexpr (std::is_arithmetic_v<T>) {
      if (const auto range_object = std::dynamic_pointer_cast<const RangeFilter<T>>(statistics_object)) {
        range_filter = range_object;
        return;
      }
    }

    Fail("Statistics object type not yet supported.");
  }
}

template <typename T>
std::shared_ptr<const BaseAttributeStatistics> AttributeStatistics<T>::scaled(const Selectivity selectivity) const {
  // We do not create adapted versions of the underlying statistics objects if the selectivity is 1.0 (+/- uncertainty).
  if (std::fabs(1.0f - selectivity) <= std::numeric_limits<Selectivity>::epsilon()) {
    return this->shared_from_this();
  }

  const auto statistics = std::make_shared<AttributeStatistics<T>>();

  if (histogram) {
    statistics->set_statistics_object(histogram->scaled(selectivity));
  }

  if (null_value_ratio) {
    statistics->set_statistics_object(null_value_ratio->scaled(selectivity));
  }

  if (min_max_filter) {
    statistics->set_statistics_object(min_max_filter->scaled(selectivity));
  }

  if constexpr (std::is_arithmetic_v<T>) {
    if (range_filter) {
      statistics->set_statistics_object(range_filter->scaled(selectivity));
    }
  }

  if (distinct_value_count) {
    statistics->set_statistics_object(distinct_value_count->scaled(selectivity));
  }

  return statistics;
}

template <typename T>
std::shared_ptr<const BaseAttributeStatistics> AttributeStatistics<T>::sliced(
    const PredicateCondition predicate_condition, const AllTypeVariant& variant_value,
    const std::optional<AllTypeVariant>& variant_value2) const {
  const auto statistics = std::make_shared<AttributeStatistics<T>>();

  if (histogram) {
    statistics->set_statistics_object(histogram->sliced(predicate_condition, variant_value, variant_value2));
  }

  if (null_value_ratio) {
    statistics->set_statistics_object(null_value_ratio->sliced(predicate_condition, variant_value, variant_value2));
  }

  if (min_max_filter) {
    statistics->set_statistics_object(min_max_filter->sliced(predicate_condition, variant_value, variant_value2));
  }

  if constexpr (std::is_arithmetic_v<T>) {
    if (range_filter) {
      statistics->set_statistics_object(range_filter->sliced(predicate_condition, variant_value, variant_value2));
    }
  }

  // We do not slice the distinct value count because we do not know how it changes.
  return statistics;
}

template <typename T>
std::shared_ptr<const BaseAttributeStatistics> AttributeStatistics<T>::pruned(
    const size_t num_values_pruned, const PredicateCondition predicate_condition, const AllTypeVariant& variant_value,
    const std::optional<AllTypeVariant>& variant_value2) const {
  const auto statistics = std::make_shared<AttributeStatistics<T>>();

  if (histogram) {
    statistics->set_statistics_object(
        histogram->pruned(num_values_pruned, predicate_condition, variant_value, variant_value2));
  }

  if (null_value_ratio) {
    // As the null value ratio statistics have no absolute row counts, we cannot prune here. Create an unmodified copy.
    statistics->set_statistics_object(std::make_shared<NullValueRatioStatistics>(null_value_ratio->ratio));
  }

  // As pruning is on a table-level granularity, it does not make too much sense to implement pruning on chunk-level
  // statistics such as the filters below.

  if (min_max_filter) {
    Fail("Pruning is not implemented for min/max filters.");
  }

  if constexpr (std::is_arithmetic_v<T>) {
    if (range_filter) {
      Fail("Pruning is not implemented for range filters.");
    }
  }

  if (distinct_value_count) {
    Fail("Pruning is not implemented for distinct value count.");
  }

  return statistics;
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(AttributeStatistics);

}  // namespace hyrise
