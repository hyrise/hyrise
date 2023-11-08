#include "attribute_statistics.hpp"

#include <memory>

#include "resolve_type.hpp"
#include "statistics/statistics_objects/abstract_histogram.hpp"
#include "statistics/statistics_objects/generic_histogram.hpp"
#include "statistics/statistics_objects/min_max_filter.hpp"
#include "statistics/statistics_objects/range_filter.hpp"

namespace hyrise {

template <typename T>
AttributeStatistics<T>::AttributeStatistics() : BaseAttributeStatistics(data_type_from_type<T>()) {}

template <typename T>
void AttributeStatistics<T>::set_statistics_object(const std::shared_ptr<AbstractStatisticsObject>& statistics_object) {
  // We allow call patterns like `c.set_statistics_object(o.scaled(0.1f))` where `o.scaled()` might return nullptr
  // because, e.g., scaling is not possible for `o`.
  if (!statistics_object) {
    return;
  }

  if (const auto histogram_object = std::dynamic_pointer_cast<AbstractHistogram<T>>(statistics_object)) {
    _histogram = histogram_object;
  } else if (const auto min_max_object = std::dynamic_pointer_cast<MinMaxFilter<T>>(statistics_object)) {
    _min_max_filter = min_max_object;
  } else if (const auto null_value_ratio_object =
                 std::dynamic_pointer_cast<NullValueRatioStatistics>(statistics_object)) {
    _null_value_ratio = null_value_ratio_object;
  } else if (const auto distinct_value_count_object =
                 std::dynamic_pointer_cast<DistinctValueCount>(statistics_object)) {
    _distinct_value_count = distinct_value_count_object;
  } else {
    if constexpr (std::is_arithmetic_v<T>) {
      if (const auto range_object = std::dynamic_pointer_cast<RangeFilter<T>>(statistics_object)) {
        _range_filter = range_object;
        return;
      }
    }

    Fail("Statistics object type not yet supported.");
  }
}

template <typename T>
std::shared_ptr<BaseAttributeStatistics> AttributeStatistics<T>::scaled(const Selectivity selectivity) const {
  load_column_when_necessary();
  const auto statistics = std::make_shared<AttributeStatistics<T>>();

  if (_histogram) {
    statistics->set_statistics_object(_histogram->scaled(selectivity));
  }

  if (_null_value_ratio) {
    statistics->set_statistics_object(_null_value_ratio->scaled(selectivity));
  }

  if (_min_max_filter) {
    statistics->set_statistics_object(_min_max_filter->scaled(selectivity));
  }

  if constexpr (std::is_arithmetic_v<T>) {
    if (_range_filter) {
      statistics->set_statistics_object(_range_filter->scaled(selectivity));
    }
  }

  if (_distinct_value_count) {
    statistics->set_statistics_object(_distinct_value_count->scaled(selectivity));
  }

  return statistics;
}

template <typename T>
std::shared_ptr<BaseAttributeStatistics> AttributeStatistics<T>::sliced(
    const PredicateCondition predicate_condition, const AllTypeVariant& variant_value,
    const std::optional<AllTypeVariant>& variant_value2) const {
  load_column_when_necessary();
  const auto statistics = std::make_shared<AttributeStatistics<T>>();

  if (_histogram) {
    statistics->set_statistics_object(_histogram->sliced(predicate_condition, variant_value, variant_value2));
  }

  if (_null_value_ratio) {
    statistics->set_statistics_object(_null_value_ratio->sliced(predicate_condition, variant_value, variant_value2));
  }

  if (_min_max_filter) {
    statistics->set_statistics_object(_min_max_filter->sliced(predicate_condition, variant_value, variant_value2));
  }

  if constexpr (std::is_arithmetic_v<T>) {
    if (_range_filter) {
      statistics->set_statistics_object(_range_filter->sliced(predicate_condition, variant_value, variant_value2));
    }

    // We do not slice the distinct value count, since we do not know how it changes.
  }

  return statistics;
}

template <typename T>
std::shared_ptr<BaseAttributeStatistics> AttributeStatistics<T>::pruned(
    const size_t num_values_pruned, const PredicateCondition predicate_condition, const AllTypeVariant& variant_value,
    const std::optional<AllTypeVariant>& variant_value2) const {
  load_column_when_necessary();
  const auto statistics = std::make_shared<AttributeStatistics<T>>();

  if (_histogram) {
    statistics->set_statistics_object(
        _histogram->pruned(num_values_pruned, predicate_condition, variant_value, variant_value2));
  }

  if (_null_value_ratio) {
    // As the null value ratio statistics have no absolute row counts, we cannot prune here. Create an unmodified copy.
    statistics->set_statistics_object(std::make_shared<NullValueRatioStatistics>(_null_value_ratio->ratio));
  }

  // As pruning is on a table-level granularity, it does not make too much sense to implement pruning on chunk-level
  // statistics such as the filters below.

  if (_min_max_filter) {
    Fail("Pruning is not implemented for min/max filters");
  }

  if constexpr (std::is_arithmetic_v<T>) {
    if (_range_filter) {
      Fail("Pruning is not implemented for range filters");
    }
  }

  if (_distinct_value_count) {
    Fail("Pruning is not implemented for distinct value count");
  }

  return statistics;
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(AttributeStatistics);

}  // namespace hyrise
