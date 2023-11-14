#pragma once

#include <iostream>
#include <memory>
#include <thread>

#include "base_attribute_statistics.hpp"
#include "hyrise.hpp"
#include "statistics/statistics_objects/distinct_value_count.hpp"
#include "statistics/statistics_objects/equal_distinct_count_histogram.hpp"
#include "statistics/statistics_objects/generic_histogram.hpp"
#include "statistics/statistics_objects/null_value_ratio_statistics.hpp"
#include "statistics/table_statistics.hpp"
#include "types.hpp"
#include "utils/data_loading_utils.hpp"
#include "utils/settings/data_loading_setting.hpp"

namespace hyrise {

template <typename T>
class AbstractHistogram;
class AbstractStatisticsObject;
template <typename T>
class MinMaxFilter;
template <typename T>
class RangeFilter;
template <typename T>
class CountingQuotientFilter;

/**
 * For docs, see BaseAttributeStatistics
 */
template <typename T>
class AttributeStatistics : public BaseAttributeStatistics {
 public:
  AttributeStatistics();

  void set_statistics_object(const std::shared_ptr<AbstractStatisticsObject>& statistics_object) override;

  std::shared_ptr<BaseAttributeStatistics> scaled(const Selectivity selectivity) const override;

  std::shared_ptr<BaseAttributeStatistics> sliced(
      const PredicateCondition predicate_condition, const AllTypeVariant& variant_value,
      const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const override;

  std::shared_ptr<BaseAttributeStatistics> pruned(
      const size_t num_values_pruned, const PredicateCondition predicate_condition, const AllTypeVariant& variant_value,
      const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const override;

  const std::shared_ptr<AbstractHistogram<T>>& histogram() const {
    if (!_histogram) {
      if (_column_id != INVALID_COLUMN_ID) {
        data_loading_utils::load_column_when_necessary(_table_name, _column_id);
        const auto& table_statistics = Hyrise::get().storage_manager.get_table(_table_name)->table_statistics();
        const auto& attribute_statistics = static_cast<const AttributeStatistics<T>&>(*(table_statistics->column_statistics[_column_id]));
        Assert(attribute_statistics.histogram(), "Table should have a set histogram.");
        return attribute_statistics.histogram();
      }
    }

    return _histogram;
  }

  void set_histogram(std::shared_ptr<AbstractHistogram<T>> histogram) {
    _histogram = histogram;
  }

  const std::shared_ptr<MinMaxFilter<T>>& min_max_filter() const {
    if (!_min_max_filter) {
      if (_column_id != INVALID_COLUMN_ID) {
        data_loading_utils::load_column_when_necessary(_table_name, _column_id);
        // We do not have no min_max filters here as column statistics don't have them (only chunk/pruning statistics).
        // Request loading nonetheless.
      }
    }

    return _min_max_filter;
  }

  void set_min_max_filter(std::shared_ptr<MinMaxFilter<T>> min_max_filter) {
    _min_max_filter = min_max_filter;
  }

  template<class Q = T>
  typename std::enable_if<std::is_arithmetic_v<Q>, const std::shared_ptr<RangeFilter<T>>&>::type range_filter() const {
    return _range_filter;
  }

  template<class Q = T>
  typename std::enable_if<std::is_arithmetic_v<Q>, void>::type set_range_filter(std::shared_ptr<RangeFilter<T>> range_filter) {
    _range_filter = range_filter;
  }

  const std::shared_ptr<NullValueRatioStatistics>& null_value_ratio() const {
    return _null_value_ratio;
  }

  void set_null_value_ratio(std::shared_ptr<NullValueRatioStatistics> null_value_ratio) {
    _null_value_ratio = null_value_ratio;
  }

  const std::shared_ptr<DistinctValueCount>& distinct_value_count() const {
    return _distinct_value_count;
  }

  void set_distinct_value_count(std::shared_ptr<DistinctValueCount> distinct_value_count) {
    _distinct_value_count = distinct_value_count;
  }

  void set_table_origin(const std::shared_ptr<Table>& table, const std::string& table_name, const ColumnID column_id, const std::optional<ChunkID> chunk_id = std::nullopt) {
    auto shared_ptr_table = _table.lock();

    Assert(table, "Passed table not initialized.");
    Assert(!shared_ptr_table && !_histogram && !_min_max_filter && !_null_value_ratio&& !_distinct_value_count,
             "Expected statistics to be uninitialized.");
    if constexpr (std::is_arithmetic_v<T>) {
      Assert(!_range_filter,
             "Expected statistics to be uninitialized.");
    }

    _table = table;
    _table_name = table_name;
    _column_id = column_id;
    if (chunk_id) {
      _chunk_id = chunk_id;
    }
  }

 private:
  std::shared_ptr<AbstractHistogram<T>> _histogram{};
  std::shared_ptr<MinMaxFilter<T>> _min_max_filter{};
  std::shared_ptr<RangeFilter<T>> _range_filter{};
  std::shared_ptr<NullValueRatioStatistics> _null_value_ratio{};
  std::shared_ptr<DistinctValueCount> _distinct_value_count{};

  std::mutex _load_mutex{};

  std::weak_ptr<Table> _table{};
  std::string _table_name{};
  ColumnID _column_id{INVALID_COLUMN_ID};
  std::optional<ChunkID> _chunk_id{std::nullopt};
};

template <typename T>
std::ostream& operator<<(std::ostream& stream, const AttributeStatistics<T>& attribute_statistics) {
  stream << "{" << std::endl;

  if (const auto& histogram = attribute_statistics.histogram()) {
    stream << histogram->description() << std::endl;
  }

  if (attribute_statistics.min_max_filter()) {
    // TODO(anybody) implement printing of MinMaxFilter if ever required
    stream << "Has MinMaxFilter" << std::endl;
  }

  if constexpr (std::is_arithmetic_v<T>) {
    if (attribute_statistics.range_filter()) {
      // TODO(anybody) implement printing of RangeFilter if ever required
      stream << "Has RangeFilter" << std::endl;
    }
  }

  if (attribute_statistics.null_value_ratio()) {
    stream << "NullValueRatio: " << attribute_statistics.null_value_ratio()->ratio << std::endl;
  }

  if (attribute_statistics.distinct_value_count()) {
    stream << "DistinctValueCount: " << attribute_statistics.distinct_value_count()->count << std::endl;
  }

  stream << "}" << std::endl;

  return stream;
}

EXPLICITLY_DECLARE_DATA_TYPES(AttributeStatistics);

}  // namespace hyrise
