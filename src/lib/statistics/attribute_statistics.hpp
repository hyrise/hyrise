#pragma once

#include <iostream>
#include <memory>

#include "base_attribute_statistics.hpp"
#include "hyrise.hpp"
#include "statistics/statistics_objects/distinct_value_count.hpp"
#include "statistics/statistics_objects/equal_distinct_count_histogram.hpp"
#include "statistics/statistics_objects/generic_histogram.hpp"
#include "statistics/statistics_objects/null_value_ratio_statistics.hpp"
#include "types.hpp"

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
      const auto shared_ptr_table = _table.lock();
      Assert(shared_ptr_table, "AttributeStatistics not set up properly.");
      std::cout << &*shared_ptr_table << ":col" << _column_id << " WE NEED TO LOAD HISTOGRAM." << std::endl;
    }

    // TODO: fetch created histogram from storage_manager, where the new table now is.

    return _histogram;
  }

  void set_histogram(std::shared_ptr<AbstractHistogram<T>> histogram) {
    _histogram = histogram;
  }

  const std::shared_ptr<MinMaxFilter<T>>& min_max_filter() const {
    const auto shared_ptr_table = _table.lock();
    if (!_min_max_filter) {
      std::cout << &*shared_ptr_table << ":col" << _column_id << " WE NEED TO LOAD MIN/MAX." << std::endl;

      auto& plugin_manager = Hyrise::get().plugin_manager;
      const auto& plugins = plugin_manager.loaded_plugins();
      Assert(std::binary_search(plugins.cbegin(), plugins.cend(), "hyriseDataLoadingPlugin"),
             "Data Loading plugin is not loaded.");
      Assert(plugin_manager.user_executable_functions().contains({"hyriseDataLoadingPlugin", "LoadTableAndStatistics"}),
             "Function 'LoadTableAndStatistics' not found.");

      plugin_manager.exec_user_function("hyriseDataLoadingPlugin", "LoadTableAndStatistics");
    }
    // Assert(shared_ptr_table, "AttributeStatistics not set up properly.");

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

  void set_table_origin(const std::shared_ptr<Table>& table, const ColumnID column_id, const std::optional<ChunkID> chunk_id = std::nullopt) {
    auto shared_ptr_table = _table.lock();

    if constexpr (std::is_arithmetic_v<T>) {
      Assert(!shared_ptr_table && !_histogram && !_min_max_filter  && !_null_value_ratio&& !_distinct_value_count,
             "Expected statistics to be uninitialized.");
    } else {
      // Assert(!shared_ptr_table && !_histogram && !_range_filter && !_min_max_filter  && !_null_value_ratio&& !_distinct_value_count,
             // "Expected statistics to be uninitialized.");
    }

    _table = table;
    _column_id = column_id;
    if (chunk_id) {
      _chunk_id = chunk_id;
    }
  }

  void load_column_when_necessary() const {
    // We assume that all desired statistics are created once we found at least one.
    if (_histogram || _null_value_ratio || _min_max_filter || _distinct_value_count) {
      auto shared_ptr_table = _table.lock();
      std::cout << &*shared_ptr_table << ":col" << _column_id << " ALREADY LOADED." << std::endl;
      return;
    }

    auto shared_ptr_table = _table.lock();

    std::cout << &*shared_ptr_table << ":col" << _column_id << " WE NEED TO LOAD." << std::endl;

    Assert(shared_ptr_table, "Cannot lazily create statistics if base table is not set.");

    auto& plugin_manager = Hyrise::get().plugin_manager;
    const auto& plugins = plugin_manager.loaded_plugins();
    if (!std::binary_search(plugins.cbegin(), plugins.cend(), "DataLoadingPlugin")) {
      Fail("Data Loading plugin is not loaded.");
    }

    if (plugin_manager.user_executable_functions().contains({"DataLoadingPlugin", "load_table_and_statistics"})) {
      std::cout << "Function 'load_table_and_statistics' not found." << std::endl;
    }
  }


 private:
  std::shared_ptr<AbstractHistogram<T>> _histogram{};
  std::shared_ptr<MinMaxFilter<T>> _min_max_filter{};
  std::shared_ptr<RangeFilter<T>> _range_filter{};
  std::shared_ptr<NullValueRatioStatistics> _null_value_ratio{};
  std::shared_ptr<DistinctValueCount> _distinct_value_count{};

  std::weak_ptr<Table> _table{};
  ColumnID _column_id{0};
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
