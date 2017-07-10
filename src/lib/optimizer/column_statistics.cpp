#include "column_statistics.hpp"

#include <algorithm>
#include <memory>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "all_parameter_variant.hpp"
#include "common.hpp"
#include "operators/aggregate.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/table.hpp"
#include "type_cast.hpp"
#include "types.hpp"

namespace opossum {

template <typename ColumnType>
ColumnStatistics<ColumnType>::ColumnStatistics(const std::weak_ptr<Table> table, const ColumnID column_id)
    : _table(table), _column_id(column_id) {}

template <typename ColumnType>
ColumnStatistics<ColumnType>::ColumnStatistics(double distinct_count, ColumnType min, ColumnType max,
                                               const ColumnID column_id)
    : _table(std::weak_ptr<Table>()), _column_id(column_id), _distinct_count(distinct_count), _min(min), _max(max) {}

template <typename ColumnType>
double ColumnStatistics<ColumnType>::distinct_count() {
  if (!_distinct_count) {
    update_distinct_count();
  }
  return *_distinct_count;
}

template <typename ColumnType>
ColumnType ColumnStatistics<ColumnType>::min() {
  if (!_min) {
    update_min_max();
  }
  return *_min;
}

template <typename ColumnType>
ColumnType ColumnStatistics<ColumnType>::max() {
  if (!_max) {
    update_min_max();
  }
  return *_max;
}

template <typename ColumnType>
void ColumnStatistics<ColumnType>::update_distinct_count() {
  auto shared_table = _table.lock();
  auto table_wrapper = std::make_shared<TableWrapper>(shared_table);
  table_wrapper->execute();
  auto aggregate = std::make_shared<Aggregate>(table_wrapper, std::vector<std::pair<std::string, AggregateFunction>>{},
                                               std::vector<std::string>{shared_table->column_name(_column_id)});
  aggregate->execute();
  auto aggregate_table = aggregate->get_output();
  _distinct_count = aggregate_table->row_count();
}

template <typename ColumnType>
void ColumnStatistics<ColumnType>::update_min_max() {
  auto shared_table = _table.lock();
  auto table_wrapper = std::make_shared<TableWrapper>(shared_table);
  table_wrapper->execute();
  const std::string &column_name = shared_table->column_name(_column_id);
  auto aggregate_args = std::vector<std::pair<std::string, AggregateFunction>>{std::make_pair(column_name, Min),
                                                                               std::make_pair(column_name, Max)};
  auto aggregate = std::make_shared<Aggregate>(table_wrapper, aggregate_args, std::vector<std::string>{});
  aggregate->execute();
  auto aggregate_table = aggregate->get_output();
  _min = aggregate_table->template get_value<ColumnType>(ColumnID{0}, 0);
  _max = aggregate_table->template get_value<ColumnType>(ColumnID{1}, 0);
}

/**
 * Predicate selectivity for constants,
 * specialized for strings.
 */
template <>
std::tuple<double, std::shared_ptr<AbstractColumnStatistics>> ColumnStatistics<std::string>::predicate_selectivity(
    const ScanType scan_type, const AllTypeVariant value, const optional<AllTypeVariant> value2) {
  auto casted_value1 = type_cast<std::string>(value);
  switch (scan_type) {
    case ScanType::OpEquals: {
      if (casted_value1 < min() || casted_value1 > max()) {
        return {0., nullptr};
      }
      auto column_statistics = std::make_shared<ColumnStatistics>(1, casted_value1, casted_value1, _column_id);
      return {1. / distinct_count(), column_statistics};
    }
    case ScanType::OpNotEquals: {
      if (casted_value1 < min() || casted_value1 > max()) {
        return {1., nullptr};
      }
      auto column_statistics = std::make_shared<ColumnStatistics>(distinct_count() - 1, min(), max(), _column_id);
      return {1. - 1. / distinct_count(), column_statistics};
    }
    // TODO(anybody) implement other table-scan operators for string.
    default: { return {1., nullptr}; }
  }
}

/**
 * Predicate selectivity for constants,
 * every type but strings.
 */
template <typename ColumnType>
std::tuple<double, std::shared_ptr<AbstractColumnStatistics>> ColumnStatistics<ColumnType>::predicate_selectivity(
    const ScanType scan_type, const AllTypeVariant value, const optional<AllTypeVariant> value2) {
  auto casted_value1 = type_cast<ColumnType>(value);

  switch (scan_type) {
    case ScanType::OpEquals: {
      if (casted_value1 < min() || casted_value1 > max()) {
        return {0., nullptr};
      }
      auto column_statistics = std::make_shared<ColumnStatistics>(1, casted_value1, casted_value1, _column_id);
      return {1. / distinct_count(), column_statistics};
    }
    case ScanType::OpNotEquals: {
      if (casted_value1 < min() || casted_value1 > max()) {
        return {1., nullptr};
      }
      auto column_statistics = std::make_shared<ColumnStatistics>(distinct_count() - 1, min(), max(), _column_id);
      return {(distinct_count() - 1.) / distinct_count(), column_statistics};
    }
    case ScanType::OpLessThan: {
      if (std::is_integral<ColumnType>::value) {
        if (casted_value1 <= min()) {
          return {0., nullptr};
        }
        double selectivity = (casted_value1 - min()) / static_cast<double>(max() - min() + 1);
        auto column_statistics =
            std::make_shared<ColumnStatistics>(selectivity * distinct_count(), min(), casted_value1 - 1, _column_id);
        return {selectivity, column_statistics};
      }
      // intentionally no break
    }
    case ScanType::OpLessThanEquals: {
      if (casted_value1 < min() || (scan_type == ScanType::OpLessThan && casted_value1 <= min())) {
        return {0., nullptr};
      } else if (casted_value1 >= max()) {
        return {1., nullptr};
      }
      double selectivity = (casted_value1 - min() + 1) / static_cast<double>(max() - min() + 1);
      auto column_statistics =
          std::make_shared<ColumnStatistics>(selectivity * distinct_count(), min(), casted_value1, _column_id);
      return {selectivity, column_statistics};
    }
    case ScanType::OpGreaterThan: {
      if (std::is_integral<ColumnType>::value) {
        if (casted_value1 >= max()) {
          return {0., nullptr};
        }
        double selectivity = (max() - casted_value1) / static_cast<double>(max() - min() + 1);
        auto column_statistics =
            std::make_shared<ColumnStatistics>(selectivity * distinct_count(), casted_value1 + 1, max(), _column_id);
        return {selectivity, column_statistics};
      }
      // intentionally no break
    }
    case ScanType::OpGreaterThanEquals: {
      if (casted_value1 > max() || (scan_type == ScanType::OpGreaterThan && casted_value1 >= max())) {
        return {0., nullptr};
      } else if (casted_value1 <= min()) {
        return {1., nullptr};
      }
      double selectivity = (max() - casted_value1 + 1) / static_cast<double>(max() - min() + 1);
      auto column_statistics =
          std::make_shared<ColumnStatistics>(selectivity * distinct_count(), casted_value1, max(), _column_id);
      return {selectivity, column_statistics};
    }
    case ScanType::OpBetween: {
      if (!value2) {
        Fail(std::string("operator BETWEEN should get two parameters, second is missing!"));
      }
      auto casted_value2 = type_cast<ColumnType>(*value2);
      if (casted_value1 > casted_value2 || casted_value1 > max() || casted_value2 < min()) {
        return {0., nullptr};
      }
      casted_value1 = std::max(casted_value1, min());
      casted_value2 = std::min(casted_value2, max());
      double selectivity = (casted_value2 - casted_value1 + 1) / static_cast<double>(max() - min() + 1);
      auto column_statistics =
          std::make_shared<ColumnStatistics>(selectivity * distinct_count(), casted_value1, casted_value2, _column_id);
      return {selectivity, column_statistics};
    }
    default: { return {1. / 3., nullptr}; }
  }
}

/**
 * Predicate selectivity for two columns,
 * specialized for strings.
 */
template <>
std::tuple<double, std::shared_ptr<AbstractColumnStatistics>, std::shared_ptr<AbstractColumnStatistics>>
ColumnStatistics<std::string>::predicate_selectivity(
    const ScanType scan_type, const std::shared_ptr<AbstractColumnStatistics> abstract_value_column_statistics,
    const optional<AllTypeVariant> value2) {
  // TODO(anybody)
  return {1., nullptr, nullptr};
}

/**
 * Predicate selectivity for two columns,
 * every type but strings.
 */
template <typename ColumnType>
std::tuple<double, std::shared_ptr<AbstractColumnStatistics>, std::shared_ptr<AbstractColumnStatistics>>
ColumnStatistics<ColumnType>::predicate_selectivity(
    const ScanType scan_type, const std::shared_ptr<AbstractColumnStatistics> abstract_value_column_statistics,
    const optional<AllTypeVariant> value2) {
  auto value_column_statistics =
      std::dynamic_pointer_cast<ColumnStatistics<ColumnType>>(abstract_value_column_statistics);

  auto common_min = std::max(min(), value_column_statistics->min());
  auto common_max = std::min(max(), value_column_statistics->max());

  switch (scan_type) {
    case ScanType::OpEquals: {
      if (common_min > common_max) {
        return {0., nullptr, nullptr};
      }

      double overlapping_ratio_this = (common_max - common_min + 1) / static_cast<double>(max() - min() + 1);
      double overlapping_ratio_value =
          (common_max - common_min + 1) /
          static_cast<double>(value_column_statistics->max() - value_column_statistics->min() + 1);
      auto overlapping_distinct_count_this = overlapping_ratio_this * distinct_count();
      auto overlapping_distinct_count_value = overlapping_ratio_value * value_column_statistics->distinct_count();
      auto overlapping_distinct_count = std::min(overlapping_distinct_count_this, overlapping_distinct_count_value);
      auto probability_hit_value = 1. / distinct_count() / value_column_statistics->distinct_count();

      auto column_statistics_this =
          std::make_shared<ColumnStatistics>(overlapping_distinct_count, common_min, common_max, _column_id);
      auto column_statistics_value = std::make_shared<ColumnStatistics>(
          overlapping_distinct_count, common_min, common_max, value_column_statistics->_column_id);
      return {overlapping_distinct_count * probability_hit_value, column_statistics_this, column_statistics_value};
    }
    // TODO(Jonathan, Fabian) finish predicates for multi-columns
    default: { return {1., nullptr, nullptr}; }
  }
}

/**
 * Predicate selectivity for prepared statements.
 */
template <typename ColumnType>
std::tuple<double, std::shared_ptr<AbstractColumnStatistics>> ColumnStatistics<ColumnType>::predicate_selectivity(
    const ScanType scan_type, const ValuePlaceholder value, const optional<AllTypeVariant> value2) {
  switch (scan_type) {
    case ScanType::OpEquals: {
      auto column_statistics = std::make_shared<ColumnStatistics>(1, min(), max(), _column_id);
      return {1. / distinct_count(), column_statistics};
    }
    case ScanType::OpNotEquals: {
      auto column_statistics = std::make_shared<ColumnStatistics>(distinct_count() - 1, min(), max(), _column_id);
      return {(distinct_count() - 1.) / distinct_count(), column_statistics};
    }
    default: { return {1. / 2., nullptr}; }
  }
}

template <typename ColumnType>
std::ostream &ColumnStatistics<ColumnType>::to_stream(std::ostream &os) {
  os << "Col Stats id: " << _column_id << std::endl;
  os << "  dist. " << _distinct_count << std::endl;
  os << "  min   " << _min << std::endl;
  os << "  max   " << _max;
  return os;
}

template class ColumnStatistics<int32_t>;
template class ColumnStatistics<int64_t>;
template class ColumnStatistics<float>;
template class ColumnStatistics<double>;
template class ColumnStatistics<std::string>;

}  // namespace opossum
