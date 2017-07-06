#include "column_statistics.hpp"

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

template <typename T>
ColumnStatistics<T>::ColumnStatistics(const std::weak_ptr<Table> table, const ColumnID column_id)
    : _table(table), _column_id(column_id) {}

template <typename T>
ColumnStatistics<T>::ColumnStatistics(double distinct_count, AllTypeVariant min, AllTypeVariant max,
                                      const ColumnID column_id)
    : _table(std::weak_ptr<Table>()),
      _column_id(column_id),
      _distinct_count(distinct_count),
      _min(get<T>(min)),
      _max(get<T>(max)) {}

template <typename T>
ColumnStatistics<T>::ColumnStatistics(double distinct_count, T min, T max, const ColumnID column_id)
    : _table(std::weak_ptr<Table>()), _column_id(column_id), _distinct_count(distinct_count), _min(min), _max(max) {}

template <typename T>
ColumnStatistics<T>::ColumnStatistics(double distinct_count, const ColumnID column_id)
    : _table(std::weak_ptr<Table>()), _column_id(column_id), _distinct_count(distinct_count) {}

template <typename T>
double ColumnStatistics<T>::distinct_count() {
  if (!_distinct_count) {
    update_distinct_count();
  }
  return *_distinct_count;
}

template <typename T>
T ColumnStatistics<T>::min() {
  if (!_min) {
    update_min_max();
  }
  return *_min;
}

template <typename T>
T ColumnStatistics<T>::max() {
  if (!_max) {
    update_min_max();
  }
  return *_max;
}

template <typename T>
void ColumnStatistics<T>::update_distinct_count() {
  auto shared_table = _table.lock();
  auto table_wrapper = std::make_shared<TableWrapper>(shared_table);
  table_wrapper->execute();
  auto aggregate = std::make_shared<Aggregate>(table_wrapper, std::vector<std::pair<std::string, AggregateFunction>>{},
                                               std::vector<std::string>{shared_table->column_names()[_column_id]});
  aggregate->execute();
  auto aggregate_table = aggregate->get_output();
  _distinct_count = aggregate_table->row_count();
}

template <typename T>
void ColumnStatistics<T>::update_min_max() {
  auto shared_table = _table.lock();
  auto table_wrapper = std::make_shared<TableWrapper>(shared_table);
  table_wrapper->execute();
  auto &column_name = shared_table->column_names()[_column_id];
  auto aggregate_args = std::vector<std::pair<std::string, AggregateFunction>>{std::make_pair(column_name, Min),
                                                                               std::make_pair(column_name, Max)};
  auto aggregate = std::make_shared<Aggregate>(table_wrapper, aggregate_args, std::vector<std::string>{});
  aggregate->execute();
  auto aggregate_table = aggregate->get_output();
  _min = aggregate_table->template get_value<T>(ColumnID{0}, 0);
  _max = aggregate_table->template get_value<T>(ColumnID{1}, 0);
}

// string specialization
template <>
std::tuple<double, std::shared_ptr<AbstractColumnStatistics>> ColumnStatistics<std::string>::predicate_selectivity(
    const ScanType scan_type, const AllTypeVariant value, const optional<AllTypeVariant> value2) {
  auto casted_value1 = type_cast<std::string>(value);
  if (scan_type == ScanType::OpEquals) {
    if (casted_value1 < min() || casted_value1 > max()) {
      return {0.0, nullptr};
    }
    auto column_statistics = std::make_shared<ColumnStatistics>(1, _column_id);
    return {1.0 / distinct_count(), column_statistics};
  } else if (scan_type == ScanType::OpNotEquals) {
    if (casted_value1 < min() || casted_value1 > max()) {
      return {1.0, nullptr};
    }
    auto column_statistics = std::make_shared<ColumnStatistics>(distinct_count() - 1, _column_id);
    return {1. - 1. / distinct_count(), column_statistics};
  }
  // TODO(anybody) implement other table-scan operators for string.
  return {1.0, nullptr};
}

template <typename T>
std::tuple<double, std::shared_ptr<AbstractColumnStatistics>> ColumnStatistics<T>::predicate_selectivity(
    const ScanType scan_type, const AllTypeVariant value, const optional<AllTypeVariant> value2) {
  auto casted_value1 = type_cast<T>(value);

  if (scan_type == ScanType::OpEquals) {
    if (casted_value1 < min() || casted_value1 > max()) {
      return {0.0, nullptr};
    }
    auto column_statistics = std::make_shared<ColumnStatistics>(1, casted_value1, casted_value1, _column_id);
    return {1.0 / distinct_count(), column_statistics};
  } else if (scan_type == ScanType::OpNotEquals) {
    if (casted_value1 < min() || casted_value1 > max()) {
      return {1.0, nullptr};
    }
    // disregarding A != 5 AND A = 5
    // (just don't put this into a query!)
    auto column_statistics = std::make_shared<ColumnStatistics>(distinct_count() - 1, min(), max(), _column_id);
    return {(-1.0 + distinct_count()) / distinct_count(), column_statistics};
  } else if (scan_type == ScanType::OpLessThan && std::is_integral<T>::value) {
    if (casted_value1 <= min()) {
      return {0.0, nullptr};
    }
    double selectivity = (casted_value1 - min()) / static_cast<double>(max() - min() + 1);
    auto column_statistics =
        std::make_shared<ColumnStatistics>(selectivity * distinct_count(), min(), casted_value1 - 1, _column_id);
    return {selectivity, column_statistics};
  } else if (scan_type == ScanType::OpLessThanEquals ||
             (scan_type == ScanType::OpLessThan && !std::is_integral<T>::value)) {
    if (casted_value1 < min() || (scan_type == ScanType::OpLessThan && casted_value1 <= min())) {
      return {0.0, nullptr};
    } else if (casted_value1 >= max()) {
      return {1.0, nullptr};
    }
    double selectivity = (casted_value1 - min() + 1) / static_cast<double>(max() - min() + 1);
    auto column_statistics =
        std::make_shared<ColumnStatistics>(selectivity * distinct_count(), min(), casted_value1, _column_id);
    return {selectivity, column_statistics};
  } else if (scan_type == ScanType::OpGreaterThan && std::is_integral<T>::value) {
    if (casted_value1 >= max()) {
      return {0.0, nullptr};
    }
    double selectivity = (max() - casted_value1) / static_cast<double>(max() - min() + 1);
    auto column_statistics =
        std::make_shared<ColumnStatistics>(selectivity * distinct_count(), casted_value1 + 1, max(), _column_id);
    return {selectivity, column_statistics};
  } else if (scan_type == ScanType::OpGreaterThanEquals ||
             (scan_type == ScanType::OpGreaterThan && !std::is_integral<T>::value)) {
    if (casted_value1 > max() || (scan_type == ScanType::OpGreaterThan && casted_value1 >= max())) {
      return {0.0, nullptr};
    } else if (casted_value1 <= min()) {
      return {1.0, nullptr};
    }
    double selectivity = (max() - casted_value1 + 1) / static_cast<double>(max() - min() + 1);
    auto column_statistics =
        std::make_shared<ColumnStatistics>(selectivity * distinct_count(), casted_value1, max(), _column_id);
    return {selectivity, column_statistics};
  } else if (scan_type == ScanType::OpBetween) {
    if (!value2) {
      Fail(std::string("operator BETWEEN should get two parameters, second is missing!"));
    }
    auto casted_value2 = type_cast<T>(*value2);
    if (casted_value1 > casted_value2 || casted_value1 > max() || casted_value2 < min()) {
      return {0.0, nullptr};
    }
    double selectivity = (casted_value2 - casted_value1 + 1) / static_cast<double>(max() - min() + 1);
    auto column_statistics =
        std::make_shared<ColumnStatistics>(selectivity * distinct_count(), casted_value1, casted_value2, _column_id);
    return {selectivity, column_statistics};
  } else {
    // Brace yourselves.
    return {1.0 / distinct_count(), nullptr};
  }
  return {1.0, nullptr};
}

template <typename T>
std::tuple<double, std::shared_ptr<AbstractColumnStatistics>> ColumnStatistics<T>::predicate_selectivity(
    const ScanType scan_type, const std::shared_ptr<AbstractColumnStatistics> value_column_statistics,
    const optional<AllTypeVariant> value2) {
  // auto casted_value1 = type_cast<T>(value);

  if (scan_type == ScanType::OpEquals) {
    // if (casted_value1 < min() || casted_value1 > max()) {
    //   return {0.0, nullptr};
    // }
    auto column_statistics = std::make_shared<ColumnStatistics>(1, min(), max(), _column_id);
    return {1.0 / 5.0, column_statistics};
  }

  // } else if (scan_type == ScanType::OpNotEquals) {
  //   // disregarding A = 5 AND A != 5
  //   // (just don't put this into a query!)
  //   auto column_statistics = std::make_shared<ColumnStatistics>(distinct_count() - 1, min(), max(), _column_id);
  //   return {(-1.0 + distinct_count()) / distinct_count(), column_statistics};
  // } else if (scan_type == ScanType::OpLessThan && std::is_integral<T>::value) {
  //   if (casted_value1 <= min()) {
  //     return {0.0, nullptr};
  //   }
  //   double selectivity = (casted_value1 - min()) / static_cast<double>(max() - min() + 1);
  //   auto column_statistics =
  //       std::make_shared<ColumnStatistics>(selectivity * distinct_count(), min(), casted_value1 - 1, _column_id);
  //   return {selectivity, column_statistics};
  // } else if (scan_type == ScanType::OpLessThanEquals || (scan_type == ScanType::OpLessThan &&
  // !std::is_integral<T>::value)) {
  //   if (casted_value1 < min()) {
  //     return {0.0, nullptr};
  //   }
  //   double selectivity = (casted_value1 - min() + 1) / static_cast<double>(max() - min() + 1);
  //   auto column_statistics =
  //       std::make_shared<ColumnStatistics>(selectivity * distinct_count(), min(), casted_value1, _column_id);
  //   return {selectivity, column_statistics};
  // } else if (scan_type == ScanType::OpGreaterThan && std::is_integral<T>::value) {
  //   if (casted_value1 >= max()) {
  //     return {0.0, nullptr};
  //   }
  //   double selectivity = (max() - casted_value1) / static_cast<double>(max() - min() + 1);
  //   auto column_statistics =
  //       std::make_shared<ColumnStatistics>(selectivity * distinct_count(), casted_value1 + 1, max(), _column_id);
  //   return {selectivity, column_statistics};
  // } else if (scan_type == ScanType::OpGreaterThanEquals || (scan_type == ScanType::OpLessThan &&
  // !std::is_integral<T>::value)) {
  //   if (casted_value1 > max()) {
  //     return {0.0, nullptr};
  //   }
  //   double selectivity = (max() - casted_value1 + 1) / static_cast<double>(max() - min() + 1);
  //   auto column_statistics =
  //       std::make_shared<ColumnStatistics>(selectivity * distinct_count(), casted_value1, max(), _column_id);
  //   return {selectivity, column_statistics};
  // } else if (scan_type == ScanType::OpBetween) {
  //   if (!value2) {
  //     Fail(std::string("operator ") + scan_type + std::string("should get two parameters, second is missing!"));
  //   }
  //   auto casted_value2 = type_cast<T>(*value2);
  //   if (casted_value1 > casted_value2 || casted_value1 > max() || casted_value2 < min()) {
  //     return {0.0, nullptr};
  //   }
  //   double selectivity = (casted_value2 - casted_value1 + 1) / static_cast<double>(max() - min() + 1);
  //   auto column_statistics =
  //       std::make_shared<ColumnStatistics>(selectivity * distinct_count(), casted_value1, casted_value2,
  //       _column_id);
  //   return {selectivity, column_statistics};
  // } else {
  //   // Brace yourselves.
  //   return {1.0 / distinct_count(), nullptr};
  // }
  return {1.0, nullptr};
}

template <typename T>
std::ostream &ColumnStatistics<T>::to_stream(std::ostream &os) {
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
