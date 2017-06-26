#include "column_statistics.hpp"

#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "all_parameter_variant.hpp"
#include "common.hpp"
#include "operators/aggregate.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/table.hpp"
#include "type_cast.hpp"

namespace opossum {

template <typename T>
ColumnStatistics<T>::ColumnStatistics(const std::weak_ptr<Table> table, const std::string &column_name)
    : _table(table), _column_name(column_name) {}

template <typename T>
ColumnStatistics<T>::ColumnStatistics(size_t distinct_count, AllTypeVariant min, AllTypeVariant max,
                                      const std::string &column_name)
    : _column_name(column_name), _distinct_count(distinct_count), _min(get<T>(min)), _max(get<T>(max)) {}

template <typename T>
ColumnStatistics<T>::ColumnStatistics(size_t distinct_count, T min, T max, const std::string &column_name)
    : _column_name(column_name), _distinct_count(distinct_count), _min(min), _max(max) {}

template <typename T>
size_t ColumnStatistics<T>::get_distinct_count() {
  if (!_distinct_count) {
    update_distinct_count();
  }
  return *_distinct_count;
}

template <typename T>
T ColumnStatistics<T>::get_min() {
  if (!_min) {
    update_min_max();
  }
  return *_min;
}

template <typename T>
T ColumnStatistics<T>::get_max() {
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
                                               std::vector<std::string>{_column_name});
  aggregate->execute();
  auto aggregate_table = aggregate->get_output();
  _distinct_count = aggregate_table->row_count();
}

template <typename T>
void ColumnStatistics<T>::update_min_max() {
  auto shared_table = _table.lock();
  auto table_wrapper = std::make_shared<TableWrapper>(shared_table);
  table_wrapper->execute();
  auto aggregate_args = std::vector<std::pair<std::string, AggregateFunction>>{std::make_pair(_column_name, Min),
                                                                               std::make_pair(_column_name, Max)};
  auto aggregate = std::make_shared<Aggregate>(table_wrapper, aggregate_args, std::vector<std::string>{});
  aggregate->execute();
  auto aggregate_table = aggregate->get_output();
  _min = aggregate_table->template get_value<T>(0, 0);
  _max = aggregate_table->template get_value<T>(1, 0);
}
template <typename T>
std::tuple<double, std::shared_ptr<AbstractColumnStatistics>> ColumnStatistics<T>::predicate_selectivity(
    const std::string &op, const AllTypeVariant value, const optional<AllTypeVariant> value2) {
  auto casted_value1 = get<T>(value);
  //  auto casted_value2 = get<T>(value);
  if (op == "=") {
    if (casted_value1 < get_min() || casted_value1 > get_max()) {
      return {0.0, nullptr};
    }
    auto column_statistics = std::make_shared<ColumnStatistics>(1, casted_value1, casted_value1, _column_name);
    return {1.0 / get_distinct_count(), column_statistics};
  } else if (op == "!=") {
    // disregarding A = 5 AND A != 5
    // (just don't put this into a query!)
    auto column_statistics =
        std::make_shared<ColumnStatistics>(get_distinct_count() - 1, get_min(), get_max(), _column_name);
    return {(-1.0 + get_distinct_count()) / get_distinct_count(), column_statistics};
  } else if (op == "<") {
    if (casted_value1 <= get_min()) {
      return {0.0, nullptr};
    }
    auto min = get_min();
    auto max = get_max();
    //    std::cout << "rc " << _row_count << ", casted value " << casted_value1 << std::endl;
    std::cout << (casted_value1 - min) << std::endl;
    std::cout << (max - min + 1) << std::endl;
    auto column_statistics = std::make_shared<ColumnStatistics>(get_distinct_count(), min, casted_value1, _column_name);
    return {(casted_value1 - min) / (max - min + 1), column_statistics};
    // } else if (op == "<=") {
    //   Fail(std::string("operator not yet implemented: ") + op);
    // } else if (op == ">") {
    //   Fail(std::string("operator not yet implemented: ") + op);
    // } else if (op == ">=") {
    //   Fail(std::string("operator not yet implemented: ") + op);
    // } else if (op == "BETWEEN") {
    //   Fail(std::string("operator not yet implemented: ") + op);
  } else {
    // TODO(mp): extend for other comparison operators
    // Brace yourselves.
    //    auto distinct_count = get_distinct_count();
    //    clone = std::make_shared<TableStatistics>(*this);
    //    clone->_row_count = _row_count / static_cast<double>(distinct_count);
    // Fail(std::string("unknown operator ") + op);
    return {1.0 / get_distinct_count(), nullptr};
  }
}

template <typename T>
std::ostream &ColumnStatistics<T>::to_stream(std::ostream &os) {
  os << "Col Stats " << _column_name << std::endl;
  os << "  dist. " << *_distinct_count << std::endl;
  os << "  min   " << *_min << std::endl;
  os << "  max   " << *_max;
  return os;
}

template class ColumnStatistics<int32_t>;
template class ColumnStatistics<int64_t>;
template class ColumnStatistics<float>;
template class ColumnStatistics<double>;
template class ColumnStatistics<std::string>;

}  // namespace opossum
