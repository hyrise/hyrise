#pragma once

#include <iostream>
#include <memory>
#include <string>
#include <tuple>

#include "abstract_column_statistics.hpp"
#include "all_parameter_variant.hpp"
#include "common.hpp"

namespace opossum {

class Aggregate;
class Table;
class TableWrapper;

template <typename T>
std::ostream& operator<<(std::ostream& os, opossum::optional<T>& obj) {
  if (obj) {
    return os << *obj;
  } else {
    return os << "NA";
  }
}

template <typename T>
class ColumnStatistics : public AbstractColumnStatistics {
 protected:
  void update_distinct_count();

 public:
  ColumnStatistics(const std::weak_ptr<Table> table, const std::string& column_name);
  ColumnStatistics(size_t distinct_count, AllTypeVariant min, AllTypeVariant max, const std::string& column_name);
  ColumnStatistics(size_t distinct_count, T min, T max, const std::string& column_name);
  virtual size_t get_distinct_count();
  T get_min();
  T get_max();

  virtual ~ColumnStatistics() = default;
  virtual std::tuple<double, std::shared_ptr<AbstractColumnStatistics>> predicate_selectivity(
      const std::string& op, const AllTypeVariant value, const optional<AllTypeVariant> value2);

 protected:
  virtual std::ostream& to_stream(std::ostream& os);

  void update_min_max();

  const std::weak_ptr<Table> _table;
  const std::string& _column_name;
  optional<size_t> _distinct_count;
  optional<T> _min;
  optional<T> _max;
};

}  // namespace opossum
