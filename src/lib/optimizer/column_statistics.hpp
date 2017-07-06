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
  ColumnStatistics(double distinct_count, AllTypeVariant min, AllTypeVariant max, const std::string& column_name);
  ColumnStatistics(double distinct_count, T min, T max, const std::string& column_name);
  ColumnStatistics(double distinct_count, const std::string& column_name);
  double distinct_count();
  T min();
  T max();

  ~ColumnStatistics() override = default;
  std::tuple<double, std::shared_ptr<AbstractColumnStatistics>> predicate_selectivity(
      const ScanType scan_type, const AllTypeVariant value, const optional<AllTypeVariant> value2) override;
  std::tuple<double, std::shared_ptr<AbstractColumnStatistics>> predicate_selectivity(
      const ScanType scan_type, const std::shared_ptr<AbstractColumnStatistics> value_column_statistics,
      const optional<AllTypeVariant> value2) override;

 protected:
  std::ostream& to_stream(std::ostream& os) override;

  void update_min_max();

  const std::weak_ptr<Table> _table;
  const std::string& _column_name;
  optional<double> _distinct_count;
  optional<T> _min;
  optional<T> _max;
};

}  // namespace opossum
