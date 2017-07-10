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
  ColumnStatistics(const std::weak_ptr<Table> table, const ColumnID column_id);
  ColumnStatistics(double distinct_count, AllTypeVariant min, AllTypeVariant max, const ColumnID column_id);
  ColumnStatistics(double distinct_count, T min, T max, const ColumnID column_id);
  ColumnStatistics(double distinct_count, const ColumnID column_id);
  double distinct_count();
  T min();
  T max();

  ~ColumnStatistics() override = default;

  std::tuple<double, std::shared_ptr<AbstractColumnStatistics>> predicate_selectivity(
      const ScanType scan_type, const AllTypeVariant value, const optional<AllTypeVariant> value2) override;

  std::tuple<double, std::shared_ptr<AbstractColumnStatistics>, std::shared_ptr<AbstractColumnStatistics>>
  predicate_selectivity(const ScanType scan_type,
                        const std::shared_ptr<AbstractColumnStatistics> abstract_value_column_statistics,
                        const optional<AllTypeVariant> value2) override;

  std::tuple<double, std::shared_ptr<AbstractColumnStatistics>> predicate_selectivity(
      const ScanType scan_type, const ValuePlaceholder value, const optional<AllTypeVariant> value2);

 protected:
  std::ostream& to_stream(std::ostream& os) override;

  void update_min_max();

  const std::weak_ptr<Table> _table;
  const ColumnID _column_id;
  optional<double> _distinct_count;
  optional<T> _min;
  optional<T> _max;
};

}  // namespace opossum
