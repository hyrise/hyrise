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

template <typename ColumnType>
class ColumnStatistics : public AbstractColumnStatistics {
 public:
  ColumnStatistics(const ColumnID column_id, const std::weak_ptr<Table> table);
  ColumnStatistics(const ColumnID column_id, float distinct_count, ColumnType min, ColumnType max);
  ~ColumnStatistics() override = default;

  std::tuple<float, std::shared_ptr<AbstractColumnStatistics>> predicate_selectivity(
      const ScanType scan_type, const AllTypeVariant value, const optional<AllTypeVariant> value2) override;

  std::tuple<float, std::shared_ptr<AbstractColumnStatistics>, std::shared_ptr<AbstractColumnStatistics>>
  predicate_selectivity(const ScanType scan_type,
                        const std::shared_ptr<AbstractColumnStatistics> abstract_value_column_statistics,
                        const optional<AllTypeVariant> value2) override;

  std::tuple<float, std::shared_ptr<AbstractColumnStatistics>> predicate_selectivity(
      const ScanType scan_type, const ValuePlaceholder value, const optional<AllTypeVariant> value2) override;

  float distinct_count();
  ColumnType min();
  ColumnType max();

 protected:
  std::ostream& to_stream(std::ostream& os) override;

  void update_distinct_count();
  void update_min_max();

  const ColumnID _column_id;

  // Only available for statistics of tables in the StorageManager.
  // This is a weak_ptr, as
  // Table --shared_ptr--> TableStatistics --shared_ptr--> ColumnStatistics
  const std::weak_ptr<Table> _table;

  // those can be lazy initialized
  optional<float> _distinct_count;
  optional<ColumnType> _min;
  optional<ColumnType> _max;
};

template <typename ColumnType>
std::ostream& operator<<(std::ostream& os, opossum::optional<ColumnType>& obj) {
  if (obj) {
    return os << *obj;
  } else {
    return os << "NA";
  }
}

}  // namespace opossum
