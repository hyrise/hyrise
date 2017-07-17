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

/**
 * See abstract_column_statistics.hpp for method comments for virtual methods
 */
template <typename ColumnType>
class ColumnStatistics : public AbstractColumnStatistics {
 public:
  ColumnStatistics(const ColumnID column_id, const std::weak_ptr<Table> table);
  ColumnStatistics(const ColumnID column_id, float distinct_count, ColumnType min, ColumnType max);
  ~ColumnStatistics() override = default;

  ColumnStatisticsContainer predicate_selectivity(const ScanType scan_type, const AllTypeVariant &value,
                                                  const optional<AllTypeVariant> &value2) override;

  ColumnStatisticsContainer predicate_selectivity(const ScanType scan_type, const ValuePlaceholder &value,
                                                  const optional<AllTypeVariant> &value2) override;

 protected:
  std::ostream &print_to_stream(std::ostream &os) const override;

  float distinct_count() const;
  ColumnType min() const;
  ColumnType max() const;

  /**
   * Calcute min and max values from table.
   */
  void initialize_min_max() const;

  const ColumnID _column_id;

  // Only available for statistics of tables in the StorageManager.
  // This is a weak_ptr, as
  // Table --shared_ptr--> TableStatistics --shared_ptr--> ColumnStatistics
  const std::weak_ptr<Table> _table;

  // those can be lazy initialized
  mutable optional<float> _distinct_count;
  mutable optional<ColumnType> _min;
  mutable optional<ColumnType> _max;
};

template <typename ColumnType>
inline std::ostream &operator<<(std::ostream &os, const opossum::optional<ColumnType> &obj) {
  if (obj) {
    return os << *obj;
  } else {
    return os << "NA";
  }
}

}  // namespace opossum
