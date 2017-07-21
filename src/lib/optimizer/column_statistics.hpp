#pragma once

#include <memory>
#include <ostream>
#include <string>
#include <tuple>

#include "all_parameter_variant.hpp"
#include "base_column_statistics.hpp"
#include "common.hpp"

namespace opossum {

class Aggregate;
class Table;
class TableWrapper;

/**
 * See base_column_statistics.hpp for method comments for virtual methods
 */
template <typename ColumnType>
class ColumnStatistics : public BaseColumnStatistics {
 public:
  /**
   * Create a new column statistics object from a column within a table.
   * The statistics values distinct_count, min and max are not set till used.
   * This constructor is used by table statistics when a non existent table statistics is requestet.
   * @param column_id: id of corresponding column
   * @param table: table, which contains the column
   */
  ColumnStatistics(const ColumnID column_id, const std::weak_ptr<Table> table);
  /**
   * Create a new column statistics object from given parameters.
   * Distinct_count, min and max are set with creation.
   * Therefor, table is not set as table is only used to calculate min, max and distinct_count.
   * This constructor is used by column statistics when returning a new column statistics from estimate selectivity
   * functions.
   */
  ColumnStatistics(const ColumnID column_id, float distinct_count, ColumnType min, ColumnType max);
  ~ColumnStatistics() override = default;

  ColumnSelectivityResult estimate_selectivity_for_predicate(const ScanType scan_type, const AllTypeVariant &value,
                                                             const optional<AllTypeVariant> &value2 = nullopt) override;

  ColumnSelectivityResult estimate_selectivity_for_predicate(const ScanType scan_type, const ValuePlaceholder &value,
                                                             const optional<AllTypeVariant> &value2 = nullopt) override;

 protected:
  std::ostream &print_to_stream(std::ostream &os) const override;

  /**
   * Accessors for class variable optionals. Compute values, if not available.
   * See _distinct_count declaration below for explanation of float type.
   */
  float distinct_count() const;
  ColumnType min() const;
  ColumnType max() const;

  ColumnSelectivityResult calculate_selectivity_for_range_and_create_output(ColumnType new_min, ColumnType new_max);

  ColumnSelectivityResult calculate_selectivity_for_equals_and_create_output(ColumnType value);

  ColumnSelectivityResult calculate_selectivity_for_unequals_and_create_output(ColumnType value);

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

  // distinct count is not an integer as it can be a predicted value
  // it is multiplied with selectivity of a corresponding operator to predict the operator's output distinct count
  // precision is lost, if row count is rounded
  mutable optional<float> _distinct_count;

  mutable optional<ColumnType> _min;
  mutable optional<ColumnType> _max;
};

template <typename ColumnType>
inline std::ostream &operator<<(std::ostream &os, const opossum::optional<ColumnType> &obj) {
  if (obj) {
    return os << *obj;
  } else {
    return os << "N/A";
  }
}

}  // namespace opossum
