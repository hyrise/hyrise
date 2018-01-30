#pragma once

#include <memory>
#include <optional>
#include <ostream>
#include <string>

#include "all_type_variant.hpp"
#include "base_column_statistics.hpp"

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
   * The column statistics values distinct count, min and max are not set till used.
   * This constructor is used by table statistics when a non-existent column statistics is requested.
   * @param column_id: id of corresponding column
   * @param table: table, which contains the column
   */
  ColumnStatistics(const ColumnID column_id, const std::weak_ptr<Table> table);
  /**
   * Create a new column statistics object from given parameters.
   * Distinct count, min and max are set during the creation. Non-null value ratio can be optionally set.
   * Therefore, _table is not set as it is only used to calculate min, max and distinct_count.
   * This constructor is used by column statistics when returning a new column statistics from estimate selectivity
   * functions.
   */
  ColumnStatistics(const ColumnID column_id, const float distinct_count, const ColumnType min, const ColumnType max,
                   const float non_null_value_ratio = 1.f);
  ~ColumnStatistics() override = default;

  ColumnSelectivityResult estimate_selectivity_for_predicate(
      const PredicateCondition predicate_condition, const AllTypeVariant& value,
      const std::optional<AllTypeVariant>& value2 = std::nullopt) override;

  ColumnSelectivityResult estimate_selectivity_for_predicate(
      const PredicateCondition predicate_condition, const ValuePlaceholder& value,
      const std::optional<AllTypeVariant>& value2 = std::nullopt) override;

  TwoColumnSelectivityResult estimate_selectivity_for_two_column_predicate(
      const PredicateCondition predicate_condition,
      const std::shared_ptr<BaseColumnStatistics>& right_base_column_statistics,
      const std::optional<AllTypeVariant>& value2 = std::nullopt) override;

  /**
   * Accessors for class variable optionals. Compute values, if not available.
   * See _distinct_count declaration below for explanation of float type.
   */
  float distinct_count() const override;

  std::shared_ptr<BaseColumnStatistics> clone() const override;

 protected:
  std::ostream& _print_to_stream(std::ostream& os) const override;
  ColumnType _get_or_calculate_min() const;
  ColumnType _get_or_calculate_max() const;

  /**
   * Returns a column statistics identical to this which does not have null values.
   * @return shared pointer of this or copy of this, if column has null values.
   */
  std::shared_ptr<BaseColumnStatistics> _this_without_null_values();

  /**
   * Create column statistics and estimate selectivity based on new range.
   * @param minimum, maximum: Min and max for new column statistics. Note: Minimum can be greater than maximum.
   * @return Selectivity and new column statistics.
   */
  ColumnSelectivityResult _create_column_stats_for_range_predicate(ColumnType minimum, ColumnType maximum);

  /**
   * Estimate selectivity based on new and current range between min and max.
   * @param minimum, maximum: Min and max for new range. Minimum must be smaller or equal to maximum.
   * @return Selectivity.
   */
  float estimate_selectivity_for_range(ColumnType minimum, ColumnType maximum);

  /**
   * Create column statistics and estimate selectivity for predicate with predicate condition equals and constant value.
   * @param value: constant value of aggregate
   * @return Selectivity and new column statistics.
   */
  ColumnSelectivityResult _create_column_stats_for_equals_predicate(ColumnType value);

  /**
   * Create column statistics and estimate selectivity for predicate with predicate condition not equals and constant value.
   * @param value: constant value of aggregate
   * @return Selectivity and new column statistics.
   */
  ColumnSelectivityResult _create_column_stats_for_not_equals_predicate(ColumnType value);

  /**
   * Calculate min and max values from table.
   */
  void _initialize_min_max() const;

  const ColumnID _column_id;

  // Only available for statistics of tables in the StorageManager.
  // This is a weak_ptr, as
  // Table --shared_ptr--> TableStatistics --shared_ptr--> ColumnStatistics
  const std::weak_ptr<Table> _table;

  // those can be lazy initialized

  // distinct count is not an integer as it can be a predicted value
  // it is multiplied with selectivity of a corresponding operator to predict the operator's output distinct count
  // precision is lost, if row count is rounded
  mutable std::optional<float> _distinct_count;

  mutable std::optional<ColumnType> _min;
  mutable std::optional<ColumnType> _max;
};

template <typename ColumnType>
inline std::ostream& operator<<(std::ostream& os, const std::optional<ColumnType>& obj) {
  if (obj) {
    return os << *obj;
  } else {
    return os << "N/A";
  }
}

}  // namespace opossum
