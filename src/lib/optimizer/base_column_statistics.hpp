#pragma once

#include <memory>
#include <optional>
#include <ostream>
#include <string>

#include "all_type_variant.hpp"

namespace opossum {

struct ColumnSelectivityResult;
struct TwoColumnSelectivityResult;

/**
 * Most prediction computation is delegated from table statistics to typed column statistics.
 * This enables the possibility to work with the column type for min and max values.
 *
 * Therefore, column statistics implements functions for all operators
 * so that the corresponding table statistics functions can delegate all predictions to column statistics.
 * These functions return a column selectivity result object combining the selectivity of the operator
 * and if changed the newly created column statistics.
 *
 * The selectivities are calculated with the min, max, distinct count and non-null value ratio of the column in the
 * derived class. Only the non-null value ratio is stored in the base class to enable easy access and manipulation of it
 * by table statistics.
 * A predicate on a null value will never evaluate to true, unless the column is explicitly checked for NULL values
 * (column IS NULL) which is currently not supported.
 * To start with the calculation of a predicate's selectivity, null values can be ignored during the calculation of the
 * selectivity. The non-null value ratio can be interpreted as a second selectivity.
 * Therefore, the result selectivity is the product of the selectivity of the predicate and the non-null value ratio.
 * The returned column statistics will always have a non-null value ratio of 1 as currently all predicates remove null
 * values.
 *
 * Find more information in our Blog: https://medium.com/hyrise/the-brain-of-every-database-c622aaba7d75
 *                                    https://medium.com/hyrise/how-much-is-the-fish-a8ea1f4a0577
 *                      and our Wiki: https://github.com/hyrise/hyrise/wiki/Statistics-Component
 *                                    https://github.com/hyrise/hyrise/wiki/gathering_statistics
 */
class BaseColumnStatistics : public std::enable_shared_from_this<BaseColumnStatistics> {
 public:
  explicit BaseColumnStatistics(const DataType data_type, const float non_null_value_ratio = 1.f)
      : _data_type(data_type), _non_null_value_ratio(non_null_value_ratio) {}
  virtual ~BaseColumnStatistics() = default;

  /**
   * Estimate selectivity for predicate with constants.
   * Predict result of a table scan with constant values.
   * @return Selectivity and new column statistics.
   */
  virtual ColumnSelectivityResult estimate_selectivity_for_predicate(
      const PredicateCondition predicate_condition, const AllTypeVariant& value,
      const std::optional<AllTypeVariant>& value2 = std::nullopt) = 0;

  /**
   * Estimate selectivity for predicate with prepared statements.
   * In comparison to predicates with constants, value is not known yet.
   * Therefore, when necessary, default selectivity values are used for predictions.
   * @return Selectivity and new column statistics.
   */
  virtual ColumnSelectivityResult estimate_selectivity_for_predicate(
      const PredicateCondition predicate_condition, const ValuePlaceholder& value,
      const std::optional<AllTypeVariant>& value2 = std::nullopt) = 0;

  /**
   * Estimate selectivity for predicate on columns.
   * In comparison to predicates with constants, value is another column.
   * For predicate "col_left < col_right", selectivity is calculated in column statistics of col_left with parameters
   * predicate_condition = "<" and right_base_column_statistics = col_right statistics.
   * @return Selectivity and two new column statistics.
   */
  virtual TwoColumnSelectivityResult estimate_selectivity_for_two_column_predicate(
      const PredicateCondition predicate_condition,
      const std::shared_ptr<BaseColumnStatistics>& right_base_column_statistics,
      const std::optional<AllTypeVariant>& value2 = std::nullopt) = 0;

  DataType data_type() const;

  /**
   * Gets distinct count of column.
   * See _distinct_count declaration in column_statistics.hpp for explanation of float type.
   */
  virtual float distinct_count() const = 0;

  /**
   * Copies the derived object and returns a base class pointer to it.
   */
  virtual std::shared_ptr<BaseColumnStatistics> clone() const = 0;

  /**
   * Adjust null value ratio of a column after a left/right/full outer join.
   */
  void set_null_value_ratio(const float null_value_ratio);

  /**
   * Gets null value ratio of a column for calculation of null values for left/right/full outer join.
   */
  float null_value_ratio() const;

 protected:
  const DataType _data_type;

  // Column statistics uses the non-null value ratio for calculation of selectivity.
  // Table statistics uses the null value ratio when calculating join statistics.
  float _non_null_value_ratio;

  /**
   * In order to to call insertion operator on ostream with BaseColumnStatistics with values of ColumnStatistics<T>,
   * std::ostream &operator<< with BaseColumnStatistics calls virtual function print_to_stream
   * This approach allows printing ColumnStatistics<T> without the need to cast BaseColumnStatistics to
   * ColumnStatistics<T>.
   */
  virtual std::ostream& _print_to_stream(std::ostream& os) const = 0;
  friend std::ostream& operator<<(std::ostream& os, BaseColumnStatistics& obj);
};

/**
 * Return type of selectivity functions for operations on one column.
 */
struct ColumnSelectivityResult {
  float selectivity;
  std::shared_ptr<BaseColumnStatistics> column_statistics;
};

/**
 * Return type of selectivity functions for operations on two columns.
 */
struct TwoColumnSelectivityResult : public ColumnSelectivityResult {
  TwoColumnSelectivityResult(float selectivity, const std::shared_ptr<BaseColumnStatistics>& column_stats,
                             const std::shared_ptr<BaseColumnStatistics>& second_column_stats)
      : ColumnSelectivityResult{selectivity, column_stats}, second_column_statistics(second_column_stats) {}

  std::shared_ptr<BaseColumnStatistics> second_column_statistics;
};

std::ostream& operator<<(std::ostream& os, BaseColumnStatistics& obj);

}  // namespace opossum
