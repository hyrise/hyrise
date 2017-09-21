#pragma once

#include <memory>
#include <ostream>
#include <string>

#include "all_type_variant.hpp"
#include "common.hpp"

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
 * Find more information in our Blog: https://medium.com/hyrise/the-brain-of-every-database-c622aaba7d75
 *                                    https://medium.com/hyrise/how-much-is-the-fish-a8ea1f4a0577
 *                      and our Wiki: https://github.com/hyrise/zweirise/wiki/Statistics-Component
 *                                    https://github.com/hyrise/zweirise/wiki/gathering_statistics
 */
class BaseColumnStatistics {
 public:
  virtual ~BaseColumnStatistics() = default;

  /**
   * Estimate selectivity for predicate with constants.
   * Predict result of a table scan with constant values.
   * @return Selectivity and new column statistics, if selectivity is not 0 or 1.
   */
  virtual ColumnSelectivityResult estimate_selectivity_for_predicate(
      const ScanType scan_type, const AllTypeVariant &value, const optional<AllTypeVariant> &value2 = nullopt) = 0;

  /**
   * Estimate selectivity for predicate with prepared statements.
   * In comparison to predicates with constants, value is not known yet.
   * Therefore, when necessary, default selectivity values are used for predictions.
   * @return Selectivity and new column statistics, if selectivity is not 0 or 1.
   */
  virtual ColumnSelectivityResult estimate_selectivity_for_predicate(
      const ScanType scan_type, const ValuePlaceholder &value, const optional<AllTypeVariant> &value2 = nullopt) = 0;

  /**
   * Estimate selectivity for predicate on columns.
   * In comparison to predicates with constants, value is another column.
   * For predicate "col_left < col_right", selectivity is calculated in column statistics of col_left with parameters
   * scan_type = "<" and right_base_column_statistics = col_right statistics.
   * @return Selectivity and two new column statistics, if selectivity is not 0 or 1.
   */
  virtual TwoColumnSelectivityResult estimate_selectivity_for_two_column_predicate(
      const ScanType scan_type, const std::shared_ptr<BaseColumnStatistics> &right_base_column_statistics,
      const optional<AllTypeVariant> &value2 = nullopt) = 0;

 protected:
  /**
   * In order to to call insertion operator on ostream with BaseColumnStatistics with values of ColumnStatistics<T>,
   * std::ostream &operator<< with BaseColumnStatistics calls virtual function print_to_stream
   * This approach allows printing ColumnStatistics<T> without the need to cast BaseColumnStatistics to
   * ColumnStatistics<T>.
   */
  virtual std::ostream &print_to_stream(std::ostream &os) const = 0;
  friend std::ostream &operator<<(std::ostream &os, BaseColumnStatistics &obj);
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
  TwoColumnSelectivityResult(float selectivity, const std::shared_ptr<BaseColumnStatistics> &column_stats,
                             const std::shared_ptr<BaseColumnStatistics> &second_column_stats)
      : ColumnSelectivityResult{selectivity, column_stats}, second_column_statistics(second_column_stats) {}

  std::shared_ptr<BaseColumnStatistics> second_column_statistics;
};

inline std::ostream &operator<<(std::ostream &os, BaseColumnStatistics &obj) { return obj.print_to_stream(os); }

}  // namespace opossum
