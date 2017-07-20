#pragma once

#include <memory>
#include <ostream>
#include <string>
#include <tuple>

#include "all_parameter_variant.hpp"
#include "common.hpp"

namespace opossum {

struct ColumnSelectivityResult;

/**
 * Most prediction computation is delegated from table statistics to typed column statistics.
 * This enables the possibility to work with the column type for min and max values.
 *
 * Therefore, column statistics implements functions for all operators
 * so that the corresponding table statistics functions can delegate all predictions to column statistics.
 * These functions return a column selectivity result object combining the selectivity the operator
 * and if changed the newly created column statistics.
 */
class AbstractColumnStatistics {
 public:
  virtual ~AbstractColumnStatistics() = default;

  /**
   * Predicate selectivity for constants.
   * Predict result of a table scan with constant values.
   */
  virtual ColumnSelectivityResult predicate_selectivity(const ScanType scan_type, const AllTypeVariant &value,
                                                        const optional<AllTypeVariant> &value2 = nullopt) = 0;

  /**
   * Predicate selectivity for prepared statements.
   * In comparison to predicate selectivity for constants value is not known yet.
   * Therefore, when necessary default selectivity values are used for predictions.
   */
  virtual ColumnSelectivityResult predicate_selectivity(const ScanType scan_type, const ValuePlaceholder &value,
                                                        const optional<AllTypeVariant> &value2 = nullopt) = 0;

 protected:
  /**
   * In order to to call insertion operator on ostream with AbstractColumnStatistics with values of ColumnStatistics<T>,
   * std::ostream &operator<< with AbstractColumnStatistics calls virtual function print_to_stream
   */
  virtual std::ostream &print_to_stream(std::ostream &os) const = 0;
  friend std::ostream &operator<<(std::ostream &os, AbstractColumnStatistics &obj);
};

/**
 * Return type of selectivity functions for operations on one column.
 */
struct ColumnSelectivityResult {
  float selectivity = 0.f;
  std::shared_ptr<AbstractColumnStatistics> column_statistics;
};

inline std::ostream &operator<<(std::ostream &os, AbstractColumnStatistics &obj) { return obj.print_to_stream(os); }

}  // namespace opossum
