#pragma once

#include <iostream>
#include <memory>
#include <string>
#include <tuple>

#include "all_parameter_variant.hpp"
#include "common.hpp"

namespace opossum {

struct ColumnStatisticsContainer;

class AbstractColumnStatistics {
 public:
  virtual ~AbstractColumnStatistics() = default;

  /**
   * Predicate selectivity for constants.
   */
  virtual ColumnStatisticsContainer predicate_selectivity(const ScanType scan_type, const AllTypeVariant &value,
                                                          const optional<AllTypeVariant> &value2) = 0;

  /**
   * Predicate selectivity for prepared statements.
   */
  virtual ColumnStatisticsContainer predicate_selectivity(const ScanType scan_type, const ValuePlaceholder &value,
                                                          const optional<AllTypeVariant> &value2) = 0;

 protected:
  /**
   * In order to to call insertion operator on ostream with AbstractColumnStatistics with values of ColumnStatistics<T>,
   * std::ostream &operator<< with AbstractColumnStatistics calls virtual function print_to_stream
   */
  virtual std::ostream &print_to_stream(std::ostream &os) const = 0;
  friend std::ostream &operator<<(std::ostream &os, AbstractColumnStatistics &obj);
};

/**
 * Return type of get selectivity functions for operations on one column
 */
struct ColumnStatisticsContainer {
  float selectivity;
  std::shared_ptr<AbstractColumnStatistics> column_statistics;
};

inline std::ostream &operator<<(std::ostream &os, AbstractColumnStatistics &obj) { return obj.print_to_stream(os); }

}  // namespace opossum
