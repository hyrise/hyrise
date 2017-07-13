#pragma once

#include <iostream>
#include <memory>
#include <string>
#include <tuple>

#include "all_parameter_variant.hpp"
#include "common.hpp"

namespace opossum {

struct ColumnStatisticsContainer;
struct TwoColumnStatisticsContainer;

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

  /**
   * Predicate selectivity for two columns.
   */
  virtual TwoColumnStatisticsContainer predicate_selectivity(
      const ScanType scan_type, const std::shared_ptr<AbstractColumnStatistics> abstract_value_column_statistics,
      const optional<AllTypeVariant> &value2) = 0;

  friend std::ostream &operator<<(std::ostream &os, AbstractColumnStatistics &obj);

 protected:
  virtual std::ostream &print_to_stream(std::ostream &os) const = 0;
};

/**
 * Return type of get selectivity functions for operations on one column
 */
struct ColumnStatisticsContainer {
  float selectivity;
  std::shared_ptr<AbstractColumnStatistics> column_statistics;
};

/**
 * Return type of get selectivity functions for operations on two column
 */
struct TwoColumnStatisticsContainer : public ColumnStatisticsContainer {
  TwoColumnStatisticsContainer(float selectivity, std::shared_ptr<AbstractColumnStatistics> column_stats,
                               std::shared_ptr<AbstractColumnStatistics> second_colomn_stats)
      : ColumnStatisticsContainer{selectivity, column_stats}, second_column_statistics(second_colomn_stats) {}

  std::shared_ptr<AbstractColumnStatistics> second_column_statistics;
};

inline std::ostream &operator<<(std::ostream &os, AbstractColumnStatistics &obj) { return obj.print_to_stream(os); }

}  // namespace opossum
