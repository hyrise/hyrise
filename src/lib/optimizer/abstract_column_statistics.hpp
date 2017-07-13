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
  AbstractColumnStatistics() = default;
  virtual ~AbstractColumnStatistics() = default;

  virtual ColumnStatisticsContainer predicate_selectivity(
      const ScanType scan_type, const AllTypeVariant value, const optional<AllTypeVariant> value2) = 0;

  virtual ColumnStatisticsContainer predicate_selectivity(
      const ScanType scan_type, const ValuePlaceholder value, const optional<AllTypeVariant> value2) = 0;

  virtual TwoColumnStatisticsContainer
  predicate_selectivity(const ScanType scan_type,
                        const std::shared_ptr<AbstractColumnStatistics> abstract_value_column_statistics,
                        const optional<AllTypeVariant> value2) = 0;

  friend std::ostream &operator<<(std::ostream &os, AbstractColumnStatistics &obj) { return obj.to_stream(os); }

 protected:
  virtual std::ostream &to_stream(std::ostream &os) = 0;
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
    TwoColumnStatisticsContainer (float selectivity, std::shared_ptr<AbstractColumnStatistics> column_stats, std::shared_ptr<AbstractColumnStatistics> second_colomn_stats)
            : ColumnStatisticsContainer{selectivity, column_stats}, second_column_statistics(second_colomn_stats) {}

    std::shared_ptr<AbstractColumnStatistics> second_column_statistics;
};

}  // namespace opossum
