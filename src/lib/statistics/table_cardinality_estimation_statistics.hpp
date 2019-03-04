#pragma once

#include <atomic>
#include <iostream>
#include <memory>
#include <optional>
#include <unordered_map>
#include <vector>

#include "all_type_variant.hpp"
#include "cardinality.hpp"

namespace opossum {

class BaseVerticalStatisticsSlice;
class Table;

/**
 * Container for all cardinality estimation statistics gathered about a Table. Also used to represent the estimation of
 * a temporary Table during Optimization.
 *
 * The Table is partitioned horizontally into slices and statistics are assigned to each slice independently. Each slice
 * might cover any number of rows/chunks and is not bound to the Chunks in the original Table.
 */
class TableCardinalityEstimationStatistics {
 public:
  /**
   * Generates histograms for all columns
   */
  static std::shared_ptr<TableCardinalityEstimationStatistics> from_table(const Table& table);

  TableCardinalityEstimationStatistics(std::vector<std::shared_ptr<BaseVerticalStatisticsSlice>>&& column_statistics, const Cardinality row_count);

  /**
   * @return column_statistics[column_id]->data_type
   */
  DataType column_data_type(const ColumnID column_id) const;

  const std::vector<std::shared_ptr<BaseVerticalStatisticsSlice>> column_statistics;
  Cardinality row_count;

  // A hopefully temporary means to represent the number of rows deleted from a Table by the Delete operator.
  std::atomic<size_t> approx_invalid_row_count{0};
};

std::ostream& operator<<(std::ostream& stream, const TableCardinalityEstimationStatistics& table_statistics);

}  // namespace opossum
