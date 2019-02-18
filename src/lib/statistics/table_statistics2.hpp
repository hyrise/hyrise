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

class TableStatisticsSlice;

class TableStatistics2 {
 public:
  explicit TableStatistics2(const std::vector<DataType>& column_data_types);

  Cardinality row_count() const;
  size_t column_count() const;

  const std::vector<DataType> column_data_types;

  std::unordered_map<ChunkID, std::shared_ptr<TableStatisticsSlice>> chunk_pruning_statistics;
  std::vector<std::shared_ptr<TableStatisticsSlice>> cardinality_estimation_slices;

  std::atomic<size_t> approx_invalid_row_count{0};
};

std::ostream& operator<<(std::ostream& stream, const TableStatistics2& table_statistics);

}  // namespace opossum
