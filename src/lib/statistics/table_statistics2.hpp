#pragma once

#include <memory>
#include <optional>
#include <vector>

#include "cardinality.hpp"
#include "all_type_variant.hpp"

namespace opossum {

class ChunkStatistics2;

class TableStatistics2 {
 public:
  Cardinality row_count() const;

  size_t column_count() const;
  DataType column_data_type(const ColumnID column_id);

  const std::vector<std::shared_ptr<ChunkStatistics2>>& chunk_statistics_default() const;
  const std::vector<std::shared_ptr<ChunkStatistics2>>& chunk_statistics_compact() const;

  std::vector<std::shared_ptr<ChunkStatistics2>> chunk_statistics_primary;
  std::optional<std::vector<std::shared_ptr<ChunkStatistics2>>> chunk_statistics_secondary;
};

}  // namespace opossum
