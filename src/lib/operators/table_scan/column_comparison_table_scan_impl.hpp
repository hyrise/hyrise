#pragma once

#include <memory>

#include "base_table_scan_impl.hpp"

#include "types.hpp"

namespace opossum {

class Table;

/**
 * @brief Compares two columns to each other
 *
 * Does not support:
 * - comparison of string columns to numerical columns
 * - reference columns to data columns (value, dictionary)
 */
class ColumnComparisonTableScanImpl : public BaseTableScanImpl {
 public:
  ColumnComparisonTableScanImpl(std::shared_ptr<const Table> in_table, const ColumnID left_column_id,
                                const ScanType &scan_type, const ColumnID right_column_id);

  PosList scan_chunk(const ChunkID &chunk_id) override;

 private:
  const ColumnID _right_column_id;
};

}  // namespace opossum
