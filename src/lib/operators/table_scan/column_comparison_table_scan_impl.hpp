#pragma once

#include <memory>

#include "base_table_scan_impl.hpp"

#include "types.hpp"

namespace opossum {

class Table;

/**
 * @brief Compares two columns to each other
 *
 * Supports:
 * - comparing columns of different numerical data types
 * - comparing dictionary and value columns
 * - comparing reference columns
 *
 * Note: Since we have ruled out the possibility that a table might have
 *       reference columns and data columns, comparing a reference to a
 *       data column is not supported.
 */
class ColumnComparisonTableScanImpl : public BaseTableScanImpl {
 public:
  ColumnComparisonTableScanImpl(std::shared_ptr<const Table> in_table, const ColumnID left_column_id,
                                const ScanType &scan_type, const ColumnID right_column_id);

  PosList scan_chunk(ChunkID chunk_id) override;

 private:
  const ColumnID _right_column_id;
};

}  // namespace opossum
