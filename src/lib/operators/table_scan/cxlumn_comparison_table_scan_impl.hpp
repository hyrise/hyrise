#pragma once

#include <memory>

#include "base_table_scan_impl.hpp"

#include "types.hpp"

namespace opossum {

class Table;

/**
 * @brief Compares two cxlumns to each other
 *
 * Supports:
 * - comparing cxlumns of different numerical data types
 * - comparing dictionary and value segments
 * - comparing reference segments
 *
 * Note: Since we have ruled out the possibility that a table might have
 *       reference segments and data segments, comparing a reference to a
 *       data segment is not supported.
 */
class CxlumnComparisonTableScanImpl : public BaseTableScanImpl {
 public:
  CxlumnComparisonTableScanImpl(const std::shared_ptr<const Table>& in_table, const CxlumnID left_cxlumn_id,
                                const PredicateCondition& predicate_condition, const CxlumnID right_cxlumn_id);

  std::shared_ptr<PosList> scan_chunk(ChunkID chunk_id) override;

 private:
  const CxlumnID _right_cxlumn_id;
};

}  // namespace opossum
