#pragma once

#include <memory>

#include "abstract_table_scan_impl.hpp"

#include "types.hpp"

namespace opossum {

class Table;

/**
 * @brief Compares two columns to each other
 *
 * Supports:
 * - comparing columns of different numerical data types
 * - comparing dictionary and value segments
 * - comparing reference segments
 *
 * Note: Since we have ruled out the possibility that a table might have
 *       reference segments and data segments, comparing a reference to a
 *       data segment is not supported.
 */
class ColumnVsColumnTableScanImpl : public AbstractTableScanImpl {
 public:
  ColumnVsColumnTableScanImpl(const std::shared_ptr<const Table>& in_table, const ColumnID left_column_id,
                              const PredicateCondition& predicate_condition, const ColumnID right_column_id);

  std::string description() const override;

  std::shared_ptr<PosList> scan_chunk(ChunkID chunk_id) const override;

 private:
  const std::shared_ptr<const Table> _in_table;
  const ColumnID _left_column_id;
  const PredicateCondition _predicate_condition;
  const ColumnID _right_column_id;

  template <EraseTypes erase_comparator_type, typename LeftIterable, typename RightIterable>
  std::shared_ptr<PosList> _typed_scan_chunk(ChunkID chunk_id, const LeftIterable& left_iterable,
                                             const RightIterable& right_iterable) const;
};

}  // namespace opossum
