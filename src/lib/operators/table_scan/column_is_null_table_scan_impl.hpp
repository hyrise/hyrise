#pragma once

#include <functional>
#include <memory>

#include "abstract_dereferenced_column_table_scan_impl.hpp"

#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

class Table;
class BaseValueSegment;

// Scans for the presence or absence of NULL values in a given column. This is not a
// AbstractDereferencedColumnTableScanImpl because that super class drops NULL values in the referencing column, which
// would break the `NOT NULL` scan.
class ColumnIsNullTableScanImpl : public AbstractTableScanImpl {
 public:
  ColumnIsNullTableScanImpl(const std::shared_ptr<const Table>& in_table, const ColumnID column_id,
                            const PredicateCondition& predicate_condition);

  std::string description() const override;

  std::shared_ptr<PosList> scan_chunk(const ChunkID chunk_id) const override;

 protected:
  void _scan_generic_segment(const BaseSegment& segment, const ChunkID chunk_id, PosList& matches) const;

  // Optimized scan on ValueSegments
  void _scan_value_segment(const BaseValueSegment& segment, const ChunkID chunk_id, PosList& matches) const;

  /**
   * @defgroup Methods used for handling value segments
   * @{
   */

  bool _matches_all(const BaseValueSegment& segment) const;

  bool _matches_none(const BaseValueSegment& segment) const;

  static void _add_all(const ChunkID chunk_id, PosList& matches, const size_t segment_size);

  /**@}*/

  const std::shared_ptr<const Table> _in_table;
  const ColumnID _column_id;
  const PredicateCondition _predicate_condition;
};

}  // namespace opossum
