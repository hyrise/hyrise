#pragma once

#include <algorithm>
#include <functional>
#include <memory>
#include <string>

#include "abstract_dereferenced_column_table_scan_impl.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

class Table;
class BaseValueSegment;

// Scans for the presence or absence of NULL values in a given column. This is not a
// AbstractDereferencedColumnTableScanImpl because that super class drops NULL values in the referencing column, which
// would break the `NOT NULL` scan.
class ColumnIsNullTableScanImpl : public AbstractDereferencedColumnTableScanImpl {
 public:
  ColumnIsNullTableScanImpl(const std::shared_ptr<const Table>& in_table, const ColumnID column_id,
                            const PredicateCondition& init_predicate_condition);

  std::string description() const override;

 protected:
  void _scan_non_reference_segment(const AbstractSegment& segment, const ChunkID chunk_id, RowIDPosList& matches,
                                   const std::shared_ptr<const AbstractPosList>& position_filter) override;

  void _scan_generic_segment(const AbstractSegment& segment, const ChunkID chunk_id, RowIDPosList& matches,
                             const std::shared_ptr<const AbstractPosList>& position_filter) const;

  void _scan_generic_sorted_segment(const AbstractSegment& segment, const ChunkID chunk_id, RowIDPosList& matches,
                                    const std::shared_ptr<const AbstractPosList>& position_filter,
                                    const SortMode sorted_by) const;

  template <typename BaseSegmentType>
  void _scan_encoded_segment(const BaseSegmentType& segment, const ChunkID chunk_id, RowIDPosList& matches,
                             const std::shared_ptr<const AbstractPosList>& position_filter);

  /**
   * @defgroup Methods used for handling different segments
   * @{
   */

  template <typename BaseIterableType>
  void _scan_iterable_for_null_values(const BaseIterableType& iterable, const ChunkID chunk_id, RowIDPosList& matches,
                                      const std::shared_ptr<const AbstractPosList>& position_filter);

  template <typename BaseSegmentType>
  bool _matches_all(const BaseSegmentType& segment) const;

  template <typename BaseSegmentType>
  bool _matches_none(const BaseSegmentType& segment) const;

  static void _add_all(const ChunkID chunk_id, RowIDPosList& matches, const size_t segment_size);

  /**@}*/
};

}  // namespace hyrise
