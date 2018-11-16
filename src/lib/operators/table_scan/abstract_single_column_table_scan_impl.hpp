#pragma once

#include <memory>
#include <unordered_map>
#include <utility>

#include "abstract_table_scan_impl.hpp"

#include "storage/abstract_segment_visitor.hpp"

#include "types.hpp"

namespace opossum {

class Table;
class ReferenceSegment;
class AttributeVectorIterable;

/**
 * @brief The base class of table scan impls that scan a single column
 *
 * Resolves reference segments. The position list of reference segments
 * is split by the referenced segments and then each is visited separately.
 */
class AbstractSingleColumnTableScanImpl : public AbstractTableScanImpl {
 public:
  AbstractSingleColumnTableScanImpl(const std::shared_ptr<const Table>& in_table, const ColumnID column_id,
                                    const PredicateCondition predicate_condition);

  std::shared_ptr<PosList> scan_chunk(const ChunkID chunk_id) const override;

 protected:
  void _scan_reference_segment(const ReferenceSegment& segment, const ChunkID chunk_id, PosList& matches) const;

  // Implemented by the separate Impls. They do not need to deal with ReferenceSegments anymore, as this class
  // takes care of that. We take `matches` as an in/out parameter instead of returning it because scans on multiple
  // referenced segments of a single ReferenceSegment should result in only one PosList. Storing it as a member is
  // no option because it would break multithreading.
  virtual void _scan_non_reference_segment(const BaseSegment& segment, const ChunkID chunk_id, PosList& matches,
                                           const std::shared_ptr<const PosList>& position_filter) const = 0;

  const std::shared_ptr<const Table> _in_table;
  const ColumnID _column_id;
  const PredicateCondition _predicate_condition;
};

}  // namespace opossum
