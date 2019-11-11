#pragma once

#include <memory>
#include <unordered_map>
#include <utility>

#include "abstract_table_scan_impl.hpp"

#include "types.hpp"

namespace opossum {

class Table;
class ReferenceSegment;
class BaseSegment;
class BaseDictionarySegment;
class AttributeVectorIterable;

/**
 * @brief The base class of table scan implementations that operate on a single column and profit from references being
 *        resolved. Most prominently, this is the case when dictionary segments are referenced. We split the input
 *        by chunk so that the implementation can operate on a single dictionary segment. There, it can use all the
 *        optimizations possible only for dictionary encoding (early outs, scanning value IDs instead of values).
 */
class AbstractDereferencedColumnTableScanImpl : public AbstractTableScanImpl {
 public:
  AbstractDereferencedColumnTableScanImpl(const std::shared_ptr<const Table>& in_table, const ColumnID column_id,
                                          const PredicateCondition predicate_condition);

  std::shared_ptr<PosList> scan_chunk(const ChunkID chunk_id) const override;

  const PredicateCondition predicate_condition;

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
};

}  // namespace opossum
