#pragma once

#include <memory>
#include <unordered_map>
#include <utility>

#include "base_table_scan_impl.hpp"

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
class BaseSingleColumnTableScanImpl : public BaseTableScanImpl, public AbstractSegmentVisitor {
 public:
  BaseSingleColumnTableScanImpl(const std::shared_ptr<const Table>& in_table, const ColumnID column_id,
                                const PredicateCondition predicate_condition);

  std::shared_ptr<PosList> scan_chunk(ChunkID chunk_id) override;

  void handle_segment(const ReferenceSegment& segment, std::shared_ptr<SegmentVisitorContext> base_context) override;

 protected:
  /**
   * @brief the context used for the segments' visitor pattern
   */
  struct Context : public SegmentVisitorContext {
    Context(const ChunkID chunk_id, PosList& matches_out) : _chunk_id{chunk_id}, _matches_out{matches_out} {}

    Context(const ChunkID chunk_id, PosList& matches_out, const std::shared_ptr<const PosList>& position_filter)
        : _chunk_id{chunk_id}, _matches_out{matches_out}, _position_filter{position_filter} {}

    const ChunkID _chunk_id;
    PosList& _matches_out;

    const std::shared_ptr<const PosList> _position_filter;
  };
};

}  // namespace opossum
