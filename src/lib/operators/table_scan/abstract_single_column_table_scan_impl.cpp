#include "abstract_single_column_table_scan_impl.hpp"

#include <memory>
#include <unordered_map>
#include <utility>

#include "resolve_type.hpp"
#include "storage/chunk.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/reference_segment.hpp"
#include "storage/split_pos_list_by_chunk_id.hpp"
#include "storage/table.hpp"
#include "storage/value_segment.hpp"

namespace opossum {

AbstractSingleColumnTableScanImpl::AbstractSingleColumnTableScanImpl(const std::shared_ptr<const Table>& in_table,
                                                                     const ColumnID column_id,
                                                                     const PredicateCondition predicate_condition)
    : _in_table(in_table), _column_id(column_id), _predicate_condition(predicate_condition) {}

std::shared_ptr<PosList> AbstractSingleColumnTableScanImpl::scan_chunk(const ChunkID chunk_id) const {
  const auto& chunk = _in_table->get_chunk(chunk_id);
  const auto& segment = chunk->get_segment(_column_id);

  auto matches = std::make_shared<PosList>();

  if (const auto& reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(segment)) {
    _scan_reference_segment(*reference_segment, chunk_id, *matches);
  } else {
    _scan_non_reference_segment(*segment, chunk_id, *matches, nullptr);
  }

  return matches;
}

void AbstractSingleColumnTableScanImpl::_scan_reference_segment(const ReferenceSegment& segment, const ChunkID chunk_id,
                                                                PosList& matches) const {
  const auto& pos_list = segment.pos_list();

  if (pos_list->references_single_chunk() && !pos_list->empty()) {
    // Fast path :)

    const auto chunk = segment.referenced_table()->get_chunk(pos_list->common_chunk_id());
    auto referenced_segment = chunk->get_segment(segment.referenced_column_id());

    _scan_non_reference_segment(*referenced_segment, chunk_id, matches, pos_list);

    return;
  }

  // Slow path - we are looking at multiple referenced chunks and need to split the pos list first
  auto referenced_chunk_count = segment.referenced_table()->chunk_count();
  auto chunk_offsets_by_chunk_id = split_pos_list_by_chunk_id(pos_list, referenced_chunk_count);

  // Visit each referenced segment
  for (auto referenced_chunk_id = ChunkID{0}; referenced_chunk_id < referenced_chunk_count; ++referenced_chunk_id) {
    const auto& sub_pos_list = chunk_offsets_by_chunk_id[referenced_chunk_id];
    const auto& position_filter = sub_pos_list.row_ids;
    if (!position_filter || position_filter->empty()) continue;

    const auto chunk = segment.referenced_table()->get_chunk(referenced_chunk_id);
    auto referenced_segment = chunk->get_segment(segment.referenced_column_id());

    const auto num_previous_matches = matches.size();

    _scan_non_reference_segment(*referenced_segment, chunk_id, matches, position_filter);

    // The scan has filled `matches` assuming that `position_filter` was the entire ReferenceSegment, so we need to fix
    // that:
    for (auto match_idx = static_cast<ChunkOffset>(num_previous_matches); match_idx < matches.size(); ++match_idx) {
      matches[match_idx].chunk_offset = sub_pos_list.original_positions[matches[match_idx].chunk_offset];
    }
  }
}

}  // namespace opossum
