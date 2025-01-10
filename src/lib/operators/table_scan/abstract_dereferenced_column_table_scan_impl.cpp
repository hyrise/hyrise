#include "abstract_dereferenced_column_table_scan_impl.hpp"

#include <memory>

#include "storage/pos_lists/row_id_pos_list.hpp"
#include "storage/reference_segment.hpp"
#include "storage/split_pos_list_by_chunk_id.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

AbstractDereferencedColumnTableScanImpl::AbstractDereferencedColumnTableScanImpl(
    const std::shared_ptr<const Table>& in_table, const ColumnID column_id,
    const PredicateCondition init_predicate_condition)
    : predicate_condition(init_predicate_condition), _in_table(in_table), _column_id(column_id) {}

std::shared_ptr<RowIDPosList> AbstractDereferencedColumnTableScanImpl::scan_chunk(const ChunkID chunk_id) {
  const auto chunk = _in_table->get_chunk(chunk_id);
  const auto& segment = chunk->get_segment(_column_id);

  auto matches = std::make_shared<RowIDPosList>();

  if (const auto& reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(segment)) {
    _scan_reference_segment(*reference_segment, chunk_id, *matches);
  } else {
    _scan_non_reference_segment(*segment, chunk_id, *matches, nullptr);
  }

  return matches;
}

void AbstractDereferencedColumnTableScanImpl::_scan_reference_segment(const ReferenceSegment& segment,
                                                                      const ChunkID chunk_id, RowIDPosList& matches) {
  const auto& pos_list = segment.pos_list();

  if (pos_list->references_single_chunk() && !pos_list->empty()) {
    // Fast path :)

    const auto chunk = segment.referenced_table()->get_chunk(pos_list->common_chunk_id());
    auto referenced_segment = chunk->get_segment(segment.referenced_column_id());

    _scan_non_reference_segment(*referenced_segment, chunk_id, matches, pos_list);

    return;
  }

  // Slow path - we are looking at multiple referenced chunks and need to split the pos list first
  const auto referenced_chunk_count = segment.referenced_table()->chunk_count();
  auto chunk_offsets_by_chunk_id = PosListsByChunkID{};
  if (predicate_condition == PredicateCondition::IsNull) {
    chunk_offsets_by_chunk_id = split_pos_list_by_chunk_id<true>(pos_list, referenced_chunk_count);
  } else {
    chunk_offsets_by_chunk_id = split_pos_list_by_chunk_id<false>(pos_list, referenced_chunk_count);
  }

  // Visit each referenced segment
  for (auto referenced_chunk_id = ChunkID{0}; referenced_chunk_id < referenced_chunk_count; ++referenced_chunk_id) {
    const auto& sub_pos_list = chunk_offsets_by_chunk_id[referenced_chunk_id];
    const auto& position_filter = sub_pos_list.row_ids;
    if (!position_filter || position_filter->empty()) {
      continue;
    }

    const auto chunk = segment.referenced_table()->get_chunk(referenced_chunk_id);
    auto referenced_segment = chunk->get_segment(segment.referenced_column_id());

    const auto num_previous_matches = matches.size();

    _scan_non_reference_segment(*referenced_segment, chunk_id, matches, position_filter);

    const auto num_matches = matches.size();

    // The scan has filled `matches` assuming that `position_filter` was the entire ReferenceSegment, so we need to fix
    // that:
    for (auto match_idx = static_cast<ChunkOffset>(num_previous_matches);
         match_idx < static_cast<ChunkOffset>(num_matches); ++match_idx) {
      DebugAssert(sub_pos_list.original_positions.size() > matches[match_idx].chunk_offset,
                  "Missing original_position for match.");
      matches[match_idx].chunk_offset = sub_pos_list.original_positions[matches[match_idx].chunk_offset];
    }
  }

  if (predicate_condition != PredicateCondition::IsNull) {
    return;
  }

  // For PredicateCondition::IsNull, split_pos_list_by_chunk_id() stores all NULL_ROW_IDs in an extra SubPosList at the
  // end of chunk_offsets_by_chunk_id. These are then retrieved and written to the matches.

  DebugAssert(chunk_offsets_by_chunk_id.size() == referenced_chunk_count + 1, "NULL_ROW_IDs lost in split.");

  const auto& sub_pos_list = chunk_offsets_by_chunk_id[referenced_chunk_count];
  const auto& remaining_null_row_id_positions = *sub_pos_list.row_ids;

  const auto num_previous_matches = matches.size();
  const auto num_remaining_null_row_id_positions = remaining_null_row_id_positions.size();
  matches.resize(num_previous_matches + num_remaining_null_row_id_positions);

  for (auto index = ChunkOffset{0}; index < num_remaining_null_row_id_positions; ++index) {
    matches[num_previous_matches + index] = RowID{chunk_id, remaining_null_row_id_positions[index].chunk_offset};
  }
}

}  // namespace hyrise
