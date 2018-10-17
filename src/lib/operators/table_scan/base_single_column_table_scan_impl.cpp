#include "base_single_column_table_scan_impl.hpp"

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

BaseSingleColumnTableScanImpl::BaseSingleColumnTableScanImpl(const std::shared_ptr<const Table>& in_table,
                                                             const ColumnID column_id,
                                                             const PredicateCondition predicate_condition)
    : BaseTableScanImpl{in_table, column_id, predicate_condition} {}

std::shared_ptr<PosList> BaseSingleColumnTableScanImpl::scan_chunk(ChunkID chunk_id) {
  const auto chunk = _in_table->get_chunk(chunk_id);
  const auto segment = chunk->get_segment(_left_column_id);

  auto matches_out = std::make_shared<PosList>();
  auto context = std::make_shared<Context>(chunk_id, *matches_out);

  resolve_data_and_segment_type(*segment, [&](const auto data_type_t, const auto& resolved_segment) {
    static_cast<AbstractSegmentVisitor*>(this)->handle_segment(resolved_segment, context);
  });

  return matches_out;
}

void BaseSingleColumnTableScanImpl::handle_segment(const ReferenceSegment& segment,
                                                   std::shared_ptr<SegmentVisitorContext> base_context) {
  auto context = std::static_pointer_cast<Context>(base_context);
  const ChunkID chunk_id = context->_chunk_id;
  auto& matches_out = context->_matches_out;

  const auto& pos_list = segment.pos_list();

  if (pos_list->references_single_chunk() && !pos_list->empty()) {
    // Fast path :)

    const auto chunk = segment.referenced_table()->get_chunk((*pos_list)[0].chunk_id);
    auto referenced_segment = chunk->get_segment(segment.referenced_column_id());

    auto new_context = std::make_shared<Context>(chunk_id, matches_out, pos_list);

    resolve_data_and_segment_type(*referenced_segment, [&](const auto data_type_t, const auto& resolved_segment) {
      static_cast<AbstractSegmentVisitor*>(this)->handle_segment(resolved_segment, new_context);
    });

    return;
  }

  // Slow path - we are looking at multiple referenced chunks and need to split the pos list first
  auto referenced_chunk_count = segment.referenced_table()->chunk_count();
  auto chunk_offsets_by_chunk_id = split_pos_list_by_chunk_id(pos_list, referenced_chunk_count);

  // Visit each referenced segment
  for (auto referenced_chunk_id = ChunkID{0}; referenced_chunk_id < referenced_chunk_count; ++referenced_chunk_id) {
    auto& splitted_pos_list = chunk_offsets_by_chunk_id[referenced_chunk_id];
    auto& position_filter = splitted_pos_list.position_filter;
    if (!position_filter || position_filter->empty()) continue;

    const auto chunk = segment.referenced_table()->get_chunk(referenced_chunk_id);
    auto referenced_segment = chunk->get_segment(segment.referenced_column_id());

    const auto num_previous_matches = matches_out.size();

    auto new_context = std::make_shared<Context>(chunk_id, matches_out, position_filter);

    // std::cout << "before scan, matches_out.size() is " << matches_out.size() << std::endl;
    resolve_data_and_segment_type(*referenced_segment, [&](const auto data_type_t, const auto& resolved_segment) {
      static_cast<AbstractSegmentVisitor*>(this)->handle_segment(resolved_segment, new_context);
    });
    // std::cout << "after scan, matches_out.size() is " << matches_out.size() << std::endl;

    // The scan has filled matches_out assuming that position_filter was the entire ReferenceSegment, so we need to fix
    // that:

    // std::cout << ">>>>>>>>>>>>>>>>>>" << std::endl;
    for(auto match_idx = static_cast<ChunkOffset>(num_previous_matches); match_idx < matches_out.size(); ++match_idx) {
      // std::cout << "match_idx == " << match_idx << std::endl;
      // std::cout << "position_filter.size == " << splitted_pos_list.position_filter->size() << std::endl;
      // std::cout << "original_positions.size == " << splitted_pos_list.original_positions.size() << std::endl;
      // std::cout << "matches_out[match_idx].chunk_offset == " << matches_out[match_idx].chunk_offset << std::endl << std::endl;
      matches_out[match_idx].chunk_offset = splitted_pos_list.original_positions.at(matches_out[match_idx].chunk_offset);  // TODO remove at
    }
    // std::cout << "<<<<<<<<<<<<<<<<<<" << std::endl;
  }
}

}  // namespace opossum
