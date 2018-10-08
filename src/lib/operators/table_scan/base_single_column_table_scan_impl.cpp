#include "base_single_column_table_scan_impl.hpp"

#include <memory>
#include <unordered_map>
#include <utility>

#include "resolve_type.hpp"
#include "storage/chunk.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/reference_segment.hpp"
#include "storage/segment_iterables/chunk_offset_mapping.hpp"
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

  auto referenced_chunk_count = segment.referenced_table()->chunk_count();
  auto chunk_offsets_by_chunk_id = split_pos_list_by_chunk_id(*segment.pos_list(), referenced_chunk_count);

  // Visit each referenced segment
  for (auto referenced_chunk_id = ChunkID{0}; referenced_chunk_id < referenced_chunk_count; ++referenced_chunk_id) {
    auto& mapped_chunk_offsets = chunk_offsets_by_chunk_id[referenced_chunk_id];
    if (mapped_chunk_offsets.empty()) continue;

    const auto chunk = segment.referenced_table()->get_chunk(referenced_chunk_id);
    auto referenced_segment = chunk->get_segment(segment.referenced_column_id());

    auto mapped_chunk_offsets_ptr = std::make_unique<ChunkOffsetsList>(std::move(mapped_chunk_offsets));

    auto new_context = std::make_shared<Context>(chunk_id, matches_out, std::move(mapped_chunk_offsets_ptr));

    resolve_data_and_segment_type(*referenced_segment, [&](const auto data_type_t, const auto& resolved_segment) {
      static_cast<AbstractSegmentVisitor*>(this)->handle_segment(resolved_segment, new_context);
    });
  }
}

}  // namespace opossum
