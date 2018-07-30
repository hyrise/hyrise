#include "base_single_column_table_scan_impl.hpp"

#include <memory>
#include <unordered_map>
#include <utility>

#include "resolve_type.hpp"
#include "storage/chunk.hpp"
#include "storage/column_iterables/chunk_offset_mapping.hpp"
#include "storage/dictionary_column.hpp"
#include "storage/reference_column.hpp"
#include "storage/table.hpp"
#include "storage/value_column.hpp"

namespace opossum {

BaseSingleColumnTableScanImpl::BaseSingleColumnTableScanImpl(const std::shared_ptr<const Table>& in_table,
                                                             const ColumnID left_column_id,
                                                             const PredicateCondition predicate_condition)
    : BaseTableScanImpl{in_table, left_column_id, predicate_condition} {}

std::shared_ptr<PosList> BaseSingleColumnTableScanImpl::scan_chunk(ChunkID chunk_id) {
  const auto chunk = _in_table->get_chunk(chunk_id);
  const auto left_column = chunk->get_column(_left_column_id);

  auto matches_out = std::make_shared<PosList>();
  auto context = std::make_shared<Context>(chunk_id, *matches_out);

  resolve_data_and_column_type(*left_column, [&](const auto data_type_t, const auto& resolved_column) {
    static_cast<AbstractColumnVisitor*>(this)->handle_column(resolved_column, context);
  });

  return matches_out;
}

void BaseSingleColumnTableScanImpl::handle_column(const ReferenceColumn& left_column,
                                                  std::shared_ptr<ColumnVisitorContext> base_context) {
  auto context = std::static_pointer_cast<Context>(base_context);
  const ChunkID chunk_id = context->_chunk_id;
  auto& matches_out = context->_matches_out;

  auto chunk_offsets_by_chunk_id = split_pos_list_by_chunk_id(*left_column.pos_list());

  // Visit each referenced column
  for (auto& pair : chunk_offsets_by_chunk_id) {
    const auto& referenced_chunk_id = pair.first;
    auto& mapped_chunk_offsets = pair.second;

    const auto chunk = left_column.referenced_table()->get_chunk(referenced_chunk_id);
    auto referenced_column = chunk->get_column(left_column.referenced_column_id());

    auto mapped_chunk_offsets_ptr = std::make_unique<ChunkOffsetsList>(std::move(mapped_chunk_offsets));

    auto new_context = std::make_shared<Context>(chunk_id, matches_out, std::move(mapped_chunk_offsets_ptr));

    resolve_data_and_column_type(*referenced_column, [&](const auto data_type_t, const auto& resolved_column) {
      static_cast<AbstractColumnVisitor*>(this)->handle_column(resolved_column, new_context);
    });
  }
}

}  // namespace opossum
