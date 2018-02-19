#include "base_single_column_table_scan_impl.hpp"

#include <memory>
#include <unordered_map>
#include <utility>

#include "storage/chunk.hpp"
#include "storage/column_iterables/chunk_offset_mapping.hpp"
#include "storage/deprecated_dictionary_column.hpp"
#include "storage/deprecated_dictionary_column/deprecated_attribute_vector_iterable.hpp"
#include "storage/dictionary_column.hpp"
#include "storage/dictionary_column/attribute_vector_iterable.hpp"
#include "storage/reference_column.hpp"
#include "storage/table.hpp"
#include "storage/value_column.hpp"

namespace opossum {

BaseSingleColumnTableScanImpl::BaseSingleColumnTableScanImpl(std::shared_ptr<const Table> in_table,
                                                             const ColumnID left_column_id,
                                                             const PredicateCondition predicate_condition)
    : BaseTableScanImpl{in_table, left_column_id, predicate_condition} {}

PosList BaseSingleColumnTableScanImpl::scan_chunk(ChunkID chunk_id) {
  const auto chunk = _in_table->get_chunk(chunk_id);
  const auto left_column = chunk->get_column(_left_column_id);

  auto matches_out = PosList{};
  auto context = std::make_shared<Context>(chunk_id, matches_out);

  left_column->visit(*this, context);

  return matches_out;
}

void BaseSingleColumnTableScanImpl::handle_column(const ReferenceColumn& left_column,
                                                  std::shared_ptr<ColumnVisitableContext> base_context) {
  auto context = std::static_pointer_cast<Context>(base_context);

  const auto& pos_list = *left_column.pos_list();

  const auto references_single_chunk = left_column.pos_list_type() == PosListType::SingleChunk;
  const auto is_empty = pos_list.empty();

  if (is_empty) {
    return;
  }

  if (references_single_chunk) {
    const auto chunk_id = pos_list.front().chunk_id;
    _visit_referenced(left_column, chunk_id, *context, {pos_list.cbegin(), pos_list.cend(), ChunkOffset{0u}});
    return;
  }

  auto current_chunk_id = pos_list.front().chunk_id;

  auto begin_pos_list_it = pos_list.cbegin();
  auto end_pos_list_it = pos_list.cbegin();
  auto begin_chunk_offset = ChunkOffset{0u};

  while (begin_pos_list_it != pos_list.cend()) {
    ++end_pos_list_it;

    if (end_pos_list_it->chunk_id != current_chunk_id || end_pos_list_it == pos_list.cend()) {
      _visit_referenced(left_column, current_chunk_id, *context,
                        {begin_pos_list_it, end_pos_list_it, begin_chunk_offset});

      begin_chunk_offset += static_cast<ChunkOffset>(std::distance(begin_pos_list_it, end_pos_list_it));
      begin_pos_list_it = end_pos_list_it;
      current_chunk_id = begin_pos_list_it->chunk_id;
    }
  }
}

AttributeVectorIterable BaseSingleColumnTableScanImpl::_create_attribute_vector_iterable(
    const BaseDictionaryColumn& column) {
  return AttributeVectorIterable{*column.attribute_vector(), column.null_value_id()};
}

DeprecatedAttributeVectorIterable BaseSingleColumnTableScanImpl::_create_attribute_vector_iterable(
    const BaseDeprecatedDictionaryColumn& column) {
  return DeprecatedAttributeVectorIterable{*column.attribute_vector()};
}

void BaseSingleColumnTableScanImpl::_visit_referenced(const ReferenceColumn& left_column,
                                                      const ChunkID referenced_chunk_id, Context& context,
                                                      ColumnPointAccessPlan access_plan) {
  const auto chunk = left_column.referenced_table()->get_chunk(referenced_chunk_id);
  auto referenced_column = chunk->get_column(left_column.referenced_column_id());

  auto new_context = std::make_shared<Context>(context, std::move(access_plan));
  referenced_column->visit(*this, new_context);
}

}  // namespace opossum
