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
                                                             const PredicateCondition predicate_condition,
                                                             const bool skip_null_row_ids)
    : BaseTableScanImpl{in_table, left_column_id, predicate_condition}, _skip_null_row_ids{skip_null_row_ids} {}

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

  auto chunk_offsets_by_chunk_id = split_pos_list_by_chunk_id(*left_column.pos_list(),
                                                              left_column.pos_list_type(),
                                                              _skip_null_row_ids);

  for (auto& [referenced_chunk_id, chunk_offsets_list] : chunk_offsets_by_chunk_id) {
    _visit_referenced(left_column, referenced_chunk_id, *context, std::move(chunk_offsets_list));
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
                                                      const ChunkID referenced_chunk_id,
                                                      Context& context,
                                                      ChunkOffsetsList chunk_offsets_list) {
  const auto chunk = left_column.referenced_table()->get_chunk(referenced_chunk_id);
  auto referenced_column = chunk->get_column(left_column.referenced_column_id());

  auto chunk_offsets_list_ptr = std::make_unique<ChunkOffsetsList>(std::move(chunk_offsets_list));

  auto new_context = std::make_shared<Context>(context, std::move(chunk_offsets_list_ptr));
  referenced_column->visit(*this, new_context);
}

}  // namespace opossum
