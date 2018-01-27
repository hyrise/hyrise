#include "is_null_table_scan_impl.hpp"

#include <memory>

#include "storage/base_dictionary_column.hpp"
#include "storage/base_value_column.hpp"
#include "storage/iterables/attribute_vector_iterable.hpp"
#include "storage/iterables/null_value_vector_iterable.hpp"

#include "utils/assert.hpp"

namespace opossum {

IsNullTableScanImpl::IsNullTableScanImpl(std::shared_ptr<const Table> in_table, const ColumnID left_column_id,
                                         const PredicateCondition& predicate_condition)
    : BaseSingleColumnTableScanImpl{in_table, left_column_id, predicate_condition, false} {}

void IsNullTableScanImpl::handle_value_column(const BaseValueColumn& base_column,
                                              std::shared_ptr<ColumnVisitableContext> base_context) {
  auto context = std::static_pointer_cast<Context>(base_context);
  const auto& mapped_chunk_offsets = context->_mapped_chunk_offsets;
  auto& left_column = static_cast<const BaseValueColumn&>(base_column);

  if (_matches_all(left_column)) {
    _add_all(*context, left_column.size());
    return;
  }

  if (_matches_none(left_column)) {
    return;
  }

  DebugAssert(left_column.is_nullable(),
              "Columns that are not nullable should have been caught by edge case handling.");

  auto left_column_iterable = NullValueVectorIterable{left_column.null_values()};

  left_column_iterable.with_iterators(mapped_chunk_offsets.get(),
                                      [&](auto left_it, auto left_end) { this->_scan(left_it, left_end, *context); });
}

void IsNullTableScanImpl::handle_dictionary_column(const BaseDictionaryColumn& base_column,
                                                   std::shared_ptr<ColumnVisitableContext> base_context) {
  auto context = std::static_pointer_cast<Context>(base_context);
  const auto& mapped_chunk_offsets = context->_mapped_chunk_offsets;
  auto& left_column = static_cast<const BaseDictionaryColumn&>(base_column);

  auto left_column_iterable = AttributeVectorIterable{*left_column.attribute_vector()};

  left_column_iterable.with_iterators(mapped_chunk_offsets.get(),
                                      [&](auto left_it, auto left_end) { this->_scan(left_it, left_end, *context); });
}

bool IsNullTableScanImpl::_matches_all(const BaseValueColumn& column) {
  switch (_predicate_condition) {
    case PredicateCondition::IsNull:
      return false;

    case PredicateCondition::IsNotNull:
      return !column.is_nullable();

    default:
      Fail("Unsupported comparison type encountered");
  }
}

bool IsNullTableScanImpl::_matches_none(const BaseValueColumn& column) {
  switch (_predicate_condition) {
    case PredicateCondition::IsNull:
      return !column.is_nullable();

    case PredicateCondition::IsNotNull:
      return false;

    default:
      Fail("Unsupported comparison type encountered");
  }
}

void IsNullTableScanImpl::_add_all(Context& context, size_t column_size) {
  auto& matches_out = context._matches_out;
  const auto chunk_id = context._chunk_id;
  const auto& mapped_chunk_offsets = context._mapped_chunk_offsets;

  if (mapped_chunk_offsets) {
    for (const auto& chunk_offsets : *mapped_chunk_offsets) {
      matches_out.push_back(RowID{chunk_id, chunk_offsets.into_referencing});
    }
  } else {
    for (auto chunk_offset = 0u; chunk_offset < column_size; ++chunk_offset) {
      matches_out.push_back(RowID{chunk_id, chunk_offset});
    }
  }
}

}  // namespace opossum
