#include "is_null_table_scan_impl.hpp"

#include <memory>

#include "storage/base_value_column.hpp"
#include "storage/column_iterables/create_iterable_from_attribute_vector.hpp"
#include "storage/create_iterable_from_column.hpp"
#include "storage/resolve_encoded_column_type.hpp"
#include "storage/value_column/null_value_vector_iterable.hpp"

#include "resolve_type.hpp"
#include "utils/assert.hpp"

namespace opossum {

IsNullTableScanImpl::IsNullTableScanImpl(std::shared_ptr<const Table> in_table, const ColumnID left_column_id,
                                         const PredicateCondition& predicate_condition)
    : BaseSingleColumnTableScanImpl{in_table, left_column_id, predicate_condition} {
  DebugAssert(predicate_condition == PredicateCondition::IsNull || predicate_condition == PredicateCondition::IsNotNull,
              "Invalid PredicateCondition");
}

void IsNullTableScanImpl::handle_column(const ReferenceColumn& left_column,
                                        std::shared_ptr<ColumnVisitableContext> base_context) {
  auto context = std::static_pointer_cast<Context>(base_context);
  BaseSingleColumnTableScanImpl::handle_column(left_column, base_context);

  const auto pos_list = *left_column.pos_list();

  // Additionally to the null values in the referencED column, we need to find null values in the referencING column
  if (_predicate_condition == PredicateCondition::IsNull) {
    for (ChunkOffset chunk_offset{0}; chunk_offset < pos_list.size(); ++chunk_offset) {
      if (pos_list[chunk_offset].is_null()) context->_matches_out.emplace_back(context->_chunk_id, chunk_offset);
    }
  }
}

void IsNullTableScanImpl::handle_column(const BaseValueColumn& base_column,
                                        std::shared_ptr<ColumnVisitableContext> base_context) {
  auto context = std::static_pointer_cast<Context>(base_context);
  const auto& mapped_chunk_offsets = context->_mapped_chunk_offsets;

  if (_matches_all(base_column)) {
    _add_all(*context, base_column.size());
    return;
  }

  if (_matches_none(base_column)) {
    return;
  }

  DebugAssert(base_column.is_nullable(),
              "Columns that are not nullable should have been caught by edge case handling.");

  auto left_column_iterable = NullValueVectorIterable{base_column.null_values()};

  left_column_iterable.with_iterators(mapped_chunk_offsets.get(),
                                      [&](auto left_it, auto left_end) { this->_scan(left_it, left_end, *context); });
}

void IsNullTableScanImpl::handle_column(const BaseDictionaryColumn& left_column,
                                        std::shared_ptr<ColumnVisitableContext> base_context) {
  auto context = std::static_pointer_cast<Context>(base_context);
  const auto& mapped_chunk_offsets = context->_mapped_chunk_offsets;

  auto left_column_iterable = create_iterable_from_attribute_vector(left_column);

  left_column_iterable.with_iterators(mapped_chunk_offsets.get(),
                                      [&](auto left_it, auto left_end) { this->_scan(left_it, left_end, *context); });
}

void IsNullTableScanImpl::handle_column(const BaseEncodedColumn& base_column,
                                        std::shared_ptr<ColumnVisitableContext> base_context) {
  auto context = std::static_pointer_cast<Context>(base_context);
  const auto& mapped_chunk_offsets = context->_mapped_chunk_offsets;

  const auto left_column_type = _in_table->column_data_type(_left_column_id);

  resolve_data_type(left_column_type, [&](auto type) {
    using Type = typename decltype(type)::type;

    resolve_encoded_column_type<Type>(base_column, [&](const auto& typed_column) {
      auto left_column_iterable = create_iterable_from_column(typed_column);

      left_column_iterable.with_iterators(
          mapped_chunk_offsets.get(), [&](auto left_it, auto left_end) { this->_scan(left_it, left_end, *context); });
    });
  });
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
