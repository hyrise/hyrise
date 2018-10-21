#include "is_null_table_scan_impl.hpp"

#include <memory>

#include "storage/base_value_segment.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "storage/resolve_encoded_segment_type.hpp"
#include "storage/segment_iterables/create_iterable_from_attribute_vector.hpp"
#include "storage/value_segment/null_value_vector_iterable.hpp"

#include "resolve_type.hpp"
#include "utils/assert.hpp"

namespace opossum {

IsNullTableScanImpl::IsNullTableScanImpl(const std::shared_ptr<const Table>& in_table, const ColumnID base_column_id,
                                         const PredicateCondition& predicate_condition)
    : BaseSingleColumnTableScanImpl{in_table, base_column_id, predicate_condition} {
  DebugAssert(predicate_condition == PredicateCondition::IsNull || predicate_condition == PredicateCondition::IsNotNull,
              "Invalid PredicateCondition");
}

std::string IsNullTableScanImpl::description() const { return "IsNullScan"; }

void IsNullTableScanImpl::handle_segment(const ReferenceSegment& base_segment,
                                         std::shared_ptr<SegmentVisitorContext> base_context) {
  auto context = std::static_pointer_cast<Context>(base_context);
  BaseSingleColumnTableScanImpl::handle_segment(base_segment, base_context);

  const auto pos_list = *base_segment.pos_list();

  // Additionally to the null values in the referencED segment, we need to find null values in the referencING segment
  if (_predicate_condition == PredicateCondition::IsNull) {
    for (ChunkOffset chunk_offset{0}; chunk_offset < pos_list.size(); ++chunk_offset) {
      if (pos_list[chunk_offset].is_null()) context->_matches_out.emplace_back(context->_chunk_id, chunk_offset);
    }
  }
}

void IsNullTableScanImpl::handle_segment(const BaseValueSegment& base_segment,
                                         std::shared_ptr<SegmentVisitorContext> base_context) {
  auto context = std::static_pointer_cast<Context>(base_context);
  const auto& mapped_chunk_offsets = context->_mapped_chunk_offsets;

  if (_matches_all(base_segment)) {
    _add_all(*context, base_segment.size());
    return;
  }

  if (_matches_none(base_segment)) {
    return;
  }

  DebugAssert(base_segment.is_nullable(),
              "Columns that are not nullable should have been caught by edge case handling.");

  auto base_segment_iterable = NullValueVectorIterable{base_segment.null_values()};

  base_segment_iterable.with_iterators(mapped_chunk_offsets.get(),
                                       [&](auto left_it, auto left_end) { this->_scan(left_it, left_end, *context); });
}

void IsNullTableScanImpl::handle_segment(const BaseDictionarySegment& base_segment,
                                         std::shared_ptr<SegmentVisitorContext> base_context) {
  auto context = std::static_pointer_cast<Context>(base_context);
  const auto& mapped_chunk_offsets = context->_mapped_chunk_offsets;

  auto base_segment_iterable = create_iterable_from_attribute_vector(base_segment);

  base_segment_iterable.with_iterators(mapped_chunk_offsets.get(),
                                       [&](auto left_it, auto left_end) { this->_scan(left_it, left_end, *context); });
}

void IsNullTableScanImpl::handle_segment(const BaseEncodedSegment& base_segment,
                                         std::shared_ptr<SegmentVisitorContext> base_context) {
  auto context = std::static_pointer_cast<Context>(base_context);
  const auto& mapped_chunk_offsets = context->_mapped_chunk_offsets;

  const auto base_column_type = _in_table->column_data_type(_left_column_id);

  resolve_data_type(base_column_type, [&](auto type) {
    using Type = typename decltype(type)::type;

    resolve_encoded_segment_type<Type>(base_segment, [&](const auto& typed_segment) {
      auto base_segment_iterable = create_iterable_from_segment(typed_segment);

      base_segment_iterable.with_iterators(
          mapped_chunk_offsets.get(), [&](auto left_it, auto left_end) { this->_scan(left_it, left_end, *context); });
    });
  });
}

bool IsNullTableScanImpl::_matches_all(const BaseValueSegment& segment) {
  switch (_predicate_condition) {
    case PredicateCondition::IsNull:
      return false;

    case PredicateCondition::IsNotNull:
      return !segment.is_nullable();

    default:
      Fail("Unsupported comparison type encountered");
  }
}

bool IsNullTableScanImpl::_matches_none(const BaseValueSegment& segment) {
  switch (_predicate_condition) {
    case PredicateCondition::IsNull:
      return !segment.is_nullable();

    case PredicateCondition::IsNotNull:
      return false;

    default:
      Fail("Unsupported comparison type encountered");
  }
}

void IsNullTableScanImpl::_add_all(Context& context, size_t segment_size) {
  auto& matches_out = context._matches_out;
  const auto chunk_id = context._chunk_id;
  const auto& mapped_chunk_offsets = context._mapped_chunk_offsets;

  if (mapped_chunk_offsets) {
    for (const auto& chunk_offsets : *mapped_chunk_offsets) {
      matches_out.emplace_back(RowID{chunk_id, chunk_offsets.into_referencing});
    }
  } else {
    for (auto chunk_offset = 0u; chunk_offset < segment_size; ++chunk_offset) {
      matches_out.emplace_back(RowID{chunk_id, chunk_offset});
    }
  }
}

}  // namespace opossum
