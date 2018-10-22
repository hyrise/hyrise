#include "between_table_scan_impl.hpp"

#include <memory>
#include <string>
#include <type_traits>

#include "storage/chunk.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "storage/segment_iterables/create_iterable_from_attribute_vector.hpp"
#include "storage/table.hpp"

#include "utils/assert.hpp"

#include "resolve_type.hpp"
#include "type_comparison.hpp"

namespace opossum {

BetweenTableScanImpl::BetweenTableScanImpl(const std::shared_ptr<const Table>& in_table, const ColumnID left_column_id,
                                           const AllTypeVariant& left_value, const AllTypeVariant& right_value)
    : BaseSingleColumnTableScanImpl{in_table, left_column_id, PredicateCondition::Between},
      _left_value{left_value},
      _right_value{right_value} {}

std::string BetweenTableScanImpl::description() const { return "BetweenScan"; }

void BetweenTableScanImpl::handle_segment(const BaseValueSegment& base_segment,
                                          std::shared_ptr<SegmentVisitorContext> base_context) {
  auto context = std::static_pointer_cast<Context>(base_context);
  auto& matches_out = context->_matches_out;
  const auto& mapped_chunk_offsets = context->_mapped_chunk_offsets;
  const auto chunk_id = context->_chunk_id;

  // TODO(anyone): A lot of code is duplicated here, below, and in the other table scans.
  // This can be improved once we have #1145.

  const auto left_column_type = _in_table->column_data_type(_left_column_id);

  resolve_data_type(left_column_type, [&](auto type) {
    using ColumnDataType = typename decltype(type)::type;

    auto& left_segment = static_cast<const ValueSegment<ColumnDataType>&>(base_segment);

    auto left_segment_iterable = create_iterable_from_segment(left_segment);

    left_segment_iterable.with_iterators(mapped_chunk_offsets.get(), [&](auto left_it, auto left_end) {
      _between_scan_with_value<true>(left_it, left_end, type_cast<ColumnDataType>(_left_value),
                                     type_cast<ColumnDataType>(_right_value), chunk_id, matches_out);
    });
  });
}

void BetweenTableScanImpl::handle_segment(const BaseEncodedSegment& base_segment,
                                          std::shared_ptr<SegmentVisitorContext> base_context) {
  auto context = std::static_pointer_cast<Context>(base_context);
  auto& matches_out = context->_matches_out;
  const auto& mapped_chunk_offsets = context->_mapped_chunk_offsets;
  const auto chunk_id = context->_chunk_id;

  const auto left_column_type = _in_table->column_data_type(_left_column_id);

  resolve_data_type(left_column_type, [&](auto type) {
    using Type = typename decltype(type)::type;

    resolve_encoded_segment_type<Type>(base_segment, [&](const auto& typed_segment) {
      auto left_segment_iterable = create_iterable_from_segment(typed_segment);

      left_segment_iterable.with_iterators(mapped_chunk_offsets.get(), [&](auto left_it, auto left_end) {
        _between_scan_with_value<true>(left_it, left_end, type_cast<Type>(_left_value), type_cast<Type>(_right_value),
                                       chunk_id, matches_out);
      });
    });
  });
}

void BetweenTableScanImpl::handle_segment(const BaseDictionarySegment& base_segment,
                                          std::shared_ptr<SegmentVisitorContext> base_context) {
  auto context = std::static_pointer_cast<Context>(base_context);
  auto& matches_out = context->_matches_out;
  const auto chunk_id = context->_chunk_id;
  const auto& mapped_chunk_offsets = context->_mapped_chunk_offsets;

  const auto left_value_id = base_segment.lower_bound(_left_value);
  const auto right_value_id = base_segment.upper_bound(_right_value);

  auto column_iterable = create_iterable_from_attribute_vector(base_segment);

  if (left_value_id == ValueID{0} &&  // NOLINT
      right_value_id == static_cast<ValueID>(base_segment.unique_values_count())) {
    // all values match
    column_iterable.with_iterators(mapped_chunk_offsets.get(), [&](auto left_it, auto left_end) {
      static const auto always_true = [](const auto&) { return true; };
      this->_unary_scan(always_true, left_it, left_end, chunk_id, matches_out);
    });

    return;
  }

  if (left_value_id == INVALID_VALUE_ID || left_value_id == right_value_id) {
    // no values match
    return;
  }

  column_iterable.with_iterators(mapped_chunk_offsets.get(), [&](auto left_it, auto left_end) {
    this->_between_scan_with_value<false>(left_it, left_end, left_value_id, right_value_id, chunk_id, matches_out);
  });
}

}  // namespace opossum
