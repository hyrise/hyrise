#include "column_between_table_scan_impl.hpp"

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

ColumnBetweenTableScanImpl::ColumnBetweenTableScanImpl(const std::shared_ptr<const Table>& in_table,
                                                       const ColumnID column_id, const AllTypeVariant& left_value,
                                                       const AllTypeVariant& right_value)
    : AbstractSingleColumnTableScanImpl{in_table, column_id, PredicateCondition::Between},
      _left_value{left_value},
      _right_value{right_value} {}

std::string ColumnBetweenTableScanImpl::description() const { return "BetweenScan"; }

void ColumnBetweenTableScanImpl::_scan_non_reference_segment(
    const BaseSegment& segment, const ChunkID chunk_id, PosList& matches,
    const std::shared_ptr<const PosList>& position_filter) const {
  // early outs for specific NULL semantics
  if (variant_is_null(_left_value) || variant_is_null(_right_value)) {
    /**
     * Comparing anything with NULL (without using IS [NOT] NULL) will result in NULL.
     * Therefore, these scans will always return an empty position list.
     */
    return;
  }

  resolve_data_and_segment_type(segment, [&](const auto type, const auto& typed_segment) {
    _scan_segment(typed_segment, chunk_id, matches, position_filter);
  });
}

void ColumnBetweenTableScanImpl::_scan_segment(const BaseSegment& segment, const ChunkID chunk_id, PosList& matches,
                                               const std::shared_ptr<const PosList>& position_filter) const {
  resolve_data_and_segment_type(segment, [&](const auto type, const auto& typed_segment) {
    if constexpr (std::is_same_v<decltype(typed_segment), const ReferenceSegment&>) {
      Fail("Expected ReferenceSegments to be handled before calling this method");
    } else {
      using ColumnDataType = typename decltype(type)::type;

      auto typed_left_value = type_cast_variant<ColumnDataType>(_left_value);
      auto typed_right_value = type_cast_variant<ColumnDataType>(_right_value);
      auto comparator = [typed_left_value, typed_right_value](const auto& iterator_value) {
        return iterator_value.value() >= typed_left_value && iterator_value.value() <= typed_right_value;
      };

      auto iterable = create_iterable_from_segment(typed_segment);

      iterable.with_iterators(position_filter, [&](auto left_it, auto left_end) {
        _scan_with_iterators<true>(comparator, left_it, left_end, chunk_id, matches);
      });
    }
  });
}

void ColumnBetweenTableScanImpl::_scan_segment(const BaseDictionarySegment& segment, const ChunkID chunk_id,
                                               PosList& matches,
                                               const std::shared_ptr<const PosList>& position_filter) const {
  const auto left_value_id = segment.lower_bound(_left_value);
  const auto right_value_id = segment.upper_bound(_right_value);

  auto column_iterable = create_iterable_from_attribute_vector(segment);

  // NOLINTNEXTLINE - cpplint is drunk
  if (left_value_id == ValueID{0} && right_value_id == static_cast<ValueID>(segment.unique_values_count())) {
    // all values match
    column_iterable.with_iterators(position_filter, [&](auto left_it, auto left_end) {
      static const auto always_true = [](const auto&) { return true; };
      _scan_with_iterators<false>(always_true, left_it, left_end, chunk_id, matches);
    });

    return;
  }

  if (left_value_id == INVALID_VALUE_ID || left_value_id == right_value_id) {
    // no values match
    return;
  }

  const auto value_id_diff = right_value_id - left_value_id;
  const auto comparator = [left_value_id, value_id_diff](const auto& iterator_value) {
    // Using < here because the right value id is the upper_bound. Also, because the value ids are integers, we can do
    // a little hack here: (x >= a && x < b) === ((x - a) < (b - a)); cf. https://stackoverflow.com/a/17095534/2204581
    return (iterator_value.value() - left_value_id) < value_id_diff;
  };

  column_iterable.with_iterators(position_filter, [&](auto left_it, auto left_end) {
    _scan_with_iterators<true>(comparator, left_it, left_end, chunk_id, matches);
  });
}

}  // namespace opossum
