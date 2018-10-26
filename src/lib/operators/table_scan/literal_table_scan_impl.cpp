#include "literal_table_scan_impl.hpp"

#include <memory>
#include <utility>
#include <vector>

#include "storage/base_dictionary_segment.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "storage/resolve_encoded_segment_type.hpp"
#include "storage/segment_iterables/create_iterable_from_attribute_vector.hpp"

#include "resolve_type.hpp"
#include "type_comparison.hpp"

namespace opossum {

LiteralTableScanImpl::LiteralTableScanImpl(const std::shared_ptr<const Table>& in_table, const ColumnID column_id,
                                           const PredicateCondition& predicate_condition, const AllTypeVariant& value)
    : AbstractSingleColumnTableScanImpl{in_table, column_id, predicate_condition}, _value{value} {}

std::string LiteralTableScanImpl::description() const { return "LiteralTableScan"; }

void LiteralTableScanImpl::_on_scan(const BaseSegment& segment, const ChunkID chunk_id, PosList& results,
                                    const std::shared_ptr<const PosList>& position_filter) const {
  // early outs for specific NULL semantics
  if (variant_is_null(_value)) {
    /**
     * Comparing anything with NULL (without using IS [NOT] NULL) will result in NULL.
     * Therefore, these scans will always return an empty position list.
     * Because OpIsNull/OpIsNotNull are handled separately in IsNullTableScanImpl,
     * we can assume that comparing with NULLs here will always return nothing.
     */
    return;
  }

  resolve_data_and_segment_type(segment, [&](const auto type, const auto& typed_segment) {
    _scan_segment(typed_segment, chunk_id, results, position_filter);
  });
}

void LiteralTableScanImpl::_scan_segment(const BaseSegment& segment, const ChunkID chunk_id, PosList& results,
                                         const std::shared_ptr<const PosList>& position_filter) const {
  resolve_data_and_segment_type(segment, [&](const auto type, const auto& typed_segment) {
    if constexpr (std::is_same_v<decltype(typed_segment), const ReferenceSegment&>) {
      Fail("Expected ReferenceSegments to be handled before calling this method");
    } else {
      using ColumnDataType = typename decltype(type)::type;
      auto typed_value = type_cast<ColumnDataType>(_value);

      auto segment_iterable = create_iterable_from_segment(typed_segment);

      with_comparator(_predicate_condition, [&](auto comparator) {
        auto comparator_with_value = [comparator, typed_value](const auto& iterator_value) {
          return comparator(iterator_value.value(), typed_value);
        };
        segment_iterable.with_iterators(position_filter, [&](auto it, auto end) {
          _scan_with_iterators<true>(comparator_with_value, it, end, chunk_id, results, true);
        });
      });
    }
  });
}

void LiteralTableScanImpl::_scan_segment(const BaseDictionarySegment& segment, const ChunkID chunk_id, PosList& results,
                                         const std::shared_ptr<const PosList>& position_filter) const {
  /*
   * ValueID value_id; // left value id
   * Variant value; // right value
   *
   * A ValueID value_id from the attribute vector is included in the result iff
   *
   * Operator           |  Condition
   * value_id == value  |  dict.value_by_value_id(dict.lower_bound(value)) == value && value_id == dict.lower_bound(value)
   * value_id != value  |  dict.value_by_value_id(dict.lower_bound(value)) != value || value_id != dict.lower_bound(value)
   * value_id <  value  |  value_id < dict.lower_bound(value)
   * value_id <= value  |  value_id < dict.upper_bound(value)
   * value_id >  value  |  value_id >= dict.upper_bound(value)
   * value_id >= value  |  value_id >= dict.lower_bound(value)
   */

  const auto search_value_id = _get_search_value_id(segment);

  /**
   * Early Outs
   *
   * Operator          | All                                   | None
   * value_id == value | !None && unique_values_count == 1     | search_vid == dict.upper_bound(value)
   * value_id != value | search_vid == dict.upper_bound(value) | !All && unique_values_count == 1
   * value_id <  value | search_vid == INVALID_VALUE_ID        | search_vid == 0
   * value_id <= value | search_vid == INVALID_VALUE_ID        | search_vid == 0
   * value_id >  value | search_vid == 0                       | search_vid == INVALID_VALUE_ID
   * value_id >= value | search_vid == 0                       | search_vid == INVALID_VALUE_ID
   */

  auto iterable = create_iterable_from_attribute_vector(segment);

  if (_value_matches_all(segment, search_value_id)) {
    iterable.with_iterators(position_filter, [&](auto it, auto end) {
      static const auto always_true = [](const auto&) { return true; };
      _scan_with_iterators<false>(always_true, it, end, chunk_id, results, true);
    });

    return;
  }

  if (_value_matches_none(segment, search_value_id)) {
    return;
  }

  _with_operator_for_dict_segment_scan(_predicate_condition, [&](auto comparator) {
    auto comparator_with_value = [comparator, search_value_id](const auto& iterator_value) {
      return comparator(iterator_value.value(), search_value_id);
    };
    iterable.with_iterators(position_filter, [&](auto it, auto end) {
      if (_predicate_condition == PredicateCondition::GreaterThan ||
          _predicate_condition == PredicateCondition::GreaterThanEquals) {
        // For GreaterThan(Equals), INVALID_VALUE_ID would compare greater than the search_value_id, even though the
        // value is NULL. Thus, we need to check for is_null as well.
        _scan_with_iterators<true>(comparator_with_value, it, end, chunk_id, results, true);
      } else {
        // No need for NULL checks here, because INVALID_VALUE_ID is always greater.
        _scan_with_iterators<false>(comparator_with_value, it, end, chunk_id, results, true);
      }
    });
  });
}

ValueID LiteralTableScanImpl::_get_search_value_id(const BaseDictionarySegment& segment) const {
  switch (_predicate_condition) {
    case PredicateCondition::Equals:
    case PredicateCondition::NotEquals:
    case PredicateCondition::LessThan:
    case PredicateCondition::GreaterThanEquals:
      return segment.lower_bound(_value);

    case PredicateCondition::LessThanEquals:
    case PredicateCondition::GreaterThan:
      return segment.upper_bound(_value);

    default:
      Fail("Unsupported comparison type encountered");
  }
}

bool LiteralTableScanImpl::_value_matches_all(const BaseDictionarySegment& segment,
                                              const ValueID search_value_id) const {
  switch (_predicate_condition) {
    case PredicateCondition::Equals:
      return search_value_id != segment.upper_bound(_value) && segment.unique_values_count() == size_t{1u};

    case PredicateCondition::NotEquals:
      return search_value_id == segment.upper_bound(_value);

    case PredicateCondition::LessThan:
    case PredicateCondition::LessThanEquals:
      return search_value_id == INVALID_VALUE_ID;

    case PredicateCondition::GreaterThanEquals:
    case PredicateCondition::GreaterThan:
      return search_value_id == ValueID{0u};

    default:
      Fail("Unsupported comparison type encountered");
  }
}

bool LiteralTableScanImpl::_value_matches_none(const BaseDictionarySegment& segment,
                                               const ValueID search_value_id) const {
  switch (_predicate_condition) {
    case PredicateCondition::Equals:
      return search_value_id == segment.upper_bound(_value);

    case PredicateCondition::NotEquals:
      return search_value_id == segment.upper_bound(_value) && segment.unique_values_count() == size_t{1u};

    case PredicateCondition::LessThan:
    case PredicateCondition::LessThanEquals:
      return search_value_id == ValueID{0u};

    case PredicateCondition::GreaterThan:
    case PredicateCondition::GreaterThanEquals:
      return search_value_id == INVALID_VALUE_ID;

    default:
      Fail("Unsupported comparison type encountered");
      return false;
  }
}

}  // namespace opossum
