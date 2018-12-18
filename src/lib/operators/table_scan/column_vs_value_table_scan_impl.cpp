#include "column_vs_value_table_scan_impl.hpp"

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

ColumnVsValueTableScanImpl::ColumnVsValueTableScanImpl(const std::shared_ptr<const Table>& in_table,
                                                       const ColumnID column_id,
                                                       const PredicateCondition& predicate_condition,
                                                       const AllTypeVariant& value)
    : AbstractSingleColumnTableScanImpl{in_table, column_id, predicate_condition}, _value{value} {}

std::string ColumnVsValueTableScanImpl::description() const { return "LiteralTableScan"; }

void ColumnVsValueTableScanImpl::_scan_non_reference_segment(
    const BaseSegment& segment, const ChunkID chunk_id, PosList& matches,
    const std::shared_ptr<const PosList>& position_filter) const {
  // early outs for specific NULL semantics
  if (variant_is_null(_value)) {
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

void ColumnVsValueTableScanImpl::_scan_segment(const BaseSegment& segment, const ChunkID chunk_id, PosList& matches,
                                               const std::shared_ptr<const PosList>& position_filter) const {
  resolve_data_and_segment_type(segment, [&](const auto type, const auto& typed_segment) {
    if constexpr (std::is_same_v<decltype(typed_segment), const ReferenceSegment&>) {
      Fail("Expected ReferenceSegments to be handled before calling this method");
    } else {
      using ColumnDataType = typename decltype(type)::type;
      auto typed_value = type_cast_variant<ColumnDataType>(_value);

      auto segment_iterable = create_iterable_from_segment(typed_segment);

      with_comparator(_predicate_condition, [&](auto predicate_comparator) {
        auto comparator = [predicate_comparator, typed_value](const auto& iterator_value) {
          return predicate_comparator(iterator_value.value(), typed_value);
        };
        segment_iterable.with_iterators(position_filter, [&](auto it, auto end) {
          _scan_with_iterators<true>(comparator, it, end, chunk_id, matches);
        });
      });
    }
  });
}

void ColumnVsValueTableScanImpl::_scan_segment(const BaseDictionarySegment& segment, const ChunkID chunk_id,
                                               PosList& matches,
                                               const std::shared_ptr<const PosList>& position_filter) const {
  /**
   * ValueID search_vid;              // left value id
   * AllTypeVariant search_vid_value; // dict.value_by_value_id(search_vid)
   * Variant _value;                  // right value
   *
   * A ValueID value_id from the attribute vector is included in the result iff
   *
   * Operator          |  Condition
   * column == _value  |  dict.value_by_value_id(dict.lower_bound(_value)) == _value && value_id == dict.lower_bound(_value)
   * column != _value  |  dict.value_by_value_id(dict.lower_bound(value)) != _value || value_id != dict.lower_bound(_value)
   * column <  _value  |  value_id < dict.lower_bound(_value)
   * column <= _value  |  value_id < dict.upper_bound(_value)
   * column >  _value  |  value_id >= dict.upper_bound(_value)
   * column >= _value  |  value_id >= dict.lower_bound(_value)
   */

  const auto search_value_id = _get_search_value_id(segment);

  /**
   * Early Outs
   *
   * Operator         | All rows match if:                                      | No rows match if:
   * column == _value | search_vid_value == _value && unique_values_count == 1  | search_vid_value != _value
   * column != _value | search_vid_value != _value                              | search_vid_value == _value && unique_values_count == 1
   * column <  _value | search_vid == INVALID_VALUE_ID                          | search_vid == 0
   * column <= _value | search_vid == INVALID_VALUE_ID                          | search_vid == 0
   * column >  _value | search_vid == 0                                         | search_vid == INVALID_VALUE_ID
   * column >= _value | search_vid == 0                                         | search_vid == INVALID_VALUE_ID
   */

  auto iterable = create_iterable_from_attribute_vector(segment);

  if (_value_matches_all(segment, search_value_id)) {
    iterable.with_iterators(position_filter, [&](auto it, auto end) {
      static const auto always_true = [](const auto&) { return true; };
      // MatchesAll, so include all rows except those with NULLs in the result.
      _scan_with_iterators<true>(always_true, it, end, chunk_id, matches);
    });

    return;
  }

  if (_value_matches_none(segment, search_value_id)) {
    return;
  }

  _with_operator_for_dict_segment_scan(_predicate_condition, [&](auto predicate_comparator) {
    auto comparator = [predicate_comparator, search_value_id](const auto& iterator_value) {
      return predicate_comparator(iterator_value.value(), search_value_id);
    };
    iterable.with_iterators(position_filter, [&](auto it, auto end) {
      if (_predicate_condition == PredicateCondition::GreaterThan ||
          _predicate_condition == PredicateCondition::GreaterThanEquals) {
        // For GreaterThan(Equals), INVALID_VALUE_ID would compare greater than the search_value_id, even though the
        // value is NULL. Thus, we need to check for is_null as well.
        _scan_with_iterators<true>(comparator, it, end, chunk_id, matches);
      } else {
        // No need for NULL checks here, because INVALID_VALUE_ID is always greater.
        _scan_with_iterators<false>(comparator, it, end, chunk_id, matches);
      }
    });
  });
}

ValueID ColumnVsValueTableScanImpl::_get_search_value_id(const BaseDictionarySegment& segment) const {
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

bool ColumnVsValueTableScanImpl::_value_matches_all(const BaseDictionarySegment& segment,
                                                    const ValueID search_value_id) const {
  switch (_predicate_condition) {
    case PredicateCondition::Equals:
      return search_value_id != INVALID_VALUE_ID && segment.value_of_value_id(search_value_id) == _value &&
             segment.unique_values_count() == size_t{1u};

    case PredicateCondition::NotEquals:
      return search_value_id == INVALID_VALUE_ID || segment.value_of_value_id(search_value_id) != _value;

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

bool ColumnVsValueTableScanImpl::_value_matches_none(const BaseDictionarySegment& segment,
                                                     const ValueID search_value_id) const {
  switch (_predicate_condition) {
    case PredicateCondition::Equals:
      return search_value_id == INVALID_VALUE_ID || _value != segment.value_of_value_id(search_value_id);

    case PredicateCondition::NotEquals:
      return search_value_id != INVALID_VALUE_ID && _value == segment.value_of_value_id(search_value_id) &&
             segment.unique_values_count() == size_t{1u};

    case PredicateCondition::LessThan:
    case PredicateCondition::LessThanEquals:
      return search_value_id == ValueID{0u};

    case PredicateCondition::GreaterThan:
    case PredicateCondition::GreaterThanEquals:
      return search_value_id == INVALID_VALUE_ID;

    default:
      Fail("Unsupported comparison type encountered");
  }
}

}  // namespace opossum
