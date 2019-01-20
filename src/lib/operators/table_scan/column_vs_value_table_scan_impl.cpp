#include "column_vs_value_table_scan_impl.hpp"

#include <memory>
#include <utility>
#include <vector>

#include "storage/base_dictionary_segment.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "storage/resolve_encoded_segment_type.hpp"
#include "storage/segment_iterables/create_iterable_from_attribute_vector.hpp"
#include "storage/segment_iterate.hpp"

#include "resolve_type.hpp"
#include "type_comparison.hpp"

namespace opossum {

ColumnVsValueTableScanImpl::ColumnVsValueTableScanImpl(const std::shared_ptr<const Table>& in_table,
                                                       const ColumnID column_id,
                                                       const PredicateCondition& predicate_condition,
                                                       const AllTypeVariant& value)
    : AbstractSingleColumnTableScanImpl{in_table, column_id, predicate_condition}, _value{value} {}

std::string ColumnVsValueTableScanImpl::description() const { return "ColumnVsValue"; }

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

  // Select optimized or generic scanning implementation based on segment type
  if (const auto* dictionary_segment = dynamic_cast<const BaseDictionarySegment*>(&segment)) {
    _scan_dictionary_segment(*dictionary_segment, chunk_id, matches, position_filter);
  } else {
    _scan_generic_segment(segment, chunk_id, matches, position_filter);
  }
}

void ColumnVsValueTableScanImpl::_scan_generic_segment(const BaseSegment& segment, const ChunkID chunk_id,
                                                       PosList& matches,
                                                       const std::shared_ptr<const PosList>& position_filter) const {
  segment_with_iterators_filtered(segment, position_filter, [&](auto it, const auto end) {
    using ColumnDataType = typename decltype(it)::ValueType;
    auto typed_value = type_cast_variant<ColumnDataType>(_value);

    with_comparator(_predicate_condition, [&](auto predicate_comparator) {
      auto comparator = [predicate_comparator, typed_value](const auto& position) {
        return predicate_comparator(position.value(), typed_value);
      };
      _scan_with_iterators<true>(comparator, it, end, chunk_id, matches);
    });
  });
}

void ColumnVsValueTableScanImpl::_scan_dictionary_segment(const BaseDictionarySegment& segment, const ChunkID chunk_id,
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
      // Matches all, so include all rows except those with NULLs in the result.
      _scan_with_iterators<true>(always_true, it, end, chunk_id, matches);
    });

    return;
  }

  if (_value_matches_none(segment, search_value_id)) {
    return;
  }

  _with_operator_for_dict_segment_scan(_predicate_condition, [&](auto predicate_comparator) {
    auto comparator = [predicate_comparator, search_value_id](const auto& position) {
      return predicate_comparator(position.value(), search_value_id);
    };
    iterable.with_iterators(position_filter, [&](auto it, auto end) {
      // dictionary.size() represents a NULL in the AttributeVector. For some PredicateConditions, we can
      // avoid explicitly checking for it, since the condition (e.g., LessThan) would never return true for
      // dictionary.size() anyway.
      if (_predicate_condition == PredicateCondition::Equals ||
          _predicate_condition == PredicateCondition::LessThanEquals ||
          _predicate_condition == PredicateCondition::LessThan) {
        _scan_with_iterators<false>(comparator, it, end, chunk_id, matches);
      } else {
        _scan_with_iterators<true>(comparator, it, end, chunk_id, matches);
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
