#include "column_vs_value_table_scan_impl.hpp"

#include <memory>
#include <utility>
#include <vector>

#include "sorted_segment_search.hpp"
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
                                                       const PredicateCondition& init_predicate_condition,
                                                       const AllTypeVariant& value)
    : AbstractDereferencedColumnTableScanImpl{in_table, column_id, init_predicate_condition}, value{value} {
  Assert(in_table->column_data_type(column_id) == data_type_from_all_type_variant(value),
         "Cannot use ColumnVsValueTableScanImpl for scan where column and value data type do not match. Use "
         "ExpressionEvaluatorTableScanImpl.");
}

std::string ColumnVsValueTableScanImpl::description() const { return "ColumnVsValue"; }

void ColumnVsValueTableScanImpl::_scan_non_reference_segment(
    const BaseSegment& segment, const ChunkID chunk_id, PosList& matches,
    const std::shared_ptr<const PosList>& position_filter) const {
  const auto ordered_by = _in_table->get_chunk(chunk_id)->ordered_by();
  if (ordered_by && ordered_by->first == _column_id) {
    _scan_sorted_segment(segment, chunk_id, matches, position_filter, ordered_by->second);
  } else {
    // Select optimized or generic scanning implementation based on segment type
    if (const auto* dictionary_segment = dynamic_cast<const BaseDictionarySegment*>(&segment)) {
      _scan_dictionary_segment(*dictionary_segment, chunk_id, matches, position_filter);
    } else {
      _scan_generic_segment(segment, chunk_id, matches, position_filter);
    }
  }
}

void ColumnVsValueTableScanImpl::_scan_generic_segment(const BaseSegment& segment, const ChunkID chunk_id,
                                                       PosList& matches,
                                                       const std::shared_ptr<const PosList>& position_filter) const {
  segment_with_iterators_filtered(segment, position_filter, [&](auto it, [[maybe_unused]] const auto end) {
    // Don't instantiate this for this for DictionarySegments and ReferenceSegments to save compile time.
    // DictionarySegments are handled in _scan_dictionary_segment()
    // ReferenceSegments are handled via position_filter
    if constexpr (!is_dictionary_segment_iterable_v<typename decltype(it)::IterableType> &&
                  !is_reference_segment_iterable_v<typename decltype(it)::IterableType>) {
      using ColumnDataType = typename decltype(it)::ValueType;

      const auto typed_value = boost::get<ColumnDataType>(value);

      with_comparator(predicate_condition, [&](auto predicate_comparator) {
        auto comparator = [predicate_comparator, typed_value](const auto& position) {
          return predicate_comparator(position.value(), typed_value);
        };
        _scan_with_iterators<true>(comparator, it, end, chunk_id, matches);
      });
    } else {
      Fail("Dictionary- and ReferenceSegments have their own code paths and should be handled there");
    }
  });
}

void ColumnVsValueTableScanImpl::_scan_dictionary_segment(const BaseDictionarySegment& segment, const ChunkID chunk_id,
                                                          PosList& matches,
                                                          const std::shared_ptr<const PosList>& position_filter) const {
  /**
   * ValueID search_vid;              // left value id
   * AllTypeVariant search_vid_value; // dict.value_by_value_id(search_vid)
   * Variant value;                  // right value
   *
   * A ValueID value_id from the attribute vector is included in the result iff
   *
   * Operator         |  Condition
   * column == value  |  dict.value_by_value_id(dict.lower_bound(value)) == value && value_id == dict.lower_bound(value)
   * column != value  |  dict.value_by_value_id(dict.lower_bound(value)) != value || value_id != dict.lower_bound(value)
   * column <  value  |  value_id < dict.lower_bound(value)
   * column <= value  |  value_id < dict.upper_bound(value)
   * column >  value  |  value_id >= dict.upper_bound(value)
   * column >= value  |  value_id >= dict.lower_bound(value)
   */

  const auto search_value_id = _get_search_value_id(segment);

  /**
   * Early Outs
   *
   * Operator        | All rows match if:                                     | No rows match if:
   * column == value | search_vid_value == value && unique_values_count == 1  | search_vid_value != value
   * column != value | search_vid_value != value                              | search_vid_value == value && unique_values_count == 1
   * column <  value | search_vid == INVALID_VALUE_ID                         | search_vid == 0
   * column <= value | search_vid == INVALID_VALUE_ID                         | search_vid == 0
   * column >  value | search_vid == 0                                        | search_vid == INVALID_VALUE_ID
   * column >= value | search_vid == 0                                        | search_vid == INVALID_VALUE_ID
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

  _with_operator_for_dict_segment_scan([&](auto predicate_comparator) {
    auto comparator = [predicate_comparator, search_value_id](const auto& position) {
      return predicate_comparator(position.value(), search_value_id);
    };
    iterable.with_iterators(position_filter, [&](auto it, auto end) {
      // dictionary.size() represents a NULL in the AttributeVector. For some PredicateConditions, we can
      // avoid explicitly checking for it, since the condition (e.g., LessThan) would never return true for
      // dictionary.size() anyway.
      if (predicate_condition == PredicateCondition::Equals ||
          predicate_condition == PredicateCondition::LessThanEquals ||
          predicate_condition == PredicateCondition::LessThan) {
        _scan_with_iterators<false>(comparator, it, end, chunk_id, matches);
      } else {
        _scan_with_iterators<true>(comparator, it, end, chunk_id, matches);
      }
    });
  });
}

void ColumnVsValueTableScanImpl::_scan_sorted_segment(const BaseSegment& segment, const ChunkID chunk_id,
                                                      PosList& matches,
                                                      const std::shared_ptr<const PosList>& position_filter,
                                                      const OrderByMode order_by_mode) const {
  resolve_data_and_segment_type(segment, [&](const auto type, const auto& typed_segment) {
    using ColumnDataType = typename decltype(type)::type;

    if constexpr (std::is_same_v<std::decay_t<decltype(typed_segment)>, ReferenceSegment>) {
      Fail("Expected ReferenceSegments to be handled before calling this method");
    } else {
      auto segment_iterable = create_iterable_from_segment(typed_segment);
      segment_iterable.with_iterators(position_filter, [&](auto segment_begin, auto segment_end) {
        auto sorted_segment_search = SortedSegmentSearch(segment_begin, segment_end, order_by_mode, predicate_condition,
                                                         boost::get<ColumnDataType>(value));

        sorted_segment_search.scan_sorted_segment([&](auto begin, auto end) {
          if (begin == end) return;

          // General note: If the predicate is NotEquals, there might be two matching ranges. scan_sorted_segment
          // combines these two ranges into a single one via boost::join(range_1, range_2).
          // See sorted_segment_search.hpp for further details.

          size_t output_idx = matches.size();

          matches.resize(matches.size() + std::distance(begin, end));

          /**
           * If the range of matches consists of continuous ChunkOffsets we can speed up the writing
           * by calculating the offsets based on the first offset instead of calling chunk_offset()
           * for every match.
           * ChunkOffsets in position_filter are not necessarily continuous. The same is true for
           * NotEquals because the result might consist of 2 ranges.
           */
          if (position_filter || predicate_condition == PredicateCondition::NotEquals) {
            for (; begin != end; ++begin) {
              matches[output_idx++] = RowID{chunk_id, begin->chunk_offset()};
            }
          } else {
            const auto first_offset = begin->chunk_offset();
            const auto distance = std::distance(begin, end);

            for (auto chunk_offset = 0; chunk_offset < distance; ++chunk_offset) {
              matches[output_idx++] = RowID{chunk_id, first_offset + chunk_offset};
            }
          }
        });
      });
    }
  });
}

ValueID ColumnVsValueTableScanImpl::_get_search_value_id(const BaseDictionarySegment& segment) const {
  switch (predicate_condition) {
    case PredicateCondition::Equals:
    case PredicateCondition::NotEquals:
    case PredicateCondition::LessThan:
    case PredicateCondition::GreaterThanEquals:
      return segment.lower_bound(value);

    case PredicateCondition::LessThanEquals:
    case PredicateCondition::GreaterThan:
      return segment.upper_bound(value);

    default:
      Fail("Unsupported comparison type encountered");
  }
}

bool ColumnVsValueTableScanImpl::_value_matches_all(const BaseDictionarySegment& segment,
                                                    const ValueID search_value_id) const {
  switch (predicate_condition) {
    case PredicateCondition::Equals:
      return search_value_id != INVALID_VALUE_ID && segment.value_of_value_id(search_value_id) == value &&
             segment.unique_values_count() == size_t{1u};

    case PredicateCondition::NotEquals:
      return search_value_id == INVALID_VALUE_ID || segment.value_of_value_id(search_value_id) != value;

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
  switch (predicate_condition) {
    case PredicateCondition::Equals:
      return search_value_id == INVALID_VALUE_ID || value != segment.value_of_value_id(search_value_id);

    case PredicateCondition::NotEquals:
      return search_value_id != INVALID_VALUE_ID && value == segment.value_of_value_id(search_value_id) &&
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
