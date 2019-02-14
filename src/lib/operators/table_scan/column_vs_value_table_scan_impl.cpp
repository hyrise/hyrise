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

  resolve_data_and_segment_type(segment, [&](const auto type, const auto& typed_segment) {
    if (typed_segment.sort_order()) {
      _scan_sorted_segment(segment, chunk_id, matches, position_filter);
    } else {
      // Select optimized or generic scanning implementation based on segment type
      if (const auto* dictionary_segment = dynamic_cast<const BaseDictionarySegment*>(&segment)) {
        _scan_dictionary_segment(*dictionary_segment, chunk_id, matches, position_filter);
      } else {
        _scan_generic_segment(segment, chunk_id, matches, position_filter);
      }
    }
  });
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

void ColumnVsValueTableScanImpl::_scan_sorted_segment(const BaseSegment& segment, const ChunkID chunk_id,
                                                      PosList& matches,
                                                      const std::shared_ptr<const PosList>& position_filter) const {
  resolve_data_and_segment_type(segment, [&](const auto type, const auto& typed_segment) {
    if constexpr (std::is_same_v<decltype(typed_segment), const ReferenceSegment&>) {
      Fail("Expected ReferenceSegments to be handled before calling this method");
    } else {
      // TODO(johannes-schneider): extract because of code duplication
      // early outs for dictionary segments
      // TODO(cmfcmf): This breaks some of our tests.
      /*
      if (const auto* dictionary_segment = dynamic_cast<const BaseDictionarySegment*>(&segment)) {
        const auto search_value_id = _get_search_value_id(*dictionary_segment);
        auto iterable = create_iterable_from_attribute_vector(*dictionary_segment);
        if (_value_matches_all(*dictionary_segment, search_value_id)) {
          iterable.with_iterators(position_filter, [&](auto begin, auto end) {
            static const auto always_true = [](const auto&) { return true; };
            _scan_with_iterators<true>(always_true, begin, end, chunk_id, matches);
          });
          return;
        }
        if (_value_matches_none(*dictionary_segment, search_value_id)) {
          return;
        }
      }*/

      Assert(segment.sort_order().value() == OrderByMode::AscendingNullsLast ||
                 segment.sort_order().value() == OrderByMode::Ascending ||
                 segment.sort_order().value() == OrderByMode::DescendingNullsLast ||
                 segment.sort_order().value() == OrderByMode::Descending,
             "Unsupported sort type");

      // TODO(hendraet): Support Null values correctly
      auto segment_iterable = create_iterable_from_segment(typed_segment);
      segment_iterable.with_iterators(position_filter, [&](auto begin, auto end) {
        auto bounds = get_sorted_bounds(position_filter, begin, end, typed_segment);
        auto lower_it = std::get<0>(bounds);
        auto upper_it = std::get<1>(bounds);
        auto exclude_range = std::get<2>(bounds);

        // const auto non_null_begin = segment.get_non_null_begin_offset(position_filter);
        // const auto non_null_end = segment.get_non_null_end_offset(position_filter);

        //         std::cout << "Segment contains " << std::distance(begin, end) << " elements." << std::endl
        //                   << "Bounds contain " << std::distance(lower_it, upper_it) << " elements, start at "
        //                   << std::distance(begin, lower_it) << " and end at " << std::distance(begin, upper_it)
        //                   << std::endl
        //                   << "However, the first non-null value is at " << non_null_begin
        //                   << " and the last at " << non_null_end
        //                   << std::endl;

        size_t output_idx = matches.size();

        if (exclude_range) {
          const auto non_null_begin = segment.get_non_null_begin_offset(position_filter);
          const auto non_null_end = segment.get_non_null_end_offset(position_filter);

          const auto tmp = std::distance(upper_it, end) - (std::distance(begin, end) - non_null_end);

          boost::advance(begin, non_null_begin);

          matches.resize(matches.size() + non_null_end - non_null_begin - std::distance(lower_it, upper_it));

          // Insert all values from the first non null value up to lower_it
          for (; begin != lower_it; ++begin) {
            const auto& value = *begin;
            matches[output_idx++] = RowID(chunk_id, value.chunk_offset());
          }

          boost::advance(begin, std::distance(lower_it, upper_it));

          // Insert all values from upper_it to the first null value
          for (auto i = 0; i < tmp; ++begin, ++i) {
            const auto& value = *begin;
            matches[output_idx++] = RowID(chunk_id, value.chunk_offset());
          }
        } else {
          // Resizing the matches and overwriting each entry is about 5x faster than reserving the memory
          // and emplacing the entries.
          matches.resize(matches.size() + std::distance(lower_it, upper_it));

          if (position_filter) {
            // Slow path
            for (; lower_it != upper_it; ++lower_it) {
              matches[output_idx++] = RowID(chunk_id, lower_it->chunk_offset());
            }
          } else {
            // Fast path
            const auto first_offset = lower_it->chunk_offset();
            const auto dist = std::distance(lower_it, upper_it);

            for (auto i = 0; i < dist; ++i) {
              matches[output_idx++] = RowID(chunk_id, first_offset + i);
            }
          }
        }
      });
    }
  });
}

template <typename IteratorType, typename SegmentType>
std::tuple<IteratorType, IteratorType, bool> ColumnVsValueTableScanImpl::get_sorted_bounds(
    const std::shared_ptr<const PosList>& position_filter, IteratorType begin, IteratorType end,
    const SegmentType& segment) const {
  auto lower_it = begin;
  auto upper_it = begin;

  const auto is_ascending = segment.sort_order().value() == OrderByMode::Ascending ||
                            segment.sort_order().value() == OrderByMode::AscendingNullsLast;

  if ((_predicate_condition == PredicateCondition::GreaterThanEquals && is_ascending) ||
      (_predicate_condition == PredicateCondition::LessThanEquals && !is_ascending)) {
    const auto lower_bound = segment.get_first_offset(_value, position_filter);
    if (lower_bound == INVALID_CHUNK_OFFSET) {
      return std::make_tuple(lower_it, upper_it, false);
    }
    boost::advance(lower_it, lower_bound);
    boost::advance(upper_it, segment.get_non_null_end_offset(position_filter));
    return std::make_tuple(lower_it, upper_it, false);
  }

  if ((_predicate_condition == PredicateCondition::GreaterThan && is_ascending) ||
      (_predicate_condition == PredicateCondition::LessThan && !is_ascending)) {
    const auto lower_bound = segment.get_last_offset(_value, position_filter);
    if (lower_bound == INVALID_CHUNK_OFFSET) {
      return std::make_tuple(lower_it, upper_it, false);
    }
    boost::advance(lower_it, lower_bound);
    boost::advance(upper_it, segment.get_non_null_end_offset(position_filter));
    return std::make_tuple(lower_it, upper_it, false);
  }

  if ((_predicate_condition == PredicateCondition::LessThanEquals && is_ascending) ||
      (_predicate_condition == PredicateCondition::GreaterThanEquals && !is_ascending)) {
    const auto upper_bound = segment.get_last_offset(_value, position_filter);
    if (upper_bound != INVALID_CHUNK_OFFSET) {
      boost::advance(upper_it, upper_bound);
    } else {
      boost::advance(upper_it, std::distance(begin, end));
    }
    boost::advance(lower_it, segment.get_non_null_begin_offset(position_filter));
    return std::make_tuple(lower_it, upper_it, false);
  }

  if ((_predicate_condition == PredicateCondition::LessThan && is_ascending) ||
      (_predicate_condition == PredicateCondition::GreaterThan && !is_ascending)) {
    const auto upper_bound = segment.get_first_offset(_value, position_filter);
    if (upper_bound != INVALID_CHUNK_OFFSET) {
      boost::advance(upper_it, upper_bound);
    } else {
      boost::advance(upper_it, std::distance(begin, end));
    }
    boost::advance(lower_it, segment.get_non_null_begin_offset(position_filter));
    return std::make_tuple(lower_it, upper_it, false);
  }

  if (_predicate_condition == PredicateCondition::Equals || _predicate_condition == PredicateCondition::NotEquals) {
    const auto is_not_equals = _predicate_condition == PredicateCondition::NotEquals;
    const auto lower_bound = segment.get_first_offset(_value, position_filter);
    const auto upper_bound = segment.get_last_offset(_value, position_filter);
    if (lower_bound == INVALID_CHUNK_OFFSET) {
      return std::make_tuple(lower_it, upper_it, is_not_equals);
    } else {
      boost::advance(lower_it, lower_bound);
    }
    if (upper_bound == INVALID_CHUNK_OFFSET) {
      boost::advance(upper_it, segment.get_non_null_end_offset(position_filter));
    } else {
      boost::advance(upper_it, upper_bound);
    }
    return std::make_tuple(lower_it, upper_it, is_not_equals);
  }

  Fail("Unsupported comparison type encountered");
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
