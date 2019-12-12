#pragma once

#include <type_traits>

#include <boost/range.hpp>
#include <boost/range/join.hpp>

#include "all_type_variant.hpp"
#include "constant_mappings.hpp"
#include "types.hpp"

namespace opossum {

// Generic class which handles the actual scanning of a sorted segment
template <typename IteratorType, typename SearchValueType>
class SortedSegmentBetweenSearch {
 public:
  SortedSegmentBetweenSearch(IteratorType begin, IteratorType end, const OrderByMode& order_by,
                             const PredicateCondition& predicate_condition, const SearchValueType& left_value,
                             const SearchValueType& right_value)
      : _begin{begin},
        _end{end},
        _predicate_condition{predicate_condition},
        _left_value{left_value},
        _right_value{right_value},
        _is_ascending{order_by == OrderByMode::Ascending || order_by == OrderByMode::AscendingNullsLast},
        _is_nulls_first{order_by == OrderByMode::Ascending || order_by == OrderByMode::Descending} {}

 private:
  /**
   * _get_first_bound and _get_last_bound are used to retrieve the lower and upper bound in a sorted segment but are
   * independent of its sort order. _get_first_bound will always return the bound with the smaller offset and
   * _get_last_bound will return the bigger offset.
   * On a segment sorted in ascending order they would work analogously to lower_bound and upper_bound. For descending
   * sort order _get_first_bound will actually return an upper bound and _get_last_bound the lower one. However, the
   * first offset will always point to an entry matching the search value, whereas last offset points to the entry
   * behind the last matching one.
   */
  IteratorType _get_first_bound(const SearchValueType& search_value) const {
    if (_is_ascending) {
      return std::lower_bound(_begin, _end, search_value, [](const auto& segment_position, const auto& value) {
        return segment_position.value() < value;
      });
    } else {
      return std::lower_bound(_begin, _end, search_value, [](const auto& segment_position, const auto& value) {
        return segment_position.value() > value;
      });
    }
  }

  IteratorType _get_last_bound(const SearchValueType& search_value) const {
    if (_is_ascending) {
      return std::upper_bound(_begin, _end, search_value, [](const auto& value, const auto& segment_position) {
        return segment_position.value() > value;
      });
    } else {
      return std::upper_bound(_begin, _end, search_value, [](const auto& value, const auto& segment_position) {
        return segment_position.value() < value;
      });
    }
  }

  // This function sets the offset(s) which delimit the result set based on the predicate condition and the sort order
  void _set_begin_and_end() {
    if (_is_ascending) {
      switch (_predicate_condition) {
        case PredicateCondition::BetweenInclusive:
          _begin = _get_first_bound(_left_value);
          _end = _get_last_bound(_right_value);
          return;
        case PredicateCondition::BetweenLowerExclusive:  // upper inclusive
          _begin = _get_last_bound(_left_value);
          _end = _get_last_bound(_right_value);
          return;
        case PredicateCondition::BetweenUpperExclusive:
          _begin = _get_first_bound(_left_value);
          _end = _get_first_bound(_right_value);
          return;
        case PredicateCondition::BetweenExclusive:
          _begin = _get_last_bound(_left_value);
          _end = _get_first_bound(_right_value);
          return;
        default:
          Fail("Unsupported predicate condition encountered");
      }
    } else {
      switch (_predicate_condition) {
        case PredicateCondition::BetweenInclusive:
          _begin = _get_first_bound(_right_value);
          _end = _get_last_bound(_left_value);
          return;
        case PredicateCondition::BetweenLowerExclusive:  // upper inclusive
          _begin = _get_first_bound(_right_value);
          _end = _get_first_bound(_left_value);
          return;
        case PredicateCondition::BetweenUpperExclusive:
          _begin = _get_last_bound(_right_value);
          _end = _get_last_bound(_left_value);
          return;
        case PredicateCondition::BetweenExclusive:
          _begin = _get_last_bound(_right_value);
          _end = _get_first_bound(_left_value);
          return;
        default:
          Fail("Unsupported predicate condition encountered");
      }
    }
  }

 public:
  template <typename ResultConsumer>
  void scan_sorted_segment(const ResultConsumer& result_consumer) {
    // decrease the effective sort range by excluding null values based on their ordering
    if (_is_nulls_first) {
      _begin = std::lower_bound(_begin, _end, false,
                                [](const auto& segment_position, const auto& _) { return segment_position.is_null(); });
    } else {
      _end = std::lower_bound(_begin, _end, true,
                              [](const auto& segment_position, const auto& _) { return !segment_position.is_null(); });
    }
    _set_begin_and_end();
    result_consumer(_begin, _end);
  }

 private:
  // _begin and _end will be modified to match the search range and will be passed to the ResultConsumer
  IteratorType _begin;
  IteratorType _end;
  const PredicateCondition _predicate_condition;
  const SearchValueType _left_value;
  const SearchValueType _right_value;
  const bool _is_ascending;
  const bool _is_nulls_first;
};

}  // namespace opossum
