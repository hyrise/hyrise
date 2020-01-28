#pragma once

#include <functional>
#include <thread>
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
  SortedSegmentBetweenSearch(IteratorType begin, IteratorType end, const OrderByMode& order_by, const bool nullable,
                             const PredicateCondition& predicate_condition, const SearchValueType& left_value,
                             const SearchValueType& right_value)
      : _begin{begin},
        _end{end},
        _predicate_condition{predicate_condition},
        _left_value{left_value},
        _right_value{right_value},
        _nullable{nullable},
        _is_ascending{order_by == OrderByMode::Ascending || order_by == OrderByMode::AscendingNullsLast},
        _is_nulls_first{order_by == OrderByMode::Ascending || order_by == OrderByMode::Descending} {}

 private:
  /**
     * Uses exponential search to find lower_bound of null values to exclude them from further scanning.
     * This version of exponential search first reduces the range where the search for NULL values will be performed
     * and performs a binary search for the exact bound within this range afterwards.

     * Since the null values are either in the beginning or the end of each segment (depending on
     * how they are ordered) and there are typically very few null values, the amount of steps
     * used in exponential_search are typically less than the amount of steps taken using only
     * binary search.
     */
  void _exponential_search_for_nulls(IteratorType it_first, IteratorType it_last) {
    if (it_first == it_last) return;
    // early return if no null values are present
    if (_is_nulls_first && !it_first->is_null()) {
      return;
    }
    if (!_is_nulls_first && !(it_last - 1)->is_null()) {
      return;
    }

    using difference_type = typename std::iterator_traits<IteratorType>::difference_type;
    const difference_type segment_size = std::distance(it_first, it_last);
    difference_type step_size = 1;

    if (_is_nulls_first) {
      while (step_size < segment_size && (it_first + step_size)->is_null()) {
        step_size *= 2;
      }

      auto end = it_first + std::min(step_size, segment_size);
      _begin = std::lower_bound(it_first + (step_size / 2), end, false,
                                [](const auto& segment_position, const auto& _) { return segment_position.is_null(); });
    } else {
      while (step_size < segment_size && (it_first + (segment_size - step_size))->is_null()) {
        step_size *= 2;
      }

      auto start = it_first + (segment_size - std::min(step_size, segment_size));
      _end = std::lower_bound(start, it_first + (segment_size - step_size / 2), true,
                              [](const auto& segment_position, const auto& _) { return !segment_position.is_null(); });
    }
  }

  /**
   * _get_first_bound and _get_last_bound are used to retrieve the lower and upper bound in a sorted segment but are
   * independent of its sort order. _get_first_bound will always return the bound with the smaller offset and
   * _get_last_bound will return the bigger offset.
   * On a segment sorted in ascending order they would work analogously to lower_bound and upper_bound. For descending
   * sort order _get_first_bound will actually return an upper bound and _get_last_bound the lower one. However, the
   * first offset will always point to an entry matching the search value, whereas last offset points to the entry
   * behind the last matching one.
   */
  IteratorType _get_first_bound(const SearchValueType& search_value, const IteratorType begin, const IteratorType end) {
    if (_is_ascending) {
      return std::lower_bound(begin, end, search_value, [](const auto& segment_position, const auto& value) {
        return segment_position.value() < value;
      });
    } else {
      return std::lower_bound(begin, end, search_value, [](const auto& segment_position, const auto& value) {
        return segment_position.value() > value;
      });
    }
  }

  IteratorType _get_last_bound(const SearchValueType& search_value, const IteratorType begin, const IteratorType end) {
    if (_is_ascending) {
      return std::upper_bound(begin, end, search_value, [](const auto& value, const auto& segment_position) {
        return segment_position.value() > value;
      });
    } else {
      return std::upper_bound(begin, end, search_value, [](const auto& value, const auto& segment_position) {
        return segment_position.value() < value;
      });
    }
  }

  // This function sets the offset(s) which delimit the result set based on the predicate condition and the sort order
  void _set_begin_and_end() {
    if (_begin == _end) return;

    if (_is_ascending) {
      // early out everything matches
      if (_begin->value() > _left_value && (_end - 1)->value() < _right_value) {
        return;
      }

      // early out nothing matches
      if (_begin->value() > _right_value || (_end - 1)->value() < _left_value) {
        _begin = _end;
        return;
      }
    } else {
      // early out everything matches
      if (_begin->value() < _right_value && (_end - 1)->value() > _left_value) {
        return;
      }
      // early out nothing matches
      if ((_end - 1)->value() > _right_value || _begin->value() < _left_value) {
        _begin = _end;
        return;
      }
    }

    if (_is_ascending) {
      switch (_predicate_condition) {
        case PredicateCondition::BetweenInclusive:
          _begin = _get_first_bound(_left_value, _begin, _end);
          _end = _get_last_bound(_right_value, _begin, _end);
          return;
        case PredicateCondition::BetweenLowerExclusive:  // upper inclusive
          _begin = _get_last_bound(_left_value, _begin, _end);
          _end = _get_last_bound(_right_value, _begin, _end);
          return;
        case PredicateCondition::BetweenUpperExclusive:
          _begin = _get_first_bound(_left_value, _begin, _end);
          _end = _get_first_bound(_right_value, _begin, _end);
          return;
        case PredicateCondition::BetweenExclusive:
          _begin = _get_last_bound(_left_value, _begin, _end);
          _end = _get_first_bound(_right_value, _begin, _end);
          return;
        default:
          Fail("Unsupported predicate condition encountered");
      }
    } else {
      switch (_predicate_condition) {
        case PredicateCondition::BetweenInclusive:
          _begin = _get_first_bound(_right_value, _begin, _end);
          _end = _get_last_bound(_left_value, _begin, _end);
          return;
        case PredicateCondition::BetweenLowerExclusive:  // upper inclusive
          _begin = _get_first_bound(_right_value, _begin, _end);
          _end = _get_first_bound(_left_value, _begin, _end);
          return;
        case PredicateCondition::BetweenUpperExclusive:
          _begin = _get_last_bound(_right_value, _begin, _end);
          _end = _get_last_bound(_left_value, _begin, _end);
          return;
        case PredicateCondition::BetweenExclusive:
          _begin = _get_last_bound(_right_value, _begin, _end);
          _end = _get_first_bound(_left_value, _begin, _end);
          return;
        default:
          Fail("Unsupported predicate condition encountered");
      }
    }
  }

 public:
  template <typename ResultConsumer>
  void scan_sorted_segment(const ResultConsumer& result_consumer) {
    // decrease the effective sort range by excluding null values based on their ordering (first or last)
    if (_nullable) {
      _exponential_search_for_nulls(_begin, _end);
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
  const bool _nullable;
  const bool _is_ascending;
  const bool _is_nulls_first;
};

}  // namespace opossum
