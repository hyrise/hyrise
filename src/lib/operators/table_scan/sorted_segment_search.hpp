#pragma once

#include <boost/range.hpp>
#include <boost/range/join.hpp>
#include <type_traits>

#include "all_type_variant.hpp"
#include "constant_mappings.hpp"
#include "types.hpp"

namespace opossum {

namespace detail {

// Generic class which handles the actual scanning of a sorted segment
template <typename IteratorType, typename SearchValueType>
class SortedSegmentSearch {
 public:
  SortedSegmentSearch(IteratorType begin, IteratorType end, const OrderByMode& order_by,
                      const PredicateCondition& predicate_condition, const SearchValueType& search_value)
      : _begin{begin},
        _end{end},
        _predicate_condition{predicate_condition},
        _search_value{search_value},
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
  IteratorType _get_first_bound() const {
    if (_is_ascending) {
      return std::lower_bound(_begin, _end, _search_value, [](const auto& segment_position, const auto& search_value) {
        return segment_position.value() < search_value;
      });
    } else {
      return std::lower_bound(_begin, _end, _search_value, [](const auto& segment_position, const auto& search_value) {
        return segment_position.value() > search_value;
      });
    }
  }

  IteratorType _get_last_bound() const {
    if (_is_ascending) {
      return std::upper_bound(_begin, _end, _search_value, [](const auto& search_value, const auto& segment_position) {
        return segment_position.value() > search_value;
      });
    } else {
      return std::upper_bound(_begin, _end, _search_value, [](const auto& search_value, const auto& segment_position) {
        return segment_position.value() < search_value;
      });
    }
  }

  // This function sets the offset(s) which delimit the result set based on the predicate condition and the sort order
  void _set_begin_and_end() {
    if (_predicate_condition == PredicateCondition::Equals) {
      _begin = _get_first_bound();
      _end = _get_last_bound();
      return;
    }

    // clang-format off
    if (_is_ascending) {
      switch (_predicate_condition) {
        case PredicateCondition::GreaterThanEquals: _begin = _get_first_bound(); return;
        case PredicateCondition::GreaterThan: _begin = _get_last_bound(); return;
        case PredicateCondition::LessThanEquals: _end = _get_last_bound(); return;
        case PredicateCondition::LessThan: _end = _get_first_bound(); return;
        default: Fail("Unsupported comparison type encountered");
      }
    } else {
      switch (_predicate_condition) {
        case PredicateCondition::LessThanEquals: _begin = _get_first_bound(); return;
        case PredicateCondition::LessThan: _begin = _get_last_bound(); return;
        case PredicateCondition::GreaterThanEquals: _end = _get_last_bound(); return;
        case PredicateCondition::GreaterThan: _end = _get_first_bound(); return;
        default: Fail("Unsupported comparison type encountered");
      }
    }
    // clang-format on
  }

  // NotEquals might result in two matching ranges (one below and one above the search_value) and needs special handling
  template <typename Functor>
  void _handle_not_equals(const Functor& functor) {
    const auto end_first_range = _get_first_bound();
    if (end_first_range == _end) {
      functor(_begin, _end);
      return;
    }

    const auto begin_second_range = _get_last_bound();
    if (begin_second_range == _end) {
      functor(_begin, end_first_range);
      return;
    }

    const auto range = boost::join(boost::make_iterator_range(_begin, end_first_range),
                                   boost::make_iterator_range(begin_second_range, _end));
    functor(range.begin(), range.end());
  }

 public:
  template <typename Functor>
  void scan_sorted_segment(const Functor& functor) {
    // decrease the effective sort range by excluding null values based on their ordering
    if (_is_nulls_first) {
      _begin = std::lower_bound(_begin, _end, false,
                                [](const auto& segment_position, const auto& _) { return segment_position.is_null(); });
    } else {
      _end = std::lower_bound(_begin, _end, true,
                              [](const auto& segment_position, const auto& _) { return !segment_position.is_null(); });
    }

    if (_predicate_condition == PredicateCondition::NotEquals) {
      _handle_not_equals(functor);
    } else {
      _set_begin_and_end();
      functor(_begin, _end);
    }
  }

 private:
  IteratorType _begin;
  IteratorType _end;
  const PredicateCondition _predicate_condition;
  const SearchValueType _search_value;
  const bool _is_ascending;
  const bool _is_nulls_first;
};

}  // namespace detail

template <typename IteratorType, typename SearchValueType, typename Functor>
void scan_sorted_segment(IteratorType begin, IteratorType end, const OrderByMode& order_by,
                         const PredicateCondition& predicate, const SearchValueType& search_value,
                         const Functor& functor) {
  auto sorted_segment_search = detail::SortedSegmentSearch(begin, end, order_by, predicate, search_value);
  sorted_segment_search.scan_sorted_segment(functor);
}

}  // namespace opossum
