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
                      const PredicateCondition& predicateCondition, const SearchValueType& search_value)
      : _original_begin{begin},
        _original_end{end},
        _begin_offset{0},
        _end_offset{ChunkOffset(std::distance(begin, end))},
        _predicate_condition{predicateCondition},
        _search_value{search_value},
        _is_ascending{order_by == OrderByMode::Ascending || order_by == OrderByMode::AscendingNullsLast},
        _is_nulls_first{order_by == OrderByMode::Ascending || order_by == OrderByMode::Descending} {}

 private:
  // TODO(cmfcmf): This is only needed until #1512 is fixed and we can use std::lower_bound.
  template <typename Comparator, typename T>
  ChunkOffset _lower_offset(IteratorType begin, IteratorType end, T value, Comparator comparator) const {
    auto len = std::distance(begin, end);

    auto begin_offset = 0;

    while (len > 0) {
      auto half = len / 2;
      auto middle = begin + begin_offset;
      std::advance(middle, half);
      if (comparator(*middle, value)) {
        begin_offset += half;
        ++begin_offset;
        len = len - half - 1;
      } else {
        len = half;
      }
    }
    return static_cast<ChunkOffset>(begin_offset);
  }

  // TODO(cmfcmf): This is only needed until #1512 is fixed and we can use std::upper_bound.
  template <typename Comparator, typename T>
  ChunkOffset _upper_offset(IteratorType begin, IteratorType end, T value, Comparator comparator) const {
    auto len = std::distance(begin, end);

    auto begin_offset = 0;

    while (len > 0) {
      auto half = len / 2;
      auto middle = begin + begin_offset;
      std::advance(middle, half);
      if (comparator(value, *middle)) {
        len = half;
      } else {
        begin_offset += half;
        ++begin_offset;
        len = len - half - 1;
      }
    }
    return static_cast<ChunkOffset>(begin_offset);
  }

  IteratorType _begin() const { return _original_begin + _begin_offset; }

  IteratorType _end() const { return _original_begin + _end_offset; }

  /**
   * _get_first_offset and _get_last_offset are used to retrieve the lower and upper bound in a sorted segment but are
   * independent of its sort order. _get_first_offset will always return the bound with the smaller offset and
   * _get_last_offset will return the bigger offset.
   * On a segment sorted in ascending order they would work analogously to lower_bound and upper_bound. For descending
   * sort order _get_first_offset will actually return an upper bound and _get_last_offset the lower one. However, the
   * first offset will always point to an entry matching the search value, whereas last offset points to the entry
   * behind the last matching one.
   */
  ChunkOffset _get_first_offset() const {
    if (_is_ascending) {
      const auto result =
          _lower_offset(_begin(), _end(), _search_value, [](const auto& segment_position, const auto& search_value) {
            return segment_position.value() < search_value;
          });
      if (result == _end_offset) {
        return INVALID_CHUNK_OFFSET;
      }
      return result;
    } else {
      const auto result =
          _lower_offset(_begin(), _end(), _search_value, [](const auto& segment_position, const auto& search_value) {
            return segment_position.value() > search_value;
          });
      if (result == _end_offset) {
        return INVALID_CHUNK_OFFSET;
      }
      return result;
    }
  }

  ChunkOffset _get_last_offset() const {
    if (_is_ascending) {
      const auto result =
          _upper_offset(_begin(), _end(), _search_value, [](const auto& search_value, const auto& segment_position) {
            return segment_position.value() > search_value;
          });
      if (result == _end_offset) {
        return INVALID_CHUNK_OFFSET;
      }
      return result;
    } else {
      const auto result =
          _upper_offset(_begin(), _end(), _search_value, [](const auto& search_value, const auto& segment_position) {
            return segment_position.value() < search_value;
          });
      if (result == _end_offset) {
        return INVALID_CHUNK_OFFSET;
      }
      return result;
    }
  }

  // This function sets the offset(s) which delimit the result set based on the predicate condition and the sort order
  void _set_begin_and_end() {
    if ((_predicate_condition == PredicateCondition::GreaterThanEquals && _is_ascending) ||
        (_predicate_condition == PredicateCondition::LessThanEquals && !_is_ascending)) {
      const auto lower_bound = _get_first_offset();
      if (lower_bound == INVALID_CHUNK_OFFSET) {
        _begin_offset = _end_offset;
        return;
      }
      _begin_offset += lower_bound;
      return;
    }

    if ((_predicate_condition == PredicateCondition::GreaterThan && _is_ascending) ||
        (_predicate_condition == PredicateCondition::LessThan && !_is_ascending)) {
      const auto lower_bound = _get_last_offset();
      if (lower_bound == INVALID_CHUNK_OFFSET) {
        _begin_offset = _end_offset;
        return;
      }
      _begin_offset += lower_bound;
      return;
    }

    if ((_predicate_condition == PredicateCondition::LessThanEquals && _is_ascending) ||
        (_predicate_condition == PredicateCondition::GreaterThanEquals && !_is_ascending)) {
      const auto upper_bound = _get_last_offset();
      if (upper_bound != INVALID_CHUNK_OFFSET) {
        _end_offset -= (_end_offset - _begin_offset - upper_bound);
      }
      return;
    }

    if ((_predicate_condition == PredicateCondition::LessThan && _is_ascending) ||
        (_predicate_condition == PredicateCondition::GreaterThan && !_is_ascending)) {
      const auto upper_bound = _get_first_offset();
      if (upper_bound != INVALID_CHUNK_OFFSET) {
        _end_offset -= (_end_offset - _begin_offset - upper_bound);
      }
      return;
    }

    if (_predicate_condition == PredicateCondition::Equals) {
      const auto lower_bound = _get_first_offset();
      if (lower_bound == INVALID_CHUNK_OFFSET) {
        _begin_offset = _end_offset;
        return;
      }

      _begin_offset += lower_bound;

      const auto upper_bound = _get_last_offset();
      if (upper_bound != INVALID_CHUNK_OFFSET) {
        _end_offset -= (_end_offset - _begin_offset - upper_bound);
      }
      return;
    }

    Fail("Unsupported comparison type encountered");
  }

  // NotEquals might result in two matching ranges (one below and one above the search_value) and needs special handling
  template <typename Functor>
  void _handle_not_equals(const Functor& functor) {
    const auto lower_bound = _get_first_offset();
    auto end_first_range = _begin_offset;
    auto begin_second_range = _begin_offset;
    if (lower_bound == INVALID_CHUNK_OFFSET) {
      functor(_original_begin, _original_begin);
      return;
    }

    end_first_range += lower_bound;

    const auto upper_bound = _get_last_offset();
    if (upper_bound == INVALID_CHUNK_OFFSET) {
      functor(_begin(), _original_begin + end_first_range);
      return;
    }

    begin_second_range += upper_bound;

    // TODO(cmfcmf): Once #1512 is fixed, we can use the approach below which only invokes the functor once.
    functor(_begin(), _original_begin + end_first_range);
    functor(_original_begin + begin_second_range, _end());

    // const auto range = boost::join(boost::make_iterator_range(_begin(), _original_begin + end_first_range),
    //                                boost::make_iterator_range(_original_begin + begin_second_range, _end()));
    // functor(range.begin(), range.end());
  }

 public:
  template <typename Functor>
  void scan_sorted_segment(const Functor& functor) {
    // decrease the effective sort range by excluding null values based on their ordering
    if (_is_nulls_first) {
      _begin_offset = _lower_offset(_begin(), _end(), false, [](const auto& segment_position, const auto& _) {
        return segment_position.is_null();
      });
    } else {
      _end_offset = _lower_offset(_begin(), _end(), true, [](const auto& segment_position, const auto& _) {
        return !segment_position.is_null();
      });
    }

    if (_predicate_condition == PredicateCondition::NotEquals) {
      _handle_not_equals(functor);
    } else {
      _set_begin_and_end();
      functor(_begin(), _end());
    }
  }

 private:
  const IteratorType _original_begin;
  const IteratorType _original_end;
  ChunkOffset _begin_offset;
  ChunkOffset _end_offset;
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
