#pragma once

#include <boost/range.hpp>
#include <boost/range/join.hpp>
#include <type_traits>

#include "all_type_variant.hpp"
#include "constant_mappings.hpp"
#include "types.hpp"

namespace opossum {

namespace {

template <typename IteratorType, typename SearchValueType>
class SortedSegmentSearch {
 public:
  SortedSegmentSearch(IteratorType begin, IteratorType end, const OrderByMode& order_by,
                      const PredicateCondition& predicate, const SearchValueType& search_value)
      : _begin{begin},
        _end{end},
        _order_by{order_by},
        _predicate{predicate},
        _search_value{search_value},
        _is_ascending{order_by == OrderByMode::Ascending || order_by == OrderByMode::AscendingNullsLast},
        _is_nulls_first{order_by == OrderByMode::Ascending || order_by == OrderByMode::Descending} {}

 private:
  void _set_begin_to_non_null_begin() {
    if (!_is_nulls_first) {
      return;
    }
    _begin = std::lower_bound(_begin, _end, false,
                              [](const auto& segment_position, const auto& _) { return segment_position.is_null(); });
  }

  void _set_end_to_non_null_end() {
    if (_is_nulls_first) {
      return;
    }
    _end = std::lower_bound(_begin, _end, true,
                            [](const auto& segment_position, const auto& _) { return !segment_position.is_null(); });
  }

  ChunkOffset _get_first_offset() const {
    if (_is_ascending) {
      const auto result =
          std::lower_bound(_begin, _end, _search_value, [](const auto& segment_position, const auto& search_value) {
            return segment_position.value() < search_value;
          });
      if (result == _end) {
        return INVALID_CHUNK_OFFSET;
      }
      return static_cast<ChunkOffset>(std::distance(_begin, result));
    }

    const auto result = std::lower_bound(
        _begin, _end, _search_value,
        [](const auto& segment_position, const auto& search_value) { return segment_position.value() > search_value; });
    if (result == _end) {
      return INVALID_CHUNK_OFFSET;
    }
    return static_cast<ChunkOffset>(std::distance(_begin, result));
  }

  ChunkOffset _get_last_offset() const {
    if (_is_ascending) {
      const auto result =
          std::upper_bound(_begin, _end, _search_value, [](const auto& search_value, const auto& segment_position) {
            return segment_position.value() > search_value;
          });
      if (result == _end) {
        return INVALID_CHUNK_OFFSET;
      }
      return static_cast<ChunkOffset>(std::distance(_begin, result));
    }

    const auto result = std::upper_bound(
        _begin, _end, _search_value,
        [](const auto& search_value, const auto& segment_position) { return segment_position.value() < search_value; });
    if (result == _end) {
      return INVALID_CHUNK_OFFSET;
    }
    return static_cast<ChunkOffset>(std::distance(_begin, result));
  }

  void _set_begin_and_end() {
    if ((_predicate == PredicateCondition::GreaterThanEquals && _is_ascending) ||
        (_predicate == PredicateCondition::LessThanEquals && !_is_ascending)) {
      const auto lower_bound = _get_first_offset();
      if (lower_bound == INVALID_CHUNK_OFFSET) {
        _begin = _end;
        return;
      }
      _begin += lower_bound;
      return;
    }

    if ((_predicate == PredicateCondition::GreaterThan && _is_ascending) ||
        (_predicate == PredicateCondition::LessThan && !_is_ascending)) {
      const auto lower_bound = _get_last_offset();
      if (lower_bound == INVALID_CHUNK_OFFSET) {
        _begin = _end;
        return;
      }
      _begin += lower_bound;
      return;
    }

    if ((_predicate == PredicateCondition::LessThanEquals && _is_ascending) ||
        (_predicate == PredicateCondition::GreaterThanEquals && !_is_ascending)) {
      const auto upper_bound = _get_last_offset();
      if (upper_bound != INVALID_CHUNK_OFFSET) {
        _end -= (std::distance(_begin, _end) - upper_bound);
      }
      return;
    }

    if ((_predicate == PredicateCondition::LessThan && _is_ascending) ||
        (_predicate == PredicateCondition::GreaterThan && !_is_ascending)) {
      const auto upper_bound = _get_first_offset();
      if (upper_bound != INVALID_CHUNK_OFFSET) {
        _end -= (std::distance(_begin, _end) - upper_bound);
      }
      return;
    }

    if (_predicate == PredicateCondition::Equals) {
      const auto lower_bound = _get_first_offset();
      if (lower_bound == INVALID_CHUNK_OFFSET) {
        _begin = _end;
        return;
      }

      _begin += lower_bound;

      const auto upper_bound = _get_last_offset();
      if (upper_bound != INVALID_CHUNK_OFFSET) {
        _end -= (std::distance(_begin, _end) - upper_bound);
      }
      return;
    }

    Fail("Unsupported comparison type encountered");
  }

  template <typename Functor>
  void _handle_not_equals(const Functor& functor) {
    const auto lower_bound = _get_first_offset();
    auto end1 = _begin;
    auto begin2 = _begin;
    if (lower_bound == INVALID_CHUNK_OFFSET) {
      functor(_begin, _begin);
      return;
    }

    end1 += lower_bound;

    const auto upper_bound = _get_last_offset();
    if (upper_bound == INVALID_CHUNK_OFFSET) {
      functor(_begin, end1);
      return;
    }

    begin2 += upper_bound;

    const auto range = boost::join(boost::make_iterator_range(_begin, end1), boost::make_iterator_range(begin2, _end));
    functor(range.begin(), range.end());
  }

 public:
  template <typename Functor>
  void scan_sorted_segment(const Functor& functor) {
    _set_begin_to_non_null_begin();
    _set_end_to_non_null_end();

    if (_predicate == PredicateCondition::NotEquals) {
      _handle_not_equals(functor);
      return;
    }

    _set_begin_and_end();
    functor(_begin, _end);
  }

 private:
  IteratorType _begin;
  IteratorType _end;
  const OrderByMode _order_by;
  const PredicateCondition _predicate;
  const SearchValueType _search_value;

  const bool _is_ascending;
  const bool _is_nulls_first;
};

}  // namespace

template <typename IteratorType, typename SearchValueType, typename Functor>
void scan_sorted_segment(IteratorType begin, IteratorType end, const OrderByMode& order_by,
                         const PredicateCondition& predicate, const SearchValueType& search_value,
                         const Functor& functor) {
  auto sorted_segment_search = SortedSegmentSearch(begin, end, order_by, predicate, search_value);
  sorted_segment_search.scan_sorted_segment(functor);
}

}  // namespace opossum
