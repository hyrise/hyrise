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
      : _original_begin{begin},
        _original_end{end},
        _begin_offset{0},
        _end_offset{ChunkOffset(std::distance(begin, end))},
        _order_by{order_by},
        _predicate{predicate},
        _search_value{search_value},
        _is_ascending{order_by == OrderByMode::Ascending || order_by == OrderByMode::AscendingNullsLast},
        _is_nulls_first{order_by == OrderByMode::Ascending || order_by == OrderByMode::Descending} {}

 private:
  // TODO(cmfcmf): This is only needed until #1512 is fixed and we can use std::lower_bound.
  template<typename Comparator, typename T>
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
  template<typename Comparator, typename T>
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

  IteratorType _begin() const {
    return _original_begin + _begin_offset;
  }

  IteratorType _end() const {
    return _original_begin + _end_offset;
  }

  void _set_begin_to_non_null_begin() {
    if (!_is_nulls_first) {
      return;
    }
    _begin_offset = _lower_offset(_begin(), _end(), false,
                              [](const auto& segment_position, const auto& _) { return segment_position.is_null(); });
  }

  void _set_end_to_non_null_end() {
    if (_is_nulls_first) {
      return;
    }
    _end_offset = _lower_offset(_begin(), _end(), true,
                            [](const auto& segment_position, const auto& _) { return !segment_position.is_null(); });
  }

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
    }

    const auto result = _lower_offset(
        _begin(), _end(), _search_value,
        [](const auto& segment_position, const auto& search_value) { return segment_position.value() > search_value; });
    if (result == _end_offset) {
      return INVALID_CHUNK_OFFSET;
    }
    return result;
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
    }

    const auto result = _upper_offset(
        _begin(), _end(), _search_value,
        [](const auto& search_value, const auto& segment_position) { return segment_position.value() < search_value; });
    if (result == _end_offset) {
      return INVALID_CHUNK_OFFSET;
    }
    return result;
  }

  void _set_begin_and_end() {
    if ((_predicate == PredicateCondition::GreaterThanEquals && _is_ascending) ||
        (_predicate == PredicateCondition::LessThanEquals && !_is_ascending)) {
      const auto lower_bound = _get_first_offset();
      if (lower_bound == INVALID_CHUNK_OFFSET) {
        _begin_offset = _end_offset;
        return;
      }
      _begin_offset += lower_bound;
      return;
    }

    if ((_predicate == PredicateCondition::GreaterThan && _is_ascending) ||
        (_predicate == PredicateCondition::LessThan && !_is_ascending)) {
      const auto lower_bound = _get_last_offset();
      if (lower_bound == INVALID_CHUNK_OFFSET) {
        _begin_offset = _end_offset;
        return;
      }
      _begin_offset += lower_bound;
      return;
    }

    if ((_predicate == PredicateCondition::LessThanEquals && _is_ascending) ||
        (_predicate == PredicateCondition::GreaterThanEquals && !_is_ascending)) {
      const auto upper_bound = _get_last_offset();
      if (upper_bound != INVALID_CHUNK_OFFSET) {
        _end_offset -= (_end_offset - _begin_offset - upper_bound);
      }
      return;
    }

    if ((_predicate == PredicateCondition::LessThan && _is_ascending) ||
        (_predicate == PredicateCondition::GreaterThan && !_is_ascending)) {
      const auto upper_bound = _get_first_offset();
      if (upper_bound != INVALID_CHUNK_OFFSET) {
        _end_offset -= (_end_offset - _begin_offset - upper_bound);
      }
      return;
    }

    if (_predicate == PredicateCondition::Equals) {
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

  template <typename Functor>
  void _handle_not_equals(const Functor& functor) {
    const auto lower_bound = _get_first_offset();
    auto end1 = _begin_offset;
    auto begin2 = _begin_offset;
    if (lower_bound == INVALID_CHUNK_OFFSET) {
      functor(_original_begin, _original_begin);
      return;
    }

    end1 += lower_bound;

    const auto upper_bound = _get_last_offset();
    if (upper_bound == INVALID_CHUNK_OFFSET) {
      functor(_begin(), _original_begin + end1);
      return;
    }

    begin2 += upper_bound;


    // TODO(cmfcmf): Once #1512 is fixed, we can use the approach below which only invokes the functor once.
    functor(_begin(), _original_begin + end1);
    functor(_original_begin + begin2, _end());

    // const auto range = boost::join(boost::make_iterator_range(_begin(), _original_begin + end1),
    //                                boost::make_iterator_range(_original_begin + begin2, _end()));
    // functor(range.begin(), range.end());
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
    functor(_begin(), _end());
  }

 private:
  const IteratorType _original_begin;
  const IteratorType _original_end;
  ChunkOffset _begin_offset;
  ChunkOffset _end_offset;
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
