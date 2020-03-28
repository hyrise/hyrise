#pragma once

#include <functional>
#include <optional>
#include <type_traits>

#include <boost/range.hpp>
#include <boost/range/join.hpp>

#include "all_type_variant.hpp"
#include "constant_mappings.hpp"
#include "storage/pos_lists/rowid_pos_list.hpp"
#include "types.hpp"

namespace opossum {

// Generic class which handles the actual scanning of a sorted segment
template <typename IteratorType, typename SearchValueType>
class SortedSegmentSearch {
 public:
  SortedSegmentSearch(IteratorType begin, IteratorType end, const OrderByMode& order_by, const bool nullable,
                      const PredicateCondition& predicate_condition, const SearchValueType& search_value)
      : _begin{begin},
        _end{end},
        _predicate_condition{predicate_condition},
        _first_search_value{search_value},
        _second_search_value{std::nullopt},
        _nullable{nullable},
        _is_ascending{order_by == OrderByMode::Ascending || order_by == OrderByMode::AscendingNullsLast},
        _is_nulls_first{order_by == OrderByMode::Ascending || order_by == OrderByMode::Descending} {}

  // For SortedSegmentBetweenSearch
  SortedSegmentSearch(IteratorType begin, IteratorType end, const OrderByMode& order_by, const bool nullable,
                      const PredicateCondition& predicate_condition, const SearchValueType& left_value,
                      const SearchValueType& right_value)
      : _begin{begin},
        _end{end},
        _predicate_condition{predicate_condition},
        _first_search_value{left_value},
        _second_search_value{right_value},
        _nullable{nullable},
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
  IteratorType _get_first_bound(const SearchValueType& search_value, const IteratorType begin,
                                const IteratorType end) const {
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

  IteratorType _get_last_bound(const SearchValueType& search_value, const IteratorType begin,
                               const IteratorType end) const {
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
  void _set_begin_and_end_positions_for_vs_value_scan() {
    if (_predicate_condition == PredicateCondition::Equals) {
      _begin = _get_first_bound(_first_search_value, _begin, _end);
      _end = _get_last_bound(_first_search_value, _begin, _end);
      return;
    }

    // clang-format off
    if (_is_ascending) {
      switch (_predicate_condition) {
        case PredicateCondition::GreaterThanEquals:
          _begin = _get_first_bound(_first_search_value, _begin, _end);
          return;
        case PredicateCondition::GreaterThan:
          _begin = _get_last_bound(_first_search_value, _begin, _end);
          return;
        case PredicateCondition::LessThanEquals:
          _end = _get_last_bound(_first_search_value, _begin, _end);
          return;
        case PredicateCondition::LessThan:
          _end = _get_first_bound(_first_search_value, _begin, _end);
          return;
        default: Fail("Unsupported predicate condition encountered");
      }
    } else {
      switch (_predicate_condition) {
        case PredicateCondition::LessThanEquals:
          _begin = _get_first_bound(_first_search_value, _begin, _end);
          return;
        case PredicateCondition::LessThan:
          _begin = _get_last_bound(_first_search_value, _begin, _end);
          return;
        case PredicateCondition::GreaterThanEquals:
          _end = _get_last_bound(_first_search_value, _begin, _end);
          return;
        case PredicateCondition::GreaterThan:
          _end = _get_first_bound(_first_search_value, _begin, _end);
          return;
        default: Fail("Unsupported predicate condition encountered");
      }
    }
    // clang-format on
  }

  // This function sets the offset(s) that delimit the result set based on the predicate condition and the sort order
  void _set_begin_and_end_positions_for_between_scan() {
    DebugAssert(_second_search_value, "Second Search Value must be set for between scan");
    if (_begin == _end) return;

    auto first_value = _begin->value();
    auto last_value = (_end - 1)->value();

    auto predicate_condition = _predicate_condition;
    auto first_search_value = _first_search_value;
    auto second_search_value = *_second_search_value;

    // if descending: exchange predicate conditions, search values and first/last values
    if (!_is_ascending) {
      switch (_predicate_condition) {
        case PredicateCondition::BetweenLowerExclusive:
          predicate_condition = PredicateCondition::BetweenUpperExclusive;
          break;
        case PredicateCondition::BetweenUpperExclusive:
          predicate_condition = PredicateCondition::BetweenLowerExclusive;
          break;
        case PredicateCondition::BetweenInclusive:
        case PredicateCondition::BetweenExclusive:
          break;
        default:
          Fail("Unsupported predicate condition encountered");
      }

      std::swap(first_value, last_value);
      std::swap(first_search_value, second_search_value);
    }

    // early out everything matches
    if (first_value > _first_search_value && last_value < *_second_search_value) return;

    // early out nothing matches
    if (first_value > *_second_search_value || last_value < _first_search_value) {
      _begin = _end;
      return;
    }

    // This implementation uses behaviour which resembles std::equal_range's
    // behaviour since it, too, calculates two different bounds.
    // However, equal_range is designed to compare to a single search value,
    // whereas in this case, the upper and lower search value (if given) will differ.
    switch (predicate_condition) {
      case PredicateCondition::BetweenInclusive:
        _begin = _get_first_bound(first_search_value, _begin, _end);
        _end = _get_last_bound(second_search_value, _begin, _end);
        return;
      case PredicateCondition::BetweenLowerExclusive:  // upper inclusive
        _begin = _get_last_bound(first_search_value, _begin, _end);
        _end = _get_last_bound(second_search_value, _begin, _end);
        return;
      case PredicateCondition::BetweenUpperExclusive:
        _begin = _get_first_bound(first_search_value, _begin, _end);
        _end = _get_first_bound(second_search_value, _begin, _end);
        return;
      case PredicateCondition::BetweenExclusive:
        _begin = _get_last_bound(first_search_value, _begin, _end);
        _end = _get_first_bound(second_search_value, _begin, _end);
        return;
      default:
        Fail("Unsupported predicate condition encountered");
    }
  }

  /*
   * NotEquals may result in two matching ranges (one below and one above the search_value) and needs special handling.
   * The function contains four early outs. These are all only for performance reasons and, if removed, would not
   * change the functionality.
   *
   * Note: All comments within this method are written from the point of ranges in ascending order.
   */
  template <typename ResultConsumer>
  void _handle_not_equals(const ResultConsumer& result_consumer) {
    const auto first_bound = _get_first_bound(_first_search_value, _begin, _end);
    if (first_bound == _end) {
      // Neither the _search_value nor anything greater than it are found. Call the result_consumer on the whole range
      // and skip the call to _get_last_bound().
      result_consumer(_begin, _end);
      return;
    }

    if (first_bound->value() != _first_search_value) {
      // If the first value >= _search_value is not equal to _search_value, then _search_value doesn't occur at all.
      // Call the result_consumer on the whole range and skip the call to _get_last_bound().
      result_consumer(_begin, _end);
      return;
    }

    // At this point, first_bound points to the first occurrence of _search_value.

    const auto last_bound = _get_last_bound(_first_search_value, _begin, _end);
    if (last_bound == _end) {
      // If no value > _search_value is found, call the result_consumer from start to first occurrence and skip the
      // need for boost::join().
      result_consumer(_begin, first_bound);
      return;
    }

    if (first_bound == _begin) {
      // If _search_value is right at the start, call the result_consumer from the first value > _search_value
      // to end and skip the need for boost::join().
      result_consumer(last_bound, _end);
      return;
    }

    const auto range = boost::range::join(boost::make_iterator_range(_begin, first_bound),
                                          boost::make_iterator_range(last_bound, _end));
    result_consumer(range.begin(), range.end());
  }

 public:
  template <typename ResultConsumer>
  void scan_sorted_segment(const ResultConsumer& result_consumer) {
    if (_second_search_value) {
      // decrease the effective sort range by excluding null values based on their ordering (first or last)
      if (_nullable) {
        if (_is_nulls_first) {
          _begin = std::lower_bound(_begin, _end, false, [](const auto& segment_position, const auto& _) {
            return segment_position.is_null();
          });
        } else {
          _end = std::lower_bound(_begin, _end, true, [](const auto& segment_position, const auto& _) {
            return !segment_position.is_null();
          });
        }
      }
      _set_begin_and_end_positions_for_between_scan();
      result_consumer(_begin, _end);
    } else {
      // decrease the effective sort range by excluding null values based on their ordering
      if (_is_nulls_first) {
        _begin = std::lower_bound(_begin, _end, false, [](const auto& segment_position, const auto& _) {
          return segment_position.is_null();
        });
      } else {
        _end = std::lower_bound(_begin, _end, true, [](const auto& segment_position, const auto& _) {
          return !segment_position.is_null();
        });
      }

      if (_predicate_condition == PredicateCondition::NotEquals) {
        _handle_not_equals(result_consumer);
      } else {
        _set_begin_and_end_positions_for_vs_value_scan();
        result_consumer(_begin, _end);
      }
    }
  }

  template <typename ResultIteratorType>
  void _write_rows_to_matches(ResultIteratorType begin, ResultIteratorType end, const ChunkID chunk_id,
                              RowIDPosList& matches,
                              const std::shared_ptr<const AbstractPosList>& position_filter) const {
    if (begin == end) return;

    // General note: If the predicate is NotEquals, there might be two ranges that match.
    // These two ranges might have been combined into a single one via boost::join(range_1, range_2).
    // See _handle_not_equals for further details.

    size_t output_idx = matches.size();

    matches.resize(matches.size() + std::distance(begin, end));

    /**
     * If the range of matches consists of continuous ChunkOffsets we can speed up the writing
     * by calculating the offsets based on the first offset instead of calling chunk_offset()
     * for every match.
     * ChunkOffsets in position_filter are not necessarily continuous. The same is true for
     * NotEquals because the result might consist of 2 ranges.
     */
    if (position_filter || _predicate_condition == PredicateCondition::NotEquals) {
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
  }

 private:
  // _begin and _end will be modified to match the search range and will be passed to the ResultConsumer, except when
  // handling NotEquals (see _handle_not_equals).
  IteratorType _begin;
  IteratorType _end;
  const PredicateCondition _predicate_condition;
  const SearchValueType _first_search_value;
  const std::optional<SearchValueType> _second_search_value;
  const bool _nullable;
  const bool _is_ascending;
  const bool _is_nulls_first;
};

}  // namespace opossum
