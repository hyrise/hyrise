#pragma once

#include <chrono>

#include <type_traits>
#include <thread>
#include <functional>

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


  void _exponential_search_for_nulls(IteratorType it_first, IteratorType it_last) {


    if (it_first == it_last) return;
    // If no null values are present
    if (_is_nulls_first && !it_first->is_null()) {
      return;
    }
    if (!_is_nulls_first && !(it_last - 1)->is_null()){
      return;
    }

    std::cout << "no early out for null search" << std::endl;

    using difference_type = typename std::iterator_traits<IteratorType>::difference_type;
    difference_type size = std::distance(it_first, it_last), bound = 1;

    if (_is_nulls_first) {
      while (bound < size && (it_first + bound)->is_null()) {
        bound *= 2;
      }

      auto end = it_first + std::min(bound, size);
      _begin = std::lower_bound(it_first + (bound / 2), end, false,
                              [](const auto& segment_position, const auto& _) { return segment_position.is_null(); });
    } else {
      while (bound < size && (it_first + (size - bound))->is_null()) {
        bound *= 2;
      }

      auto start = it_first + (size - std::min(bound, size));
      _end = std::lower_bound(start, it_first + (size - bound / 2), true,
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

  IteratorType _find_middle_element() {
    DebugAssert(_left_value <= _right_value, "_left_value must be less than or equal to _right_value");
    // find an element with value in range [_left_value, _right_value] as a starting point for the searches
    // also update _begin and _end accordingly
    typename std::iterator_traits<IteratorType>::difference_type count, step;
    count = std::distance(_begin, _end);

    while (count > 0) {
      auto it = _begin;
      step = count / 2;
      std::advance(it, step);
      auto value = it->value();
      if (_is_ascending) {
        // if ascending, check if value is smaller than left_value
        if (_left_value > value) {
          // there are no larger values left of the current iterator position
          // continue search from the next position
          _begin = ++it;
          count -= step + 1;
          // otherwise, check if value is larger than right_value
        } else if (value > _right_value) {
          // there are no smaller values right of the current iterator position
          // continue search up to this point
          _end = it;
          count = step;
        }
        else  // iterator value is in range [_left_value, _right_value]
          return it;
      } else {  // descending
        if (_left_value > value) {
          // there are no larger values right of the current iterator position
          // continue search up to this point
          _end = it;
          count = step;
        } else if (value > _right_value) {
          // there are no smaller values left of the current iterator position
          // continue search from the next position
          _begin = ++it;
          count -= step + 1;
        }
        else // iterator value is in range [_left_value, _right_value]
          return it;
      }
    }
    return _begin;
  }

  // This function sets the offset(s) which delimit the result set based on the predicate condition and the sort order
  void _set_begin_and_end() {
    const IteratorType& middle = _find_middle_element();
    std::cout << "set begin and end" << std::endl;

    std::cout << "left value " << _left_value << std::endl;
    std::cout << "right value " << _right_value << std::endl;
    std::cout << "begin " << _begin->value() << std::endl;
    std::cout << "end " << (_end - 1)->value() << std::endl;

    if (_begin == _end) return;
    
    if (_is_ascending) {
      std::cout << "ascending" << std::endl;
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
      std::cout << "descending" << std::endl;
      if (_begin->value() < _right_value && (_end - 1)->value() > _left_value) {
        std::cout << "all match" << std::endl;
        return;
      }
      std::cout << "not all match" << std::endl;
      if ((_end - 1)->value() > _right_value || _begin->value() < _left_value) {
        _begin = _end; 
        std::cout << "no match" << std::endl;
        return;
      }
    }

    if (_is_ascending) {
      switch (_predicate_condition) {
        case PredicateCondition::BetweenInclusive:
          {
            std::thread get_first_bound_thread ([this, &middle](){
              _begin = _get_first_bound(_left_value, _begin, middle + 1);
            });
            _end = _get_last_bound(_right_value, middle, _end);
            get_first_bound_thread.join();
          }
          return;
        case PredicateCondition::BetweenLowerExclusive:  // upper inclusive
          {
            std::thread get_lower_bound_thread ([this, &middle](){
              _begin = _get_last_bound(_left_value, _begin, middle + 1);
            });
            _end = _get_last_bound(_right_value, middle, _end);
            get_lower_bound_thread.join();
          }
          return;
        case PredicateCondition::BetweenUpperExclusive:
          {
            std::thread get_lower_bound_thread ([this, &middle](){
              _begin = _get_first_bound(_left_value, _begin, middle + 1);
            });
            _end = _get_first_bound(_right_value, middle, _end);
            get_lower_bound_thread.join();
          }
          return;
        case PredicateCondition::BetweenExclusive:
          {
            std::thread get_lower_bound_thread ([this, &middle](){
              _begin = _get_last_bound(_left_value, _begin, middle + 1);
            });
            _end = _get_first_bound(_right_value, middle, _end);
            get_lower_bound_thread.join();
          }
          return;
        default:
          Fail("Unsupported predicate condition encountered");
      }
    } else {
      switch (_predicate_condition) {
        case PredicateCondition::BetweenInclusive:
          {
            std::thread get_lower_bound_thread ([this, &middle](){
              _begin = _get_first_bound(_right_value, _begin, middle + 1);
            });
            _end = _get_last_bound(_left_value, middle, _end);
            get_lower_bound_thread.join();
          }
          return;
        case PredicateCondition::BetweenLowerExclusive:  // upper inclusive
          {
            std::thread get_lower_bound_thread ([this, &middle](){
              _begin = _get_first_bound(_right_value, _begin, middle + 1);
            });
            _end = _get_first_bound(_left_value, middle, _end);
            get_lower_bound_thread.join();
          }
          return;
        case PredicateCondition::BetweenUpperExclusive:
          {
            std::thread get_lower_bound_thread ([this, &middle](){
              _begin = _get_last_bound(_right_value, _begin, middle + 1);
            });
            _end = _get_last_bound(_left_value, middle, _end);
            get_lower_bound_thread.join();
          }
          return;
        case PredicateCondition::BetweenExclusive:
          {
            std::thread get_lower_bound_thread ([this, &middle](){
              _begin = _get_last_bound(_right_value, _begin, middle + 1);
            });
            _end = _get_first_bound(_left_value, middle, _end);
            get_lower_bound_thread.join();
          }
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
    //auto start = std::chrono::system_clock::now();
    if (_nullable)
      _exponential_search_for_nulls(_begin, _end);
    //auto end = std::chrono::system_clock::now();
    //std::cout << "exponential search: " << std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count() << "ns" << std::endl;
    //start = std::chrono::system_clock::now();
    _set_begin_and_end();
    //end = std::chrono::system_clock::now();
    //std::cout << "star begin: " << std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count() << "ns" << std::endl;
    //start = std::chrono::system_clock::now();
    result_consumer(_begin, _end);
    //end = std::chrono::system_clock::now();
    //std::cout << "result consumer: " << std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count() << "ns" << std::endl;
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
