#pragma once

#include <functional>
#include <memory>

#include "abstract_table_scan_impl.hpp"
#include "storage/segment_iterables.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

class Table;

/**
 * Base class for table scans operating on either one or two columns of an input table
 */
class BaseTableScanImpl : public AbstractTableScanImpl {
 public:
  BaseTableScanImpl(std::shared_ptr<const Table> in_table, const ColumnID left_column_id,
                    const PredicateCondition predicate_condition)
      : _in_table{in_table}, _left_column_id{left_column_id}, _predicate_condition{predicate_condition} {}

 protected:
  /**
   * @defgroup The hot loops of the table scan
   * @{
   */

  template <typename UnaryFunctor, typename LeftIterator>
  // noinline reduces compile time drastically
  void __attribute__((noinline)) _unary_scan(const UnaryFunctor& func, LeftIterator left_it, LeftIterator left_end,
                                             const ChunkID chunk_id, PosList& matches_out) {
    for (; left_it != left_end; ++left_it) {
      const auto left = *left_it;

      if (left.is_null()) continue;

      if (func(left.value())) {
        matches_out.push_back(RowID{chunk_id, left.chunk_offset()});
      }
    }
  }

  // Version with a constant value on the right side. Sometimes we prefer this over _unary_scan because we can use
  // with_comparator.
  // noinline reduces compile time drastically
  template <typename BinaryFunctor, typename LeftIterator, typename RightValue>
  void __attribute__((noinline))
  _unary_scan_with_value(const BinaryFunctor& func, LeftIterator left_it, LeftIterator left_end, RightValue right_value,
                         const ChunkID chunk_id, PosList& matches_out) {
    for (; left_it != left_end; ++left_it) {
      const auto left = *left_it;

      if (left.is_null()) continue;

      if (func(left.value(), right_value)) {
        matches_out.push_back(RowID{chunk_id, left.chunk_offset()});
      }
    }
  }

  // noinline reduces compile time drastically
  template <typename BinaryFunctor, typename LeftIterator, typename RightIterator>
  void __attribute__((noinline)) _binary_scan(const BinaryFunctor& func, LeftIterator left_it, LeftIterator left_end,
                                              RightIterator right_it, const ChunkID chunk_id, PosList& matches_out) {
    for (; left_it != left_end; ++left_it, ++right_it) {
      const auto left = *left_it;
      const auto right = *right_it;

      if (left.is_null() || right.is_null()) continue;

      if (func(left.value(), right.value())) {
        matches_out.push_back(RowID{chunk_id, left.chunk_offset()});
      }
    }
  }

  // If we search for an actual value, BETWEEN has the semantics of including both sides: a BETWEEN 4 and 6 is true for
  // any a IN (4,5,6). In that case, we set `IncludeRightBoundary = true`.
  // If we search for value ids, the right side is an upper bound and needs to be excluded.
  template <bool IncludeRightBoundary, typename ColumnIterator, typename Value>
  void __attribute__((noinline))
  _between_scan_with_value(ColumnIterator column_it, ColumnIterator column_end, Value left_value, Value right_value,
                           const ChunkID chunk_id, PosList& matches_out) {
    for (; column_it != column_end; ++column_it) {
      const auto& column_value = *column_it;

      if (column_value.is_null()) continue;

      if constexpr (IncludeRightBoundary) {
        if (column_value.value() >= left_value && column_value.value() <= right_value) {
          matches_out.push_back(RowID{chunk_id, column_value.chunk_offset()});
        }
      } else {
        if (column_value.value() >= left_value && column_value.value() < right_value) {
          matches_out.push_back(RowID{chunk_id, column_value.chunk_offset()});
        }
      }
    }
  }

  /**@}*/

 protected:
  const std::shared_ptr<const Table> _in_table;
  const ColumnID _left_column_id;
  const PredicateCondition _predicate_condition;
};

}  // namespace opossum
