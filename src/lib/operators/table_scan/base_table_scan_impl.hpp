#pragma once

#include <functional>
#include <memory>

#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

class Table;

/**
 * @brief the base class of all table scan impls
 */
class BaseTableScanImpl {
 public:
  BaseTableScanImpl(std::shared_ptr<const Table> in_table, const ColumnID left_column_id,
                    const PredicateCondition predicate_condition)
      : _in_table{in_table}, _left_column_id{left_column_id}, _predicate_condition{predicate_condition} {}

  virtual ~BaseTableScanImpl() = default;

  virtual PosList scan_chunk(ChunkID chunk_id) = 0;

 protected:
  /**
   * @defgroup The hot loops of the table scan
   * @{
   */

  template <typename UnaryFunctor, typename LeftIterator>
  void _unary_scan(const UnaryFunctor& func, LeftIterator left_it, LeftIterator left_end, const ChunkID chunk_id,
                   PosList& matches_out) {
    for (; left_it != left_end; ++left_it) {
      const auto left = *left_it;

      if (left.is_null()) continue;

      if (func(left.value())) {
        matches_out.push_back(RowID{chunk_id, left.chunk_offset()});
      }
    }
  }

  template <typename BinaryFunctor, typename LeftIterator, typename RightIterator>
  void _binary_scan(const BinaryFunctor& func, LeftIterator left_it, LeftIterator left_end, RightIterator right_it,
                    const ChunkID chunk_id, PosList& matches_out) {
    for (; left_it != left_end; ++left_it, ++right_it) {
      const auto left = *left_it;
      const auto right = *right_it;

      if (left.is_null() || right.is_null()) continue;

      if (func(left.value(), right.value())) {
        matches_out.push_back(RowID{chunk_id, left.chunk_offset()});
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
