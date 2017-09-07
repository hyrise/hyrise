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
  BaseTableScanImpl(std::shared_ptr<const Table> in_table, const ColumnID left_column_id, const ScanType scan_type)
      : _in_table{in_table}, _left_column_id{left_column_id}, _scan_type{scan_type} {}

  virtual ~BaseTableScanImpl() = default;

  virtual PosList scan_chunk(ChunkID chunk_id) = 0;

 protected:
  template <typename Functor>
  static void _with_operator(const ScanType scan_type, const Functor &func) {
    switch (scan_type) {
      case ScanType::OpEquals:
        func(std::equal_to<void>{});
        break;

      case ScanType::OpNotEquals:
        func(std::not_equal_to<void>{});
        break;

      case ScanType::OpLessThan:
        func(std::less<void>{});
        break;

      case ScanType::OpLessThanEquals:
        func(std::less_equal<void>{});
        break;

      case ScanType::OpGreaterThan:
        func(std::greater<void>{});
        break;

      case ScanType::OpGreaterThanEquals:
        func(std::greater_equal<void>{});
        break;

      case ScanType::OpBetween:
        Fail("This method should only be called when ScanType::OpBetween has been ruled out.");

      case ScanType::OpLike:
        Fail("This method should only be called when ScanType::OpLike has been ruled out.");

      default:
        Fail("Unsupported operator.");
    }
  }

  /**
   * @defgroup The hot loops of the table scan
   * @{
   */

  template <typename UnaryFunctor, typename LeftIterator>
  void _unary_scan(const UnaryFunctor &func, LeftIterator left_it, LeftIterator left_end, const ChunkID chunk_id,
                   PosList &matches_out) {
    for (; left_it != left_end; ++left_it) {
      const auto left = *left_it;

      if (left.is_null()) continue;

      if (func(left.value())) {
        matches_out.push_back(RowID{chunk_id, left.chunk_offset()});
      }
    }
  }

  template <typename BinaryFunctor, typename LeftIterator, typename RightIterator>
  void _binary_scan(const BinaryFunctor &func, LeftIterator left_it, LeftIterator left_end, RightIterator right_it,
                    const ChunkID chunk_id, PosList &matches_out) {
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
  const ScanType _scan_type;
};

}  // namespace opossum
