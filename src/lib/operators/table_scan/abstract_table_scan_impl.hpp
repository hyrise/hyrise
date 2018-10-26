#pragma once

#include <array>

#include "storage/pos_list.hpp"
#include "types.hpp"

namespace opossum {

/**
 * @brief the base class of all table scan impls
 */
class AbstractTableScanImpl {
 public:
  virtual ~AbstractTableScanImpl() = default;

  virtual std::string description() const = 0;

  virtual std::shared_ptr<PosList> scan_chunk(ChunkID chunk_id) const = 0;

 protected:
  /**
   * @defgroup The hot loop of the table scan
   * @{
   */

  template <bool CheckForNull, typename BinaryFunctor, typename LeftIterator>
  void __attribute__((noinline))
  _scan_with_iterators(const BinaryFunctor func, LeftIterator left_it, const LeftIterator left_end,
                       const ChunkID chunk_id, PosList& matches_out) const {
    // Can't use a default argument for this because default arguments are non-type deduced contexts
    auto false_type = std::false_type{};
    _scan_with_iterators<CheckForNull>(func, left_it, left_end, chunk_id, matches_out, false_type);
  }

  template <bool CheckForNull, typename BinaryFunctor, typename LeftIterator, typename RightIterator>
  // noinline reduces compile time drastically
  void __attribute__((noinline))
  _scan_with_iterators(const BinaryFunctor func, LeftIterator left_it, const LeftIterator left_end,
                       const ChunkID chunk_id, PosList& matches_out, [[maybe_unused]] RightIterator right_it) const {
    for (; left_it != left_end; ++left_it) {
      if constexpr (std::is_same_v<RightIterator, std::false_type>) {
        const auto left = *left_it;

        if ((!CheckForNull || !left.is_null()) && func(left)) {
          matches_out.emplace_back(RowID{chunk_id, left.chunk_offset()});
        }
      } else {
        const auto left = *left_it;
        const auto right = *right_it;
        if ((!CheckForNull || (!left.is_null() && !right.is_null())) && func(left, right)) {
          matches_out.emplace_back(RowID{chunk_id, left.chunk_offset()});
        }
        ++right_it;
      }
    }
  }

  /**@}*/
};

}  // namespace opossum
