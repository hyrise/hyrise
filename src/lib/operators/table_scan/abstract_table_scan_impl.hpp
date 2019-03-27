#pragma once

#ifdef __AVX512VL__
#include <x86intrin.h>
#endif

#include <array>

#include "storage/pos_list.hpp"
#include "storage/segment_iterables.hpp"
#include "storage/segment_iterables/any_segment_iterator.hpp"
#include "types.hpp"
#include "utils/performance_warning.hpp"

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
  static void _scan_with_iterators(const BinaryFunctor func, LeftIterator left_it, const LeftIterator left_end,
                                   const ChunkID chunk_id, PosList& matches_out) {
    // Can't use a default argument for this because default arguments are non-type deduced contexts
    auto false_type = std::false_type{};
    _scan_with_iterators<CheckForNull>(func, left_it, left_end, chunk_id, matches_out, false_type);
  }

  template <bool CheckForNull, typename BinaryFunctor, typename LeftIterator, typename RightIterator>
  // This is a function that is critical for our performance. We want the compiler to try its best in optimizing it.
  // Also, we want all functions called inside to be inlined (flattened) and the function itself being always aligned
  // at a page boundary. Finally, it should not be inlined because that might break the alignment.
  static void __attribute__((hot, flatten, aligned(256), noinline))
  _scan_with_iterators(const BinaryFunctor func, LeftIterator left_it, const LeftIterator left_end,
                       const ChunkID chunk_id, PosList& matches_out, [[maybe_unused]] RightIterator right_it) {
    // The major part of the table is scanned using SIMD. Only the remainder is handled in this method.
    // For a description of the SIMD code, have a look at the comments in that method.
    _simd_scan_with_iterators<CheckForNull>(func, left_it, left_end, chunk_id, matches_out, right_it);

    // Do the remainder the easy way. If we did not use the optimization above, left_it was not yet touched, so we
    // iterate over the entire input data.
    for (; left_it != left_end; ++left_it) {
      const auto left = *left_it;
      if constexpr (std::is_same_v<RightIterator, std::false_type>) {
        if ((!CheckForNull || !left.is_null()) && func(left)) {
          matches_out.emplace_back(RowID{chunk_id, left.chunk_offset()});
        }
      } else {
        const auto right = *right_it;
        if ((!CheckForNull || (!left.is_null() && !right.is_null())) && func(left, right)) {
          matches_out.emplace_back(RowID{chunk_id, left.chunk_offset()});
        }
        ++right_it;
      }
    }
  }

  template <bool CheckForNull, typename BinaryFunctor, typename LeftIterator, typename RightIterator>
  static void _simd_scan_with_iterators(const BinaryFunctor func, LeftIterator& left_it, const LeftIterator left_end,
                                        const ChunkID chunk_id, PosList& matches_out,
                                        [[maybe_unused]] RightIterator& right_it) {
    // Concept: Partition the vector into blocks of BLOCK_SIZE entries. The remainder is handled by the caller without
    // optimization. We first check if the rows match and set the `mask` to 1 at the appropriate positions.
    // If the mask is empty, we continue with the next block. If at least one row matches (and the mask is not empty),
    // we fill `offsets` with the ChunkOffsets of ALL rows in the block, not only those that match. A SIMD optimization
    // is used to move the matching rows into `matches_out`. There, we do not push_back/emplace_back values, but
    // resize the vector first and directly write values into the next position, given by matches_out_index. This
    // avoids calls into the stdlib from the hot loop.
    //
    // Most of the performance gain does not come from scanning the input data but from inserting into matches_out more
    // efficiently. That is why you will see bigger benefits for scans that select more rows.
    //
    // Finally, this implementation is only better if we are on a machine that supports AVX-512 VL. If we are not, we
    // still do one iteration, simply to reduce the amount of dead / diverging code. After that, we return to the
    // non-SIMD scan.

    // We assume a maximum SIMD register size of 256 bit. Smaller registers simply lead to the inner loop being unrolled
    // more than once. Even on machines with 512-bit SIMD registers, we prefer to use 256 bits, because current Intel
    // CPUs clock down when 512-bit is used. If you want to run this with 512-bit registers, you need to change the
    // SIMD_SIZE here as well as replace _m256 with _m512 twice below.
    constexpr size_t SIMD_SIZE = 256 / 8;
    constexpr size_t BLOCK_SIZE = SIMD_SIZE / sizeof(ChunkOffset);

    // The index at which we will write the next matching row
    auto matches_out_index = matches_out.size();

    // Make sure that we have enough space for the first iteration. We might resize later on.
    matches_out.resize(matches_out.size() + BLOCK_SIZE, RowID{chunk_id, 0});

    // As we access the offsets after we already moved the iterator, we need a copy of it. Creating this copy outside
    // of the while loop keeps the surprisingly high costs for copying an iterator to a minimum.
    auto left_it_for_offsets = left_it;

    // Continue the following until we have too few rows left to run over a whole block
    while (static_cast<size_t>(left_end - left_it) > BLOCK_SIZE) {
#ifdef __AVX512VL__
      // __mmask8 would be sufficient, but either GCC or the CPU likes a 16-bit mask better, which is good for the
      // performance.
      __mmask16 mask = 0;
#else
      // Using uint16_t here for consistency with the above.
      uint16_t mask = 0;
#endif

      // The OpenMP Pragma makes the compiler try harder to vectorize this and gives some hints to help with this.
      // We do not use the OpenMP runtime, but only the compiler pragmas (look up -fopenmp-simd).
      // NOLINTNEXTLINE
      ;  // clang-format off
      #pragma omp simd reduction(|:mask) safelen(BLOCK_SIZE)
      // clang-format on
      for (size_t i = 0; i < BLOCK_SIZE; ++i) {
        // Fill `mask` with 1s at positions where the condition is fulfilled
        const auto& left = *left_it;

        if constexpr (std::is_same_v<RightIterator, std::false_type>) {
          mask |= ((!CheckForNull | !left.is_null()) & func(left)) << i;
        } else {
          const auto& right = *right_it;
          mask |= ((!CheckForNull | (!left.is_null() && !right.is_null())) & func(left, right)) << i;
        }

        ++left_it;
        if constexpr (!std::is_same_v<RightIterator, std::false_type>) ++right_it;
      }

      if (!mask) {
        left_it_for_offsets += BLOCK_SIZE;
        continue;
      }

      // Next, write *all* offsets in the block into `offsets`
      auto offsets = std::array<ChunkOffset, BLOCK_SIZE>{};

      if constexpr (!std::is_base_of_v<BasePointAccessSegmentIterator<std::decay_t<decltype(left_it)>,
                                                                      std::decay_t<decltype(*left_it)>>,
                                       std::decay_t<decltype(left_it)>>) {
        // Fast path: If this is a sequential iterator, we know that the chunk offsets are incremented by 1, so we can
        // save us the memory lookup
        const auto first_offset = left_it_for_offsets->chunk_offset();

        // NOLINTNEXTLINE
        ;  // clang-format off
        #pragma omp simd safelen(BLOCK_SIZE)
        // clang-format on
        for (size_t i = 0; i < BLOCK_SIZE; ++i) {
          offsets[i] = first_offset + static_cast<ChunkOffset>(i);
        }

        left_it_for_offsets += BLOCK_SIZE;
      } else {
        // Slow path - the chunk offsets are not guaranteed to be linear

        // NOLINTNEXTLINE
        ;  // clang-format off
        #pragma omp simd safelen(BLOCK_SIZE)
        // clang-format on
        for (auto i = size_t{0}; i < BLOCK_SIZE; ++i) {
          offsets[i] = left_it_for_offsets->chunk_offset();
          ++left_it_for_offsets;
        }
      }

      // Now write the matches into matches_out.
#ifndef __AVX512VL__
      // "Slow" path for non-AVX512VL systems
      for (auto i = size_t{0}; i < BLOCK_SIZE; ++i) {
        if (mask >> i & 1) {
          matches_out[matches_out_index++].chunk_offset = offsets[i];
        }
      }

      // We have done one iteration. As described above, we stop the SIMD code here, because it won't be faster but
      // might be significantly slower.
      break;
#else
      // Fast path for AVX512VL systems

      // Compress `offsets`, i.e., move all values where the mask is set to 1 to the front
      auto offsets_simd =
          _mm256_maskz_compress_epi32(static_cast<unsigned char>(mask), reinterpret_cast<__m256i&>(offsets));

      // Copy all offsets into `matches_out` - even those that are set to 0 (which are located at the end). This does
      // not matter because they will be overwritten in the next round anyway. Copying more than necessary is better
      // than stopping at the number of matching rows because we do not need a branch for this. The loop will be
      // vectorized automatically.

      // NOLINTNEXTLINE
      ;  // clang-format off
      #pragma omp simd safelen(BLOCK_SIZE)
      // clang-format on
      for (auto i = size_t{0}; i < BLOCK_SIZE; ++i) {
        matches_out[matches_out_index + i].chunk_offset = (reinterpret_cast<ChunkOffset*>(&offsets_simd))[i];
      }

      // Count the number of matches and increase the index of the next write to matches_out accordingly
      matches_out_index += __builtin_popcount(mask);
#endif

      // As we write directly into the matches_out vector, we have to make sure that is big enough. We grow the vector
      // more aggressively than its default behavior as the potentially wasted space is only ephemeral.
      if (matches_out_index + BLOCK_SIZE >= matches_out.size()) {
        matches_out.resize((BLOCK_SIZE + matches_out.size()) * 3, RowID{chunk_id, 0});
      }
    }

    // Remove all entries that we have overallocated
    matches_out.resize(matches_out_index);

    // The remainder is now done by the regular scan
  }

  /**@}*/
};

}  // namespace opossum
