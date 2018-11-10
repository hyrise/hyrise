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
  static void __attribute__((noinline))
  _scan_with_iterators(const BinaryFunctor func, LeftIterator left_it, const LeftIterator left_end,
                       const ChunkID chunk_id, PosList& matches_out, bool functor_is_vectorizable) {
    // Can't use a default argument for this because default arguments are non-type deduced contexts
    auto false_type = std::false_type{};
    _scan_with_iterators<CheckForNull>(func, left_it, left_end, chunk_id, matches_out, functor_is_vectorizable,
                                       false_type);
  }

  template <bool CheckForNull, typename BinaryFunctor, typename LeftIterator, typename RightIterator>
  // noinline reduces compile time drastically
  static void __attribute__((noinline))
  _scan_with_iterators(const BinaryFunctor func, LeftIterator left_it, const LeftIterator left_end,
                       const ChunkID chunk_id, PosList& matches_out, [[maybe_unused]] bool functor_is_vectorizable,
                       [[maybe_unused]] RightIterator right_it) {
    // SIMD has no benefit for iterators that block vectorization (mostly iterators that do not operate on contiguous
    // storage). Because of that, it is only enabled for std::vector (currently used by FixedSizeByteAlignedVector).
    // Also, the AnySegmentIterator is not vectorizable because it relies on virtual method calls. While the check for
    // `IS_DEBUG` is redudant, it makes people aware of this. Unfortunately, vectorization is only really beneficial
    // when we can use AVX-512VL. However, since this branch is not slower on CPUs without it, we still use it there as
    // well, as this reduces the divergence across different systems. Finally, we only use the vectorized scan for
    // tables with a certain size. This is because firing up the AVX units has some cost on current CPUs. Using 1000
    // as the boundary is just an educated guess - a machine-dependent fine-tuning could find better values, but as
    // long as scans with a handful of results are not vectorized, the benefits of fine-tuning should not be too big.

#if !HYRISE_DEBUG
    if constexpr (LeftIterator::IsVectorizable) {
      if (functor_is_vectorizable && left_end - left_it > 1000) {
        _simd_scan_with_iterators<CheckForNull>(func, left_it, left_end, chunk_id, matches_out, right_it);
      }
    }
#endif

    // Do the remainder the easy way. If we did not use the optimization above, left_it was not yet touched, so we
    // iterate over the entire input data.
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

  template <bool CheckForNull, typename BinaryFunctor, typename LeftIterator, typename RightIterator>
  // noinline reduces compile time drastically
  void __attribute__((noinline))
  _simd_scan_with_iterators(const BinaryFunctor func, LeftIterator left_it, const LeftIterator left_end,
                            const ChunkID chunk_id, PosList& matches_out,
                            [[maybe_unused]] RightIterator right_it) {
    // Concept: Partition the vector into blocks of BLOCK_SIZE entries. The remainder is handled outside of this
    // optimization. For each row write 0 to `offsets` if the row does not match, or `chunk_offset + 1` if the row
    // matches. The reason why we need `+1` is given below. This set can be parallelized using auto-vectorization/SIMD.
    // Afterwards, add all matching rows into `matches_out`.

    auto matches_out_index = matches_out.size();
    constexpr long SIMD_SIZE = 64;  // Assuming a SIMD register size of 512 bit
    constexpr long BLOCK_SIZE = SIMD_SIZE / sizeof(ValueID);

    // Continue doing this until we have too few rows left to run over a whole block
    while (left_end - left_it > BLOCK_SIZE) {
      alignas(SIMD_SIZE) std::array<ChunkOffset, SIMD_SIZE / sizeof(ChunkOffset)> offsets;

      // The pragmas promise to the compiler that there are no data dependencies within the loop. If you run into any
      // issues with the optimization, make sure that you only have only set IsVectorizable on iterators that use
      // linear storage and where the access methods do not change any state.
      //
      // Also, when using clang, this causes an error to be thrown if the loop could not be vectorized. This, however
      // does not guarantee that, if no error is thrown, every instruction in the loop is using SIMD.

      // NOLINTNEXTLINE
      ;  // clang-format off
      #pragma GCC ivdep
      #pragma clang loop vectorize(assume_safety)
      // clang-format on
      for (auto i = size_t{0}; i < BLOCK_SIZE; ++i) {
        const auto& left = *left_it;

        bool matches;
        if constexpr (std::is_same_v<RightIterator, std::false_type>) {
          matches = (!CheckForNull | !left.is_null()) & func(left);
        } else {
          const auto& right = *left_it;
          matches = (!CheckForNull | (!left.is_null() & !right.is_null())) & func(left, right);
        }

        // If the row matches, write its offset plus one into `offsets`, otherwise write 0. We need to increment the
        // offset because otherwise a zero would also be written for offset 0. This is safe because the last
        // possible chunk offset is defined as INVALID_CHUNK_OFFSET anyway.
        offsets[i] = matches * (left.chunk_offset() + 1);

        ++left_it;
        if constexpr (!std::is_same_v<RightIterator, std::false_type>) ++right_it;
      }

      // As we write directly into the matches_out vector, make sure that is has enough size
      if (matches_out_index + BLOCK_SIZE >= matches_out.size()) {
        matches_out.resize((BLOCK_SIZE + matches_out.size()) * 3, RowID{chunk_id, 0});
      }

      // Now write the matches into matches_out. For better understanding, first look at the non-AVX12VL block.
#ifdef __AVX512VL__
      // Build a mask where a bit indicates if the row in `offsets` matched the criterion.
      const auto mask = _mm512_cmpneq_epu32_mask(*reinterpret_cast<__m512i*>(&offsets), __m512i{});

      if (!mask) continue;

      // Compress `offsets`, that is move all values where the mask is set to 1 to the front. This is essentially
      // std::remove(offsets.begin(), offsets.end(), ChunkOffset{0});
      *reinterpret_cast<__m512i*>(&offsets) = _mm512_maskz_compress_epi32(mask, *reinterpret_cast<__m512i*>(&offsets));

      // Copy all offsets into `matches_out` - even those that are set to 0. This does not matter because they will
      // be overwritten in the next round anyway. Copying more than necessary is better than stopping at the number
      // of matching rows because we do not need a branch for this.
      for (auto i = size_t{0}; i < BLOCK_SIZE; ++i) {
        matches_out[matches_out_index + i].chunk_offset = offsets[i] - 1;
      }

      // Count the number of matches and increase the index of the next write to matches_out accordingly
      matches_out_index += __builtin_popcount(mask);
#else
      for (auto i = size_t{0}; i < BLOCK_SIZE; ++i) {
        if (offsets[i]) {
          matches_out[matches_out_index++].chunk_offset = offsets[i] - 1;
        }
      }
#endif
    }

    // Remove all the entries that we have overallocated
    matches_out.resize(matches_out_index);

    // The remainder is now done by the regular scan
  }

  /**@}*/
};

}  // namespace opossum
