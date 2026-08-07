#pragma once

#include <algorithm>
#include <bit>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <vector>

#include "operators/aggregate_dyod/aggregate_dyod_config.hpp"
#include "types.hpp"

namespace hyrise {

/**
 * A fixed-precision HyperLogLog sketch that estimates group-by cardinality during the estimate phase.
 *
 * AggregateDYOD sizes its radix split from this estimate: the merged sketch's estimate() feeds
 * choose_partition_count(), which sets how many partitions the scatter phase produces and how large each partition's
 * dense merge map must be. It is a planning-time estimator, never a user-facing COUNT(DISTINCT): it consumes the
 * packed group-by key's precomputed 64-bit hash -- exactly the value the scatter and merge phases hash and route on
 * -- not individual column values, so its estimate predicts the quantity those later phases actually encounter.
 *
 * Precision is fixed at HLL_PRECISION, giving 2^HLL_PRECISION single-byte registers (~1.6% standard error). The
 * estimate need only be accurate enough to pick a power-of-two partition count and pre-size the per-partition maps.
 *
 * Invariants:
 *   - Every sketch has exactly 2^HLL_PRECISION registers, so any two sketches are register-compatible for merge().
 *   - Each register holds a 6-bit rank stored one-per-byte, for branch-free updates.
 *   - add() and merge() only raise registers (register-wise maximum), so estimate() is monotone non-decreasing.
 *
 * Ownership/lifetime/threading: one sketch per worker for the duration of the estimate phase. A worker mutates
 * only its own sketch via add(), with no synchronization. After the phase barrier (which establishes happens-before),
 * all per-worker sketches are combined register-wise into one sketch via merge(), and that sketch's estimate() drives
 * choose_partition_count(). A single sketch must not be mutated concurrently. Copying is disabled (Noncopyable);
 * combine sketches with merge() rather than by copying.
 *
 * @see choose_partition_count, HLL_PRECISION and keys_budget() in aggregate_dyod_config.hpp.
 */
class HllSketch : private Noncopyable {
 public:
  /**
   * Constructs an empty sketch: all 2^HLL_PRECISION registers zeroed.
   *
   * @post estimate() returns 0 until the first add() or merge().
   */
  HllSketch();

  /**
   * Folds one key's precomputed hash into the sketch.
   *
   * The high HLL_PRECISION bits of the hash select a register; the leading-zero rank of the remaining bits is combined
   * into that register by register-wise maximum. Takes the hash rather than the key so the estimate is taken over
   * exactly the packed-key hash the scatter and merge phases route on.
   *
   * @param key_hash 64-bit hash of the packed group-by key, as produced by the query's KeySchema. Any 64-bit value is
   *   valid; there is no reserved or sentinel hash.
   * @pre Runs during the estimate phase on the calling worker's own sketch; a single sketch must not be mutated
   *   concurrently.
   * @post estimate() is monotone non-decreasing across calls.
   */
  void add(uint64_t key_hash);

  /**
   * Combines another sketch into this one by taking the register-wise maximum.
   *
   * This is what lets independent per-worker sketches combine losslessly: the merged sketch yields the same estimate a
   * single-threaded pass over all the same hashes would have produced.
   *
   * @param other The sketch to fold in; borrowed and left unchanged. Has the same precision as *this (every HllSketch
   *   has 2^HLL_PRECISION registers).
   * @pre Called after the estimate phase barrier, which establishes the happens-before needed to read another worker's
   *   sketch.
   * @post Every register of *this is >= both its previous value and the corresponding register of `other`.
   * Complexity: O(2^HLL_PRECISION).
   */
  void merge(const HllSketch& other);

  /**
   * Returns the bias-corrected distinct-count estimate over every hash add()ed, directly or via merge().
   *
   * @return Estimated number of distinct group-by keys; 0 for an empty sketch. Carries HyperLogLog's sampling error
   *   (~1.6% standard error at HLL_PRECISION), so it is an estimate, not an exact count.
   * @note A caller that derives a power of two from the result must still guard the 0 case; choose_partition_count()
   *   does exactly that.
   * Complexity: O(2^HLL_PRECISION).
   */
  size_t estimate() const;

 private:
  static constexpr auto REGISTER_COUNT = size_t{1} << HLL_PRECISION;

  // One 6-bit rank per register, stored one-per-byte for branch-free updates; 2^HLL_PRECISION entries.
  std::vector<uint8_t> _registers;
};

inline HllSketch::HllSketch() : _registers(REGISTER_COUNT, uint8_t{0}) {}

inline void HllSketch::add(const uint64_t key_hash) {
  constexpr auto REMAINING_HASH_BITS = 64 - HLL_PRECISION;

  const auto register_index = static_cast<size_t>(key_hash >> REMAINING_HASH_BITS);
  const auto remaining_hash_bits = key_hash << HLL_PRECISION;

  const auto rank = remaining_hash_bits == 0 ? static_cast<uint8_t>(REMAINING_HASH_BITS + 1)
                                             : static_cast<uint8_t>(std::countl_zero(remaining_hash_bits) + 1);
  _registers[register_index] = std::max(_registers[register_index], rank);
}

inline void HllSketch::merge(const HllSketch& other) {
  for (auto register_index = size_t{0}; register_index < REGISTER_COUNT; ++register_index) {
    _registers[register_index] = std::max(_registers[register_index], other._registers[register_index]);
  }
}

inline size_t HllSketch::estimate() const {
  auto inverse_sum = 0.0;
  auto zero_register_count = size_t{0};

  for (const auto rank : _registers) {
    inverse_sum += std::ldexp(1.0, -static_cast<int>(rank));
    if (rank == 0) {
      ++zero_register_count;
    }
  }

  constexpr auto register_count = static_cast<double>(REGISTER_COUNT);
  constexpr auto alpha = 0.7213 / (1.0 + 1.079 / register_count);
  const auto raw_estimate = alpha * register_count * register_count / inverse_sum;

  if (raw_estimate <= 2.5 * register_count && zero_register_count > 0) {
    return static_cast<size_t>(
        std::round(register_count * std::log(register_count / static_cast<double>(zero_register_count))));
  }

  if (raw_estimate >= static_cast<double>(std::numeric_limits<size_t>::max())) {
    return std::numeric_limits<size_t>::max();
  }

  return static_cast<size_t>(std::round(raw_estimate));
}

/**
 * Chooses the radix partition count P for a query from its estimated group-by cardinality.
 *
 * Computes clamp(next_pow2(ceil(cardinality_estimate / keys_budget())), min(next_pow2(max(worker_count, 1)), ceiling),
 * ceiling), where ceiling = max_partition_count(stream_count). Dividing by keys_budget() targets that many distinct
 * keys per partition, so each partition's dense merge map is expected to stay resident in the merge worker's share of
 * the last-level cache -- the cache residency the radix split exists to buy. The lower bound keeps at least one
 * partition per worker so every core has merge work; the ceiling keeps the scatter phase's SWWC staging inside L2.
 *
 * @param cardinality_estimate Estimated distinct group-by keys, from a merged HllSketch::estimate(). 0 is valid (empty
 *   input) and yields the floor.
 * @param worker_count Number of merge workers (scheduler CPUs); sets the lower bound via max(worker_count, 1), so a
 *   worker_count of 0 still yields at least one partition.
 * @param stream_count Number of scatter streams the query will use (packed key, value streams, optional row-id and
 *   value-null-bitmap streams); sets the L2-derived ceiling. 0 is treated as 1.
 * @return A power of two in [min(next_pow2(max(worker_count, 1)), ceiling), ceiling], so a partition index is the low
 *   log2(P) bits of a key's hash.
 * @note This is a total function: every input, including a 0 estimate, yields a valid P. Empty input is additionally
 *   short-circuited upstream.
 */
inline PartitionCount choose_partition_count(const size_t cardinality_estimate, const size_t worker_count,
                                             const size_t stream_count) {
  const auto ceil_divide = [](const size_t dividend, const size_t divisor) {
    return (dividend / divisor) + (dividend % divisor == 0 ? 0 : 1);
  };

  const auto maximum_partition_count = static_cast<size_t>(max_partition_count(stream_count));
  const auto minimum_partition_count =
      std::bit_ceil(std::min(std::max(size_t{1}, worker_count), maximum_partition_count));
  const auto target_partition_count =
      std::bit_ceil(std::min(ceil_divide(cardinality_estimate, keys_budget()), maximum_partition_count));
  const auto partition_count = std::clamp(target_partition_count, minimum_partition_count, maximum_partition_count);

  return static_cast<PartitionCount>(partition_count);
}

}  // namespace hyrise
