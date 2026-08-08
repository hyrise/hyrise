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
 * dense merge map must be.
 */
class HllSketch : private Noncopyable {
 public:
  /**
   * Constructs an empty sketch. All registers are zeroed.
   */
  HllSketch();

  /**
   * Folds one key's precomputed hash into the sketch.
   */
  void add(uint64_t key_hash);

  /**
   * Combines another sketch into this one by taking the register-wise maximum.
   */
  void merge(const HllSketch& other);

  /**
   * Returns the bias-corrected distinct-count estimate over every hash added, directly or via merge().
   */
  size_t estimate() const;

 private:
  static constexpr auto REGISTER_COUNT = size_t{1} << HLL_PRECISION;

  std::vector<uint8_t> _registers;
};

inline HllSketch::HllSketch() : _registers(REGISTER_COUNT, uint8_t{0}) {}

// MurmurHash3's 64-bit finalizer. An unfinalized FNV-1a key hash collides in its high bits -- the only ones a sketch
// reads -- badly enough at 15 M distinct keys to halve the estimate, so add() mixes whatever hash it is fed.
inline uint64_t mix64(uint64_t hash) {
  hash ^= hash >> 33;
  hash *= 0xff51afd7ed558ccdull;
  hash ^= hash >> 33;
  hash *= 0xc4ceb9fe1a85ec53ull;
  return hash ^ (hash >> 33);
}

inline void HllSketch::add(const uint64_t key_hash) {
  constexpr auto REMAINING_HASH_BITS = 64 - HLL_PRECISION;

  const auto mixed_hash = mix64(key_hash);
  const auto register_index = static_cast<size_t>(mixed_hash >> REMAINING_HASH_BITS);
  const auto remaining_hash_bits = mixed_hash << HLL_PRECISION;

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
