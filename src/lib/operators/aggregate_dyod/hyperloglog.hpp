#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

#include "operators/aggregate_dyod/aggregate_dyod_config.hpp"
#include "types.hpp"

namespace hyrise {

// A HyperLogLog sketch used only to estimate group-by cardinality during the ESTIMATE phase, so the operator can size
// the partition count and each partition's dense merge map. This is a planning-time estimator, never a user-facing
// COUNT(DISTINCT) -- it is fed the packed-key hash, not individual column values.
//
// Ownership/threading: one sketch per worker during the estimate phase. A worker updates only its own sketch (no
// synchronization). After the phase barrier, all per-worker sketches are combined register-wise with merge() into one
// sketch, whose estimate() drives choose_partition_count(). Register storage is 2^HLL_PRECISION bytes.
class HllSketch : private Noncopyable {
 public:
  HllSketch();

  // Fold one key's precomputed hash into the sketch: index = high bits, rank = leading-zero count of the remainder.
  // Takes the hash (not the key) so the estimate is over exactly the value the scatter/merge phases hash and route on.
  void add(uint64_t key_hash);

  // Register-wise max of `other` into `*this` -- the operation that makes per-worker sketches combine losslessly into
  // the estimate a single-threaded pass would have produced.
  void merge(const HllSketch& other);

  // Bias-corrected distinct-count estimate over everything add()ed (directly or via merge()). Returns 0 for an empty
  // sketch; callers that derive a power-of-two from this must still guard 0 (see choose_partition_count).
  size_t estimate() const;

 private:
  // One 6-bit rank per register, stored one-per-byte for branch-free updates; 2^HLL_PRECISION entries.
  std::vector<uint8_t> _registers;
};

// Choose the radix partition count P for a query from the estimated group-by cardinality. Returns a power of two in
// [max(worker_count, 1), MAX_PARTITION_COUNT] -- the lower bound keeps at least one partition per worker so every core
// has merge work. Concretely: clamp(next_pow2(cardinality_estimate / KEYS_BUDGET), max(worker_count, 1), MAX).
//
// Totality: next_pow2 is guarded so an estimate of 0 (legitimately produced by an empty input) yields the floor rather
// than tripping undefined behavior in a leading-zero intrinsic -- empty input is additionally short-circuited before
// this is reached, but the function stays total as a defensive guard.
PartitionCount choose_partition_count(size_t cardinality_estimate, size_t worker_count);

}  // namespace hyrise
