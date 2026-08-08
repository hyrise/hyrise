#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <tuple>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "operators/aggregate_dyod/aggregate_dyod_config.hpp"
#include "operators/aggregate_dyod/hyperloglog.hpp"
#include "operators/aggregate_dyod/key_primitives.hpp"

namespace hyrise {

namespace {

// Splitmix64 finalizer for stable, well-distributed test hashes.
uint64_t mix(const uint64_t value) {
  auto mixed = value + 0x9e3779b97f4a7c15ull;
  mixed = (mixed ^ (mixed >> 30)) * 0xbf58476d1ce4e5b9ull;
  mixed = (mixed ^ (mixed >> 27)) * 0x94d049bb133111ebull;
  return mixed ^ (mixed >> 31);
}

// What the estimate phase feeds the sketch for a single non-nullable Int group-by column.
uint64_t packed_key_hash(const int32_t value) {
  const auto encoded = encode_lane_value(value);
  auto key = std::array<std::byte, sizeof(encoded)>{};
  std::memcpy(key.data(), &encoded, sizeof(encoded));
  return hash_bytes(key.data(), key.size());
}

// Preimage under mix64 of a hash that selects `register_index` and leaves no remaining set bits.
uint64_t saturating_hash(const uint64_t register_index) {
  auto value = register_index << (64 - HLL_PRECISION);
  value ^= value >> 33;
  value *= 0x9cb4b2f8129337dbull;
  value ^= value >> 33;
  value *= 0x4f74430c22a54005ull;
  return value ^ (value >> 33);
}

void add_distinct_values(HllSketch& sketch, const size_t count, const uint64_t offset = 0) {
  for (auto value = uint64_t{0}; value < count; ++value) {
    sketch.add(mix(value + offset));
  }
}

bool is_power_of_two(const PartitionCount partition_count) {
  return partition_count > 0 && (partition_count & (partition_count - 1)) == 0;
}

}  // namespace

class HllSketchTest : public BaseTest {};

TEST_F(HllSketchTest, EmptySketchEstimatesZero) {
  const auto sketch = HllSketch{};
  EXPECT_EQ(sketch.estimate(), 0);
}

TEST_F(HllSketchTest, SingleHashProducesSmallEstimate) {
  auto sketch = HllSketch{};
  sketch.add(mix(0));

  EXPECT_NEAR(static_cast<double>(sketch.estimate()), 1.0, 1.0);
}

TEST_F(HllSketchTest, ZeroHashProducesSmallEstimate) {
  auto sketch = HllSketch{};
  sketch.add(0);

  EXPECT_NEAR(static_cast<double>(sketch.estimate()), 1.0, 1.0);
}

TEST_F(HllSketchTest, DuplicateHashesDoNotIncreaseEstimate) {
  auto sketch = HllSketch{};
  const auto hash = mix(17);

  for (auto repetition = size_t{0}; repetition < 1'000; ++repetition) {
    sketch.add(hash);
  }

  EXPECT_NEAR(static_cast<double>(sketch.estimate()), 1.0, 1.0);
}

TEST_F(HllSketchTest, EstimatesSmallCardinalities) {
  const auto counts_and_tolerances = std::array<std::pair<size_t, double>, 3>{{{10, 3.0}, {100, 15.0}, {1'000, 100.0}}};

  for (const auto& [count, tolerance] : counts_and_tolerances) {
    auto sketch = HllSketch{};
    add_distinct_values(sketch, count);

    EXPECT_NEAR(static_cast<double>(sketch.estimate()), static_cast<double>(count), tolerance);
  }
}

TEST_F(HllSketchTest, EstimatesLargeCardinality) {
  constexpr auto actual_count = size_t{100000};

  auto sketch = HllSketch{};
  add_distinct_values(sketch, actual_count);

  EXPECT_NEAR(static_cast<double>(sketch.estimate()), static_cast<double>(actual_count),
              static_cast<double>(actual_count) * 0.05);
}

TEST_F(HllSketchTest, EstimatesLargeCardinalityFromPackedKeyHashes) {
  constexpr auto actual_count = size_t{4'000'000};

  auto sketch = HllSketch{};
  for (auto value = size_t{1}; value <= actual_count; ++value) {
    sketch.add(packed_key_hash(static_cast<int32_t>(value)));
  }

  EXPECT_NEAR(static_cast<double>(sketch.estimate()), static_cast<double>(actual_count),
              static_cast<double>(actual_count) * 0.05);
}

TEST_F(HllSketchTest, SaturatedSketchClampsEstimate) {
  auto sketch = HllSketch{};
  for (auto register_index = uint64_t{0}; register_index < (uint64_t{1} << HLL_PRECISION); ++register_index) {
    sketch.add(saturating_hash(register_index));
  }

  EXPECT_EQ(sketch.estimate(), std::numeric_limits<size_t>::max());
}

TEST_F(HllSketchTest, MergeMatchesSingleSketch) {
  constexpr auto actual_count = size_t{100000};

  auto single_sketch = HllSketch{};
  auto first_half = HllSketch{};
  auto second_half = HllSketch{};

  for (auto value = uint64_t{0}; value < actual_count; ++value) {
    const auto hash = mix(value);
    single_sketch.add(hash);

    if (value < actual_count / 2) {
      first_half.add(hash);
    } else {
      second_half.add(hash);
    }
  }

  first_half.merge(second_half);
  EXPECT_EQ(first_half.estimate(), single_sketch.estimate());
}

TEST_F(HllSketchTest, MergeIsIdempotent) {
  auto sketch = HllSketch{};
  add_distinct_values(sketch, 10000);

  const auto estimate_before_merge = sketch.estimate();
  sketch.merge(sketch);

  EXPECT_EQ(sketch.estimate(), estimate_before_merge);
}

TEST_F(HllSketchTest, MergeWithEmptySketchDoesNotChangeEstimate) {
  auto sketch = HllSketch{};
  auto empty_sketch = HllSketch{};
  add_distinct_values(sketch, 10000);

  const auto estimate_before_merge = sketch.estimate();
  sketch.merge(empty_sketch);

  EXPECT_EQ(sketch.estimate(), estimate_before_merge);
}

TEST_F(HllSketchTest, WorkerSketchesCanBeMerged) {
  constexpr auto worker_count = size_t{8};
  constexpr auto actual_count = size_t{100000};

  auto single_sketch = HllSketch{};
  auto worker_sketches = std::vector<HllSketch>{};
  worker_sketches.reserve(worker_count);
  for (auto worker_id = size_t{0}; worker_id < worker_count; ++worker_id) {
    worker_sketches.emplace_back();
  }

  for (auto value = uint64_t{0}; value < actual_count; ++value) {
    const auto hash = mix(value);
    single_sketch.add(hash);
    worker_sketches[value % worker_count].add(hash);
  }

  auto merged_sketch = HllSketch{};
  for (const auto& worker_sketch : worker_sketches) {
    merged_sketch.merge(worker_sketch);
  }

  EXPECT_EQ(merged_sketch.estimate(), single_sketch.estimate());
  EXPECT_NEAR(static_cast<double>(merged_sketch.estimate()), static_cast<double>(actual_count),
              static_cast<double>(actual_count) * 0.05);
}

TEST_F(HllSketchTest, ChoosePartitionCountZeroEstimate) {
  EXPECT_EQ(choose_partition_count(0, 1, 2), PartitionCount{1});
}

TEST_F(HllSketchTest, ChoosePartitionCountScalesWithKeysBudget) {
  EXPECT_EQ(choose_partition_count(keys_budget() - 1, 1, 2), PartitionCount{1});
  EXPECT_EQ(choose_partition_count(keys_budget(), 1, 2), PartitionCount{1});
  EXPECT_EQ(choose_partition_count(keys_budget() + 1, 1, 2), PartitionCount{2});
  EXPECT_EQ(choose_partition_count(3 * keys_budget(), 1, 2), PartitionCount{4});
}

TEST_F(HllSketchTest, ChoosePartitionCountClampsToWorkerCount) {
  EXPECT_EQ(choose_partition_count(1, 3, 2), PartitionCount{4});
  EXPECT_EQ(choose_partition_count(1, 8, 2), PartitionCount{8});
  EXPECT_EQ(choose_partition_count(1, max_partition_count(2), 2), max_partition_count(2));
}

TEST_F(HllSketchTest, ChoosePartitionCountClampsToStagingCap) {
  const auto large_cardinality = static_cast<size_t>(MAX_PARTITION_COUNT) * keys_budget() * 16;

  EXPECT_EQ(choose_partition_count(large_cardinality, 1, 2), max_partition_count(2));
}

TEST_F(HllSketchTest, ChoosePartitionCountCapFallsWithStreamCount) {
  const auto large_cardinality = static_cast<size_t>(MAX_PARTITION_COUNT) * keys_budget() * 16;

  EXPECT_LT(choose_partition_count(large_cardinality, 1, 8), choose_partition_count(large_cardinality, 1, 2));
}

TEST_F(HllSketchTest, ChoosePartitionCountClampsOversizedWorkerCountToCap) {
  EXPECT_EQ(choose_partition_count(1, static_cast<size_t>(MAX_PARTITION_COUNT) + 1, 2), max_partition_count(2));
  EXPECT_EQ(choose_partition_count(1, std::numeric_limits<size_t>::max(), 2), max_partition_count(2));
}

TEST_F(HllSketchTest, ChoosePartitionCountAlwaysReturnsPowerOfTwo) {
  const auto inputs = std::array<std::tuple<size_t, size_t, size_t>, 8>{{{0, 0, 1},
                                                                        {1, 1, 2},
                                                                        {keys_budget() - 1, 1, 2},
                                                                        {keys_budget() + 1, 1, 3},
                                                                        {3 * keys_budget(), 1, 4},
                                                                        {100 * keys_budget(), 3, 5},
                                                                        {1'000 * keys_budget(), 64, 8},
                                                                        {size_t{1} << 62, 17, 16}}};

  for (const auto& [cardinality_estimate, worker_count, stream_count] : inputs) {
    EXPECT_TRUE(is_power_of_two(choose_partition_count(cardinality_estimate, worker_count, stream_count)));
  }
}

}  // namespace hyrise
