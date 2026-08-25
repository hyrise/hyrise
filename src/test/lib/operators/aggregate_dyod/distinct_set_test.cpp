#include <bit>
#include <cstdint>
#include <limits>
#include <string>
#include <vector>

#include "base_test.hpp"
#include "operators/aggregate_dyod/distinct_set.hpp"
#include "types.hpp"

namespace hyrise {

class AggregateDYODDistinctSetTest : public BaseTest {};

TEST_F(AggregateDYODDistinctSetTest, InsertReportsFirstSighting) {
  auto set = DistinctSet<int32_t>{};
  EXPECT_EQ(set.size(), 0u);

  EXPECT_TRUE(set.insert(0, 42));
  EXPECT_FALSE(set.insert(0, 42));
  EXPECT_TRUE(set.insert(0, -7));
  EXPECT_EQ(set.size(), 2u);
}

TEST_F(AggregateDYODDistinctSetTest, SlotsAreIndependent) {
  auto set = DistinctSet<int64_t>{};

  EXPECT_TRUE(set.insert(0, 42));
  EXPECT_TRUE(set.insert(1, 42));
  EXPECT_FALSE(set.insert(1, 42));
  EXPECT_EQ(set.size(), 2u);
}

TEST_F(AggregateDYODDistinctSetTest, GrowthPreservesMembership) {
  auto set = DistinctSet<int32_t>{};
  const auto value_count = int32_t{10'000};

  for (auto value = int32_t{0}; value < value_count; ++value) {
    EXPECT_TRUE(set.insert(static_cast<uint32_t>(value % 3), value));
  }
  EXPECT_EQ(set.size(), static_cast<size_t>(value_count));
  for (auto value = int32_t{0}; value < value_count; ++value) {
    EXPECT_FALSE(set.insert(static_cast<uint32_t>(value % 3), value));
  }
}

TEST_F(AggregateDYODDistinctSetTest, SplitDistributesEveryEntryExactlyOnce) {
  auto set = DistinctSet<int32_t>{};
  for (auto value = int32_t{0}; value < 5'000; ++value) {
    set.insert(static_cast<uint32_t>(value % 5), value);
  }

  auto targets = std::vector<DistinctSet<int32_t>>(8);
  set.split_into(targets);

  auto total = size_t{0};
  for (const auto& target : targets) {
    total += target.size();
  }
  EXPECT_EQ(total, set.size());

  auto merged = DistinctSet<int32_t>{};
  for (const auto& target : targets) {
    merged.merge(target);
  }
  EXPECT_EQ(merged.size(), set.size());
  for (auto value = int32_t{0}; value < 5'000; ++value) {
    EXPECT_FALSE(merged.insert(static_cast<uint32_t>(value % 5), value));
  }
}

TEST_F(AggregateDYODDistinctSetTest, SplitKeepsEqualValuesInTheSameTarget) {
  auto first = DistinctSet<pmr_string>{};
  auto second = DistinctSet<pmr_string>{};
  for (auto value = int32_t{0}; value < 500; ++value) {
    first.insert(0, "v" + std::to_string(value));
    second.insert(0, "v" + std::to_string(value + 250));
  }

  auto targets_first = std::vector<DistinctSet<pmr_string>>(4);
  auto targets_second = std::vector<DistinctSet<pmr_string>>(4);
  first.split_into(targets_first);
  second.split_into(targets_second);

  auto total = size_t{0};
  for (auto range = size_t{0}; range < 4; ++range) {
    targets_first[range].merge(targets_second[range]);
    total += targets_first[range].size();
  }
  EXPECT_EQ(total, 750u);
}

TEST_F(AggregateDYODDistinctSetTest, SplitIntoSingleTargetMerges) {
  auto set = DistinctSet<int32_t>{};
  set.insert(0, 1);
  set.insert(0, 2);

  auto targets = std::vector<DistinctSet<int32_t>>(1);
  targets.front().insert(0, 2);
  targets.front().insert(0, 3);
  set.split_into(targets);

  EXPECT_EQ(targets.front().size(), 3u);
}

TEST_F(AggregateDYODDistinctSetTest, ClearRetainsCapacityAndDropsContent) {
  auto set = DistinctSet<int32_t>{};
  for (auto value = int32_t{0}; value < 1'000; ++value) {
    set.insert(0, value);
  }

  set.clear();
  EXPECT_EQ(set.size(), 0u);
  EXPECT_TRUE(set.insert(0, 5));
  EXPECT_EQ(set.size(), 1u);
}

TEST_F(AggregateDYODDistinctSetTest, FloatZeroesCollapse) {
  auto set = DistinctSet<float>{};

  EXPECT_TRUE(set.insert(0, 0.0f));
  EXPECT_FALSE(set.insert(0, -0.0f));
  EXPECT_EQ(set.size(), 1u);
}

TEST_F(AggregateDYODDistinctSetTest, NanPatternsCollapse) {
  auto set = DistinctSet<double>{};
  const auto quiet_nan = std::numeric_limits<double>::quiet_NaN();
  const auto payload_nan = std::bit_cast<double>(std::bit_cast<uint64_t>(quiet_nan) | uint64_t{0xdead});

  EXPECT_TRUE(set.insert(0, quiet_nan));
  EXPECT_FALSE(set.insert(0, payload_nan));
  EXPECT_TRUE(set.insert(0, 1.5));
  EXPECT_EQ(set.size(), 2u);
}

TEST_F(AggregateDYODDistinctSetTest, StringsDedupeByContent) {
  auto set = DistinctSet<pmr_string>{};

  {
    const auto transient = std::string{"a_string_longer_than_any_small_string_optimization_buffer"};
    EXPECT_TRUE(set.insert(0, transient));
  }
  const auto other_copy = std::string{"a_string_longer_than_any_small_string_optimization_buffer"};
  EXPECT_FALSE(set.insert(0, other_copy));
  EXPECT_TRUE(set.insert(0, "a_string"));
  EXPECT_TRUE(set.insert(0, ""));
  EXPECT_FALSE(set.insert(0, std::string{}));
  EXPECT_EQ(set.size(), 3u);
}

TEST_F(AggregateDYODDistinctSetTest, MergeUnionsEntries) {
  auto first = DistinctSet<int32_t>{};
  first.insert(0, 1);
  first.insert(0, 2);
  auto second = DistinctSet<int32_t>{};
  second.insert(0, 2);
  second.insert(0, 3);
  second.insert(1, 1);

  first.merge(second);
  EXPECT_EQ(first.size(), 4u);
  EXPECT_FALSE(first.insert(0, 3));
  EXPECT_FALSE(first.insert(1, 1));
}

TEST_F(AggregateDYODDistinctSetTest, MergeReinternsStrings) {
  auto first = DistinctSet<pmr_string>{};
  first.insert(0, "shared");
  auto second = DistinctSet<pmr_string>{};
  second.insert(0, "shared");
  second.insert(0, "a_string_longer_than_any_small_string_optimization_buffer");

  first.merge(second);
  second.clear();
  second.insert(0, "b_string_longer_than_any_small_string_optimization_buffer");
  EXPECT_EQ(first.size(), 2u);
  EXPECT_FALSE(first.insert(0, "a_string_longer_than_any_small_string_optimization_buffer"));
}

}  // namespace hyrise
