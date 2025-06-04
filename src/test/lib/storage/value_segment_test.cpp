#include <cstdint>
#include <limits>
#include <string>
#include <vector>

#include "base_test.hpp"
#include "storage/value_segment.hpp"

namespace hyrise {

class StorageValueSegmentTest : public BaseTest {
 protected:
  ValueSegment<int32_t> vs_int{false, ChunkOffset{100}};
  ValueSegment<pmr_string> vs_str{false, ChunkOffset{100}};
  ValueSegment<double> vs_double{false, ChunkOffset{100}};
};

TEST_F(StorageValueSegmentTest, GetSize) {
  EXPECT_EQ(vs_int.size(), 0u);
  EXPECT_EQ(vs_str.size(), 0u);
  EXPECT_EQ(vs_double.size(), 0u);
}

TEST_F(StorageValueSegmentTest, AddValueOfSameType) {
  vs_int.append(3);
  EXPECT_EQ(vs_int.size(), 1u);

  vs_str.append("Hello");
  EXPECT_EQ(vs_str.size(), 1u);

  vs_double.append(3.14);
  EXPECT_EQ(vs_double.size(), 1u);
}

TEST_F(StorageValueSegmentTest, RetrieveValue) {
  vs_int.append(3);
  EXPECT_EQ(vs_int.values()[0], 3);

  vs_str.append("Hello");
  EXPECT_EQ(vs_str.values()[0], "Hello");

  vs_double.append(3.14);
  EXPECT_EQ(vs_double.values()[0], 3.14);
}

TEST_F(StorageValueSegmentTest, AppendNullValueWhenNotNullable) {
  EXPECT_TRUE(!vs_int.is_nullable());
  EXPECT_TRUE(!vs_str.is_nullable());
  EXPECT_TRUE(!vs_double.is_nullable());

  EXPECT_THROW(vs_int.append(NULL_VALUE), std::exception);
  EXPECT_THROW(vs_str.append(NULL_VALUE), std::exception);
  EXPECT_THROW(vs_double.append(NULL_VALUE), std::exception);
}

TEST_F(StorageValueSegmentTest, AppendNullValueWhenNullable) {
  auto vs_int = ValueSegment<int>{true};
  auto vs_str = ValueSegment<pmr_string>{true};
  auto vs_double = ValueSegment<double>{true};

  EXPECT_TRUE(vs_int.is_nullable());
  EXPECT_TRUE(vs_str.is_nullable());
  EXPECT_TRUE(vs_double.is_nullable());

  EXPECT_NO_THROW(vs_int.append(NULL_VALUE));
  EXPECT_NO_THROW(vs_str.append(NULL_VALUE));
  EXPECT_NO_THROW(vs_double.append(NULL_VALUE));
}

TEST_F(StorageValueSegmentTest, ArraySubscriptOperatorReturnsNullValue) {
  auto vs_int = ValueSegment<int>{true};
  auto vs_str = ValueSegment<pmr_string>{true};
  auto vs_double = ValueSegment<double>{true};

  vs_int.append(NULL_VALUE);
  vs_str.append(NULL_VALUE);
  vs_double.append(NULL_VALUE);

  EXPECT_TRUE(variant_is_null(vs_int[ChunkOffset{0}]));
  EXPECT_TRUE(variant_is_null(vs_str[ChunkOffset{0}]));
  EXPECT_TRUE(variant_is_null(vs_double[ChunkOffset{0}]));
}

TEST_F(StorageValueSegmentTest, MemoryUsageEstimation) {
  /**
   * As ValueSegments are pre-allocated, their size should not change when inserting data, except for strings placed
   * on the heap.
   */

  ValueSegment<int64_t> vs_long{false, ChunkOffset{100}};
  ValueSegment<float> vs_float{false, ChunkOffset{100}};

  ValueSegment<int32_t> nullable_vs_int{true, ChunkOffset{100}};
  ValueSegment<int64_t> nullable_vs_long{true, ChunkOffset{100}};
  ValueSegment<float> nullable_vs_float{true, ChunkOffset{100}};
  ValueSegment<double> nullable_vs_double{true, ChunkOffset{100}};
  ValueSegment<pmr_string> nullable_vs_str{true, ChunkOffset{100}};

  const auto empty_usage_int = vs_int.memory_usage(MemoryUsageCalculationMode::Sampled);
  const auto empty_usage_long = vs_long.memory_usage(MemoryUsageCalculationMode::Sampled);
  const auto empty_usage_float = vs_float.memory_usage(MemoryUsageCalculationMode::Sampled);
  const auto empty_usage_double = vs_double.memory_usage(MemoryUsageCalculationMode::Sampled);
  const auto empty_usage_str = vs_str.memory_usage(MemoryUsageCalculationMode::Sampled);

  const auto empty_usage_nullable_int = nullable_vs_int.memory_usage(MemoryUsageCalculationMode::Sampled);
  const auto empty_usage_nullable_long = nullable_vs_long.memory_usage(MemoryUsageCalculationMode::Sampled);
  const auto empty_usage_nullable_float = nullable_vs_float.memory_usage(MemoryUsageCalculationMode::Sampled);
  const auto empty_usage_nullable_double = nullable_vs_double.memory_usage(MemoryUsageCalculationMode::Sampled);
  const auto empty_usage_nullable_str = nullable_vs_str.memory_usage(MemoryUsageCalculationMode::Sampled);

  vs_int.append(int32_t{1});
  vs_int.append(int32_t{2});

  vs_long.append(int64_t{1});
  vs_long.append(int64_t{2});

  vs_float.append(float{42.1337f});

  vs_double.append(double{42.1337});

  nullable_vs_int.append(int32_t{1});
  nullable_vs_int.append(int32_t{2});
  nullable_vs_int.append(NULL_VALUE);

  nullable_vs_long.append(int64_t{1});
  nullable_vs_long.append(int64_t{2});
  nullable_vs_long.append(NULL_VALUE);

  nullable_vs_float.append(float{42.1337f});
  nullable_vs_float.append(NULL_VALUE);

  nullable_vs_double.append(double{42.1337});
  nullable_vs_double.append(NULL_VALUE);

  const auto short_str = pmr_string{"Hello"};
  const auto longer_str = pmr_string{"HelloWorldHaveANiceDayWithSunshineAndGoodCofefe"};

  vs_str.append(short_str);
  vs_str.append(longer_str);

  nullable_vs_str.append(short_str);
  nullable_vs_str.append(longer_str);

  EXPECT_EQ(empty_usage_int, vs_int.memory_usage(MemoryUsageCalculationMode::Sampled));
  EXPECT_EQ(empty_usage_long, vs_long.memory_usage(MemoryUsageCalculationMode::Sampled));
  EXPECT_EQ(empty_usage_float, vs_float.memory_usage(MemoryUsageCalculationMode::Sampled));
  EXPECT_EQ(empty_usage_double, vs_double.memory_usage(MemoryUsageCalculationMode::Sampled));

  EXPECT_EQ(empty_usage_nullable_int, nullable_vs_int.memory_usage(MemoryUsageCalculationMode::Sampled));
  EXPECT_EQ(empty_usage_nullable_long, nullable_vs_long.memory_usage(MemoryUsageCalculationMode::Sampled));
  EXPECT_EQ(empty_usage_nullable_float, nullable_vs_float.memory_usage(MemoryUsageCalculationMode::Sampled));
  EXPECT_EQ(empty_usage_nullable_double, nullable_vs_double.memory_usage(MemoryUsageCalculationMode::Sampled));

  EXPECT_GE(vs_str.memory_usage(MemoryUsageCalculationMode::Sampled), empty_usage_str);
  EXPECT_GE(nullable_vs_str.memory_usage(MemoryUsageCalculationMode::Sampled), empty_usage_nullable_str);
  // The short string will fit within the SSO capacity of a string and the long string will be placed on the heap.
  EXPECT_EQ(vs_str.memory_usage(MemoryUsageCalculationMode::Full), empty_usage_str + longer_str.capacity() + 1);
  EXPECT_EQ(nullable_vs_str.memory_usage(MemoryUsageCalculationMode::Full),
            empty_usage_nullable_str + longer_str.capacity() + 1);
}

}  // namespace hyrise
