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

template <typename T>
class TypedStorageValueSegmentTest : public BaseTest {
 protected:
  ValueSegment<T> vs{false, ChunkOffset{100}};
  ValueSegment<T> nullable_vs{true, ChunkOffset{100}};
};

using Types = ::testing::Types<int32_t, int64_t, float, double, pmr_string>;
TYPED_TEST_SUITE(TypedStorageValueSegmentTest, Types);

TYPED_TEST(TypedStorageValueSegmentTest, MemoryUsageEstimation) {
  /**
    * As ValueSegments are pre-allocated, their size should not change when inserting data, except for strings placed
    * on the heap.
    */

  // `this` is necessary here because we are in a derived class template.
  const auto empty_usage = this->vs.memory_usage(MemoryUsageCalculationMode::Sampled);
  const auto empty_usage_nullable = this->nullable_vs.memory_usage(MemoryUsageCalculationMode::Sampled);

  if constexpr (std::is_same_v<TypeParam, pmr_string>) {
    const auto short_str = pmr_string{"Hello"};
    const auto longer_str = pmr_string{"HelloWorldHaveANiceDayWithSunshineAndGoodCofefe"};

    this->vs.append(short_str);
    this->vs.append(longer_str);

    this->nullable_vs.append(short_str);
    this->nullable_vs.append(longer_str);
    this->nullable_vs.append(NULL_VALUE);

    EXPECT_GE(this->vs.memory_usage(MemoryUsageCalculationMode::Sampled), empty_usage);
    EXPECT_GE(this->nullable_vs.memory_usage(MemoryUsageCalculationMode::Sampled), empty_usage_nullable);
    // The short string will fit within the SSO capacity of a string and the long string will be placed on the heap.
    EXPECT_EQ(this->vs.memory_usage(MemoryUsageCalculationMode::Full), empty_usage + longer_str.capacity() + 1);
    EXPECT_EQ(this->nullable_vs.memory_usage(MemoryUsageCalculationMode::Full),
              empty_usage_nullable + longer_str.capacity() + 1);
  } else {
    this->vs.append(static_cast<TypeParam>(42.1337f));
    this->nullable_vs.append(static_cast<TypeParam>(42.1337f));
    this->nullable_vs.append(NULL_VALUE);

    EXPECT_EQ(empty_usage, this->vs.memory_usage(MemoryUsageCalculationMode::Sampled));
    EXPECT_EQ(empty_usage_nullable, this->nullable_vs.memory_usage(MemoryUsageCalculationMode::Sampled));
  }
}

}  // namespace hyrise
