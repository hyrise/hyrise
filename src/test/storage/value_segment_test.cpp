#include <limits>
#include <string>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "storage/value_segment.hpp"

namespace opossum {

class StorageValueSegmentTest : public BaseTest {
 protected:
  ValueSegment<int> vs_int;
  ValueSegment<pmr_string> vs_str;
  ValueSegment<double> vs_double;
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

TEST_F(StorageValueSegmentTest, AddValueOfDifferentType) {
  vs_int.append(3.14);
  EXPECT_EQ(vs_int.size(), 1u);
  EXPECT_THROW(vs_int.append("Hi"), std::exception);

  vs_str.append(3);
  vs_str.append(4.44);
  EXPECT_EQ(vs_str.size(), 2u);

  vs_double.append(4);
  EXPECT_EQ(vs_double.size(), 1u);
  EXPECT_THROW(vs_double.append("Hi"), std::exception);
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

  EXPECT_TRUE(variant_is_null(vs_int[0]));
  EXPECT_TRUE(variant_is_null(vs_str[0]));
  EXPECT_TRUE(variant_is_null(vs_double[0]));
}

TEST_F(StorageValueSegmentTest, MemoryUsageEstimation) {
  /**
   * WARNING: Since it's hard to assert what constitutes a correct "estimation", this just tests basic sanity of the
   * memory usage estimations
   */

  const auto empty_usage_int = vs_int.estimate_memory_usage();
  const auto empty_usage_double = vs_double.estimate_memory_usage();
  const auto empty_usage_str = vs_str.estimate_memory_usage();

  vs_int.append(1);
  vs_int.append(2);

  const auto short_str = "Hello";
  const auto longer_str = pmr_string{"HelloWorldHaveANiceDayWithSunshineAndGoodCofefe"};

  vs_str.append(short_str);
  vs_str.append(longer_str);

  vs_double.append(42.1337);

  EXPECT_EQ(empty_usage_int + sizeof(int) * 2, vs_int.estimate_memory_usage());
  EXPECT_EQ(empty_usage_double + sizeof(double), vs_double.estimate_memory_usage());
  EXPECT_GE(vs_str.estimate_memory_usage(), empty_usage_str + 2 * sizeof(pmr_string));
}

}  // namespace opossum
