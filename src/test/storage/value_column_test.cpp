#include <limits>
#include <string>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../lib/storage/value_column.hpp"

namespace opossum {

class StorageValueColumnTest : public BaseTest {
 protected:
  ValueColumn<int> vc_int;
  ValueColumn<std::string> vc_str;
  ValueColumn<double> vc_double;
};

TEST_F(StorageValueColumnTest, GetSize) {
  EXPECT_EQ(vc_int.size(), 0u);
  EXPECT_EQ(vc_str.size(), 0u);
  EXPECT_EQ(vc_double.size(), 0u);
}

TEST_F(StorageValueColumnTest, AddValueOfSameType) {
  vc_int.append(3);
  EXPECT_EQ(vc_int.size(), 1u);

  vc_str.append("Hello");
  EXPECT_EQ(vc_str.size(), 1u);

  vc_double.append(3.14);
  EXPECT_EQ(vc_double.size(), 1u);
}

TEST_F(StorageValueColumnTest, AddValueOfDifferentType) {
  vc_int.append(3.14);
  EXPECT_EQ(vc_int.size(), 1u);
  EXPECT_THROW(vc_int.append("Hi"), std::exception);

  vc_str.append(3);
  vc_str.append(4.44);
  EXPECT_EQ(vc_str.size(), 2u);

  vc_double.append(4);
  EXPECT_EQ(vc_double.size(), 1u);
  EXPECT_THROW(vc_double.append("Hi"), std::exception);
}

TEST_F(StorageValueColumnTest, RetrieveValue) {
  vc_int.append(3);
  EXPECT_EQ(vc_int.values()[0], 3);

  vc_str.append("Hello");
  EXPECT_EQ(vc_str.values()[0], "Hello");

  vc_double.append(3.14);
  EXPECT_EQ(vc_double.values()[0], 3.14);
}

TEST_F(StorageValueColumnTest, AppendNullValueWhenNotNullable) {
  EXPECT_TRUE(!vc_int.is_nullable());
  EXPECT_TRUE(!vc_str.is_nullable());
  EXPECT_TRUE(!vc_double.is_nullable());

  EXPECT_THROW(vc_int.append(NULL_VALUE), std::exception);
  EXPECT_THROW(vc_str.append(NULL_VALUE), std::exception);
  EXPECT_THROW(vc_double.append(NULL_VALUE), std::exception);
}

TEST_F(StorageValueColumnTest, AppendNullValueWhenNullable) {
  auto vc_int = ValueColumn<int>{true};
  auto vc_str = ValueColumn<std::string>{true};
  auto vc_double = ValueColumn<double>{true};

  EXPECT_TRUE(vc_int.is_nullable());
  EXPECT_TRUE(vc_str.is_nullable());
  EXPECT_TRUE(vc_double.is_nullable());

  EXPECT_NO_THROW(vc_int.append(NULL_VALUE));
  EXPECT_NO_THROW(vc_str.append(NULL_VALUE));
  EXPECT_NO_THROW(vc_double.append(NULL_VALUE));
}

TEST_F(StorageValueColumnTest, ArraySubscriptOperatorReturnsNullValue) {
  auto vc_int = ValueColumn<int>{true};
  auto vc_str = ValueColumn<std::string>{true};
  auto vc_double = ValueColumn<double>{true};

  vc_int.append(NULL_VALUE);
  vc_str.append(NULL_VALUE);
  vc_double.append(NULL_VALUE);

  EXPECT_TRUE(variant_is_null(vc_int[0]));
  EXPECT_TRUE(variant_is_null(vc_str[0]));
  EXPECT_TRUE(variant_is_null(vc_double[0]));
}

TEST_F(StorageValueColumnTest, MemoryUsageEstimation) {
  /**
   * WARNING: Since it's hard to assert what constitutes a correct "estimation", this just tests basic sanity of the
   * memory usage estimations
   */

  const auto empty_usage_int = vc_int.estimate_memory_usage();
  const auto empty_usage_double = vc_double.estimate_memory_usage();
  const auto empty_usage_str = vc_str.estimate_memory_usage();

  vc_int.append(1);
  vc_int.append(2);

  const auto short_str = "Hello";
  const auto longer_str = std::string{"HelloWorldHaveANiceDayWithSunshineAndGoodCofefe"};

  vc_str.append(short_str);
  vc_str.append(longer_str);

  vc_double.append(42.1337);

  EXPECT_EQ(empty_usage_int + sizeof(int) * 2, vc_int.estimate_memory_usage());
  EXPECT_EQ(empty_usage_double + sizeof(double), vc_double.estimate_memory_usage());
  EXPECT_GE(vc_str.estimate_memory_usage(), empty_usage_str + 2 * sizeof(std::string));
}

}  // namespace opossum
