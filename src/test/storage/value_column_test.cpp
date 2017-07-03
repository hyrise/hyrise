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
  vc_int = ValueColumn<int>{true};
  vc_str = ValueColumn<std::string>{true};
  vc_double = ValueColumn<double>{true};

  EXPECT_TRUE(vc_int.is_nullable());
  EXPECT_TRUE(vc_str.is_nullable());
  EXPECT_TRUE(vc_double.is_nullable());

  EXPECT_NO_THROW(vc_int.append(NULL_VALUE));
  EXPECT_NO_THROW(vc_str.append(NULL_VALUE));
  EXPECT_NO_THROW(vc_double.append(NULL_VALUE));
}

TEST_F(StorageValueColumnTest, ArraySubscriptOperatorReturnsNullValue) {
  vc_int = ValueColumn<int>{true};
  vc_str = ValueColumn<std::string>{true};
  vc_double = ValueColumn<double>{true};

  vc_int.append(NULL_VALUE);
  vc_str.append(NULL_VALUE);
  vc_double.append(NULL_VALUE);

  EXPECT_TRUE(is_null(vc_int[0]));
  EXPECT_TRUE(is_null(vc_str[0]));
  EXPECT_TRUE(is_null(vc_double[0]));
}

TEST_F(StorageValueColumnTest, StringTooLong) {
  EXPECT_THROW(vc_str.append(std::string(std::numeric_limits<StringLength>::max() + 1ul, 'A')), std::exception);
}

}  // namespace opossum
