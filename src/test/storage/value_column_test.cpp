#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "../../lib/storage/value_column.hpp"

class StorageValueColumnTest : public ::testing::Test {
 protected:
  opossum::ValueColumn<int> vc_int;
  opossum::ValueColumn<std::string> vc_str;
  opossum::ValueColumn<double> vc_double;
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
  EXPECT_EQ(vc_int.get_values()[0], 3);

  vc_str.append("Hello");
  EXPECT_EQ(vc_str.get_values()[0], "Hello");

  vc_double.append(3.14);
  EXPECT_EQ(vc_double.get_values()[0], 3.14);
}
