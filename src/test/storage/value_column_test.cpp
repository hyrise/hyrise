#include <string>

#include "gtest/gtest.h"

#include "../../lib/storage/value_column.hpp"

TEST(StorageValueColumnTest, CreateColumn) {
  // create column of different types
  opossum::ValueColumn<int> vc_int;
  EXPECT_EQ(vc_int.size(), 0u);

  opossum::ValueColumn<std::string> vc_str;
  EXPECT_EQ(vc_str.size(), 0u);

  opossum::ValueColumn<double> vc_double;
  EXPECT_EQ(vc_double.size(), 0u);

  // add value of same type
  vc_int.append(3);
  EXPECT_EQ(vc_int.size(), 1u);

  vc_str.append("Hello");
  EXPECT_EQ(vc_str.size(), 1u);

  vc_double.append(3.14);
  EXPECT_EQ(vc_double.size(), 1u);

  // add value of different type
  vc_int.append(3.14);
  EXPECT_EQ(vc_int.size(), 2u);
  EXPECT_THROW(vc_int.append("Hi"), std::exception);

  vc_str.append(3);
  vc_str.append(4.44);
  EXPECT_EQ(vc_str.size(), 3u);

  vc_double.append(4);
  EXPECT_EQ(vc_double.size(), 2u);
  EXPECT_THROW(vc_double.append("Hi"), std::exception);
}
