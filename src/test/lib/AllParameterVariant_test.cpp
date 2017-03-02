#include <string>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../lib/types.hpp"

namespace opossum {

class AllParameterVariantTest : public BaseTest {};

TEST_F(AllParameterVariantTest, getCurrentType) {
  {
    AllParameterVariant parameter(ColumnName("column_name"));
    EXPECT_EQ(parameter.type(), typeid(ColumnName));
    EXPECT_NE(parameter.type(), typeid(AllTypeVariant));
  }
  {
    AllParameterVariant parameter("string");
    EXPECT_NE(parameter.type(), typeid(ColumnName));
    EXPECT_EQ(parameter.type(), typeid(AllTypeVariant));
  }
  {
    AllParameterVariant parameter(static_cast<int32_t>(123));
    EXPECT_NE(parameter.type(), typeid(ColumnName));
    EXPECT_EQ(parameter.type(), typeid(AllTypeVariant));
  }
  {
    AllParameterVariant parameter(static_cast<int64_t>(123456789l));
    EXPECT_NE(parameter.type(), typeid(ColumnName));
    EXPECT_EQ(parameter.type(), typeid(AllTypeVariant));
  }
  {
    AllParameterVariant parameter(123.4f);
    EXPECT_NE(parameter.type(), typeid(ColumnName));
    EXPECT_EQ(parameter.type(), typeid(AllTypeVariant));
  }
  {
    AllParameterVariant parameter(123.4);
    EXPECT_NE(parameter.type(), typeid(ColumnName));
    EXPECT_EQ(parameter.type(), typeid(AllTypeVariant));
  }
}

TEST_F(AllParameterVariantTest, getCurrentValue) {
  {
    AllParameterVariant parameter(ColumnName("column_name"));
    EXPECT_EQ(static_cast<std::string>(boost::get<ColumnName>(parameter)), "column_name");
  }
  {
    AllParameterVariant parameter("string");
    auto value = type_cast<std::string>(boost::get<AllTypeVariant>(parameter));
    EXPECT_EQ(value, "string");
  }
  {
    AllParameterVariant parameter(static_cast<int32_t>(123));
    auto value = type_cast<int32_t>(boost::get<AllTypeVariant>(parameter));
    EXPECT_EQ(value, static_cast<int32_t>(123));
  }
  {
    AllParameterVariant parameter(static_cast<int64_t>(123456789l));
    auto value = type_cast<int64_t>(boost::get<AllTypeVariant>(parameter));
    EXPECT_EQ(value, static_cast<int64_t>(123456789l));
  }
  {
    AllParameterVariant parameter(123.4f);
    auto value = type_cast<float>(boost::get<AllTypeVariant>(parameter));
    EXPECT_EQ(value, 123.4f);
  }
  {
    AllParameterVariant parameter(123.4);
    auto value = type_cast<double>(boost::get<AllTypeVariant>(parameter));
    EXPECT_EQ(value, 123.4);
  }
}
}  // namespace opossum
