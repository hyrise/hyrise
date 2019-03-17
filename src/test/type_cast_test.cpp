#include "gtest/gtest.h"

#include "type_cast.hpp"

namespace opossum {

class TypeCastTest : public ::testing::Test{};

TEST_F(TypeCastTest, TypeCastSafeIdenticalSourceAndTarget) {
  EXPECT_EQ(type_cast_safe<int32_t>(int32_t(5)), int32_t(5));
  EXPECT_EQ(type_cast_safe<int64_t>(int64_t(5'000'000'000)), int64_t(5'000'000'000));
  EXPECT_EQ(type_cast_safe<float>(3.2f), 3.2f);
  EXPECT_EQ(type_cast_safe<double>(3.2), 3.2);
  EXPECT_EQ(type_cast_safe<pmr_string>("hello"), pmr_string("hello"));
}

TEST_F(TypeCastTest, TypeCastSafeIntegralToFloatingPoint) {
  EXPECT_EQ(type_cast_safe<float>(int32_t(3)), 3.0f);
  EXPECT_EQ(type_cast_safe<float>(int64_t(3)), 3.0f);
  EXPECT_EQ(type_cast_safe<double>(int32_t(3)), 3.0);
  EXPECT_EQ(type_cast_safe<double>(int64_t(3)), 3.0);

  EXPECT_EQ(type_cast_safe<float>(int32_t(20'000'003)), std::nullopt);
  EXPECT_EQ(type_cast_safe<float>(int64_t(20'000'003)), std::nullopt);
  EXPECT_EQ(type_cast_safe<double>(int32_t(20'000'003)), 20'000'003);
  EXPECT_EQ(type_cast_safe<double>(int64_t(20'000'003)), 20'000'003);
  
  EXPECT_EQ(type_cast_safe<float>(int32_t(-20'000'003)), std::nullopt);
  EXPECT_EQ(type_cast_safe<double>(int32_t(-20'000'003)), -20'000'003);

  EXPECT_EQ(type_cast_safe<float>(int64_t(20'123'456'789'012'345ll)), std::nullopt);
  EXPECT_EQ(type_cast_safe<double>(int64_t(20'123'456'789'012'345ll)), std::nullopt);
}

}  // namespace opossum
