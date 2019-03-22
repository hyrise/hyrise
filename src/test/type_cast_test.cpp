#include "gtest/gtest.h"

#include "type_cast.hpp"

namespace opossum {

class TypeCastTest : public ::testing::Test {};

TEST_F(TypeCastTest, TypeCastSafeIdenticalSourceAndTarget) {
  EXPECT_EQ(type_cast_safe<int32_t>(int32_t(5)), int32_t(5));
  EXPECT_EQ(type_cast_safe<int64_t>(int64_t(5'000'000'000)), int64_t(5'000'000'000));
  EXPECT_EQ(type_cast_safe<float>(3.2f), 3.2f);
  EXPECT_EQ(type_cast_safe<double>(3.2), 3.2);
  EXPECT_EQ(type_cast_safe<pmr_string>(pmr_string{"hello"}), pmr_string("hello"));

  // Cannot check for NULL == NULL, as that's not TRUE. So simply check whether non-std::nullopt was returned.
  EXPECT_NE(type_cast_safe<NullValue>(NullValue{}), std::nullopt);
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

TEST_F(TypeCastTest, TypeCastSafeFloatingPointToIntegral) {
  EXPECT_EQ(type_cast_safe<int32_t>(3.0f), 3);
  EXPECT_EQ(type_cast_safe<int32_t>(3.0), 3);
  EXPECT_EQ(type_cast_safe<int64_t>(3.0f), 3);
  EXPECT_EQ(type_cast_safe<int64_t>(3.0), 3);

  EXPECT_EQ(type_cast_safe<int32_t>(5'000'000'000.0f), std::nullopt);
  EXPECT_EQ(type_cast_safe<int32_t>(5'000'000'000.0), std::nullopt);
  EXPECT_EQ(type_cast_safe<int64_t>(5'000'000'000.0f), 5'000'000'000);
  EXPECT_EQ(type_cast_safe<int64_t>(5'000'000'000.0), 5'000'000'000);

  EXPECT_EQ(type_cast_safe<int32_t>(1.23e16f), std::nullopt);
  EXPECT_EQ(type_cast_safe<int64_t>(1.23e16f), 12'300'000'356'728'832);

  EXPECT_EQ(type_cast_safe<int64_t>(1.0e32f), std::nullopt);
  EXPECT_EQ(type_cast_safe<int64_t>(1.0e32), std::nullopt);

  EXPECT_EQ(type_cast_safe<int32_t>(3.1f), std::nullopt);
  EXPECT_EQ(type_cast_safe<int32_t>(3.1), std::nullopt);
  EXPECT_EQ(type_cast_safe<int64_t>(3.1f), std::nullopt);
  EXPECT_EQ(type_cast_safe<int64_t>(3.1), std::nullopt);
}

TEST_F(TypeCastTest, TypeCastSafeFloatingPointToDifferentFloatingPoint) {
  EXPECT_EQ(type_cast_safe<double>(3.0f), 3.0);
  EXPECT_EQ(type_cast_safe<float>(3.0), 3.0f);
  // TODO
}

TEST_F(TypeCastTest, TypeCastSafeIntegralToIntegral) {
  EXPECT_EQ(type_cast_safe<int64_t>(int32_t(3)), 3);
  EXPECT_EQ(type_cast_safe<int32_t>(int64_t(3)), 3);

  EXPECT_EQ(type_cast_safe<int32_t>(int64_t(3'000'000'000)), std::nullopt);
}

TEST_F(TypeCastTest, TypeCastSafeNullToNonNull) {
  EXPECT_EQ(type_cast_safe<int32_t>(NullValue{}), std::nullopt);
  EXPECT_EQ(type_cast_safe<int64_t>(NullValue{}), std::nullopt);
  EXPECT_EQ(type_cast_safe<float>(NullValue{}), std::nullopt);
  EXPECT_EQ(type_cast_safe<double>(NullValue{}), std::nullopt);
  EXPECT_EQ(type_cast_safe<pmr_string>(NullValue{}), std::nullopt);
}

TEST_F(TypeCastTest, TypeCastSafeStringToNonString) {
  EXPECT_EQ(type_cast_safe<int32_t>(pmr_string("a")), std::nullopt);
  EXPECT_EQ(type_cast_safe<int32_t>(pmr_string("1")), 1);
  EXPECT_EQ(type_cast_safe<int64_t>(pmr_string("a")), std::nullopt);
  EXPECT_EQ(type_cast_safe<int64_t>(pmr_string("1")), 1);
  EXPECT_EQ(type_cast_safe<float>(pmr_string("a")), std::nullopt);
  EXPECT_EQ(type_cast_safe<float>(pmr_string("1")), std::nullopt);
  EXPECT_EQ(type_cast_safe<double>(pmr_string("a")), std::nullopt);
  EXPECT_EQ(type_cast_safe<double>(pmr_string("1")), std::nullopt);

  EXPECT_EQ(type_cast_safe<int32_t>(pmr_string("3000000000")), std::nullopt);
  EXPECT_EQ(type_cast_safe<int64_t>(pmr_string("3000000000")), 3'000'000'000);
}

TEST_F(TypeCastTest, TypeCastSafeNumberToString) {
  EXPECT_EQ(type_cast_safe<pmr_string>(int32_t(2)), "2");
  EXPECT_EQ(type_cast_safe<pmr_string>(int64_t(2)), "2");
  EXPECT_EQ(type_cast_safe<pmr_string>(2.5f), std::nullopt);
  EXPECT_EQ(type_cast_safe<pmr_string>(3.333f), std::nullopt);
  EXPECT_EQ(type_cast_safe<pmr_string>(2.5), std::nullopt);
  EXPECT_EQ(type_cast_safe<pmr_string>(3.333), std::nullopt);
}

TEST_F(TypeCastTest, VariantCastSafe) {
  EXPECT_EQ(variant_cast_safe(2.0f, DataType::Null), std::nullopt);
  EXPECT_EQ(variant_cast_safe(2.0f, DataType::Int), AllTypeVariant{int32_t{2}});
  EXPECT_EQ(variant_cast_safe(2.5f, DataType::Int), std::nullopt);
  EXPECT_EQ(variant_cast_safe(int32_t{2}, DataType::Float), AllTypeVariant{2.0f});

  // Cannot check for NULL == NULL, as that's not TRUE. So simply check whether non-std::nullopt was returned.
  EXPECT_TRUE(variant_cast_safe(NullValue{}, DataType::Null));
}

}  // namespace opossum
