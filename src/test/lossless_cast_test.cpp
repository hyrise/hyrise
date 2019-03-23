#include "gtest/gtest.h"

#include "lossless_cast.hpp"

namespace opossum {

class LosslessCastTest : public ::testing::Test {};

TEST_F(LosslessCastTest, LosslessCastIdenticalSourceAndTarget) {
  EXPECT_EQ(lossless_cast<int32_t>(int32_t(5)), int32_t(5));
  EXPECT_EQ(lossless_cast<int64_t>(int64_t(5'000'000'000)), int64_t(5'000'000'000));
  EXPECT_EQ(lossless_cast<float>(3.2f), 3.2f);
  EXPECT_EQ(lossless_cast<double>(3.2), 3.2);
  EXPECT_EQ(lossless_cast<pmr_string>(pmr_string{"hello"}), pmr_string("hello"));

  // Cannot check for NULL == NULL, as that's not TRUE. So simply check whether non-std::nullopt was returned.
  EXPECT_NE(lossless_cast<NullValue>(NullValue{}), std::nullopt);
}

TEST_F(LosslessCastTest, LosslessCastIntegralToFloatingPoint) {
  EXPECT_EQ(lossless_cast<float>(int32_t(3)), 3.0f);
  EXPECT_EQ(lossless_cast<float>(int64_t(3)), 3.0f);
  EXPECT_EQ(lossless_cast<double>(int32_t(3)), 3.0);
  EXPECT_EQ(lossless_cast<double>(int64_t(3)), 3.0);

  EXPECT_EQ(lossless_cast<float>(int32_t(20'000'003)), std::nullopt);
  EXPECT_EQ(lossless_cast<float>(int64_t(20'000'003)), std::nullopt);
  EXPECT_EQ(lossless_cast<double>(int32_t(20'000'003)), 20'000'003);
  EXPECT_EQ(lossless_cast<double>(int64_t(20'000'003)), 20'000'003);

  EXPECT_EQ(lossless_cast<float>(int32_t(-20'000'003)), std::nullopt);
  EXPECT_EQ(lossless_cast<double>(int32_t(-20'000'003)), -20'000'003);

  EXPECT_EQ(lossless_cast<float>(int64_t(20'123'456'789'012'345ll)), std::nullopt);
  EXPECT_EQ(lossless_cast<double>(int64_t(20'123'456'789'012'345ll)), std::nullopt);
}

TEST_F(LosslessCastTest, LosslessCastFloatingPointToIntegral) {
  EXPECT_EQ(lossless_cast<int32_t>(0.0f), 0);
  EXPECT_EQ(lossless_cast<int32_t>(0.0), 0);
  EXPECT_EQ(lossless_cast<int32_t>(3.0f), 3);
  EXPECT_EQ(lossless_cast<int32_t>(3.0), 3);

  EXPECT_EQ(lossless_cast<int64_t>(0.0f), 0);
  EXPECT_EQ(lossless_cast<int64_t>(0.0), 0);
  EXPECT_EQ(lossless_cast<int64_t>(3.0f), 3);
  EXPECT_EQ(lossless_cast<int64_t>(3.0), 3);

  // The _float literal_ and the integer are obviously not the same, but the _stored float_ and the integer are.
  EXPECT_EQ(lossless_cast<int32_t>(2'147'483'583.0f), 2'147'483'520);
  EXPECT_EQ(lossless_cast<int32_t>(2'147'483'584.0f), std::nullopt);
  EXPECT_EQ(lossless_cast<int32_t>(-2'147'483'583.0f), -2'147'483'520);
  EXPECT_EQ(lossless_cast<int32_t>(-2'147'483'584.0f), std::nullopt);

  EXPECT_EQ(lossless_cast<int32_t>(5'000'000'000.0f), std::nullopt);
  EXPECT_EQ(lossless_cast<int32_t>(5'000'000'000.0), std::nullopt);
  EXPECT_EQ(lossless_cast<int64_t>(5'000'000'000.0f), 5'000'000'000);
  EXPECT_EQ(lossless_cast<int64_t>(5'000'000'000.0), 5'000'000'000);

  EXPECT_EQ(lossless_cast<int32_t>(1234567890123456.0f), std::nullopt);
  // The _float literal_ and the integer are obviously not the same, but the _stored float_ and the integer are.
  EXPECT_EQ(*lossless_cast<int64_t>(1234567890123456.0f), 1'234'567'948'140'544);

  EXPECT_EQ(lossless_cast<int64_t>(1.0e32f), std::nullopt);
  EXPECT_EQ(lossless_cast<int64_t>(1.0e32), std::nullopt);

  EXPECT_EQ(lossless_cast<int32_t>(3.1f), std::nullopt);
  EXPECT_EQ(lossless_cast<int32_t>(3.1), std::nullopt);
  EXPECT_EQ(lossless_cast<int64_t>(3.1f), std::nullopt);
  EXPECT_EQ(lossless_cast<int64_t>(3.1), std::nullopt);
}

TEST_F(LosslessCastTest, LosslessCastFloatingPointToDifferentFloatingPoint) {
  EXPECT_EQ(lossless_cast<double>(3.0f), 3.0);
  EXPECT_EQ(lossless_cast<float>(3.0), 3.0f);
  EXPECT_EQ(lossless_cast<float>(3.1), std::nullopt);
  EXPECT_TRUE(lossless_cast<double>(3.1f));
  // TODO(moritz)
}

TEST_F(LosslessCastTest, LosslessCastIntegralToIntegral) {
  EXPECT_EQ(lossless_cast<int64_t>(int32_t(3)), 3);
  EXPECT_EQ(lossless_cast<int32_t>(int64_t(3)), 3);

  EXPECT_EQ(lossless_cast<int32_t>(int64_t(3'000'000'000)), std::nullopt);
}

TEST_F(LosslessCastTest, LosslessCastNullToNonNull) {
  EXPECT_EQ(lossless_cast<int32_t>(NullValue{}), std::nullopt);
  EXPECT_EQ(lossless_cast<int64_t>(NullValue{}), std::nullopt);
  EXPECT_EQ(lossless_cast<float>(NullValue{}), std::nullopt);
  EXPECT_EQ(lossless_cast<double>(NullValue{}), std::nullopt);
  EXPECT_EQ(lossless_cast<pmr_string>(NullValue{}), std::nullopt);
}

TEST_F(LosslessCastTest, LosslessCastStringToNonString) {
  EXPECT_EQ(lossless_cast<int32_t>(pmr_string("a")), std::nullopt);
  EXPECT_EQ(lossless_cast<int32_t>(pmr_string("1")), 1);
  EXPECT_EQ(lossless_cast<int64_t>(pmr_string("a")), std::nullopt);
  EXPECT_EQ(lossless_cast<int64_t>(pmr_string("1")), 1);
  EXPECT_EQ(lossless_cast<float>(pmr_string("a")), std::nullopt);
  EXPECT_EQ(lossless_cast<float>(pmr_string("1")), std::nullopt);
  EXPECT_EQ(lossless_cast<double>(pmr_string("a")), std::nullopt);
  EXPECT_EQ(lossless_cast<double>(pmr_string("1")), std::nullopt);

  EXPECT_EQ(lossless_cast<int32_t>(pmr_string("3000000000")), std::nullopt);
  EXPECT_EQ(lossless_cast<int64_t>(pmr_string("3000000000")), 3'000'000'000);
}

TEST_F(LosslessCastTest, LosslessCastNumberToString) {
  EXPECT_EQ(lossless_cast<pmr_string>(int32_t(2)), "2");
  EXPECT_EQ(lossless_cast<pmr_string>(int64_t(2)), "2");
  EXPECT_EQ(lossless_cast<pmr_string>(2.5f), std::nullopt);
  EXPECT_EQ(lossless_cast<pmr_string>(3.333f), std::nullopt);
  EXPECT_EQ(lossless_cast<pmr_string>(2.5), std::nullopt);
  EXPECT_EQ(lossless_cast<pmr_string>(3.333), std::nullopt);
}

TEST_F(LosslessCastTest, VariantCastSafe) {
  EXPECT_EQ(lossless_variant_cast(2.0f, DataType::Null), std::nullopt);
  EXPECT_EQ(lossless_variant_cast(2.0f, DataType::Int), AllTypeVariant{int32_t{2}});
  EXPECT_EQ(lossless_variant_cast(2.5f, DataType::Int), std::nullopt);
  EXPECT_EQ(lossless_variant_cast(int32_t{2}, DataType::Float), AllTypeVariant{2.0f});

  // Cannot check for NULL == NULL, as that's not TRUE. So simply check whether non-std::nullopt was returned.
  EXPECT_TRUE(lossless_variant_cast(NullValue{}, DataType::Null));
}

}  // namespace opossum
