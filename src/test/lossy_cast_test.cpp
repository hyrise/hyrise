#include "base_test.hpp"

#include "lossy_cast.hpp"

namespace opossum {

class LossyCastTest : public BaseTest {};

TEST_F(LossyCastTest, LossyVariantCast) {
  EXPECT_EQ(lossy_variant_cast<int32_t>(int32_t(5)), int32_t(5));
  EXPECT_EQ(lossy_variant_cast<int32_t>(NullValue{}), std::nullopt);
  EXPECT_EQ(lossy_variant_cast<int32_t>(3.5f), int32_t{3});
  EXPECT_EQ(lossy_variant_cast<pmr_string>(3), "3");
  EXPECT_EQ(lossy_variant_cast<float>(3), 3.0f);
  EXPECT_EQ(lossy_variant_cast<int32_t>("3"), int32_t{3});
  EXPECT_EQ(lossy_variant_cast<float>("3.5"), 3.5f);
  EXPECT_EQ(lossy_variant_cast<double>("3.5"), 3.5);
  EXPECT_EQ(lossy_variant_cast<int32_t>(int64_t{5'000'000'000}), std::numeric_limits<int32_t>::max());
  EXPECT_EQ(lossy_variant_cast<int32_t>(int64_t{-5'000'000'000}), std::numeric_limits<int32_t>::min());
  EXPECT_ANY_THROW(lossy_variant_cast<int32_t>("abc"));
  EXPECT_ANY_THROW(lossy_variant_cast<int32_t>("3.5"));
}

}  // namespace opossum
