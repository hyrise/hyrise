#include <memory>
#include <string_view>

#include "../base_test.hpp"
#include "gtest/gtest.h"
#include "null_value.hpp"
#include "all_type_variant.hpp"

namespace opossum {

class NullValueTest : public BaseTest {
};

TEST_F(NullValueTest, NullValueComparators) {
  auto null_0 = NullValue{};
  auto null_1 = NullValue{};

  EXPECT_FALSE(null_0 == null_1);
  EXPECT_FALSE(null_0 != null_1);
  EXPECT_FALSE(null_0 <  null_1);
  EXPECT_FALSE(null_0 <= null_1);
  EXPECT_FALSE(null_0 >  null_1);
  EXPECT_FALSE(null_0 >= null_1);
}

TEST_F(NullValueTest, AllTypeVariantNullComparators) {
  auto null_0 = AllTypeVariant{};
  auto null_1 = AllTypeVariant{};

  EXPECT_FALSE(null_0 == null_1);
  EXPECT_FALSE(null_0 != null_1);
  EXPECT_FALSE(null_0 <  null_1);
  EXPECT_FALSE(null_0 <= null_1);
  EXPECT_FALSE(null_0 >  null_1);
  EXPECT_FALSE(null_0 >= null_1);
}

}  // namespace opossum
