#include "gtest/gtest.h"

#include "utils/div_with_rounding.hpp"

namespace opossum {

TEST(DivWithRounding, Test) {
  EXPECT_EQ(div_with_rounding(5, 2), std::make_pair(2, 3));
  EXPECT_EQ(div_with_rounding(8, 3), std::make_pair(2, 3));
  EXPECT_EQ(div_with_rounding(9, 3), std::make_pair(3, 3));
}

}  // namespace opossum
