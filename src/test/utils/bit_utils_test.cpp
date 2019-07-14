#include "gtest/gtest.h"

#include <climits>

#include "utils/bit_utils.hpp"

namespace opossum {

class BitUtilsTest : public ::testing::Test {};

TEST_F(BitUtilsTest, CeilPowerOfTwo) {
  EXPECT_EQ(ceil_power_of_two(size_t{0}), 0);
  EXPECT_EQ(ceil_power_of_two(size_t{1}), 1);
  EXPECT_EQ(ceil_power_of_two(size_t{2}), 2);
  EXPECT_EQ(ceil_power_of_two(size_t{3}), 4);
  EXPECT_EQ(ceil_power_of_two(size_t{5}), 8);
  EXPECT_EQ(ceil_power_of_two((size_t{1} << (sizeof(size_t) * CHAR_BIT - 1)) - 15), size_t{1} << (sizeof(size_t) * CHAR_BIT - 1));
}

}  // namespace opossum
