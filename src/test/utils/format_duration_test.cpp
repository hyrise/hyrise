#include "gtest/gtest.h"

#include "utils/format_duration.hpp"

namespace opossum {

TEST(Format, Duration) {
  EXPECT_EQ(format_duration(0), "0 ns");
  EXPECT_EQ(format_duration(11), "11 ns");
  EXPECT_EQ(format_duration(1'000), "1 µs 0 ns");
  EXPECT_EQ(format_duration(1'234), "1 µs 234 ns");
  EXPECT_EQ(format_duration(12'345'678), "12 ms 345 µs");
  EXPECT_EQ(format_duration(1'234'567'890), "1 s 234 ms");
  EXPECT_EQ(format_duration(60'000'000'000), "1 min 0 s");
  EXPECT_EQ(format_duration(61'234'567'890), "1 min 1 s");
}

}  // namespace opossum
