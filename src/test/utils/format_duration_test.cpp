#include "../base_test.hpp"

#include "utils/format_duration.hpp"

using namespace std::chrono_literals;  // NOLINT

namespace opossum {

class FormatDurationTest : public BaseTest {};

TEST_F(FormatDurationTest, Duration) {
  // It seems std::chrono_literals can't handle `12'345'678`, otherwise I'd use it here...
  EXPECT_EQ(format_duration(0ns), "0 ns");
  EXPECT_EQ(format_duration(11ns), "11 ns");
  EXPECT_EQ(format_duration(1000ns), "1 µs 0 ns");
  EXPECT_EQ(format_duration(1234ns), "1 µs 234 ns");
  EXPECT_EQ(format_duration(12345100ns), "12 ms 345 µs");
  EXPECT_EQ(format_duration(12345678ns), "12 ms 346 µs");
  EXPECT_EQ(format_duration(1234421012ns), "1 s 234 ms");
  EXPECT_EQ(format_duration(1234567890ns), "1 s 235 ms");
  EXPECT_EQ(format_duration(60000000000ns), "1 min 0 s");
  EXPECT_EQ(format_duration(60000000001ns), "1 min 0 s");
  EXPECT_EQ(format_duration(61234567890ns), "1 min 1 s");
  EXPECT_EQ(format_duration(61834567890ns), "1 min 2 s");
}

}  // namespace opossum
