#include "../base_test.hpp"

#include "utils/format_bytes.hpp"

namespace opossum {

class FormatBytesTest : public BaseTest {};

TEST_F(FormatBytesTest, Bytes) {
  EXPECT_EQ(format_bytes(0), "0B");
  EXPECT_EQ(format_bytes(11), "11B");
  EXPECT_EQ(format_bytes(1'234), "1.234KB");
  EXPECT_EQ(format_bytes(12'345), "12.345KB");
  EXPECT_EQ(format_bytes(12'000'000), "12.000MB");
  EXPECT_EQ(format_bytes(12'345'678), "12.345MB");
  EXPECT_EQ(format_bytes(1'234'567'890), "1.234GB");
}

}  // namespace opossum
