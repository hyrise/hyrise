#include <sstream>
#include <string>

#include "gtest/gtest.h"

#include "utils/memory_usage.hpp"

namespace {

using namespace opossum;  // NOLINT

std::string to_string(const MemoryUsage& memory_usage) {
  std::stringstream stream;
  memory_usage.print(stream);
  return stream.str();
}
}

namespace opossum {

class MemoryUsageTest : public ::testing::Test {};

TEST_F(MemoryUsageTest, Print) {
  EXPECT_EQ(to_string(MemoryUsage{0}), "0B");
  EXPECT_EQ(to_string(MemoryUsage{11}), "11B");
  EXPECT_EQ(to_string(MemoryUsage{1'234}), "1.234KB");
  EXPECT_EQ(to_string(MemoryUsage{12'345}), "12.345KB");
  EXPECT_EQ(to_string(MemoryUsage{12'345'678}), "12.345MB");
  EXPECT_EQ(to_string(MemoryUsage{1'234'567'890}), "1.234GB");
}

}  // namespace opossum
