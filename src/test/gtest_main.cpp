#include <gtest/gtest.h>

#include "utils/assert.hpp"

int main(int argc, char **argv) {
  PerformanceWarningDisabler pwd;
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
