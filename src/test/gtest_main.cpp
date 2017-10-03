#include <gtest/gtest.h>

#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

int main(int argc, char **argv) {
  PerformanceWarningDisabler pwd;
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
