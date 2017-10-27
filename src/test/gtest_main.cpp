#include <sys/stat.h>
#include <sys/types.h>

#include <gtest/gtest.h>

#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

int main(int argc, char** argv) {
  struct stat info;
  opossum::Assert(
      stat("src/test/tables", &info) == 0,
      "Cannot find src/test/tables. Are you running the test suite from the main folder of the Hyrise repository?");

  opossum::PerformanceWarningDisabler pwd;
  ::testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
