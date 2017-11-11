#include <iostream>

#include "gtest/gtest.h"
#include "utils/assert.hpp"

TEST(Hello, WorldTest) {
  auto r = testing::AssertionFailure();
  r << "Hello World";

  FAIL(r);
}

int main(int argc, char* argv[]) {
  struct stat info;
  opossum::Assert(
      stat("src/test/tables", &info) == 0,
      "Cannot find src/test/tables. Are you running the test suite from the main folder of the Hyrise repository?");

  ::testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
