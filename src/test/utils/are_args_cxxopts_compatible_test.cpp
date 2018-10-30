#include "gtest/gtest.h"

#include "utils/are_args_cxxopts_compatible.hpp"

namespace opossum {

TEST(AreArgsCxxoptCompatible, Test) {
  const char* argv_0[] = {"app", "-l", "123", "--hello=123", "-z", "h--u"};
  EXPECT_TRUE(are_args_cxxopts_compatible(6, argv_0));
  const char* argv_1[] = {"app", "-l=5"};
  EXPECT_FALSE(are_args_cxxopts_compatible(2, argv_1));
}

}  // namespace opossum
