#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "utils/singleton.hpp"

namespace opossum {

class SingletonTest : public BaseTest {};

TEST_F(SingletonTest, SingleInstance) {
  auto& a = Singleton<int>::get();
  auto& b = Singleton<int>::get();

  EXPECT_EQ(a, b);
  EXPECT_EQ(&a, &b);
}

}  // namespace opossum
