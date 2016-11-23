#pragma once

#include "gtest/gtest.h"

namespace opossum {

class BaseTest : public ::testing::Test {
 public:
  virtual ~BaseTest();
};

}  // namespace opossum
