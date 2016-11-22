#pragma once

#include "gtest/gtest.h"

namespace opossum {

class BaseTest : public ::testing::Test {
  virtual void TearDown();
};

}  // namespace opossum
