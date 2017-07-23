#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "../../base_test.hpp"
#include "gtest/gtest.h"

#include "optimizer/strategy/simplify_expression_rule.hpp"

namespace opossum {

class SimplifyExpressionRuleTest : public BaseTest {
 protected:
  void SetUp() override {}
};

TEST_F(SimplifyExpressionRuleTest, AdditionTest) {}

}  // namespace opossum
