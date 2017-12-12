#include <memory>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "optimizer/strategy/predicate_pushdown_rule.hpp"
#include "optimizer/strategy/strategy_base_test.hpp"

namespace opossum {

class PredicatePushdownRuleTest : public StrategyBaseTest {
 protected:
  void SetUp() override { _rule = std::make_shared<PredicatePushdownRule>(); }

  std::shared_ptr<PredicatePushdownRule> _rule;
};

TEST_F(PredicatePushdownRuleTest, SimplePushdownTest) {}
}  // namespace opossum
