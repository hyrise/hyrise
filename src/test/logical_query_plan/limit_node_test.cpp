#include <memory>

#include "gtest/gtest.h"

#include "base_test.hpp"

#include "logical_query_plan/limit_node.hpp"

namespace opossum {

class LimitNodeTest : public BaseTest {
 protected:
  void SetUp() override { _limit_node = LimitNode::make(10); }

  std::shared_ptr<LimitNode> _limit_node;
};

TEST_F(LimitNodeTest, Description) { EXPECT_EQ(_limit_node->description(), "[Limit] 10 rows"); }

TEST_F(LimitNodeTest, NumberOfRows) { EXPECT_EQ(_limit_node->num_rows(), 10u); }

TEST_F(LimitNodeTest, ShallowEquals) {
  EXPECT_TRUE(_limit_node->shallow_equals(*_limit_node));

  const auto other_limit_node = LimitNode::make(11);
  EXPECT_FALSE(_limit_node->shallow_equals(*other_limit_node));
}

}  // namespace opossum
