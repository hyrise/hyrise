#include <memory>

#include "gtest/gtest.h"

#include "base_test.hpp"

#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/validate_node.hpp"

namespace opossum {

class ValidateNodeTest : public BaseTest {
 protected:
  void SetUp() override { _validate_node = ValidateNode::make(); }

  std::shared_ptr<ValidateNode> _validate_node;
};

TEST_F(ValidateNodeTest, Description) { EXPECT_EQ(_validate_node->description(), "[Validate]"); }

TEST_F(ValidateNodeTest, HashEquals) {
  EXPECT_EQ(*_validate_node, *_validate_node);

  EXPECT_EQ(*_validate_node, *ValidateNode::make());
  EXPECT_EQ(_validate_node->hash(), ValidateNode::make()->hash());
}

TEST_F(ValidateNodeTest, Copy) { EXPECT_EQ(*_validate_node->deep_copy(), *_validate_node); }

TEST_F(ValidateNodeTest, NodeExpressions) { ASSERT_EQ(_validate_node->node_expressions.size(), 0u); }

}  // namespace opossum
