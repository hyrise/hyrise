#include <memory>

#include "gtest/gtest.h"

#include "base_test.hpp"

#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/insert_node.hpp"

namespace opossum {

class InsertNodeTest : public ::testing::Test {
 protected:
  void SetUp() override { _insert_node = InsertNode::make("table_a"); }

  std::shared_ptr<InsertNode> _insert_node;
};

TEST_F(InsertNodeTest, Description) { EXPECT_EQ(_insert_node->description(), "[Insert] Into table 'table_a'"); }

TEST_F(InsertNodeTest, TableName) { EXPECT_EQ(_insert_node->table_name(), "table_a"); }

TEST_F(InsertNodeTest, Equals) {
  EXPECT_TRUE(!lqp_find_subplan_mismatch(_insert_node, _insert_node));
  EXPECT_TRUE(!lqp_find_subplan_mismatch(_insert_node, InsertNode::make("table_a")));
  EXPECT_TRUE(lqp_find_subplan_mismatch(_insert_node, InsertNode::make("table_b")).has_value());
}

}  // namespace opossum
