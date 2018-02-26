#include <memory>

#include "gtest/gtest.h"

#include "base_test.hpp"

#include "logical_query_plan/insert_node.hpp"

namespace opossum {

class InsertNodeTest : public BaseTest {
 protected:
  void SetUp() override { _insert_node = InsertNode::make("table_a"); }

  std::shared_ptr<InsertNode> _insert_node;
};

TEST_F(InsertNodeTest, Description) { EXPECT_EQ(_insert_node->description(), "[Insert] Into table 'table_a'"); }

TEST_F(InsertNodeTest, TableName) { EXPECT_EQ(_insert_node->table_name(), "table_a"); }

TEST_F(InsertNodeTest, ShallowEquals) {
  EXPECT_TRUE(_insert_node->shallow_equals(*_insert_node));

  const auto other_insert_node = InsertNode::make("table_b");
  EXPECT_FALSE(other_insert_node->shallow_equals(*_insert_node));
}

}  // namespace opossum
