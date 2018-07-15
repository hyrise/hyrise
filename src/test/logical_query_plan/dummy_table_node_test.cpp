#include <memory>

#include "gtest/gtest.h"

#include "logical_query_plan/dummy_table_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"

namespace opossum {

class DummyTableNodeTest : public ::testing::Test {
 protected:
  void SetUp() override { _dummy_table_node = DummyTableNode::make(); }

  std::shared_ptr<DummyTableNode> _dummy_table_node;
};

TEST_F(DummyTableNodeTest, Description) { EXPECT_EQ(_dummy_table_node->description(), "[DummyTable]"); }

TEST_F(DummyTableNodeTest, OutputColumnExpressions) { EXPECT_EQ(_dummy_table_node->column_expressions().size(), 0u); }

TEST_F(DummyTableNodeTest, Equals) {
  EXPECT_EQ(*_dummy_table_node, *_dummy_table_node);
  EXPECT_EQ(*_dummy_table_node, *DummyTableNode::make());
}

TEST_F(DummyTableNodeTest, Copy) { EXPECT_EQ(*_dummy_table_node->deep_copy(), *DummyTableNode::make()); }

}  // namespace opossum
