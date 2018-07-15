#include <memory>
#include <vector>

#include "gtest/gtest.h"

#include "base_test.hpp"

#include "expression/expression_functional.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/update_node.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class UpdateNodeTest : public BaseTest {
 protected:
  void SetUp() override {
    _mock_node = MockNode::make(MockNode::ColumnDefinitions({{DataType::Int, "a"}}));
    _update_node = UpdateNode::make("table_a", expression_vector(6), _mock_node);
  }

  std::shared_ptr<UpdateNode> _update_node;
  std::shared_ptr<MockNode> _mock_node;
};

TEST_F(UpdateNodeTest, Description) { EXPECT_EQ(_update_node->description(), "[Update] Table: 'table_a' Columns: 6"); }

TEST_F(UpdateNodeTest, TableName) { EXPECT_EQ(_update_node->table_name, "table_a"); }

TEST_F(UpdateNodeTest, Equals) {
  EXPECT_EQ(*_update_node, *_update_node);

  const auto other_update_node_a = UpdateNode::make("table_a", expression_vector(6), _mock_node);
  const auto other_update_node_b = UpdateNode::make("table_b", expression_vector(), _mock_node);
  const auto other_update_node_c = UpdateNode::make("table_a", expression_vector(5), _mock_node);

  EXPECT_EQ(*_update_node, *other_update_node_a);
  EXPECT_NE(*_update_node, *other_update_node_b);
  EXPECT_NE(*_update_node, *other_update_node_c);
}

TEST_F(UpdateNodeTest, Copy) { EXPECT_EQ(*_update_node->deep_copy(), *_update_node); }

}  // namespace opossum
