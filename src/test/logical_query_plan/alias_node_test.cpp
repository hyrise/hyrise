#include "gtest/gtest.h"

#include "expression/lqp_column_expression.hpp"
#include "logical_query_plan/alias_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "operators/table_wrapper.hpp"
#include "testing_assert.hpp"
#include "utils/load_table.hpp"

using namespace std::string_literals;  // NOLINT

namespace opossum {

class AliasNodeTest : public ::testing::Test {
 public:
  void SetUp() override {
    const auto mock_node = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Float, "b"}});

    const auto a_expression = std::make_shared<LQPColumnExpression>(LQPColumnReference{mock_node, ColumnID{0}});
    const auto b_expression = std::make_shared<LQPColumnExpression>(LQPColumnReference{mock_node, ColumnID{1}});
    const auto expressions = std::vector<std::shared_ptr<AbstractExpression>>{{b_expression, a_expression}};

    const auto aliases = std::vector<std::string>{"x", "y"};

    alias_node = AliasNode::make(expressions, aliases, mock_node);
  }

  std::shared_ptr<AliasNode> alias_node;
};

TEST_F(AliasNodeTest, ShallowEqualsAndCopy) {
  const auto alias_node_copy = alias_node->deep_copy();
  const auto node_mapping = lqp_create_node_mapping(alias_node, alias_node_copy);

  EXPECT_TRUE(alias_node->shallow_equals(*alias_node_copy, node_mapping));
}

}  // namespace opossum
