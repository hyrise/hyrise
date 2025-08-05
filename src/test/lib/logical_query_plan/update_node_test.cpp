#include "base_test.hpp"
#include "expression/expression_functional.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/update_node.hpp"
#include "storage/table.hpp"

namespace hyrise {

using namespace expression_functional;  // NOLINT(build/namespaces)

class UpdateNodeTest : public BaseTest {
 protected:
  void SetUp() override {
    _mock_node = MockNode::make(MockNode::ColumnDefinitions({{DataType::Int, "a"}}));
    const auto table = Table::create_dummy_table({{"a", DataType::Int, false}});
    _table_id = Hyrise::get().catalog.add_table("table_a", table);
    _update_node = UpdateNode::make(ObjectID{17}, _mock_node, _mock_node);
  }

  std::shared_ptr<UpdateNode> _update_node;
  std::shared_ptr<MockNode> _mock_node;
  ObjectID _table_id;
};

TEST_F(UpdateNodeTest, Description) {
  EXPECT_EQ(_update_node->description(), "[Update] Table: 'table_a'");
}

TEST_F(UpdateNodeTest, TableID) {
  EXPECT_EQ(_update_node->table_id, _table_id);
}

TEST_F(UpdateNodeTest, HashingAndEqualityCheck) {
  EXPECT_EQ(*_update_node, *_update_node);

  const auto other_mock_node = MockNode::make(MockNode::ColumnDefinitions({{DataType::Long, "a"}}));

  const auto other_update_node_a = UpdateNode::make(_table_id, _mock_node, _mock_node);
  const auto other_update_node_b = UpdateNode::make(ObjectID{17}, _mock_node, _mock_node);
  const auto other_update_node_c = UpdateNode::make(_table_id, other_mock_node, _mock_node);
  const auto other_update_node_d = UpdateNode::make(_table_id, _mock_node, other_mock_node);
  const auto other_update_node_e = UpdateNode::make(_table_id, other_mock_node, other_mock_node);

  EXPECT_EQ(*_update_node, *other_update_node_a);
  EXPECT_NE(*_update_node, *other_update_node_b);
  EXPECT_NE(*_update_node, *other_update_node_c);
  EXPECT_NE(*_update_node, *other_update_node_d);
  EXPECT_NE(*_update_node, *other_update_node_e);

  EXPECT_EQ(_update_node->hash(), other_update_node_a->hash());
}

TEST_F(UpdateNodeTest, Copy) {
  EXPECT_EQ(*_update_node->deep_copy(), *_update_node);
}

TEST_F(UpdateNodeTest, NodeExpressions) {
  ASSERT_TRUE(_update_node->node_expressions.empty());
}

TEST_F(UpdateNodeTest, ColumnExpressions) {
  EXPECT_TRUE(_update_node->output_expressions().empty());
}

TEST_F(UpdateNodeTest, NoUniqueColumnCombinations) {
  EXPECT_THROW(_update_node->unique_column_combinations(), std::logic_error);
}

TEST_F(UpdateNodeTest, NoOrderDependencies) {
  EXPECT_THROW(_update_node->order_dependencies(), std::logic_error);
}

}  // namespace hyrise
