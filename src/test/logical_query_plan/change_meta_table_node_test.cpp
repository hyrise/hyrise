#include <memory>

#include "base_test.hpp"

#include "expression/expression_utils.hpp"
#include "logical_query_plan/change_meta_table_node.hpp"
#include "logical_query_plan/mock_node.hpp"

namespace opossum {

class ChangeMetaTableNodeTest : public BaseTest {
 protected:
  void SetUp() override {
    _mock_node = MockNode::make(MockNode::ColumnDefinitions({{DataType::String, "foo"}}));
    _change_meta_table_node =
        ChangeMetaTableNode::make("meta_table", MetaTableChangeType::Insert, _mock_node, _mock_node);
  }

  std::shared_ptr<ChangeMetaTableNode> _change_meta_table_node;
  std::shared_ptr<MockNode> _mock_node;
};

TEST_F(ChangeMetaTableNodeTest, Description) {
  EXPECT_EQ(_change_meta_table_node->description(), "[Change] Meta Table: 'meta_table'");
}

TEST_F(ChangeMetaTableNodeTest, HashingAndEqualityCheck) {
  const auto another_change_meta_table_node =
      ChangeMetaTableNode::make("meta_table", MetaTableChangeType::Insert, _mock_node, _mock_node);
  EXPECT_EQ(*_change_meta_table_node, *another_change_meta_table_node);

  EXPECT_EQ(_change_meta_table_node->hash(), another_change_meta_table_node->hash());
}

TEST_F(ChangeMetaTableNodeTest, NodeExpressions) { EXPECT_TRUE(_change_meta_table_node->node_expressions.empty()); }

TEST_F(ChangeMetaTableNodeTest, ColumnExpressions) {
  EXPECT_TRUE(_change_meta_table_node->column_expressions().empty());
}

TEST_F(ChangeMetaTableNodeTest, Copy) { EXPECT_EQ(*_change_meta_table_node, *_change_meta_table_node->deep_copy()); }

}  // namespace opossum
