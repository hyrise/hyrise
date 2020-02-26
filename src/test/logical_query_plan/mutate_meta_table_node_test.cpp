#include <memory>

#include "base_test.hpp"

#include "expression/expression_utils.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/mutate_meta_table_node.hpp"

namespace opossum {

class MutateMetaTableNodeTest : public BaseTest {
 protected:
  void SetUp() override {
    _mock_node = MockNode::make(MockNode::ColumnDefinitions({{DataType::String, "foo"}}));
    _mutate_meta_table_node =
        MutateMetaTableNode::make("meta_table", MetaTableMutation::Insert, _mock_node, _mock_node);
  }

  std::shared_ptr<MutateMetaTableNode> _mutate_meta_table_node;
  std::shared_ptr<MockNode> _mock_node;
};

TEST_F(MutateMetaTableNodeTest, Description) {
  EXPECT_EQ(_mutate_meta_table_node->description(), "[Mutate] Meta Table: 'meta_table'");
}

TEST_F(MutateMetaTableNodeTest, HashingAndEqualityCheck) {
  const auto another_mutate_meta_table_node =
      MutateMetaTableNode::make("meta_table", MetaTableMutation::Insert, _mock_node, _mock_node);
  EXPECT_EQ(*_mutate_meta_table_node, *another_mutate_meta_table_node);

  EXPECT_EQ(_mutate_meta_table_node->hash(), another_mutate_meta_table_node->hash());
}

TEST_F(MutateMetaTableNodeTest, NodeExpressions) { EXPECT_TRUE(_mutate_meta_table_node->node_expressions.empty()); }

TEST_F(MutateMetaTableNodeTest, ColumnExpressions) {
  EXPECT_TRUE(_mutate_meta_table_node->column_expressions().empty());
}

TEST_F(MutateMetaTableNodeTest, Copy) { EXPECT_EQ(*_mutate_meta_table_node, *_mutate_meta_table_node->deep_copy()); }

}  // namespace opossum
