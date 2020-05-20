#include "base_test.hpp"

#include "logical_query_plan/drop_table_node.hpp"

namespace opossum {

class DropTableNodeTest : public BaseTest {
 public:
  void SetUp() override { drop_table_node = DropTableNode::make("some_table", false); }

  std::shared_ptr<DropTableNode> drop_table_node;
};

TEST_F(DropTableNodeTest, Description) { EXPECT_EQ(drop_table_node->description(), "[DropTable] Name: 'some_table'"); }

TEST_F(DropTableNodeTest, HashingAndEqualityCheck) {
  EXPECT_EQ(*drop_table_node, *drop_table_node);

  const auto different_drop_table_node = DropTableNode::make("some_table2", false);
  EXPECT_NE(*different_drop_table_node, *drop_table_node);
  EXPECT_NE(different_drop_table_node->hash(), drop_table_node->hash());
}

TEST_F(DropTableNodeTest, NodeExpressions) { ASSERT_EQ(drop_table_node->node_expressions.size(), 0u); }

TEST_F(DropTableNodeTest, Copy) { EXPECT_EQ(*drop_table_node, *drop_table_node->deep_copy()); }

}  // namespace opossum
