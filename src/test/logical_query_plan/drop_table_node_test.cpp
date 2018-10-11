#include "gtest/gtest.h"

#include "logical_query_plan/drop_table_node.hpp"

namespace opossum {

class DropTableNodeTest : public ::testing::Test {
 public:
  void SetUp() override { drop_table_node = DropTableNode::make("some_table"); }

  std::shared_ptr<DropTableNode> drop_table_node;
};

TEST_F(DropTableNodeTest, Description) { EXPECT_EQ(drop_table_node->description(), "[DropTable] Name: 'some_table'"); }

TEST_F(DropTableNodeTest, Equals) {
  EXPECT_EQ(*drop_table_node, *drop_table_node);

  const auto different_drop_table_node = DropTableNode::make("some_table2");
  EXPECT_NE(*different_drop_table_node, *drop_table_node);
}

TEST_F(DropTableNodeTest, Copy) { EXPECT_EQ(*drop_table_node, *drop_table_node->deep_copy()); }

}  // namespace opossum
