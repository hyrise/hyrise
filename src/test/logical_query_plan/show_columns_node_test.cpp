#include <memory>

#include "gtest/gtest.h"

#include "base_test.hpp"

#include "logical_query_plan/show_columns_node.hpp"

namespace opossum {

class ShowColumnsNodeTest : public BaseTest {
 protected:
  void SetUp() override { _show_columns_node = ShowColumnsNode::make("table_a"); }

  std::shared_ptr<ShowColumnsNode> _show_columns_node;
};

TEST_F(ShowColumnsNodeTest, Description) {
  EXPECT_EQ(_show_columns_node->description(), "[ShowColumns] Table: 'table_a'");
}

TEST_F(ShowColumnsNodeTest, TableName) { EXPECT_EQ(_show_columns_node->table_name(), "table_a"); }

TEST_F(ShowColumnsNodeTest, ShallowEquals) {
  EXPECT_TRUE(_show_columns_node->shallow_equals(*_show_columns_node));

  const auto other_show_columns_node = ShowColumnsNode::make("table_b");
  EXPECT_FALSE(other_show_columns_node->shallow_equals(*_show_columns_node));
}

}  // namespace opossum
