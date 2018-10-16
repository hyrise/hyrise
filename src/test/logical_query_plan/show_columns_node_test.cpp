#include <memory>

#include "gtest/gtest.h"

#include "base_test.hpp"

#include "expression/expression_functional.hpp"
#include "logical_query_plan/show_columns_node.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class ShowColumnsNodeTest : public ::testing::Test {
 protected:
  void SetUp() override { _show_columns_node = ShowColumnsNode::make("table_a"); }

  std::shared_ptr<ShowColumnsNode> _show_columns_node;
};

TEST_F(ShowColumnsNodeTest, Columns) {
  ASSERT_EQ(_show_columns_node->column_expressions().size(), 3u);
  EXPECT_EQ(*_show_columns_node->column_expressions().at(0), *column_({_show_columns_node, ColumnID{0}}));
  EXPECT_EQ(*_show_columns_node->column_expressions().at(1), *column_({_show_columns_node, ColumnID{1}}));
  EXPECT_EQ(*_show_columns_node->column_expressions().at(2), *column_({_show_columns_node, ColumnID{2}}));
}

TEST_F(ShowColumnsNodeTest, Description) {
  EXPECT_EQ(_show_columns_node->description(), "[ShowColumns] Table: 'table_a'");
}

TEST_F(ShowColumnsNodeTest, TableName) { EXPECT_EQ(_show_columns_node->table_name(), "table_a"); }

TEST_F(ShowColumnsNodeTest, Equals) {
  EXPECT_EQ(*_show_columns_node, *_show_columns_node);

  const auto other_show_columns_node = ShowColumnsNode::make("table_b");
  EXPECT_NE(*_show_columns_node, *other_show_columns_node);
}

TEST_F(ShowColumnsNodeTest, Copy) { EXPECT_EQ(*_show_columns_node->deep_copy(), *_show_columns_node); }

}  // namespace opossum
