#include <memory>

#include "gtest/gtest.h"

#include "base_test.hpp"

#include "optimizer/abstract_syntax_tree/show_columns_node.hpp"

namespace opossum {

class ShowColumnsNodeTest : public BaseTest {
 protected:
  void SetUp() override { _show_columns_node = std::make_shared<ShowColumnsNode>("table_a"); }

  std::shared_ptr<ShowColumnsNode> _show_columns_node;
};

TEST_F(ShowColumnsNodeTest, Description) {
  EXPECT_EQ(_show_columns_node->description(), "[ShowColumns] Table: 'table_a'");
}

TEST_F(ShowColumnsNodeTest, TableName) { EXPECT_EQ(_show_columns_node->table_name(), "table_a"); }

}  // namespace opossum
