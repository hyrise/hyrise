#include <memory>

#include "gtest/gtest.h"

#include "base_test.hpp"

#include "logical_query_plan/show_cxlumns_node.hpp"

namespace opossum {

class ShowCxlumnsNodeTest : public ::testing::Test {
 protected:
  void SetUp() override { _show_cxlumns_node = ShowCxlumnsNode::make("table_a"); }

  std::shared_ptr<ShowCxlumnsNode> _show_cxlumns_node;
};

TEST_F(ShowCxlumnsNodeTest, Description) {
  EXPECT_EQ(_show_cxlumns_node->description(), "[ShowCxlumns] Table: 'table_a'");
}

TEST_F(ShowCxlumnsNodeTest, TableName) { EXPECT_EQ(_show_cxlumns_node->table_name(), "table_a"); }

TEST_F(ShowCxlumnsNodeTest, Equals) {
  EXPECT_EQ(*_show_cxlumns_node, *_show_cxlumns_node);

  const auto other_show_cxlumns_node = ShowCxlumnsNode::make("table_b");
  EXPECT_NE(*_show_cxlumns_node, *other_show_cxlumns_node);
}

TEST_F(ShowCxlumnsNodeTest, Copy) { EXPECT_EQ(*_show_cxlumns_node->deep_copy(), *_show_cxlumns_node); }

}  // namespace opossum
