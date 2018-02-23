#include <memory>

#include "gtest/gtest.h"

#include "base_test.hpp"

#include "logical_query_plan/show_tables_node.hpp"

namespace opossum {

class ShowTablesNodeTest : public BaseTest {
 protected:
  void SetUp() override { _show_tables_node = ShowTablesNode::make(); }

  std::shared_ptr<ShowTablesNode> _show_tables_node;
};

TEST_F(ShowTablesNodeTest, Description) { EXPECT_EQ(_show_tables_node->description(), "[ShowTables]"); }

}  // namespace opossum
