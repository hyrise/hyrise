#include <memory>

#include "gtest/gtest.h"

#include "base_test.hpp"

#include "logical_query_plan/dummy_table_node.hpp"

namespace opossum {

class DummyTableNodeTest : public BaseTest {
 protected:
  void SetUp() override { _dummy_table_node = DummyTableNode::make(); }

  std::shared_ptr<DummyTableNode> _dummy_table_node;
};

TEST_F(DummyTableNodeTest, Description) { EXPECT_EQ(_dummy_table_node->description(), "[DummyTable]"); }

TEST_F(DummyTableNodeTest, ImplementsOutputColumnInterface) {
  EXPECT_GE(_dummy_table_node->output_column_names().size(), 0u);
}

}  // namespace opossum
