#include <memory>
#include <string>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "optimizer/abstract_syntax_tree/projection_node.hpp"
#include "optimizer/abstract_syntax_tree/table_node.hpp"

namespace opossum {

class AbstractSyntaxTreeTest : public BaseTest {
 protected:
  void SetUp() override {
  }
};

TEST_F(AbstractSyntaxTreeTest, ParentTest) {
  const auto table_node = AbstractNode::make_shared_from_args<TableNode>("a");

  std::vector<std::string> column_names = {"c1", "c2"};
  auto projection_node = AbstractNode::make_shared_from_args<ProjectionNode>(table_node, column_names);

  ASSERT_EQ(table_node->get_parent().lock(), projection_node);
  ASSERT_NE(table_node->get_parent().lock(), table_node);
}

}  // namespace opossum
