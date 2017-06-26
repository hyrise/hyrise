#include <memory>
#include <string>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "optimizer/abstract_syntax_tree/projection_node.hpp"
#include "optimizer/abstract_syntax_tree/table_node.hpp"
#include "optimizer/abstract_syntax_tree/table_scan_node.hpp"

namespace opossum {

class AbstractSyntaxTreeTest : public BaseTest {
 protected:
  void SetUp() override {}
};

TEST_F(AbstractSyntaxTreeTest, ParentTest) {
  const auto t_n = std::make_shared<TableNode>("a");

  ASSERT_EQ(t_n->get_left(), nullptr);
  ASSERT_EQ(t_n->get_right(), nullptr);
  ASSERT_EQ(t_n->get_parent().lock(), nullptr);

  const auto ts_n = std::make_shared<TableScanNode>("c1", "=", "a");
  ts_n->set_left(t_n);

  ASSERT_EQ(t_n->get_parent().lock(), ts_n);
  ASSERT_EQ(ts_n->get_left(), t_n);
  ASSERT_EQ(ts_n->get_right(), nullptr);
  ASSERT_EQ(ts_n->get_parent().lock(), nullptr);

  std::vector<std::string> column_names = {"c1", "c2"};
  const auto p_n = std::make_shared<ProjectionNode>(column_names);
  p_n->set_left(ts_n);

  ASSERT_EQ(ts_n->get_parent().lock(), p_n);
  ASSERT_EQ(p_n->get_left(), ts_n);
  ASSERT_EQ(p_n->get_right(), nullptr);
  ASSERT_EQ(p_n->get_parent().lock(), nullptr);
}

}  // namespace opossum
