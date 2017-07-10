#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "operators/abstract_join_operator.hpp"
#include "optimizer/abstract_syntax_tree/join_node.hpp"
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

  ASSERT_EQ(t_n->left(), nullptr);
  ASSERT_EQ(t_n->right(), nullptr);
  ASSERT_EQ(t_n->parent(), nullptr);

  const auto ts_n = std::make_shared<TableScanNode>("c1", ScanType::OpEquals, "a");
  ts_n->set_left(t_n);

  ASSERT_EQ(t_n->parent(), ts_n);
  ASSERT_EQ(ts_n->left(), t_n);
  ASSERT_EQ(ts_n->right(), nullptr);
  ASSERT_EQ(ts_n->parent(), nullptr);

  std::vector<std::string> column_names = {"c1", "c2"};
  const auto p_n = std::make_shared<ProjectionNode>(column_names);
  p_n->set_left(ts_n);

  ASSERT_EQ(ts_n->parent(), p_n);
  ASSERT_EQ(p_n->left(), ts_n);
  ASSERT_EQ(p_n->right(), nullptr);
  ASSERT_EQ(p_n->parent(), nullptr);
}

TEST_F(AbstractSyntaxTreeTest, ChainSameNodesTest) {
  const auto t_n = std::make_shared<TableNode>("a");

  ASSERT_EQ(t_n->left(), nullptr);
  ASSERT_EQ(t_n->right(), nullptr);
  ASSERT_EQ(t_n->parent(), nullptr);

  const auto ts_n = std::make_shared<TableScanNode>("c1", ScanType::OpEquals, "a");
  ts_n->set_left(t_n);

  ASSERT_EQ(t_n->parent(), ts_n);
  ASSERT_EQ(ts_n->left(), t_n);
  ASSERT_EQ(ts_n->right(), nullptr);
  ASSERT_EQ(ts_n->parent(), nullptr);

  const auto ts_n_2 = std::make_shared<TableScanNode>("c2", ScanType::OpEquals, "b");
  ts_n_2->set_left(ts_n);

  ASSERT_EQ(ts_n->parent(), ts_n_2);
  ASSERT_EQ(ts_n_2->left(), ts_n);
  ASSERT_EQ(ts_n_2->right(), nullptr);
  ASSERT_EQ(ts_n_2->parent(), nullptr);

  std::vector<std::string> column_names = {"c1", "c2"};
  const auto p_n = std::make_shared<ProjectionNode>(column_names);
  p_n->set_left(ts_n_2);

  ASSERT_EQ(ts_n_2->parent(), p_n);
  ASSERT_EQ(p_n->left(), ts_n_2);
  ASSERT_EQ(p_n->right(), nullptr);
  ASSERT_EQ(p_n->parent(), nullptr);
}

TEST_F(AbstractSyntaxTreeTest, TwoInputsTest) {
  const auto join_node = std::make_shared<JoinNode>(std::pair<std::string, std::string>("col_a", "col_b"),
                                                    ScanType::OpEquals, JoinMode::Inner, "left", "right");

  ASSERT_EQ(join_node->left(), nullptr);
  ASSERT_EQ(join_node->right(), nullptr);
  ASSERT_EQ(join_node->parent(), nullptr);

  const auto table_a_node = std::make_shared<TableNode>("a");
  const auto table_b_node = std::make_shared<TableNode>("b");

  join_node->set_left(table_a_node);
  join_node->set_right(table_b_node);

  ASSERT_EQ(join_node->left(), table_a_node);
  ASSERT_EQ(join_node->right(), table_b_node);
  ASSERT_EQ(join_node->parent(), nullptr);

  ASSERT_EQ(table_a_node->parent(), join_node);
  ASSERT_EQ(table_b_node->parent(), join_node);
}

}  // namespace opossum
