#include <memory>
#include <string>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "operators/table_scan.hpp"
#include "optimizer/abstract_syntax_tree/projection_node.hpp"
#include "optimizer/abstract_syntax_tree/table_node.hpp"
#include "optimizer/abstract_syntax_tree/predicate_node.hpp"

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

  const auto pred_n = std::make_shared<PredicateNode>("c1", ScanType::OpEquals, "a");
  pred_n->set_left(t_n);

  ASSERT_EQ(t_n->parent(), pred_n);
  ASSERT_EQ(pred_n->left(), t_n);
  ASSERT_EQ(pred_n->right(), nullptr);
  ASSERT_EQ(pred_n->parent(), nullptr);

  std::vector<std::string> column_names = {"c1", "c2"};
  const auto p_n = std::make_shared<ProjectionNode>(column_names);
  p_n->set_left(pred_n);

  ASSERT_EQ(pred_n->parent(), p_n);
  ASSERT_EQ(p_n->left(), pred_n);
  ASSERT_EQ(p_n->right(), nullptr);
  ASSERT_EQ(p_n->parent(), nullptr);
}

TEST_F(AbstractSyntaxTreeTest, ChainSameNodesTest) {
  const auto t_n = std::make_shared<TableNode>("a");

  ASSERT_EQ(t_n->left(), nullptr);
  ASSERT_EQ(t_n->right(), nullptr);
  ASSERT_EQ(t_n->parent(), nullptr);

  const auto pred_n = std::make_shared<PredicateNode>("c1", ScanType::OpEquals, "a");
  pred_n->set_left(t_n);

  ASSERT_EQ(t_n->parent(), pred_n);
  ASSERT_EQ(pred_n->left(), t_n);
  ASSERT_EQ(pred_n->right(), nullptr);
  ASSERT_EQ(pred_n->parent(), nullptr);

  const auto pred_n_2 = std::make_shared<PredicateNode>("c2", ScanType::OpEquals, "b");
  pred_n_2->set_left(pred_n);

  ASSERT_EQ(pred_n->parent(), pred_n_2);
  ASSERT_EQ(pred_n_2->left(), pred_n);
  ASSERT_EQ(pred_n_2->right(), nullptr);
  ASSERT_EQ(pred_n_2->parent(), nullptr);

  std::vector<std::string> column_names = {"c1", "c2"};
  const auto p_n = std::make_shared<ProjectionNode>(column_names);
  p_n->set_left(pred_n_2);

  ASSERT_EQ(pred_n_2->parent(), p_n);
  ASSERT_EQ(p_n->left(), pred_n_2);
  ASSERT_EQ(p_n->right(), nullptr);
  ASSERT_EQ(p_n->parent(), nullptr);
}

}  // namespace opossum
